use std::future;
use std::io::BufWriter;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_ext::jni_bridge::JavaClasses;
use datafusion_ext::jni_bridge_call_method;
use futures::TryFutureExt;
use futures::TryStreamExt;
use jni::objects::JClass;
use jni::objects::JObject;
use jni::objects::JValue;
use jni::JNIEnv;
use log::{debug, info};
use once_cell::sync::OnceCell;
use plan_serde::protobuf::TaskDefinition;
use prost::Message;
use tokio::runtime::Runtime;

mod metrics;

#[cfg(feature = "mm")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "sn")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

static BACKTRACE: OnceCell<Arc<Mutex<String>>> = OnceCell::new();
static ENV_LOGGER_INIT: OnceCell<()> = OnceCell::new();
static TOKIO_RUNTIME_INSTANCE: OnceCell<Runtime> = OnceCell::new();

fn tokio_runtime(thread_num: usize) -> &'static Runtime {
    TOKIO_RUNTIME_INSTANCE.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(thread_num)
            .thread_name("blaze")
            .build()
            .unwrap()
    })
}

fn setup_backtrace_hook() {
    BACKTRACE.get_or_init(|| {
        std::panic::set_hook(Box::new(|_| {
            *BACKTRACE.get().unwrap().lock().unwrap() =
                format!("{:?}", backtrace::Backtrace::new());
        }));
        Arc::new(Mutex::new("<Backtrace not found>".to_string()))
    });
}

fn setup_env_logger() {
    ENV_LOGGER_INIT.get_or_init(|| {
        env_logger::try_init_from_env(
            env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
        )
        .unwrap();
    });
}

#[allow(non_snake_case)]
#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_blaze_JniBridge_callNative(
    env: JNIEnv,
    _: JClass,
    taskDefinition: JObject,
    metricNode: JObject,
    ipcRecordBatchDataConsumer: JObject,
) {
    let start_time = Instant::now();

    setup_backtrace_hook();
    setup_env_logger();

    // save backtrace when panics
    if let Err(e) = std::panic::catch_unwind(|| {
        blaze_call_native(
            &env,
            taskDefinition,
            metricNode,
            ipcRecordBatchDataConsumer,
            start_time,
        );
    }) {
        let panic_str = match e.downcast::<String>() {
            Ok(v) => *v,
            Err(e) => match e.downcast::<&str>() {
                Ok(v) => v.to_string(),
                _ => "Unknown blaze-rs exception".to_owned(),
            },
        };
        let backtrace = BACKTRACE.get().unwrap().lock().unwrap();
        eprintln!("{}\nBacktrace:\n{}", panic_str, *backtrace);

        if !env.exception_check().unwrap() {
            env.throw_new("java/lang/RuntimeException", panic_str)
                .unwrap();
        }
    }

    info!(
        "blaze_call_native() time cost: {} sec",
        Instant::now().duration_since(start_time).as_secs_f64(),
    );
}

#[allow(clippy::redundant_slicing)]
pub fn blaze_call_native(
    env: &JNIEnv,
    task_definition: JObject,
    metric_node: JObject,
    ipc_record_batch_data_consumer: JObject,
    start_time: Instant,
) {
    info!("Blaze native computing started");
    debug!("Initializing JavaClasses");
    JavaClasses::init(env).expect("Error initializing JavaClasses");
    let env = JavaClasses::get_thread_jnienv();
    debug!("Initializing JavaClasses succeeded");

    debug!("Decoding task definition");
    let task_definition_raw = env
        .convert_byte_array(task_definition.into_inner())
        .expect("Error getting task definition");
    let task_definition: TaskDefinition = TaskDefinition::decode(&*task_definition_raw)
        .expect("Error decoding task definition");
    debug!("Decoding task definition succeeded");

    debug!("Creating native execution plan");
    let task_id = task_definition
        .task_id
        .expect("Missing task_definition.task_id");

    let plan = &task_definition.plan.expect("Missing task_definition.plan");
    let execution_plan: Arc<dyn ExecutionPlan> =
        plan.try_into().expect("Error converting to ExecutionPlan");
    info!(
        "Creating native execution plan succeeded: task_id={:?}, execution plan:\n{}",
        task_id,
        datafusion::physical_plan::displayable(execution_plan.as_ref()).indent()
    );

    tokio_runtime(10) // TODO: we should set this through JNI param
        .block_on(async {
            // TODO: we can pass down shuffle dirs, max memory threshold and batch_size
            // by creating RuntimeEnv with specific RuntimeConfig.
            // use the default one here as placeholder now.
            let runtime = Arc::new(RuntimeEnv::default());

            let result = execution_plan
                .execute(task_id.partition_id as usize, runtime)
                .await
                .unwrap();

            let record_batches: Vec<RecordBatch> = result
                .try_filter(|b| future::ready(b.num_rows() > 0))
                .try_collect::<Vec<_>>()
                .map_err(DataFusionError::from)
                .await
                .unwrap();

            info!("Executing plan finished");

            // update spark metrics
            metrics::update_spark_metric_node(&env, metric_node, execution_plan).unwrap();

            if !record_batches.is_empty() {
                let schema = record_batches[0].schema();
                debug!("Result schema:");
                for field in schema.fields() {
                    debug!(
                        " -> col={}, type={}, nullable={}",
                        field.name(),
                        field.data_type(),
                        field.is_nullable()
                    )
                }

                const OUPTUT_IPC_ROWS_LIMIT: usize = 65536;
                let mut num_rows_total = 0;
                let mut total_buf_len = 0;
                let mut current_batch_id = 0;
                let mut current_batch_offset = 0;

                // make sure each IPC is smaller than 2GB so that java
                // bytebuffer can handle it.
                while current_batch_id < record_batches.len() {
                    debug!("Writing IPC");

                    let mut buf: Vec<u8> = vec![];
                    let mut buf_writer = BufWriter::new(&mut buf);
                    let mut arrow_writer =
                        StreamWriter::try_new(&mut buf_writer, &*schema).unwrap();
                    let mut num_ipc_rows = 0;

                    // safety:
                    // write record batches into ipcs. the real size might be
                    // slightly larger than OUTPUT_IPC_SIZE, because there are
                    // BufWriters.
                    while current_batch_id < record_batches.len()
                        && current_batch_offset
                            < record_batches[current_batch_id].num_rows()
                        && num_ipc_rows < OUPTUT_IPC_ROWS_LIMIT
                    {
                        let current_batch = &record_batches[current_batch_id];

                        if current_batch_offset == 0
                            && current_batch.num_rows() < OUPTUT_IPC_ROWS_LIMIT
                        {
                            // output the whole current batch
                            current_batch_id += 1;
                            num_ipc_rows += current_batch.num_rows();
                            num_rows_total += current_batch.num_rows();
                            arrow_writer
                                .write(current_batch)
                                .expect("Error writing IPC");
                        } else {
                            // big batch -- output slices of current batch
                            let current_batch_offset_end = current_batch
                                .num_rows()
                                .min(current_batch_id + OUPTUT_IPC_ROWS_LIMIT);
                            let current_batch_slice = current_batch
                                .slice(current_batch_offset, current_batch_offset_end);
                            num_ipc_rows += current_batch_slice.num_rows();
                            num_rows_total += current_batch_slice.num_rows();
                            arrow_writer
                                .write(&current_batch_slice)
                                .expect("Error writing IPC");

                            current_batch_offset = current_batch_offset_end;
                            if current_batch_offset >= current_batch.num_rows() {
                                current_batch_id += 1;
                                current_batch_offset = 0;
                            }
                        }
                    }
                    arrow_writer.finish().expect("Error finishing arrow writer");
                    std::mem::drop(arrow_writer);
                    std::mem::drop(buf_writer);

                    info!(
                        "Writing IPC finished: rows={}, bytes={}",
                        num_rows_total,
                        buf.len(),
                    );
                    total_buf_len += buf.len();

                    debug!("Invoking IPC data consumer");
                    let byte_buffer = env
                        .new_direct_byte_buffer(&mut buf)
                        .expect("Error creating ByteBuffer");
                    jni_bridge_call_method!(
                        env,
                        JavaConsumer.accept,
                        ipc_record_batch_data_consumer,
                        JValue::Object(byte_buffer.into())
                    )
                    .expect("Error invoking IPC data consumer");
                    debug!("Invoking IPC data consumer succeeded");
                }

                metrics::update_extra_metrics(
                    &env,
                    metric_node,
                    start_time,
                    num_rows_total,
                    total_buf_len,
                )
                .unwrap();
            } else {
                metrics::update_extra_metrics(&env, metric_node, start_time, 0, 0)
                    .unwrap();
                debug!("Empty result, no need to invoking IPC data consumer");
            }
        });

    info!("Blaze native computing finished");
}
