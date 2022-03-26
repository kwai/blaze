use std::future;
use std::io::BufWriter;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use datafusion::arrow::array::{Array, UInt64Array};
use datafusion::arrow::compute::{take, TakeOptions};
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::memory_manager::MemoryManagerConfig;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_plan::common::batch_byte_size;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::TryFutureExt;
use futures::TryStreamExt;
use jni::objects::JClass;
use jni::objects::JObject;
use jni::objects::JValue;
use jni::JNIEnv;
use log::{debug, error, info};
use once_cell::sync::OnceCell;
use prost::Message;
use tokio::runtime::Runtime;

use datafusion_ext::jni_bridge::JavaClasses;
use datafusion_ext::jni_bridge_call_method;
use datafusion_ext::shuffle_writer_exec::ShuffleWriterExec;
use plan_serde::protobuf::TaskDefinition;

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
static SESSION_CONTEXT: OnceCell<SessionContext> = OnceCell::new();

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

fn session_ctx(
    max_memory: usize,
    memory_fraction: f64,
    batch_size: usize,
) -> &'static SessionContext {
    SESSION_CONTEXT.get_or_init(|| {
        let runtime_config =
            RuntimeConfig::new().with_memory_manager(MemoryManagerConfig::New {
                max_memory,
                memory_fraction,
            });
        let runtime = Arc::new(RuntimeEnv::new(runtime_config).unwrap());
        let config = SessionConfig::new().with_batch_size(batch_size);
        SessionContext::with_config_rt(config, runtime)
    })
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
        error!("{}\nBacktrace:\n{}", panic_str, *backtrace);

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
            // TODO: pass down these settings from JNI
            let session_ctx = session_ctx(usize::MAX, 1.0, 10240);
            let task_ctx = session_ctx.task_ctx();

            let result = execution_plan
                .execute(task_id.partition_id as usize, task_ctx)
                .await
                .unwrap();

            let record_batches: Vec<RecordBatch> = result
                .try_filter(|b| future::ready(b.num_rows() > 0))
                .try_collect::<Vec<_>>()
                .map_err(DataFusionError::from)
                .await
                .unwrap();

            info!(
                "Executing plan finished, result rows: {}",
                record_batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            );
            if execution_plan
                .as_any()
                .downcast_ref::<ShuffleWriterExec>()
                .is_some()
            {
                info!(
                    "Shuffle writer output rows: {}",
                    execution_plan.children()[0]
                        .metrics()
                        .and_then(|m| m.output_rows())
                        .unwrap_or_default()
                );
            }

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

                // make sure each IPC is smaller than 2GB so that java
                // bytebuffer can handle it.
                const OUTPUT_BATCH_SLICE_LEN: usize = 10000;
                const OUTPUT_BATCH_BYTES_SIZE: usize = 67108864; // 64MB
                let mut num_rows_total = 0;
                let mut total_buf_len = 0;
                let mut batch_id = 0;
                let mut batch_offset = 0;

                while batch_id < record_batches.len() {
                    let mut buf: Vec<u8> = vec![];
                    let mut buf_writer = BufWriter::new(&mut buf);
                    let mut arrow_writer =
                        FileWriter::try_new(&mut buf_writer, &*schema).unwrap();
                    let mut num_ipc_rows = 0;
                    let mut sum_ipc_batch_bytes_size = 0;

                    // safety:
                    // write record batches into ipcs. the real size might be
                    // slightly larger than OUTPUT_BATCH_BYTES_SIZE, because there are
                    // BufWriters.
                    while batch_id < record_batches.len()
                        && sum_ipc_batch_bytes_size < OUTPUT_BATCH_BYTES_SIZE
                    {
                        let batch = &record_batches[batch_id];

                        if batch_offset == 0 && batch.num_rows() < OUTPUT_BATCH_SLICE_LEN
                        {
                            // output the whole current batch
                            batch_id += 1;
                            num_ipc_rows += batch.num_rows();
                            num_rows_total += batch.num_rows();
                            sum_ipc_batch_bytes_size += batch_byte_size(batch);
                            arrow_writer.write(batch).expect("Error writing IPC");
                        } else {
                            // big batch -- output slices of current batch
                            let batch_slice_len = batch
                                .num_rows()
                                .saturating_sub(batch_offset)
                                .min(OUTPUT_BATCH_SLICE_LEN);

                            // FIXME: RecordBatch.slice() is buggy and produces duplicated rows
                            //
                            // let batch_slice = batch.slice(
                            //     batch_offset,
                            //     batch_slice_len);
                            //
                            let batch_slice = RecordBatch::try_new(
                                batch.schema(),
                                batch
                                    .columns()
                                    .iter()
                                    .map(|c| {
                                        let batch_offset_end =
                                            batch_offset + batch_slice_len;
                                        take(
                                            c.as_ref(),
                                            &UInt64Array::from_iter_values(
                                                batch_offset as u64
                                                    ..batch_offset_end as u64,
                                            ),
                                            Some(TakeOptions {
                                                check_bounds: false,
                                            }),
                                        )
                                        .unwrap()
                                    })
                                    .collect::<Vec<Arc<dyn Array>>>(),
                            )
                            .unwrap();
                            num_ipc_rows += batch_slice_len;
                            num_rows_total += batch_slice_len;
                            sum_ipc_batch_bytes_size += batch_byte_size(&batch_slice);
                            info!(
                                "XXXXXX batch slice num_rows: {}, bytes size: {}",
                                batch_slice.num_rows(),
                                batch_byte_size(&batch_slice),
                            );
                            batch_offset += batch_slice_len;
                            if batch_offset >= batch.num_rows() {
                                batch_id += 1;
                                batch_offset = 0;
                            }
                            arrow_writer.write(&batch_slice).expect("Error writing IPC");
                        }
                    }
                    arrow_writer.finish().expect("Error finishing arrow writer");
                    std::mem::drop(arrow_writer);
                    std::mem::drop(buf_writer);

                    info!(
                        "Writing IPC finished: rows={}, bytes={}",
                        num_ipc_rows,
                        buf.len(),
                    );
                    total_buf_len += buf.len();

                    info!("Invoking IPC data consumer");
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
                    info!("Invoking IPC data consumer succeeded");
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
                info!("Empty result, no need to invoking IPC data consumer");
            }
        });

    info!("Blaze native computing finished");
}
