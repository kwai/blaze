use std::cell::Cell;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use datafusion::arrow::array::{Array, UInt64Array};
use datafusion::arrow::compute::{take, TakeOptions};
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::arrow::record_batch::RecordBatch;

use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::memory_manager::MemoryManagerConfig;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_ext::jni_bridge_call_method_no_check_java_exception;
use futures::StreamExt;
use jni::objects::JObject;
use jni::objects::JValue;
use jni::objects::{JClass, JString};
use jni::JNIEnv;
use log::{debug, error, info};
use once_cell::sync::OnceCell;
use prost::Message;
use tokio::runtime::Runtime;

use datafusion_ext::jni_bridge::JavaClasses;

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
            .thread_name_fn(|| {
                static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                format!("Blaze-native-{}", id)
            })
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
    tmp_dirs: String,
) -> &'static SessionContext {
    SESSION_CONTEXT.get_or_init(|| {
        let dirs = tmp_dirs.split(',').map(PathBuf::from).collect::<Vec<_>>();
        let runtime_config = RuntimeConfig::new()
            .with_memory_manager(MemoryManagerConfig::New {
                max_memory,
                memory_fraction,
            })
            .with_disk_manager(DiskManagerConfig::NewSpecified(dirs));
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
    poolSize: i64,
    batch_size: i64,
    nativeMemory: i64,
    memoryFraction: f64,
    tmpDirs: JString,
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
            poolSize,
            batch_size,
            nativeMemory,
            memoryFraction,
            tmpDirs,
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

#[allow(clippy::redundant_slicing, clippy::too_many_arguments)]
pub fn blaze_call_native(
    env: &JNIEnv,
    task_definition: JObject,
    pool_size: i64,
    batch_size: i64,
    native_memory: i64,
    memory_fraction: f64,
    tmp_dirs: JString,
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

    let dirs = env
        .get_string(tmp_dirs)
        .expect("Failed to convert tmp_dir string for disk_manager")
        .into();

    let batch_size = batch_size as usize;
    assert!(batch_size > 0);
    tokio_runtime(pool_size as usize).block_on(async move {
        let session_ctx =
            session_ctx(native_memory as usize, memory_fraction, batch_size, dirs);
        let task_ctx = session_ctx.task_ctx();

        // execute
        let result = execution_plan
            .execute(task_id.partition_id as usize, task_ctx)
            .await
            .unwrap();
        metrics::update_spark_metric_node(&env, metric_node, execution_plan.clone())
            .unwrap();

        // add some statistics logging
        info!(
            "Executing plan finished, result rows: {}",
            execution_plan
                .metrics()
                .and_then(|m| m.output_rows())
                .unwrap_or_default()
        );
        if let Some(shuffle_writer_exec) =
            execution_plan.as_any().downcast_ref::<ShuffleWriterExec>()
        {
            info!(
                "Shuffle writer output rows: {}",
                shuffle_writer_exec.children()[0]
                    .metrics()
                    .and_then(|m| m.output_rows())
                    .unwrap_or_default()
            );
        }
        info!("Result schema:");
        for field in execution_plan.schema().fields() {
            info!(
                " -> col={}, type={}, nullable={}",
                field.name(),
                field.data_type(),
                field.is_nullable()
            )
        }

        // output ipc
        let num_rows_total = Cell::new(0);
        let num_bytes_total = Cell::new(0);
        let stop = Cell::new(false);
        result
            .for_each(|batch| async {
                let batch = batch.unwrap();
                if stop.get() || batch.num_rows() == 0 {
                    return;
                }
                num_rows_total.set(num_rows_total.get() + batch.num_rows());

                for batch_offset in (0..batch.num_rows()).step_by(batch_size) {
                    let mut buf: Vec<u8> = vec![];
                    let mut buf_writer = BufWriter::new(&mut buf);
                    let mut arrow_writer =
                        FileWriter::try_new(&mut buf_writer, &*batch.schema()).unwrap();

                    if batch.num_rows() <= batch_size as usize {
                        arrow_writer.write(&batch).expect("Error writing IPC");
                    } else {
                        let batch_slice = record_batch_slice(
                            &batch,
                            batch_offset,
                            batch_size.min(batch.num_rows() - batch_offset),
                        )
                        .unwrap();
                        arrow_writer.write(&batch_slice).expect("Error writing IPC");
                    }
                    arrow_writer.finish().unwrap();
                    std::mem::drop(arrow_writer);
                    std::mem::drop(buf_writer);

                    consume_ipc(&env, &mut buf, ipc_record_batch_data_consumer).unwrap();
                    if env.exception_check().unwrap() {
                        log::warn!("Received consumer exception, stop outputing...");
                        stop.set(true);
                        env.exception_describe().unwrap();
                        env.exception_clear().unwrap();
                    }
                    num_bytes_total.set(num_bytes_total.get() + buf.len());
                }

                metrics::update_extra_metrics(
                    &env,
                    metric_node,
                    start_time,
                    num_rows_total.get(),
                    num_bytes_total.get(),
                )
                .unwrap();
            })
            .await;
    });
    info!("Blaze native computing finished");
}

fn consume_ipc(
    env: &JNIEnv,
    buf: &mut [u8],
    consumer: JObject,
) -> jni::errors::Result<()> {
    info!("Invoking IPC data consumer");

    let byte_buffer = env
        .new_direct_byte_buffer(buf)
        .expect("Error creating ByteBuffer");
    jni_bridge_call_method_no_check_java_exception!(
        env,
        JavaConsumer.accept,
        consumer,
        JValue::Object(byte_buffer.into())
    )?;
    info!("Invoking IPC data consumer succeeded");
    Ok(())
}

fn record_batch_slice(
    batch: &RecordBatch,
    offset: usize,
    len: usize,
) -> datafusion::arrow::error::Result<RecordBatch> {
    // FIXME: RecordBatch.slice() is buggy and produces
    //   duplicated rows
    //
    // let batch_slice = batch.slice(
    //     batch_offset,
    //     batch_slice_len);
    //
    RecordBatch::try_new(
        batch.schema(),
        batch
            .columns()
            .iter()
            .map(|c| {
                let end = offset + len;
                take(
                    c.as_ref(),
                    &UInt64Array::from_iter_values(offset as u64..end as u64),
                    Some(TakeOptions {
                        check_bounds: false,
                    }),
                )
            })
            .collect::<datafusion::arrow::error::Result<Vec<Arc<dyn Array>>>>()?,
    )
}
