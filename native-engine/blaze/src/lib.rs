use std::error::Error;
use std::io::Error as IoError;
use std::io::ErrorKind as IoErrorKind;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{export_array_into_raw, StructArray};

use datafusion::arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::memory_manager::MemoryManagerConfig;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::{displayable, SendableRecordBatchStream};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_ext::*;
use futures::StreamExt;
use jni::objects::JObject;

use jni::objects::{JClass, JString};
use jni::JNIEnv;
use log::{debug, error, info};
use once_cell::sync::OnceCell;
use prost::Message;
use tokio::runtime::Runtime;

use datafusion_ext::jni_bridge::JavaClasses;

use plan_serde::protobuf::TaskDefinition;

mod metrics;

#[cfg(feature = "mm")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "sn")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

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
) -> i64 {
    let start_time = Instant::now();
    setup_env_logger();

    // save backtrace when panics
    let result = match std::panic::catch_unwind(|| {
        info!("Blaze native computing started");
        let iter_ptr = blaze_call_native(
            &env,
            taskDefinition,
            poolSize,
            batch_size,
            nativeMemory,
            memoryFraction,
            tmpDirs,
            metricNode,
        )
        .unwrap();
        info!("Blaze native computing finished");
        iter_ptr
    }) {
        Err(e) => {
            let recover = || {
                if is_jvm_interrupted(&env)? {
                    env.exception_clear()?;
                    info!("Blaze native computing interrupted by JVM");
                    return Ok(());
                }
                let panic_message = panic_message::panic_message(&e);

                // throw jvm runtime exception
                let cause = if env.exception_check()? {
                    let throwable = env.exception_occurred()?.into();
                    env.exception_clear()?;
                    throwable
                } else {
                    JObject::null()
                };
                let msg = env.new_string(&panic_message)?;
                let _throw = jni_bridge_call_static_method_no_check_java_exception!(
                    env,
                    JniBridge.raiseThrowable,
                    jni_bridge_new_object!(env, JavaRuntimeException, msg, cause)?
                );
                Ok(())
            };
            recover().unwrap_or_else(|err: Box<dyn Error>| {
                error!("Error recovering from panic, cannot resume: {:?}", err);
                std::process::abort();
            });
            -1
        }
        Ok(ptr) => ptr,
    };

    let time_cost_sec = Instant::now().duration_since(start_time).as_secs_f64();
    info!("blaze_call_native() time cost: {} sec", time_cost_sec);
    result
}

#[allow(non_snake_case)]
#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_blaze_JniBridge_loadNext(
    _: JNIEnv,
    _: JClass,
    iter_ptr: i64,
    schema_ptr: i64,
    array_ptr: i64,
) -> i64 {
    // loadNext is always called after callNative, therefore a tokio runtime already
    tokio_runtime(0).block_on(read_stream_next(iter_ptr, schema_ptr, array_ptr))
}

async fn read_stream_next(iter_ptr: i64, schema_ptr: i64, array_ptr: i64) -> i64 {
    unsafe {
        let stream = &mut *(iter_ptr as *mut SendableRecordBatchStream);
        loop {
            return match stream.next().await {
                Some(Ok(batch)) => {
                    let num_rows = batch.num_rows();
                    if num_rows == 0 {
                        continue;
                    }
                    let array: StructArray = batch.into();
                    let out_schema = schema_ptr as *mut FFI_ArrowSchema;
                    let out_array = array_ptr as *mut FFI_ArrowArray;
                    export_array_into_raw(Arc::new(array), out_array, out_schema)
                        .unwrap();
                    num_rows as i64
                }
                _ => -1,
            };
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn blaze_call_native(
    env: &JNIEnv,
    task_definition: JObject,
    pool_size: i64,
    batch_size: i64,
    native_memory: i64,
    memory_fraction: f64,
    tmp_dirs: JString,
    metric_node: JObject,
) -> Result<i64, Box<dyn Error>> {
    debug!("Initializing JavaClasses");
    JavaClasses::init(env)?;

    let env = JavaClasses::get_thread_jnienv();
    debug!("Initializing JavaClasses succeeded");

    debug!("Decoding task definition");
    let task_definition_raw = env.convert_byte_array(task_definition.into_inner())?;
    let task_definition: TaskDefinition = TaskDefinition::decode(&*task_definition_raw)?;
    debug!("Decoding task definition succeeded");

    debug!("Creating native execution plan");
    let task_id = task_definition
        .task_id
        .ok_or_else(|| IoError::new(IoErrorKind::InvalidInput, "task id is empty"))?;
    let plan = &task_definition
        .plan
        .ok_or_else(|| IoError::new(IoErrorKind::InvalidInput, "task plan is empty"))?;

    let execution_plan: Arc<dyn ExecutionPlan> = plan.try_into()?;
    let execution_plan_displayable =
        displayable(execution_plan.as_ref()).indent().to_string();
    info!("Creating native execution plan succeeded");
    info!("  task_id={:?}", task_id);
    info!("  execution plan:\n{}", execution_plan_displayable);

    let dirs = env.get_string(tmp_dirs)?.into();
    let batch_size = batch_size as usize;
    assert!(batch_size > 0);

    tokio_runtime(pool_size as usize).block_on(async move {
        let session_ctx =
            session_ctx(native_memory as usize, memory_fraction, batch_size, dirs);
        let task_ctx = session_ctx.task_ctx();

        // execute
        let result = execution_plan
            .execute(task_id.partition_id as usize, task_ctx)
            .await?;
        metrics::update_spark_metric_node(&env, metric_node, execution_plan.clone())?;
        let stream_ptr = Box::into_raw(Box::new(result)) as i64;
        Ok(stream_ptr)
    })
}

fn is_jvm_interrupted(env: &JNIEnv) -> jni::errors::Result<bool> {
    let interrupted_exception_class = "java.lang.InterruptedException";
    if env.exception_check()? {
        let e = env.exception_occurred()?;
        let class = env.get_object_class(e)?;
        let classname = jni_bridge_call_method!(env, Class.getName, class)?.l()?;
        let classname = env.get_string(classname.into())?;
        if classname.to_string_lossy().as_ref() == interrupted_exception_class {
            return Ok(true);
        }
    }
    Ok(false)
}
