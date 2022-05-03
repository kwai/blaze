use std::alloc::Layout;
use std::error::Error;
use std::io::Error as IoError;
use std::io::ErrorKind as IoErrorKind;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{export_array_into_raw, StructArray};
use datafusion::arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::physical_plan::{displayable, ExecutionPlan};
use futures::StreamExt;
use jni::objects::JObject;
use jni::objects::{JClass, JString};
use jni::JNIEnv;
use log::{debug, error, info};
use prost::Message;

use datafusion_ext::jni_bridge::JavaClasses;
use datafusion_ext::*;
use plan_serde::protobuf::TaskDefinition;

use crate::{session_ctx, setup_env_logger, tokio_runtime, BlazeIter};

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
pub unsafe extern "system" fn Java_org_apache_spark_sql_blaze_JniBridge_loadNext(
    _: JNIEnv,
    _: JClass,
    iter_ptr: i64,
    schema_ptr: i64,
    array_ptr: i64,
) -> i64 {
    // loadNext is always called after callNative, therefore a tokio runtime already
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            let blaze_iter = &mut *(iter_ptr as *mut BlazeIter);
            loop {
                return match blaze_iter.stream.next().await {
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
        })
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_spark_sql_blaze_JniBridge_deallocIter(
    _: JNIEnv,
    _: JClass,
    iter_ptr: i64,
) {
    std::alloc::dealloc(iter_ptr as *mut u8, Layout::new::<BlazeIter>());
}

#[allow(clippy::too_many_arguments)]
fn blaze_call_native(
    env: &JNIEnv,
    task_definition: JObject,
    pool_size: i64,
    batch_size: i64,
    native_memory: i64,
    memory_fraction: f64,
    tmp_dirs: JString,
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
        let result_stream = execution_plan
            .execute(task_id.partition_id as usize, task_ctx)
            .await?;

        // safety - manually allocated memory will be released when stream is exhausted
        unsafe {
            let blaze_iter_ptr: *mut BlazeIter =
                std::alloc::alloc(Layout::new::<BlazeIter>()) as *mut BlazeIter;

            std::ptr::write(
                blaze_iter_ptr,
                BlazeIter {
                    stream: result_stream,
                    execution_plan: execution_plan.clone(),
                },
            );
            Ok(blaze_iter_ptr as i64)
        }
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
