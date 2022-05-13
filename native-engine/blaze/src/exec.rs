use std::alloc::Layout;
use std::any::Any;
use std::error::Error;
use std::io::Error as IoError;
use std::io::ErrorKind as IoErrorKind;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use datafusion::arrow::array::{export_array_into_raw, StructArray};
use datafusion::arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use futures::{FutureExt, StreamExt};

use jni::objects::{JClass, JString};
use jni::objects::{JObject, JThrowable};

use jni::JNIEnv;
use log::info;
use prost::Message;

use datafusion_ext::jni_bridge::JavaClasses;
use datafusion_ext::*;
use plan_serde::protobuf::TaskDefinition;

use crate::{session_ctx, setup_env_logger, BlazeIter};

#[allow(non_snake_case)]
#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_blaze_JniBridge_callNative(
    env: JNIEnv,
    _: JClass,
    task_definition: JObject,
    batch_size: i64,
    native_memory: i64,
    memory_fraction: f64,
    tmp_dirs: JString,
) -> i64 {
    info!("Entering blaze callNative()");
    setup_env_logger();

    match std::panic::catch_unwind(|| {
        JavaClasses::init(&env).unwrap();

        let task_definition_raw = env
            .convert_byte_array(task_definition.into_inner())
            .unwrap();
        let task_definition: TaskDefinition =
            TaskDefinition::decode(&*task_definition_raw).unwrap();
        let task_id = task_definition
            .task_id
            .ok_or_else(|| IoError::new(IoErrorKind::InvalidInput, "task id is empty"))
            .unwrap();
        let plan = &task_definition
            .plan
            .ok_or_else(|| IoError::new(IoErrorKind::InvalidInput, "task plan is empty"))
            .unwrap();

        let execution_plan: Arc<dyn ExecutionPlan> = plan.try_into().unwrap();
        let execution_plan_displayable =
            displayable(execution_plan.as_ref()).indent().to_string();
        info!("Creating native execution plan succeeded");
        info!("  task_id={:?}", task_id);
        info!("  execution plan:\n{}", execution_plan_displayable);

        let dirs = env.get_string(tmp_dirs).unwrap().into();
        let batch_size = batch_size as usize;
        assert!(batch_size > 0);

        let session_ctx =
            session_ctx(native_memory as usize, memory_fraction, batch_size, dirs);
        let task_ctx = session_ctx.task_ctx();

        // execute
        let stream = execution_plan
            .execute(task_id.partition_id as usize, task_ctx)
            .unwrap();

        // safety - manually allocated memory will be released when stream is exhausted
        unsafe {
            let blaze_iter_ptr: *mut BlazeIter =
                std::alloc::alloc(Layout::new::<BlazeIter>()) as *mut BlazeIter;

            std::ptr::write(
                blaze_iter_ptr,
                BlazeIter {
                    stream,
                    execution_plan,
                    runtime: Arc::new(
                        tokio::runtime::Builder::new_multi_thread()
                            .worker_threads(1)
                            .build()
                            .unwrap(),
                    ),
                },
            );
            blaze_iter_ptr as i64
        }
    }) {
        Err(err) => {
            handle_unwinded(err);
            -1
        }
        Ok(ptr) => ptr,
    }
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_spark_sql_blaze_JniBridge_loadNext<'env>(
    _: JNIEnv<'env>,
    _: JClass,
    iter_ptr: i64,
    schema_ptr: i64,
    array_ptr: i64,
) -> JObject<'env> {
    match std::panic::catch_unwind(|| {
        let blaze_iter = &mut *(iter_ptr as *mut BlazeIter);
        let env = JavaClasses::get_thread_jnienv();

        // create a weak global retPromise
        let ret_promise_ptr = std::mem::transmute::<_, isize>(
            jni_weak_global_ref!(
                env,
                jni_bridge_call_static_method!(
                    env,
                    ScalaPromise.apply -> JObject
                )
                .unwrap()
            )
            .unwrap(),
        );

        // get task context in current thread and propagate to spawned thread
        let task_context_ptr = std::mem::transmute::<_, isize>(
            jni_bridge_call_static_method!(
                env,
                JniBridge.getTaskContext -> JObject
            )
            .expect("get task context error"),
        );

        // spawn a thread to poll next batch
        blaze_iter.runtime.clone().spawn(async move {
            AssertUnwindSafe(async move {
                {
                    let env = JavaClasses::get_thread_jnienv();
                    let task_context =
                        std::mem::transmute::<_, JObject>(task_context_ptr);
                    jni_bridge_call_static_method!(
                        env,
                        JniBridge.setTaskContext -> (),
                        task_context
                    )
                    .expect("set task context error");
                }

                match blaze_iter.stream.next().await {
                    Some(Ok(batch)) => {
                        let num_rows = batch.num_rows();
                        let array: StructArray = batch.into();
                        let out_schema = schema_ptr as *mut FFI_ArrowSchema;
                        let out_array = array_ptr as *mut FFI_ArrowArray;

                        export_array_into_raw(Arc::new(array), out_array, out_schema)
                            .expect("export_array_into_raw error");
                        promise_success(ret_promise_ptr, num_rows as i64);
                    }
                    Some(Err(e)) => {
                        promise_failure(
                            ret_promise_ptr,
                            &format!("stream.next() error: {:?}", e),
                        );
                    }
                    None => {
                        promise_success(ret_promise_ptr, -1);
                    }
                }
            })
            .catch_unwind()
            .await
            .unwrap_or_else(|err| {
                let err = panic_message::panic_message(&err);
                promise_failure(
                    ret_promise_ptr,
                    &format!("blaze loadNext() panics: {}", err),
                );
            })
        });
        std::mem::transmute(ret_promise_ptr)
    }) {
        Err(err) => {
            handle_unwinded(err);
            JObject::null()
        }
        Ok(ret_promise) => ret_promise,
    }
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

unsafe fn promise_failure(promise_ptr: isize, msg: &str) {
    unsafe fn try_failure(
        promise_ptr: isize,
        msg: &str,
    ) -> datafusion::error::Result<()> {
        let env = JavaClasses::get_thread_jnienv();
        let promise = std::mem::transmute::<_, JObject>(promise_ptr);
        let msg = jni_map_error!(env.new_string(msg))?;
        let cause = if env.exception_check().unwrap_or(false) {
            let throwable = env
                .exception_occurred()
                .unwrap_or(JThrowable::from(JObject::null()))
                .into();
            let _ = env.exception_clear();
            throwable
        } else {
            JObject::null()
        };
        let e = jni_bridge_new_object!(env, JavaRuntimeException, msg, cause)?;
        jni_bridge_call_method!(env, ScalaPromise.failure -> JObject, promise, e)?;
        Ok(())
    }
    try_failure(promise_ptr, msg).unwrap_or_fatal();
}

unsafe fn promise_success(promise_ptr: isize, ret: i64) {
    unsafe fn try_success(promise_ptr: isize, ret: i64) -> datafusion::error::Result<()> {
        let env = JavaClasses::get_thread_jnienv();
        let promise = std::mem::transmute::<_, JObject>(promise_ptr);
        jni_bridge_call_method!(
            env,
            ScalaPromise.success -> JObject,
            promise,
            jni_bridge_new_object!(env, JavaLong, ret)?
        )?;
        Ok(())
    }
    try_success(promise_ptr, ret).unwrap_or_else(|err: DataFusionError| {
        promise_failure(
            promise_ptr,
            &format!("calling Promise.success error: {:?}", err),
        );
    });
}

fn is_jvm_interrupted(env: &JNIEnv) -> datafusion::error::Result<bool> {
    let interrupted_exception_class = "java.lang.InterruptedException";
    if env.exception_check().unwrap_or(false) {
        let e: JObject = env
            .exception_occurred()
            .unwrap_or(JThrowable::from(JObject::null()))
            .into();
        let class = jni_map_error!(env.get_object_class(e))?;
        let classname = jni_bridge_call_method!(env, Class.getName -> JObject, class)?;
        let classname = jni_map_error!(env.get_string(classname.into()))?;
        if classname.to_string_lossy().as_ref() == interrupted_exception_class {
            return Ok(true);
        }
    }
    Ok(false)
}

fn throw_runtime_exception(msg: &str, cause: JObject) -> datafusion::error::Result<()> {
    let env = JavaClasses::get_thread_jnienv();
    let msg = jni_map_error!(env.new_string(msg))?;
    let e = jni_bridge_new_object!(env, JavaRuntimeException, msg, cause)?;
    let _throw = jni_bridge_call_static_method!(
        env,
        JniBridge.raiseThrowable -> (),
        e
    );
    Ok(())
}

fn handle_unwinded(err: Box<dyn Any + Send>) {
    let env = JavaClasses::get_thread_jnienv();

    // default handling:
    //  * caused by InterruptedException: do nothing but just print a message.
    //  * other reasons: wrap it into a RuntimeException and throw.
    //  * if another error happens during handling, kill the whole JVM instance.
    let recover = || {
        if is_jvm_interrupted(&env)? {
            env.exception_clear()?;
            info!("native execution interrupted by JVM");
            return Ok(());
        }
        let panic_message = panic_message::panic_message(&err);

        // throw jvm runtime exception
        let cause = if env.exception_check()? {
            let throwable = env.exception_occurred()?.into();
            env.exception_clear()?;
            throwable
        } else {
            JObject::null()
        };
        throw_runtime_exception(panic_message, cause)?;
        Ok(())
    };
    recover().unwrap_or_else(|err: Box<dyn Error>| {
        env.fatal_error(format!(
            "Error recovering from panic, cannot resume: {:?}",
            err
        ));
    });
}
