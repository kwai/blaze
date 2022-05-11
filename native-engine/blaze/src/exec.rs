use std::alloc::Layout;
use std::error::Error;
use std::io::Error as IoError;
use std::io::ErrorKind as IoErrorKind;
use std::sync::Arc;

use datafusion::arrow::array::{export_array_into_raw, StructArray};
use datafusion::arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::physical_plan::{displayable, ExecutionPlan};
use futures::StreamExt;

use jni::objects::JObject;
use jni::objects::{JClass, JString};

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

    // save backtrace when panics
    let result = match std::panic::catch_unwind(|| {
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
        Err(e) => {
            let recover = || {
                if is_jvm_interrupted(&env)? {
                    env.exception_clear()?;
                    info!("Blaze callNative() interrupted by JVM");
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
                throw_runtime_exception(panic_message, cause)?;
                Ok(())
            };
            recover().unwrap_or_else(|err: Box<dyn Error>| {
                env.fatal_error(format!(
                    "Error recovering from panic, cannot resume: {:?}",
                    err
                ));
            });
            -1
        }
        Ok(ptr) => ptr,
    };
    result
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
    unsafe fn failure(promise_ptr: isize, msg: &str) {
        unsafe fn try_failure(promise_ptr: isize, msg: &str) -> jni::errors::Result<()> {
            let env = JavaClasses::get_thread_jnienv();
            let promise = std::mem::transmute::<_, JObject>(promise_ptr);
            let msg = env.new_string(msg).unwrap();
            let cause = if env.exception_check()? {
                let throwable = env.exception_occurred()?.into();
                env.exception_clear().unwrap();
                throwable
            } else {
                JObject::null()
            };
            let e = jni_bridge_new_object!(env, JavaRuntimeException, msg, cause)?;
            jni_bridge_call_method!(env, ScalaPromise.failure, promise, e)?;
            Ok(())
        }

        if let Err(err) = std::panic::catch_unwind(|| {
            try_failure(promise_ptr, msg).unwrap();
        }) {
            let err = panic_message::panic_message(&err);
            let env = JavaClasses::get_thread_jnienv();
            env.fatal_error(format!("calling Promise.failure error: {:?}", err));
        }
    }

    unsafe fn success(promise_ptr: isize, ret: i64) {
        unsafe fn try_success(promise_ptr: isize, ret: i64) -> jni::errors::Result<()> {
            let env = JavaClasses::get_thread_jnienv();
            let promise = std::mem::transmute::<_, JObject>(promise_ptr);
            jni_bridge_call_method!(
                env,
                ScalaPromise.success,
                promise,
                jni_bridge_new_object!(env, JavaLong, ret)?
            )?;
            Ok(())
        }
        if let Err(err) = try_success(promise_ptr, ret) {
            failure(
                promise_ptr,
                &format!("calling Promise.success error: {:?}", err),
            );
        }
    }
    let blaze_iter = &mut *(iter_ptr as *mut BlazeIter);
    let env = JavaClasses::get_thread_jnienv();

    // create a weak global retPromise
    let ret_promise_ptr = std::mem::transmute::<_, isize>({
        let local = jni_bridge_call_static_method!(env, ScalaPromise.apply)
            .unwrap()
            .l()
            .unwrap();
        match weak_global_ref!(env, local) {
            Ok(weak_global_ref) => weak_global_ref,
            Err(err) => {
                throw_runtime_exception(
                    &format!("create weak global ref for promise error: {:?}", err),
                    JObject::null(),
                )
                .unwrap();
                return JObject::null();
            }
        }
    });

    // spawn a thread to poll next batch
    blaze_iter.runtime.clone().spawn(async move {
        loop {
            match blaze_iter.stream.next().await {
                Some(Ok(batch)) => {
                    let num_rows = batch.num_rows();
                    if num_rows == 0 {
                        continue;
                    }

                    let array: StructArray = batch.into();
                    let out_schema = schema_ptr as *mut FFI_ArrowSchema;
                    let out_array = array_ptr as *mut FFI_ArrowArray;

                    if let Err(err) =
                        export_array_into_raw(Arc::new(array), out_array, out_schema)
                    {
                        break failure(
                            ret_promise_ptr,
                            &format!("export_array_into_raw error: {:?}", err),
                        );
                    }
                    break success(ret_promise_ptr, num_rows as i64);
                }
                Some(Err(e)) => {
                    break failure(
                        ret_promise_ptr,
                        &format!("stream.next() error: {:?}", e),
                    );
                }
                None => {
                    break success(ret_promise_ptr, -1);
                }
            }
        }
    });
    std::mem::transmute(ret_promise_ptr)
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

fn throw_runtime_exception(msg: &str, cause: JObject) -> jni::errors::Result<()> {
    let env = JavaClasses::get_thread_jnienv();
    let msg = env.new_string(msg)?;
    let e = jni_bridge_new_object!(env, JavaRuntimeException, msg, cause)?;
    let _throw = jni_bridge_call_static_method_no_check_java_exception!(
        env,
        JniBridge.raiseThrowable,
        e
    );
    Ok(())
}
