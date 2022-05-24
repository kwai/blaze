use std::any::Any;
use std::error::Error;

use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{export_array_into_raw, StructArray};
use datafusion::arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::memory_manager::MemoryManagerConfig;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_plan::{displayable, ExecutionPlan};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_ext::jni_bridge::JavaClasses;
use datafusion_ext::*;
use futures::{FutureExt, StreamExt};
use jni::objects::{JClass, JString};
use jni::objects::{JObject, JThrowable};
use jni::sys::{jboolean, jlong, JNI_FALSE, JNI_TRUE};
use jni::JNIEnv;
use log::LevelFilter;
use once_cell::sync::OnceCell;
use plan_serde::protobuf::TaskDefinition;
use prost::Message;
use simplelog::{ColorChoice, ConfigBuilder, TermLogger, TerminalMode, ThreadLogMode};
use tokio::runtime::Runtime;

use crate::metrics::update_spark_metric_node;

static LOGGING_INIT: OnceCell<()> = OnceCell::new();
static SESSIONCTX: OnceCell<SessionContext> = OnceCell::new();

#[allow(non_snake_case)]
#[allow(clippy::single_match)]
#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_blaze_JniBridge_initNative(
    env: JNIEnv,
    _: JClass,
    batch_size: i64,
    native_memory: i64,
    memory_fraction: f64,
    tmp_dirs: JString,
) {
    match std::panic::catch_unwind(|| {
        // init logging
        LOGGING_INIT.get_or_init(|| {
            TermLogger::init(
                LevelFilter::Info,
                ConfigBuilder::new()
                    .set_thread_mode(ThreadLogMode::Both)
                    .build(),
                TerminalMode::Stderr,
                ColorChoice::Never,
            )
            .unwrap();
        });

        // init jni java classes
        JavaClasses::init(&env);

        // init datafusion session context
        SESSIONCTX.get_or_init(|| {
            let dirs = jni_get_string!(tmp_dirs)
                .unwrap()
                .split(',')
                .map(PathBuf::from)
                .collect::<Vec<_>>();
            let max_memory = native_memory as usize;
            let batch_size = batch_size as usize;
            let runtime_config = RuntimeConfig::new()
                .with_memory_manager(MemoryManagerConfig::New {
                    max_memory,
                    memory_fraction,
                })
                .with_disk_manager(DiskManagerConfig::NewSpecified(dirs));
            let runtime = Arc::new(RuntimeEnv::new(runtime_config).unwrap());
            let config = SessionConfig::new().with_batch_size(batch_size);
            SessionContext::with_config_rt(config, runtime)
        });
    }) {
        Err(err) => {
            handle_unwinded(err);
        }
        Ok(()) => {}
    }
}

#[allow(non_snake_case)]
#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_blaze_JniBridge_callNative(
    _: JNIEnv,
    _: JClass,
    wrapper: JObject,
) {
    if let Err(err) = std::panic::catch_unwind(|| {
        log::info!("Entering blaze callNative()");

        let wrapper = Arc::new(jni_new_global_ref!(wrapper).unwrap());
        let wrapper_clone = wrapper.clone();

        let obj_true =
            jni_new_global_ref!(jni_new_object!(JavaBoolean, JNI_TRUE).unwrap()).unwrap();

        let obj_false =
            jni_new_global_ref!(jni_new_object!(JavaBoolean, JNI_FALSE).unwrap())
                .unwrap();

        // decode plan
        let raw_task_definition: JObject = jni_call!(
            BlazeCallNativeWrapper(wrapper.as_obj()).getRawTaskDefinition() -> JObject
        )
        .unwrap();

        let task_definition = TaskDefinition::decode(
            jni_convert_byte_array!(raw_task_definition.into_inner())
                .unwrap()
                .as_slice(),
        )
        .unwrap();

        let task_id = &task_definition.task_id.expect("task_id is empty");
        let plan = &task_definition.plan.expect("plan is empty");

        // get execution plan
        let execution_plan: Arc<dyn ExecutionPlan> = plan.try_into().unwrap();
        let execution_plan_displayable =
            displayable(execution_plan.as_ref()).indent().to_string();
        log::info!("Creating native execution plan succeeded");
        log::info!("  task_id={:?}", task_id);
        log::info!("  execution plan:\n{}", execution_plan_displayable);

        // execute
        let session_ctx = SESSIONCTX.get().unwrap();
        let task_ctx = session_ctx.task_ctx();
        let mut stream = execution_plan
            .execute(task_id.partition_id as usize, task_ctx)
            .unwrap();

        let task_context = jni_new_global_ref!(
            jni_call_static!(JniBridge.getTaskContext() -> JObject).unwrap()
        )
        .unwrap();

        // a runtime wrapper that calls shutdown_background on dropping
        struct RuntimeWrapper {
            runtime: Option<Runtime>,
        }
        impl Drop for RuntimeWrapper {
            fn drop(&mut self) {
                if let Some(rt) = self.runtime.take() {
                    rt.shutdown_background();
                }
            }
        }

        // spawn a thread to poll batches
        let runtime = Arc::new(RuntimeWrapper {
            runtime: Some(
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .thread_keep_alive(Duration::MAX) // always use same thread
                    .build()
                    .unwrap(),
            ),
        });
        let runtime_clone = runtime.clone();

        runtime.clone().runtime.as_ref().unwrap().spawn(async move {
            AssertUnwindSafe(async move {
                let mut total_batches = 0;
                let mut total_rows = 0;

                // propagate task context to spawned children threads
                jni_call_static!(JniBridge.setTaskContext(task_context.as_obj()) -> ()).unwrap();

                // load batches
                while let Some(r) = stream.next().await {
                    match r {
                        Ok(batch) => {
                            let num_rows = batch.num_rows();
                            if num_rows == 0 {
                                continue;
                            }
                            total_batches += 1;
                            total_rows += num_rows;

                            // value_queue -> (schema_ptr, array_ptr)
                            let mut input = JObject::null();
                            while jni_call!(BlazeCallNativeWrapper(wrapper.as_obj()).isFinished() -> jboolean).unwrap() != JNI_TRUE {
                                input = jni_call!(BlazeCallNativeWrapper(wrapper.as_obj()).dequeueWithTimeout() -> JObject).unwrap();

                                if !input.is_null() {
                                    break;
                                }
                            }
                            if input.is_null() { // wrapper.isFinished = true
                                break;
                            }

                            let schema_ptr = jni_call!(ScalaTuple2(input)._1() -> JObject).unwrap();
                            let schema_ptr = jni_call!(JavaLong(schema_ptr).longValue() -> jlong).unwrap();
                            let array_ptr = jni_call!(ScalaTuple2(input)._2() -> JObject).unwrap();
                            let array_ptr = jni_call!(JavaLong(array_ptr).longValue() -> jlong).unwrap();

                            let out_schema = schema_ptr as *mut FFI_ArrowSchema;
                            let out_array = array_ptr as *mut FFI_ArrowArray;
                            let batch: Arc<StructArray> = Arc::new(batch.into());
                            unsafe {
                                export_array_into_raw(
                                    batch,
                                    out_array,
                                    out_schema,
                                )
                                .expect("export_array_into_raw error");
                            }

                            // value_queue <- hasNext=true
                            while {
                                jni_call!(BlazeCallNativeWrapper(wrapper.as_obj()).isFinished() -> jboolean).unwrap() != JNI_TRUE &&
                                jni_call!(BlazeCallNativeWrapper(wrapper.as_obj()).enqueueWithTimeout(obj_true.as_obj()) -> jboolean).unwrap() != JNI_TRUE
                            } {}
                        }
                        Err(e) => {
                            panic!("stream.next() error: {:?}", e);
                        }
                    }
                }

                // value_queue -> (discard)
                while jni_call!(BlazeCallNativeWrapper(wrapper.as_obj()).isFinished() -> jboolean).unwrap() != JNI_TRUE {
                    let input = jni_call!(BlazeCallNativeWrapper(wrapper.as_obj()).dequeueWithTimeout() -> JObject).unwrap();
                    if !input.is_null() {
                        break;
                    }
                }

                // value_queue <- hasNext=false
                while {
                    jni_call!(BlazeCallNativeWrapper(wrapper.as_obj()).isFinished() -> jboolean).unwrap() != JNI_TRUE &&
                    jni_call!(BlazeCallNativeWrapper(wrapper.as_obj()).enqueueWithTimeout(obj_false.as_obj()) -> jboolean).unwrap() != JNI_TRUE
                } {}

                log::info!("Updating blaze exec metrics ...");
                let metrics = jni_call!(
                    BlazeCallNativeWrapper(wrapper.as_obj()).getMetrics() -> JObject
                ).unwrap();

                update_spark_metric_node(
                    metrics,
                    execution_plan.clone(),
                ).unwrap();

                log::info!("Blaze native executing finished.");
                log::info!("  total loaded batches: {}", total_batches);
                log::info!("  total loaded rows: {}", total_rows);
                std::mem::drop(runtime);
            })
            .catch_unwind()
            .await
            .map_err(|err| {
                let panic_message = panic_message::panic_message(&err);

                let e = if jni_exception_check!()? {
                    log::error!("native execution panics with an java exception");
                    log::error!("panic message: {}", panic_message);
                    jni_exception_occurred!()?.into()
                } else {
                    log::error!("native execution panics");
                    log::error!("panic message: {}", panic_message);
                    jni_new_object!(
                        JavaRuntimeException,
                        jni_new_string!("blaze native panics")?,
                        JObject::null()
                    )?
                };

                // error_queue <- exception

                while jni_call!(
                    BlazeCallNativeWrapper(wrapper_clone.as_obj()).isFinished() -> jboolean
                ).unwrap() != JNI_TRUE {
                    let enqueued = jni_call!(
                        BlazeCallNativeWrapper(wrapper_clone.as_obj()).enqueueError(e) -> jboolean
                    )?;
                    if enqueued == JNI_TRUE {
                        break;
                    }
                }
                log::info!("Blaze native executing exited with error.");
                std::mem::drop(runtime_clone);
                datafusion::error::Result::Ok(())
            })
            .unwrap();
        });

        log::info!("Blaze native thread created");
    }) {
        handle_unwinded(err);
    }
}

fn is_jvm_interrupted() -> datafusion::error::Result<bool> {
    let interrupted_exception_class = "java.lang.InterruptedException";
    if jni_exception_check!()? {
        let e: JObject = jni_exception_occurred!()?.into();
        let class = jni_get_object_class!(e)?;
        let classname_obj = jni_call!(Class(class).getName() -> JObject)?;
        let classname = jni_get_string!(classname_obj.into())?;

        if classname == interrupted_exception_class {
            return Ok(true);
        }
    }
    Ok(false)
}

fn throw_runtime_exception(msg: &str, cause: JObject) -> datafusion::error::Result<()> {
    let msg = jni_new_string!(msg)?;
    let e = jni_new_object!(JavaRuntimeException, msg, cause)?;

    if let Err(err) = jni_throw!(JThrowable::from(e)) {
        jni_fatal_error!(format!(
            "Error throwing RuntimeException, cannot result: {:?}",
            err
        ));
    }
    Ok(())
}

fn handle_unwinded(err: Box<dyn Any + Send>) {
    // default handling:
    //  * caused by InterruptedException: do nothing but just print a message.
    //  * other reasons: wrap it into a RuntimeException and throw.
    //  * if another error happens during handling, kill the whole JVM instance.
    let recover = || {
        if is_jvm_interrupted()? {
            jni_exception_clear!()?;
            log::info!("native execution interrupted by JVM");
            return Ok(());
        }
        let panic_message = panic_message::panic_message(&err);

        // throw jvm runtime exception
        let cause = if jni_exception_check!()? {
            let throwable = jni_exception_occurred!()?.into();
            jni_exception_clear!()?;
            throwable
        } else {
            JObject::null()
        };
        throw_runtime_exception(panic_message, cause)?;
        Ok(())
    };
    recover().unwrap_or_else(|err: Box<dyn Error>| {
        jni_fatal_error!(format!(
            "Error recovering from panic, cannot resume: {:?}",
            err
        ));
    });
}
