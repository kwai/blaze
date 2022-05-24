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
            let env = JavaClasses::get_thread_jnienv();
            let dirs = jni_map_error!(env.get_string(tmp_dirs))
                .unwrap()
                .to_string_lossy()
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
    env: JNIEnv,
    _: JClass,
    wrapper: JObject,
) {
    if let Err(err) = std::panic::catch_unwind(|| {
        log::info!("Entering blaze callNative()");

        let wrapper = Arc::new(jni_global_ref!(env, wrapper).unwrap());
        let wrapper_for_error = wrapper.clone();

        macro_rules! is_finished {
            ($env:expr, $wrapper:expr) => {{
                JNI_TRUE == jni_bridge_call_method!(
                    $env,
                    BlazeCallNativeWrapper.isFinished -> jboolean,
                    $wrapper.as_obj()
                ).unwrap()
            }}
        }

        let obj_true = jni_global_ref!(
            env,
            jni_bridge_new_object!(env, JavaBoolean, JNI_TRUE).unwrap()
        )
        .unwrap();
        let obj_false = jni_global_ref!(
            env,
            jni_bridge_new_object!(env, JavaBoolean, JNI_FALSE).unwrap()
        )
        .unwrap();

        // decode plan
        let raw_task_definition: JObject = jni_bridge_call_method!(
            env,
            BlazeCallNativeWrapper.getRawTaskDefinition -> JObject,
            wrapper.as_obj()
        )
        .unwrap();

        let task_definition = TaskDefinition::decode(
            jni_map_error!(env.convert_byte_array(raw_task_definition.into_inner()))
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

        let task_context = jni_global_ref!(
            env,
            jni_bridge_call_static_method!(
                env,
                JniBridge.getTaskContext -> JObject,
            )
            .unwrap()
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
                {
                    let env = JavaClasses::get_thread_jnienv();
                    jni_bridge_call_static_method!(
                        env,
                        JniBridge.setTaskContext -> (),
                        task_context.as_obj(),
                    )
                    .unwrap();
                }

                // load batches
                while let Some(r) = stream.next().await {
                    let env = JavaClasses::get_thread_jnienv();
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
                            while !is_finished!(env, wrapper) {
                                input = jni_bridge_call_method!(
                                    env,
                                    BlazeCallNativeWrapper.dequeueWithTimeout -> JObject,
                                    wrapper.as_obj()
                                )
                                .unwrap();

                                if !input.is_null() {
                                    break;
                                }
                            }
                            if input.is_null() { // wrapper.isFinished = true
                                break;
                            }

                            let schema_ptr = jni_bridge_call_method!(env, ScalaTuple2._1 -> JObject, input).unwrap();
                            let schema_ptr = jni_bridge_call_method!(env, JavaLong.longValue -> jlong, schema_ptr).unwrap();
                            let array_ptr = jni_bridge_call_method!(env, ScalaTuple2._2 -> JObject, input).unwrap();
                            let array_ptr = jni_bridge_call_method!(env, JavaLong.longValue -> jlong, array_ptr).unwrap();

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
                            while !is_finished!(env, wrapper) {
                                let enqueued = jni_bridge_call_method!(
                                    env,
                                    BlazeCallNativeWrapper.enqueueWithTimeout -> jboolean,
                                    wrapper.as_obj(),
                                    obj_true.as_obj()
                                )
                                .unwrap();

                                if enqueued == JNI_TRUE {
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            panic!("stream.next() error: {:?}", e);
                        }
                    }
                }
                let env = JavaClasses::get_thread_jnienv();

                // value_queue -> (discard)
                while !is_finished!(env, wrapper) {
                    let input = jni_bridge_call_method!(
                        env,
                        BlazeCallNativeWrapper.dequeueWithTimeout -> JObject,
                        wrapper.as_obj()
                    )
                    .unwrap();

                    if !input.is_null() {
                        break;
                    }
                }

                // value_queue <- hasNext=false
                while !is_finished!(env, wrapper) {
                    let enqueued = jni_bridge_call_method!(
                        env,
                        BlazeCallNativeWrapper.enqueueWithTimeout -> jboolean,
                        wrapper.as_obj(),
                        obj_false.as_obj()
                    )
                    .unwrap();

                    if enqueued == JNI_TRUE {
                        break;
                    }
                }

                log::info!("Updating blaze exec metrics ...");
                let metrics = jni_bridge_call_method!(
                    env,
                    BlazeCallNativeWrapper.getMetrics -> JObject,
                    wrapper.as_obj()
                )
                .unwrap();
                update_spark_metric_node(
                    &env,
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
                let env = JavaClasses::get_thread_jnienv();
                let panic_message = panic_message::panic_message(&err);

                let e = if jni_map_error!(env.exception_check())? {
                    log::error!("native execution panics with an java exception");
                    log::error!("panic message: {}", panic_message);
                    jni_map_error!(env.exception_occurred())?.into()
                } else {
                    log::error!("native execution panics");
                    log::error!("panic message: {}", panic_message);
                    jni_bridge_new_object!(
                        env,
                        JavaRuntimeException,
                        jni_map_error!(env.new_string("blaze native panics"))?,
                        JObject::null()
                    )?
                };

                // error_queue <- exception
                while !is_finished!(env, wrapper_for_error) {
                    let enqueued = jni_bridge_call_method!(
                        env,
                        BlazeCallNativeWrapper.enqueueError -> jboolean,
                        wrapper_for_error.as_obj(),
                        e
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

fn is_jvm_interrupted(env: &JNIEnv) -> datafusion::error::Result<bool> {
    let interrupted_exception_class = "java.lang.InterruptedException";
    if env.exception_check().unwrap_or(false) {
        let e: JObject = env
            .exception_occurred()
            .unwrap_or_else(|_| JThrowable::from(JObject::null()))
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
    if let Err(err) = env.throw(JThrowable::from(e)) {
        env.fatal_error(format!(
            "Error throwing RuntimeException, cannot result: {:?}",
            err
        ));
    }
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
            log::info!("native execution interrupted by JVM");
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
