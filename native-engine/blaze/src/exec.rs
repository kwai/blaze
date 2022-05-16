use std::alloc::Layout;
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
use futures::{FutureExt, StreamExt};
use jni::objects::{JClass, JString};
use jni::objects::{JObject, JThrowable};
use jni::sys::{jbyteArray, jlong, JNI_FALSE, JNI_TRUE};
use jni::JNIEnv;
use log::LevelFilter;
use once_cell::sync::OnceCell;
use prost::Message;
use simplelog::{ConfigBuilder, SimpleLogger, ThreadLogMode};
use tokio::runtime::Runtime;

use datafusion_ext::jni_bridge::JavaClasses;
use datafusion_ext::*;
use plan_serde::protobuf::TaskDefinition;

use crate::BlazeIter;

static SIMPLELOG: OnceCell<()> = OnceCell::new();
static SESSION_CONTEXT: OnceCell<SessionContext> = OnceCell::new();

#[allow(non_snake_case)]
#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_blaze_JniBridge_initNative(
    env: JNIEnv,
    _: JClass,
    batch_size: i64,
    max_native_memory: i64,
    memory_fraction: f64,
    tmp_dirs: JString,
) {
    std::panic::catch_unwind(|| {
        assert!(batch_size > 0);
        assert!(SIMPLELOG.get().is_none(), "running initNative() more than once");

        // init logging
        SIMPLELOG.get_or_init(|| {
            SimpleLogger::init(
                LevelFilter::Info,
                ConfigBuilder::new()
                    .set_time_format_rfc3339()
                    .set_thread_mode(ThreadLogMode::Both)
                    .build()
            )
                .unwrap();
        });

        // init JavaClasses
        JavaClasses::init(&env);

        // init session config
        let tmp_dirs: String = env.get_string(tmp_dirs).unwrap().into();
        SESSION_CONTEXT.get_or_init(|| {
            let dirs = tmp_dirs.split(',').map(PathBuf::from).collect::<Vec<_>>();
            let runtime_config = RuntimeConfig::new()
                .with_memory_manager(MemoryManagerConfig::New {
                    max_memory: max_native_memory as usize,
                    memory_fraction,
                })
                .with_disk_manager(DiskManagerConfig::NewSpecified(dirs));
            let runtime = Arc::new(RuntimeEnv::new(runtime_config).unwrap());
            let config = SessionConfig::new().with_batch_size(batch_size as usize);
            SessionContext::with_config_rt(config, runtime)
        });
    }).unwrap_or_else(|err| {

        std::io::Result::<()>::Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            panic_message::panic_message(&err),
        ))
        .unwrap_or_fatal();
    })
}

#[allow(non_snake_case)]
#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_blaze_JniBridge_callNative(
    env: JNIEnv,
    _: JClass,
    task_definition: jbyteArray,
) -> i64 {
    match std::panic::catch_unwind(|| {
        log::info!("Entering blaze callNative()");

        let task_definition = TaskDefinition::decode(
            env.convert_byte_array(task_definition).unwrap().as_slice()
        )
        .unwrap();
        let task_id = &task_definition.task_id.expect("task_id is empty");
        let plan = &task_definition.plan.expect("plan is empty");

        let execution_plan: Arc<dyn ExecutionPlan> = plan.try_into().unwrap();
        let execution_plan_displayable =
            displayable(execution_plan.as_ref()).indent().to_string();
        log::info!("Creating native execution plan succeeded");
        log::info!("  task_id={:?}", task_id);
        log::info!("  execution plan:\n{}", execution_plan_displayable);

        // execute
        let task_ctx = SESSION_CONTEXT.get().unwrap().task_ctx();
        let stream = execution_plan
            .execute(task_id.partition_id as usize, task_ctx)
            .unwrap();

        // create tokio runtime used for loadNext()
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async move {
                let runtime = Arc::new(
                    tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(1)
                        .thread_keep_alive(Duration::MAX) // always use same thread
                        .build()
                        .unwrap(),
                );

                // propagate task context to spawned children threads
                let env = JavaClasses::get_thread_jnienv();
                let task_context_ptr = unsafe {
                    std::mem::transmute::<_, isize>(
                        jni_bridge_call_static_method!(
                            env,
                            JniBridge.getTaskContext -> JObject
                        )
                        .unwrap(),
                    )
                };

                runtime.spawn(async move {
                    AssertUnwindSafe(async move {
                        let env = JavaClasses::get_thread_jnienv();
                        let task_context = unsafe {
                            std::mem::transmute::<_, JObject>(task_context_ptr)
                        };
                        jni_bridge_call_static_method!(
                            env,
                            JniBridge.setTaskContext -> (),
                            task_context,
                        )
                        .unwrap();
                    })
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|err| {
                        let panic_message = panic_message::panic_message(&err);
                        throw_runtime_exception(panic_message, JObject::null())
                            .unwrap_or_fatal();
                    });
                });

                runtime
            });

        // safety - manually allocated memory will be released when stream is exhausted
        unsafe {
            let blaze_iter_ptr: *mut BlazeIter =
                std::alloc::alloc(Layout::new::<BlazeIter>()) as *mut BlazeIter;

            std::ptr::write(
                blaze_iter_ptr,
                BlazeIter {
                    stream,
                    execution_plan,
                    runtime,
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
pub unsafe extern "system" fn Java_org_apache_spark_sql_blaze_JniBridge_loadBatches(
    _: JNIEnv,
    _: JClass,
    iter_ptr: i64,
    input_exchanger: JObject,
    output_exchanger: JObject,
) {
    if let Err(err) = std::panic::catch_unwind(|| {
        let env = JavaClasses::get_thread_jnienv();
        let input_exchanger_ptr = std::mem::transmute::<_, i64>(
            jni_weak_global_ref!(env, input_exchanger).unwrap(),
        );
        let output_exchanger_ptr = std::mem::transmute::<_, i64>(
            jni_weak_global_ref!(env, output_exchanger).unwrap(),
        );
        let blaze_iter = &mut *(iter_ptr as *mut BlazeIter);

        // spawn a thread to poll next batch
        blaze_iter.runtime.clone().spawn(async move {
            AssertUnwindSafe(async move {
                while let Some(r) = blaze_iter.stream.next().await {
                    match r {
                        Ok(batch) => {
                            let input_exchanger = std::mem::transmute::<_, JObject<'_>>(input_exchanger_ptr);
                            let output_exchanger = std::mem::transmute::<_, JObject<'_>>(output_exchanger_ptr);
                            let env = JavaClasses::get_thread_jnienv();

                            let num_rows = batch.num_rows();
                            if num_rows == 0 {
                                continue;
                            }

                            // input_exchanger -> (schema_ptr, array_ptr)
                            let input = jni_bridge_call_method!(
                                env,
                                JavaExchanger.exchange -> JObject,
                                input_exchanger,
                                JObject::null()
                            ).unwrap();

                            let schema_ptr = jni_bridge_call_method!(env, ScalaTuple2._1 -> JObject, input).unwrap();
                            let schema_ptr = jni_bridge_call_method!(env, JavaLong.longValue -> jlong, schema_ptr).unwrap();
                            let array_ptr = jni_bridge_call_method!(env, ScalaTuple2._2 -> JObject, input).unwrap();
                            let array_ptr = jni_bridge_call_method!(env, JavaLong.longValue -> jlong, array_ptr).unwrap();

                            let out_schema = schema_ptr as *mut FFI_ArrowSchema;
                            let out_array = array_ptr as *mut FFI_ArrowArray;
                            let batch: Arc<StructArray> = Arc::new(batch.into());
                            export_array_into_raw(
                                batch,
                                out_array,
                                out_schema,
                            )
                            .expect("export_array_into_raw error");

                            // output_exchanger <- hasNext=true
                            let r = jni_bridge_new_object!(env, JavaBoolean, JNI_TRUE).unwrap();
                            jni_bridge_call_method!(
                                env,
                                JavaExchanger.exchange -> JObject,
                                output_exchanger,
                                r
                            )
                            .unwrap();
                        }
                        Err(e) => {
                            panic!("stream.next() error: {:?}", e);
                        }
                    }
                }

                let input_exchanger = std::mem::transmute::<_, JObject<'_>>(input_exchanger_ptr);
                let output_exchanger = std::mem::transmute::<_, JObject<'_>>(output_exchanger_ptr);
                let env = JavaClasses::get_thread_jnienv();

                // input_exchanger -> (not used)
                let _input = jni_bridge_call_method!(
                    env,
                    JavaExchanger.exchange -> JObject,
                    input_exchanger,
                    JObject::null()
                ).unwrap();

                // output_exchanger <- num_rows=-1
                let r = jni_bridge_new_object!(env, JavaBoolean, JNI_FALSE).unwrap();
                jni_bridge_call_method!(
                    env,
                    JavaExchanger.exchange -> JObject,
                    output_exchanger,
                    r
                )
                .unwrap();
            })
            .catch_unwind()
            .await
            .map_err(|err| {
                let output_exchanger = std::mem::transmute::<_, JObject<'_>>(output_exchanger_ptr);
                let env = JavaClasses::get_thread_jnienv();
                let panic_message = panic_message::panic_message(&err);

                // output_exchanger <- RuntimeException
                jni_bridge_call_method!(
                    env,
                    JavaExchanger.exchange -> JObject,
                    output_exchanger,
                    jni_bridge_new_object!(
                        env,
                        JavaRuntimeException,
                        jni_map_error!(env.new_string(&panic_message))?,
                        JObject::null()
                    )?
                )?;
                datafusion::error::Result::Ok(())
            })
            .unwrap();
        });
    }) {
        handle_unwinded(err)
    }
}

#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_spark_sql_blaze_JniBridge_deallocIter(
    _: JNIEnv,
    _: JClass,
    iter_ptr: i64,
) {
    // shutdown any background threads
    // safety: safe to copy because Runtime::drop() does not do anything under ThreadPool mode
    let runtime: Runtime =
        std::mem::transmute_copy((*(iter_ptr as *mut BlazeIter)).runtime.as_ref());
    runtime.shutdown_background();

    // dealloc memory
    std::alloc::dealloc(iter_ptr as *mut u8, Layout::new::<BlazeIter>());
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
