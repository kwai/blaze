// Copyright 2022 The Blaze Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{export_array_into_raw, StructArray};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use blaze_commons::jni_bridge::JavaClasses;
use blaze_commons::*;
use blaze_serde::protobuf::TaskDefinition;
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::memory_manager::MemoryManagerConfig;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_plan::{displayable, ExecutionPlan};
use datafusion::physical_plan::metrics::Time;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::{FutureExt, StreamExt};
use jni::objects::JObject;
use jni::objects::{GlobalRef, JClass, JString};
use jni::sys::{jboolean, jlong, JNI_FALSE, JNI_TRUE};
use jni::JNIEnv;
use log::LevelFilter;
use once_cell::sync::OnceCell;
use prost::Message;
use simplelog::{ColorChoice, ConfigBuilder, TermLogger, TerminalMode, ThreadLogMode};
use tokio::runtime::Runtime;
use datafusion_ext_commons::streams::coalesce_stream::CoalesceStream;

use crate::metrics::update_spark_metric_node;
use crate::{handle_unwinded_scope, is_task_running, SESSION};

fn init_logging() {
    static LOGGING_INIT: OnceCell<()> = OnceCell::new();
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
}

fn java_true() -> &'static GlobalRef {
    static OBJ_TRUE: OnceCell<GlobalRef> = OnceCell::new();
    OBJ_TRUE.get_or_init(|| {
        jni_new_global_ref!(jni_new_object!(JavaBoolean, JNI_TRUE).unwrap()).unwrap()
    })
}

fn java_false() -> &'static GlobalRef {
    static OBJ_FALSE: OnceCell<GlobalRef> = OnceCell::new();
    OBJ_FALSE.get_or_init(|| {
        jni_new_global_ref!(jni_new_object!(JavaBoolean, JNI_FALSE).unwrap()).unwrap()
    })
}

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
    handle_unwinded_scope(|| {
        // init logging
        init_logging();

        // init jni java classes
        JavaClasses::init(&env);

        // init datafusion session context
        SESSION.get_or_init(|| {
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
        datafusion::error::Result::Ok(())
    });
}

#[allow(non_snake_case)]
#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_blaze_JniBridge_callNative(
    _: JNIEnv,
    _: JClass,
    wrapper: JObject,
) {
    handle_unwinded_scope(|| {
        log::info!("Entering blaze callNative()");

        let wrapper = Arc::new(jni_new_global_ref!(wrapper).unwrap());
        let wrapper_clone = wrapper.clone();

        // decode plan
        let raw_task_definition: JObject = jni_call!(
            BlazeCallNativeWrapper(wrapper.as_obj()).getRawTaskDefinition() -> JObject
        )
        .unwrap();

        let task_definition = TaskDefinition::decode(
            jni_convert_byte_array!(*raw_task_definition)
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
        let session_ctx = SESSION.get().unwrap();
        let task_ctx = session_ctx.task_ctx();
        let batch_size = task_ctx.session_config().batch_size();

        let stream = execution_plan
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
                    .on_thread_start(move || {
                        // propagate classloader and task context to spawned
                        // children threads
                        jni_call_static!(
                            JniBridge.setContextClassLoader(JavaClasses::get().classloader) -> ()
                        ).unwrap();
                        jni_call_static!(
                            JniBridge.setTaskContext(task_context.as_obj()) -> ()
                        ).unwrap();
                    })
                    .build()
                    .unwrap(),
            ),
        });
        let runtime_clone = runtime.clone();

        runtime.clone().runtime.as_ref().unwrap().spawn(async move {
            AssertUnwindSafe(async move {
                let output_batch = |batch: RecordBatch| -> datafusion::common::Result<bool> {
                    // value_queue -> (schema_ptr, array_ptr)
                    let mut input = JObject::null();
                    while jni_call!(BlazeCallNativeWrapper(wrapper.as_obj()).isFinished() -> jboolean)? != JNI_TRUE {
                        input = jni_call!(BlazeCallNativeWrapper(wrapper.as_obj()).dequeueWithTimeout() -> JObject)?;

                        if !input.is_null() {
                            break;
                        }
                    }
                    if input.is_null() { // wrapper.isFinished = true
                        return Ok(false);
                    }

                    let schema_ptr = jni_call!(ScalaTuple2(input)._1() -> JObject)?;
                    let schema_ptr = jni_call!(JavaLong(schema_ptr).longValue() -> jlong)?;
                    let array_ptr = jni_call!(ScalaTuple2(input)._2() -> JObject)?;
                    let array_ptr = jni_call!(JavaLong(array_ptr).longValue() -> jlong)?;

                    let out_schema = schema_ptr as *mut FFI_ArrowSchema;
                    let out_array = array_ptr as *mut FFI_ArrowArray;
                    let struct_array: Arc<StructArray> = Arc::new(batch.into());

                    unsafe {
                        export_array_into_raw(
                            struct_array,
                            out_array,
                            out_schema,
                        )
                        .expect("export_array_into_raw error");
                    }

                    // value_queue <- hasNext=true
                    while {
                        jni_call!(BlazeCallNativeWrapper(wrapper.as_obj()).isFinished() -> jboolean)? != JNI_TRUE &&
                        jni_call!(BlazeCallNativeWrapper(wrapper.as_obj()).enqueueWithTimeout(java_true().as_obj()) -> jboolean)? != JNI_TRUE
                    } {
                        // wait until finished or enqueueWithTimeout succeeded
                    }
                    Ok(true)
                };

                // load batches
                let mut total_rows = 0;
                let mut coalesced = Box::pin(
                    CoalesceStream::new(stream, batch_size, Time::new())
                );
                while let Some(r) = coalesced.next().await {
                    match r {
                        Ok(batch) => {
                            total_rows = batch.num_rows();
                            output_batch(batch).unwrap();
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
                    jni_call!(BlazeCallNativeWrapper(wrapper.as_obj()).enqueueWithTimeout(java_false().as_obj()) -> jboolean).unwrap() != JNI_TRUE
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
                log::info!("  total loaded rows: {}", total_rows);
                std::mem::drop(runtime);

                jni_call!(
                    BlazeCallNativeWrapper(wrapper.as_obj()).finishNativeThread() -> ()
                ).unwrap();
            })
            .catch_unwind()
            .await
            .unwrap_or_else(|err| handle_unwinded_scope(|| {
                let _ = jni_call!(
                    BlazeCallNativeWrapper(wrapper_clone.as_obj()).finishNativeThread() -> ()
                );

                if !is_task_running()? { // only handle running task
                    return Ok(());
                }
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
                        jni_new_string!(format!("native executing panics: {}", panic_message))?,
                        JObject::null()
                    )?
                };

                // error_queue <- exception
                while jni_call!(
                    BlazeCallNativeWrapper(wrapper_clone.as_obj()).isFinished() -> jboolean
                ).unwrap() != JNI_TRUE {
                    log::error!("native is exiting with an exception...");
                    let enqueued = jni_call!(
                        BlazeCallNativeWrapper(wrapper_clone.as_obj()).enqueueError(e) -> jboolean
                    )?;
                    if enqueued == JNI_TRUE {
                        break;
                    }
                }
                log::info!("Blaze native executing exited with error.");
                drop(runtime_clone);
                datafusion::error::Result::Ok(())
            }));
        });

        log::info!("Blaze native thread created");
        datafusion::error::Result::Ok(())
    });
}
