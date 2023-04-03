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
use std::sync::Arc;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::metrics::Time;
use datafusion::physical_plan::{ExecutionPlan, RecordBatchStream};
use futures::{FutureExt, StreamExt};
use jni::objects::{GlobalRef, JObject};
use tokio::runtime::Runtime;
use blaze_commons::{jni_call, jni_call_static, jni_exception_check, jni_exception_occurred, jni_new_global_ref, jni_new_object, jni_new_string};
use blaze_commons::is_task_running;
use blaze_commons::jni_bridge::JavaClasses;
use datafusion_ext_commons::ffi::MpscBatchReader;
use datafusion_ext_commons::streams::coalesce_stream::CoalesceStream;
use crate::handle_unwinded_scope;
use crate::metrics::update_spark_metric_node;

pub struct NativeExecutionRuntime {
    native_wrapper: GlobalRef,
    plan: Arc<dyn ExecutionPlan>,
    rt: Runtime,
    ffi_stream: Box<FFI_ArrowArrayStream>,
}

impl NativeExecutionRuntime {
    pub fn start(
        native_wrapper: GlobalRef,
        plan: Arc<dyn ExecutionPlan>,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<Self> {

        let batch_size = context.session_config().batch_size();

        // execute plan to output stream
        let stream = plan.execute(partition, context)?;

        // coalesce
        let coalesce_compute_time = Time::new();
        let mut stream = CoalesceStream::new(
            stream,
            batch_size,
            coalesce_compute_time,
        );

        // create mpsc channel for collecting batches
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);

        // create RecordBatchReader
        let batch_reader = Box::new(MpscBatchReader {
            schema: stream.schema(),
            receiver,
        });

        // create and export FFI_ArrowArrayStream
        let ffi_stream = Box::new(FFI_ArrowArrayStream::new(batch_reader));
        let ffi_stream_ptr = &*ffi_stream as *const FFI_ArrowArrayStream;
        jni_call!(BlazeCallNativeWrapper(native_wrapper.as_obj())
            .setArrowFFIStreamPtr(ffi_stream_ptr as i64) -> ())?;

        // create tokio runtime
        // propagate classloader and task context to spawned children threads
        let task_context = jni_call_static!(JniBridge.getTaskContext() -> JObject)?;
        let task_context_global = jni_new_global_ref!(task_context.as_obj())?;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .on_thread_start(move || {
                let classloader = JavaClasses::get().classloader;
                let _ = jni_call_static!(
                    JniBridge.setContextClassLoader(classloader) -> ()
                );
                let _ = jni_call_static!(
                    JniBridge.setTaskContext(task_context_global.as_obj()) -> ()
                );
            })
            .build()?;

        // spawn batch producer
        let native_wrapper_cloned = native_wrapper.clone();
        rt.spawn(async move {
            let native_wrapper = native_wrapper_cloned;
            let native_wrapper_cloned = native_wrapper.clone();

            AssertUnwindSafe(async move {
                while let Some(batch_result) = stream.next().await {
                    match batch_result {
                        Ok(batch) => {
                            if let Err(err) = sender.send(Some(Ok(batch))) {
                                let task_running = is_task_running();
                                assert!(!task_running, "error sending batch: {:?}", err);
                                break;
                            }
                        }
                        Err(err) => {
                            let task_running = is_task_running();
                            assert!(!task_running, "error executing plan: {:?}", err);
                            break;
                        }
                    }
                }
                if let Err(err) = sender.send(None) {
                    let task_running = is_task_running();
                    assert!(!task_running, "error sending batch: {:?}", err);
                }
                log::info!("native execution finished");
            })
            .catch_unwind()
            .await
            .unwrap_or_else(|err| handle_unwinded_scope(|| -> Result<()> {
                if !is_task_running() {
                    return Ok(());
                }
                let native_wrapper = native_wrapper_cloned;
                let panic_message = panic_message::panic_message(&err);
                let e = if jni_exception_check!()? {
                    log::error!("native execution panics with an java exception");
                    log::error!("panic message: {}", panic_message);
                    jni_exception_occurred!()?
                } else {
                    log::error!("native execution panics");
                    log::error!("panic message: {}", panic_message);
                    let message = jni_new_string!(
                        format!("native executing panics: {}", panic_message)
                    )?;
                    jni_new_object!(JavaRuntimeException(
                        message.as_obj(),
                        JObject::null(),
                    ))?
                };
                jni_call!(BlazeCallNativeWrapper(native_wrapper.as_obj())
                    .setError(e.as_obj()) -> ())?;

                log::info!("Blaze native executing exited with error.");
                Ok::<_, DataFusionError>(())
            }));

            Ok::<_, DataFusionError>(())
        });

        Ok(Self {
            native_wrapper,
            plan,
            rt,
            ffi_stream,
        })
    }

    pub fn finalize(self) {
        let _ = self.update_metrics();
        let _ = self.ffi_stream;
        self.rt.shutdown_background();
    }

    fn update_metrics(&self) -> Result<()> {
        let metrics = jni_call!(
            BlazeCallNativeWrapper(self.native_wrapper.as_obj()).getMetrics() -> JObject
        )?;
        update_spark_metric_node(metrics.as_obj(), self.plan.clone())?;
        Ok(())
    }
}
