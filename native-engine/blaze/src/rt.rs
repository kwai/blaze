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

use std::{panic::AssertUnwindSafe, sync::Arc};

use arrow::{
    array::{Array, StructArray},
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
};
use blaze_jni_bridge::{
    is_task_running, jni_bridge::JavaClasses, jni_call, jni_call_static, jni_exception_check,
    jni_exception_occurred, jni_new_global_ref, jni_new_object, jni_new_string,
};
use datafusion::{
    common::Result,
    error::DataFusionError,
    execution::context::TaskContext,
    physical_plan::{
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet},
        ExecutionPlan,
    },
};
use datafusion_ext_commons::streams::coalesce_stream::CoalesceInput;
use datafusion_ext_plans::common::output::WrappedRecordBatchSender;
use futures::{FutureExt, StreamExt};
use jni::objects::{GlobalRef, JObject};
use tokio::runtime::Runtime;

use crate::{handle_unwinded_scope, metrics::update_spark_metric_node};

pub struct NativeExecutionRuntime {
    native_wrapper: GlobalRef,
    plan: Arc<dyn ExecutionPlan>,
    task_context: Arc<TaskContext>,
    partition: usize,
    ffi_schema: Arc<FFI_ArrowSchema>,
    rt: Runtime,
}

impl NativeExecutionRuntime {
    pub fn start(
        native_wrapper: GlobalRef,
        plan: Arc<dyn ExecutionPlan>,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<Self> {
        // execute plan to output stream
        let stream = plan.execute(partition, context.clone())?;
        let schema = stream.schema();

        // coalesce
        let mut stream = context.coalesce_with_default_batch_size(
            stream,
            &BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), partition),
        )?;

        // init ffi schema
        let ffi_schema = Arc::new(FFI_ArrowSchema::try_from(schema.as_ref())?);
        jni_call!(BlazeCallNativeWrapper(native_wrapper.as_obj())
            .importSchema(ffi_schema.as_ref() as *const FFI_ArrowSchema as i64) -> ()
        )?;

        // create tokio runtime
        // propagate classloader and task context to spawned children threads
        let spark_task_context = jni_call_static!(JniBridge.getTaskContext() -> JObject)?;
        let spark_task_context_global = jni_new_global_ref!(spark_task_context.as_obj())?;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .on_thread_start(move || {
                let classloader = JavaClasses::get().classloader;
                let _ = jni_call_static!(
                    JniBridge.setContextClassLoader(classloader) -> ()
                );
                let _ = jni_call_static!(
                    JniBridge.setTaskContext(spark_task_context_global.as_obj()) -> ()
                );
            })
            .build()?;

        let nrt = Self {
            native_wrapper: native_wrapper.clone(),
            plan,
            partition,
            rt,
            ffi_schema,
            task_context: context,
        };

        // spawn batch producer
        let native_wrapper_cloned = native_wrapper.clone();
        let consume_stream = move || async move {
            let native_wrapper = native_wrapper_cloned;
            while let Some(batch) = AssertUnwindSafe(stream.next())
                .catch_unwind()
                .await
                .unwrap_or_else(|err| {
                    let panic_message =
                        panic_message::get_panic_message(&err).unwrap_or("unknown error");
                    Some(Err(DataFusionError::Execution(panic_message.to_owned())))
                })
                .transpose()
                .map_err(|err| DataFusionError::Execution(format!("{}", err)))?
            {
                let ffi_array = FFI_ArrowArray::new(&StructArray::from(batch).into_data());
                jni_call!(BlazeCallNativeWrapper(native_wrapper.as_obj())
                    .importBatch(&ffi_array as *const FFI_ArrowArray as i64, ) -> ()
                )?;
            }
            jni_call!(BlazeCallNativeWrapper(native_wrapper.as_obj()).importBatch(0, 0) -> ())?;

            log::info!("[partition={partition}] finished");
            Ok::<_, DataFusionError>(())
        };
        nrt.rt.spawn(async move {
            let result = consume_stream().await;
            result.unwrap_or_else(|err| handle_unwinded_scope(|| -> Result<()> {
                let task_running = is_task_running();
                if !task_running {
                    log::warn!(
                        "[partition={partition}] task completed/interrupted before native execution done",
                    );
                    return Ok(());
                }

                let cause =
                    if jni_exception_check!()? {
                        log::error!("[partition={partition}] panics with an java exception: {err}");
                        Some(jni_exception_occurred!()?)
                    } else {
                        log::error!("[partition={partition}] panics: {err}");
                        None
                    };

                set_error(
                    &native_wrapper,
                    &format!("[partition={partition}] panics: {err}"),
                    cause.map(|e| e.as_obj()),
                )?;
                log::info!("[partition={partition}] exited abnormally.");
                Ok::<_, DataFusionError>(())
            }));
        });
        Ok(nrt)
    }

    pub fn finalize(self) {
        log::info!("native execution [partition={}] finalizing", self.partition);
        let _ = self.update_metrics();
        drop(self.ffi_schema);
        drop(self.plan);
        WrappedRecordBatchSender::cancel_task(&self.task_context); // cancel all pending streams
        self.rt.shutdown_background();
        log::info!("native execution [partition={}] finalized", self.partition);
    }

    fn update_metrics(&self) -> Result<()> {
        let metrics = jni_call!(
            BlazeCallNativeWrapper(self.native_wrapper.as_obj()).getMetrics() -> JObject
        )?;
        update_spark_metric_node(metrics.as_obj(), self.plan.clone())?;
        Ok(())
    }
}

fn set_error(native_wrapper: &GlobalRef, message: &str, cause: Option<JObject>) -> Result<()> {
    let message = jni_new_string!(message.to_owned())?;
    let e = jni_new_object!(JavaRuntimeException(
        message.as_obj(),
        cause.unwrap_or(JObject::null()),
    ))?;
    jni_call!(BlazeCallNativeWrapper(native_wrapper.as_obj())
        .setError(e.as_obj()) -> ())?;
    Ok(())
}
