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

use std::{
    error::Error,
    panic::AssertUnwindSafe,
    sync::{mpsc::Receiver, Arc},
};

use arrow::{
    array::{Array, StructArray},
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
    record_batch::RecordBatch,
};
use blaze_jni_bridge::{
    is_task_running, jni_bridge::JavaClasses, jni_call, jni_call_static, jni_exception_check,
    jni_exception_occurred, jni_new_global_ref, jni_new_object, jni_new_string,
};
use datafusion::{
    common::Result,
    error::DataFusionError,
    execution::context::TaskContext,
    physical_plan::{metrics::ExecutionPlanMetricsSet, ExecutionPlan},
};
use datafusion_ext_commons::df_execution_err;
use datafusion_ext_plans::{
    common::execution_context::ExecutionContext, parquet_sink_exec::ParquetSinkExec,
};
use futures::{FutureExt, StreamExt};
use jni::objects::{GlobalRef, JObject};
use tokio::runtime::Runtime;

use crate::{handle_unwinded_scope, metrics::update_spark_metric_node};

pub struct NativeExecutionRuntime {
    exec_ctx: Arc<ExecutionContext>,
    native_wrapper: GlobalRef,
    plan: Arc<dyn ExecutionPlan>,
    batch_receiver: Receiver<Result<Option<RecordBatch>>>,
    rt: Runtime,
}

impl NativeExecutionRuntime {
    pub fn start(
        native_wrapper: GlobalRef,
        plan: Arc<dyn ExecutionPlan>,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<Self> {
        let exec_ctx = ExecutionContext::new(
            context.clone(),
            partition,
            plan.schema(),
            &ExecutionPlanMetricsSet::new(),
        );

        // init ffi schema
        let ffi_schema = FFI_ArrowSchema::try_from(exec_ctx.output_schema().as_ref())?;
        jni_call!(BlazeCallNativeWrapper(native_wrapper.as_obj())
            .importSchema(&ffi_schema as *const FFI_ArrowSchema as i64) -> ()
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

        let (batch_sender, batch_receiver) = std::sync::mpsc::sync_channel(1);
        let nrt = Self {
            exec_ctx: exec_ctx.clone(),
            native_wrapper: native_wrapper.clone(),
            plan: plan.clone(),
            rt,
            batch_receiver,
        };

        // spawn batch producer
        let err_sender = batch_sender.clone();
        let consume_stream = async move {
            // execute and coalesce plan to output stream
            let stream = exec_ctx.execute(&plan)?;
            let mut stream = if plan.as_any().downcast_ref::<ParquetSinkExec>().is_some() {
                stream // cannot coalesce parquet sink output
            } else {
                exec_ctx.coalesce_with_default_batch_size(stream)
            };

            while let Some(batch) = AssertUnwindSafe(stream.next())
                .catch_unwind()
                .await
                .unwrap_or_else(|err| {
                    let panic_message =
                        panic_message::get_panic_message(&err).unwrap_or("unknown error");
                    Some(df_execution_err!("{}", panic_message))
                })
                .transpose()
                .or_else(|err| df_execution_err!("{err}"))?
            {
                batch_sender
                    .send(Ok(Some(batch)))
                    .or_else(|err| df_execution_err!("send batch error: {err}"))?;
            }
            batch_sender
                .send(Ok(None))
                .or_else(|err| df_execution_err!("send batch error: {err}"))?;
            log::info!("[partition={partition}] finished");
            Ok::<_, DataFusionError>(())
        };
        nrt.rt.spawn(async move {
            consume_stream.await.unwrap_or_else(|err| {
                handle_unwinded_scope(|| {
                    let task_running = is_task_running();
                    if !task_running {
                        log::warn!(
                            "[partition={partition}] task completed before native execution done"
                        );
                        return Ok(());
                    }

                    let cause = if jni_exception_check!()? {
                        let err_text = format!(
                            "[partition={partition}] native execution panics with exception: {err}"
                        );
                        err_sender.send(df_execution_err!("{err_text}"))?;
                        log::error!("{err_text}");
                        Some(jni_exception_occurred!()?)
                    } else {
                        let err_text =
                            format!("[partition={partition}] native execution panics: {err}");
                        err_sender.send(df_execution_err!("{err_text}"))?;
                        log::error!("{err_text}");
                        None
                    };

                    set_error(
                        &native_wrapper,
                        &format!("[partition={partition}] panics: {err}"),
                        cause.map(|e| e.as_obj()),
                    )?;
                    log::info!("[partition={partition}] exited abnormally.");
                    Ok::<_, Box<dyn Error>>(())
                })
            });
        });
        Ok(nrt)
    }

    pub fn next_batch(&self) -> bool {
        let next_batch = || -> Result<bool> {
            match self
                .batch_receiver
                .recv()
                .or_else(|err| df_execution_err!("receive batch error: {err}"))??
            {
                Some(batch) => {
                    let struct_array = StructArray::from(batch);
                    let ffi_array = FFI_ArrowArray::new(&struct_array.to_data());
                    jni_call!(BlazeCallNativeWrapper(self.native_wrapper.as_obj())
                        .importBatch(&ffi_array as *const FFI_ArrowArray as i64) -> ()
                    )?;
                    Ok(true)
                }
                None => Ok(false),
            }
        };

        let partition = self.exec_ctx.partition_id();
        match next_batch() {
            Ok(ret) => return ret,
            Err(err) => {
                let _ = set_error(
                    &self.native_wrapper,
                    &format!("[partition={partition}] poll record batch error: {err}"),
                    None,
                );
                return false;
            }
        }
    }

    pub fn finalize(self) {
        let partition = self.exec_ctx.partition_id();

        log::info!("[partition={partition}] native execution finalizing");
        self.update_metrics().unwrap_or_default();
        drop(self.plan);

        self.exec_ctx.cancel_task(); // cancel all pending streams
        self.rt.shutdown_background();
        log::info!("[partition={partition}] native execution finalized");
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
