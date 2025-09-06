// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    error::Error,
    panic::AssertUnwindSafe,
    sync::{Arc, mpsc::Receiver},
};

use arrow::{
    array::{Array, StructArray},
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
    record_batch::RecordBatch,
};
use auron_jni_bridge::{
    conf::{IntConf, SPARK_TASK_CPUS, TOKIO_WORKER_THREADS_PER_CPU},
    is_task_running,
    jni_bridge::JavaClasses,
    jni_call, jni_call_static, jni_convert_byte_array, jni_exception_check, jni_exception_occurred,
    jni_new_global_ref, jni_new_object, jni_new_string,
};
use auron_serde::protobuf::TaskDefinition;
use datafusion::{
    common::Result,
    error::DataFusionError,
    execution::context::TaskContext,
    physical_plan::{
        ExecutionPlan, displayable, empty::EmptyExec, metrics::ExecutionPlanMetricsSet,
    },
};
use datafusion_ext_commons::{df_execution_err, downcast_any};
use datafusion_ext_plans::{
    common::execution_context::{ExecutionContext, cancel_all_tasks},
    ipc_writer_exec::IpcWriterExec,
    parquet_sink_exec::ParquetSinkExec,
    shuffle_writer_exec::ShuffleWriterExec,
};
use futures::{FutureExt, StreamExt};
use jni::objects::{GlobalRef, JObject};
use prost::Message;
use tokio::{runtime::Runtime, task::JoinHandle};

use crate::{
    handle_unwinded_scope,
    logging::{THREAD_PARTITION_ID, THREAD_STAGE_ID, THREAD_TID},
    metrics::update_spark_metric_node,
};

pub struct NativeExecutionRuntime {
    exec_ctx: Arc<ExecutionContext>,
    native_wrapper: GlobalRef,
    plan: Arc<dyn ExecutionPlan>,
    batch_receiver: Receiver<Result<Option<RecordBatch>>>,
    tokio_runtime: Runtime,
    join_handle: JoinHandle<()>,
}

impl NativeExecutionRuntime {
    pub fn start(native_wrapper: GlobalRef, context: Arc<TaskContext>) -> Result<Self> {
        // decode plan
        let native_wrapper_cloned = native_wrapper.clone();
        let raw_task_definition = jni_call!(
            AuronCallNativeWrapper(native_wrapper_cloned.as_obj()).getRawTaskDefinition() -> JObject
        )?;
        let raw_task_definition = jni_convert_byte_array!(raw_task_definition.as_obj())?;
        let task_definition = TaskDefinition::decode(raw_task_definition.as_slice())
            .or_else(|err| df_execution_err!("cannot decode execution plan: {err:?}"))?;

        let task_id = &task_definition.task_id.expect("task_id is empty");
        let stage_id = task_id.stage_id as usize;
        let partition_id = task_id.partition_id as usize;
        let tid = task_id.task_id as usize;
        let plan = &task_definition.plan.expect("plan is empty");
        drop(raw_task_definition);

        // get execution plan
        let execution_plan: Arc<dyn ExecutionPlan> = plan
            .try_into()
            .or_else(|err| df_execution_err!("cannot create execution plan: {err:?}"))?;

        let exec_ctx = ExecutionContext::new(
            context.clone(),
            partition_id,
            execution_plan.schema(),
            &ExecutionPlanMetricsSet::new(),
        );

        let num_worker_threads = {
            let worker_threads_per_cpu = TOKIO_WORKER_THREADS_PER_CPU.value().unwrap_or(0);
            let spark_task_cpus = SPARK_TASK_CPUS.value().unwrap_or(0);
            worker_threads_per_cpu * spark_task_cpus
        };

        // create tokio runtime
        // propagate classloader and task context to spawned children threads
        let spark_task_context = jni_call_static!(JniBridge.getTaskContext() -> JObject)?;
        let spark_task_context_global = jni_new_global_ref!(spark_task_context.as_obj())?;
        let mut tokio_runtime_builder = tokio::runtime::Builder::new_multi_thread();
        tokio_runtime_builder
            .thread_name(format!(
                "auron-native-stage-{stage_id}-part-{partition_id}-tid-{tid}"
            ))
            .on_thread_start(move || {
                let classloader = JavaClasses::get().classloader;
                let _ = jni_call_static!(
                    JniBridge.initNativeThread(classloader,spark_task_context_global.as_obj()) -> ()
                );
                THREAD_STAGE_ID.set(stage_id);
                THREAD_PARTITION_ID.set(partition_id);
                THREAD_TID.set(tid);
            });
        if num_worker_threads > 0 {
            tokio_runtime_builder.worker_threads(num_worker_threads as usize);
        }
        let tokio_runtime = tokio_runtime_builder.build()?;

        // spawn batch producer
        let (batch_sender, batch_receiver) = std::sync::mpsc::sync_channel(1);
        let err_sender = batch_sender.clone();
        let execution_plan_cloned = execution_plan.clone();
        let exec_ctx_cloned = exec_ctx.clone();
        let native_wrapper_cloned = native_wrapper.clone();
        let consume_stream = async move {
            // execute plan to output stream
            let displayable = displayable(execution_plan_cloned.as_ref())
                .set_show_schema(true)
                .indent(true)
                .to_string();
            log::info!("start executing plan:\n{displayable}");
            let mut stream = exec_ctx_cloned.execute(&execution_plan_cloned)?;

            // coalesce output stream if necessary
            if downcast_any!(execution_plan_cloned, EmptyExec).is_err()
                && downcast_any!(execution_plan_cloned, ParquetSinkExec).is_err()
                && downcast_any!(execution_plan_cloned, IpcWriterExec).is_err()
                && downcast_any!(execution_plan_cloned, ShuffleWriterExec).is_err()
            {
                stream = exec_ctx_cloned.coalesce_with_default_batch_size(stream);
            }

            // init ffi schema
            let ffi_schema = FFI_ArrowSchema::try_from(stream.schema().as_ref())?;
            jni_call!(AuronCallNativeWrapper(native_wrapper_cloned.as_obj())
                .importSchema(&ffi_schema as *const FFI_ArrowSchema as i64) -> ()
            )?;

            // produce batches
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
            log::info!("task finished");
            Ok::<_, DataFusionError>(())
        };

        let native_wrapper_cloned = native_wrapper.clone();
        let join_handle = tokio_runtime.spawn(async move {
            consume_stream.await.unwrap_or_else(|err| {
                handle_unwinded_scope(|| {
                    let task_running = is_task_running();
                    if !task_running {
                        log::warn!("task completed before native execution done");
                        return Ok(());
                    }

                    let cause = if jni_exception_check!()? {
                        let err_text = format!("native execution panics with exception: {err}");
                        err_sender.send(df_execution_err!("{err_text}"))?;
                        log::error!("{err_text}");
                        Some(jni_exception_occurred!()?)
                    } else {
                        let err_text = format!("native execution panics: {err}");
                        err_sender.send(df_execution_err!("{err_text}"))?;
                        log::error!("{err_text}");
                        None
                    };

                    set_error(
                        &native_wrapper_cloned,
                        &format!("task panics: {err}"),
                        cause.map(|e| e.as_obj()),
                    )?;
                    log::info!("task exited abnormally.");
                    Ok::<_, Box<dyn Error>>(())
                })
            });
        });

        let native_execution_runtime = Self {
            exec_ctx: exec_ctx.clone(),
            native_wrapper: native_wrapper.clone(),
            plan: execution_plan.clone(),
            tokio_runtime,
            batch_receiver,
            join_handle,
        };
        Ok(native_execution_runtime)
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
                    jni_call!(AuronCallNativeWrapper(self.native_wrapper.as_obj())
                        .importBatch(&ffi_array as *const FFI_ArrowArray as i64) -> ()
                    )?;
                    Ok(true)
                }
                None => Ok(false),
            }
        };

        match next_batch() {
            Ok(ret) => return ret,
            Err(err) => {
                let _ = set_error(
                    &self.native_wrapper,
                    &format!("poll record batch error: {err}"),
                    None,
                );
                return false;
            }
        }
    }

    pub fn finalize(self) {
        let partition = self.exec_ctx.partition_id();

        log::info!("(partition={partition}) native execution finalizing");
        self.update_metrics().unwrap_or_default();
        drop(self.plan);
        drop(self.batch_receiver);

        cancel_all_tasks(&self.exec_ctx.task_ctx()); // cancel all pending streams
        self.join_handle.abort();
        self.tokio_runtime.shutdown_background();
        log::info!("(partition={partition}) native execution finalized");
    }

    fn update_metrics(&self) -> Result<()> {
        let metrics = jni_call!(
            AuronCallNativeWrapper(self.native_wrapper.as_obj()).getMetrics() -> JObject
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
    jni_call!(AuronCallNativeWrapper(native_wrapper.as_obj())
        .setError(e.as_obj()) -> ())?;
    Ok(())
}
