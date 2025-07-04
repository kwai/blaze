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

use std::{any::Any, fmt::Formatter, io::Write, sync::Arc};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use blaze_jni_bridge::{
    jni_call, jni_call_static, jni_new_direct_byte_buffer, jni_new_global_ref, jni_new_string,
};
use datafusion::{
    error::Result,
    execution::context::TaskContext,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, ExecutionPlanProperties,
        PlanProperties, SendableRecordBatchStream, Statistics,
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
        stream::RecordBatchStreamAdapter,
    },
};
use futures::{StreamExt, TryStreamExt, stream::once};
use jni::objects::{GlobalRef, JObject};
use once_cell::sync::OnceCell;

use crate::common::{
    execution_context::ExecutionContext, ipc_compression::IpcCompressionWriter,
    timer_helper::TimerHelper,
};

#[derive(Debug)]
pub struct IpcWriterExec {
    input: Arc<dyn ExecutionPlan>,
    ipc_consumer_resource_id: String,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}

impl IpcWriterExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, ipc_consumer_resource_id: String) -> Self {
        Self {
            input,
            ipc_consumer_resource_id,
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
        }
    }
}

impl DisplayAs for IpcWriterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "IpcWriter")
    }
}

#[async_trait]
impl ExecutionPlan for IpcWriterExec {
    fn name(&self) -> &str {
        "IpcWriterExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        self.props.get_or_init(|| {
            PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                self.input.output_partitioning().clone(),
                ExecutionMode::Bounded,
            )
        })
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(IpcWriterExec::new(
            self.input.clone(),
            self.ipc_consumer_resource_id.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);
        let ipc_consumer_local = jni_call_static!(
            JniBridge.getResource(
                jni_new_string!(&self.ipc_consumer_resource_id)?.as_obj()) -> JObject
        )?;
        let ipc_consumer = jni_new_global_ref!(ipc_consumer_local.as_obj())?;
        let input = exec_ctx.execute_with_input_stats(&self.input)?;
        let coalesced = exec_ctx.coalesce_with_default_batch_size(input);

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            once(write_ipc(coalesced, exec_ctx, ipc_consumer)).try_flatten(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        todo!()
    }
}

pub async fn write_ipc(
    mut input: SendableRecordBatchStream,
    exec_ctx: Arc<ExecutionContext>,
    ipc_consumer: GlobalRef,
) -> Result<SendableRecordBatchStream> {
    Ok(exec_ctx
        .clone()
        .output_with_sender("IpcWrite", move |_sender| async move {
            let _timer = exec_ctx.baseline_metrics().elapsed_compute().timer();

            struct IpcConsumerWrite(GlobalRef);
            impl Write for IpcConsumerWrite {
                fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                    let buf_len = buf.len();
                    let buf = jni_new_direct_byte_buffer!(&buf).map_err(std::io::Error::other)?;
                    jni_call!(ScalaFunction1(self.0.as_obj()).apply(buf.as_obj()) -> JObject)
                        .map_err(std::io::Error::other)?;
                    Ok(buf_len)
                }

                fn flush(&mut self) -> std::io::Result<()> {
                    Ok(())
                }
            }

            let mut writer = IpcCompressionWriter::new(IpcConsumerWrite(ipc_consumer));
            while let Some(batch) = exec_ctx
                .baseline_metrics()
                .elapsed_compute()
                .exclude_timer_async(input.next())
                .await
                .transpose()?
            {
                writer.write_batch(batch.num_rows(), batch.columns())?;
                exec_ctx.baseline_metrics().record_output(batch.num_rows());
            }
            writer.finish_current_buf()?;
            Ok(())
        }))
}
