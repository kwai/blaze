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

use crate::util::ipc::write_one_batch;
use async_trait::async_trait;
use blaze_commons::{
    jni_call, jni_call_static, jni_delete_local_ref, jni_new_direct_byte_buffer,
    jni_new_global_ref, jni_new_string,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::coalesce_batches::concat_batches;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use futures::StreamExt;
use futures::TryFutureExt;
use futures::TryStreamExt;
use jni::objects::{GlobalRef, JObject};
use std::any::Any;
use std::fmt::Formatter;
use std::io::Cursor;
use std::sync::Arc;

#[derive(Debug)]
pub struct IpcWriterExec {
    input: Arc<dyn ExecutionPlan>,
    ipc_consumer_resource_id: String,
    metrics: ExecutionPlanMetricsSet,
}

impl IpcWriterExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, ipc_consumer_resource_id: String) -> Self {
        Self {
            input,
            ipc_consumer_resource_id,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

#[async_trait]
impl ExecutionPlan for IpcWriterExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "IpcWriterExec expects one children".to_string(),
            ));
        }
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
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let ipc_consumer = jni_new_global_ref!(jni_call_static!(
            JniBridge.getResource(
                jni_new_string!(&self.ipc_consumer_resource_id)?
            ) -> JObject
        )?)?;
        let input = self.input.execute(partition, context.clone())?;

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(
                write_ipc(
                    input,
                    context.session_config().batch_size(),
                    ipc_consumer,
                    baseline_metrics,
                )
                .map_err(|e| ArrowError::ExternalError(Box::new(e))),
            )
            .try_flatten(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "IpcWriterExec")
            }
        }
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}

pub async fn write_ipc(
    mut input: SendableRecordBatchStream,
    batch_size: usize,
    ipc_consumer: GlobalRef,
    metrics: BaselineMetrics,
) -> Result<SendableRecordBatchStream> {
    let schema = input.schema();
    let mut batches: Vec<RecordBatch> = vec![];
    let mut num_rows = 0;

    macro_rules! flush_batches {
        () => {{
            let timer = metrics.elapsed_compute().timer();
            let batch = concat_batches(&schema, &batches, num_rows)?;
            metrics.record_output(num_rows);
            batches.clear();
            num_rows = 0;

            let mut buffer = vec![];
            write_one_batch(
                &batch,
                &mut Cursor::new(&mut buffer),
                true,
            )?;
            std::mem::drop(timer);

            let jbuf = jni_new_direct_byte_buffer!(&mut buffer)?;
            let consumed = jni_call!(
                ScalaFunction1(ipc_consumer.as_obj()).apply(jbuf) -> JObject
            )?;
            jni_delete_local_ref!(consumed)?;
            jni_delete_local_ref!(jbuf.into())?;
        }}
    }

    while let Some(batch) = input.next().await {
        let batch = batch?;

        if batch.num_rows() == 0 {
            continue;
        }
        if num_rows + batch.num_rows() > batch_size {
            flush_batches!();
        }
        num_rows += batch.num_rows();
        batches.push(batch);
    }
    if num_rows > 0 {
        flush_batches!();
    }
    assert_eq!(num_rows, 0);

    // ipc writer always has empty output
    Ok(Box::pin(MemoryStream::try_new(vec![], schema, None)?))
}
