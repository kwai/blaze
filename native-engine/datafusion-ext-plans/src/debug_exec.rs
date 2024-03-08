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
    any::Any,
    fmt::Formatter,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch, util::pretty::pretty_format_batches};
use async_trait::async_trait;
use datafusion::{
    error::Result,
    execution::context::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
        SendableRecordBatchStream, Statistics,
    },
};
use futures::{Stream, StreamExt};

#[derive(Debug)]
pub struct DebugExec {
    input: Arc<dyn ExecutionPlan>,
    debug_id: String,
    metrics: ExecutionPlanMetricsSet,
}

impl DebugExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, debug_id: String) -> Self {
        Self {
            input,
            debug_id,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for DebugExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DebugExec")
    }
}

#[async_trait]
impl ExecutionPlan for DebugExec {
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
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DebugExec::new(
            self.input.clone(),
            self.debug_id.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let input = self.input.execute(partition, context)?;

        Ok(Box::pin(DebugStream {
            partition,
            input,
            debug_id: self.debug_id.clone(),
            metrics: Arc::new(baseline_metrics),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        todo!()
    }
}

struct DebugStream {
    partition: usize,
    input: SendableRecordBatchStream,
    debug_id: String,
    metrics: Arc<BaselineMetrics>,
}

impl RecordBatchStream for DebugStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

impl Stream for DebugStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let metrics = self.metrics.clone();
        match metrics.record_poll(self.input.poll_next_unpin(cx))? {
            Poll::Ready(Some(batch)) => {
                let mut batches = vec![batch];
                let table_str = pretty_format_batches(&batches)?
                    .to_string()
                    .replace('\n', &format!("\n{} - ", self.debug_id));

                log::info!("DebugExec(partition={}):\n{}", self.partition, table_str);
                Poll::Ready(Some(Ok(batches.pop().unwrap())))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
