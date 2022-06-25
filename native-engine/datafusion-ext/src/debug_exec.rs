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

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::error::DataFusionError;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;

use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
};

use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};

use futures::{Stream, StreamExt};

use std::any::Any;
use std::fmt::Formatter;

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "DebugExec expects one children".to_string(),
            ));
        }
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
        let baseline_metrics = BaselineMetrics::new(&self.metrics, 0);
        let input = self.input.execute(partition, context)?;

        Ok(Box::pin(DebugStream {
            input,
            debug_id: self.debug_id.clone(),
            metrics: Arc::new(baseline_metrics),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "DebugExec")
            }
        }
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}

struct DebugStream {
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
    type Item = datafusion::arrow::error::Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let metrics = self.metrics.clone();
        match metrics.record_poll(self.input.poll_next_unpin(cx))? {
            Poll::Ready(Some(batch)) => {
                let mut batches = vec![batch];
                let table_str = pretty_format_batches(&batches)?
                    .to_string()
                    .replace('\n', &format!("\n{} - ", self.debug_id));

                log::info!("{}", table_str);
                Poll::Ready(Some(Ok(batches.pop().unwrap())))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
