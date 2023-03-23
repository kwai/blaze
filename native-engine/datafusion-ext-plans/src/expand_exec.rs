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

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::common::{DataFusionError, Statistics};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
};
use datafusion::physical_plan::Partitioning::UnknownPartitioning;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};
use std::any::Any;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

#[derive(Debug, Clone)]
pub struct ExpandExec {
    schema: SchemaRef,
    projections: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    input: Arc<dyn ExecutionPlan>,
    metrics: ExecutionPlanMetricsSet,
}

impl ExpandExec {
    pub fn try_new(
        schema: SchemaRef,
        projections: Vec<Vec<Arc<dyn PhysicalExpr>>>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let input_schema = input.schema();
        for projections in &projections {
            for i in 0..schema.fields.len() {
                let schema_data_type = schema.fields[i].data_type();
                let projection_data_type = projections
                    .get(i)
                    .map(|expr| expr.data_type(&input_schema))
                    .transpose()?;

                if projection_data_type.as_ref() != Some(schema_data_type) {
                    return Err(DataFusionError::Plan(format!(
                        "ExpandExec data type not matches: {:?} vs {:?}",
                        projection_data_type, schema_data_type
                    )));
                }
            }
        }
        Ok(Self {
            schema,
            projections,
            input,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl ExecutionPlan for ExpandExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        UnknownPartitioning(0)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
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
                "ExpandExec expects one children".to_string(),
            ));
        }
        Ok(Arc::new(Self {
            schema: self.schema(),
            projections: self.projections.clone(),
            input: children[0].clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let input = self.input.execute(partition, context)?;

        Ok(Box::pin(ExpandStream {
            schema: self.schema(),
            projections: self.projections.clone(),
            input,
            metrics: Arc::new(baseline_metrics),
            current_batch: None,
            current_projection_id: 0,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ExpandExec")
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}

struct ExpandStream {
    schema: SchemaRef,
    projections: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    input: SendableRecordBatchStream,
    metrics: Arc<BaselineMetrics>,
    current_batch: Option<RecordBatch>,
    current_projection_id: usize,
}

impl RecordBatchStream for ExpandStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for ExpandStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // continue processing current batch
        if let Some(batch) = &self.current_batch {
            let _timer = self.metrics.elapsed_compute();
            let projections = &self.projections[self.current_projection_id];
            let arrays = projections
                .iter()
                .map(|expr| expr.evaluate(batch))
                .map(|r| r.map(|v| v.into_array(batch.num_rows())))
                .collect::<Result<Vec<_>>>()?;
            let output_batch = RecordBatch::try_new(self.schema.clone(), arrays)?;

            self.current_projection_id += 1;
            if self.current_projection_id >= self.projections.len() {
                self.current_batch = None;
                self.current_projection_id = 0;
            }
            return self
                .metrics
                .record_poll(Poll::Ready(Some(Ok(output_batch))));
        }

        // current batch has been consumed, poll next batch
        match ready!(self.input.poll_next_unpin(cx)).transpose()? {
            None => Poll::Ready(None),
            Some(batch) => {
                self.current_batch = Some(batch);
                self.poll_next(cx)
            }
        }
    }
}
