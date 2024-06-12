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

use arrow::{
    datatypes::{Field, Fields, Schema, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};
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

use crate::agg::AGG_BUF_COLUMN_NAME;

#[derive(Debug, Clone)]
pub struct RenameColumnsExec {
    input: Arc<dyn ExecutionPlan>,
    renamed_column_names: Vec<String>,
    renamed_schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
}

impl RenameColumnsExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        renamed_column_names: Vec<String>,
    ) -> Result<Self> {
        let input_schema = input.schema();
        let mut new_names = vec![];

        for (i, field) in input_schema
            .fields()
            .iter()
            .take(renamed_column_names.len())
            .enumerate()
        {
            if field.name() != AGG_BUF_COLUMN_NAME {
                new_names.push(renamed_column_names[i].clone());
            } else {
                new_names.push(AGG_BUF_COLUMN_NAME.to_owned());
                break;
            }
        }

        while new_names.len() < input_schema.fields().len() {
            new_names.push(input_schema.field(new_names.len()).name().clone());
        }
        let renamed_column_names = new_names;
        let renamed_schema = Arc::new(Schema::new(
            renamed_column_names
                .iter()
                .zip(input_schema.fields())
                .map(|(new_name, field)| {
                    Field::new(new_name, field.data_type().clone(), field.is_nullable())
                })
                .collect::<Fields>(),
        ));

        Ok(Self {
            input,
            renamed_column_names,
            renamed_schema,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for RenameColumnsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "RenameColumnsExec: {:?}", &self.renamed_column_names)
    }
}

#[async_trait]
impl ExecutionPlan for RenameColumnsExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.renamed_schema.clone()
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
        Ok(Arc::new(RenameColumnsExec::try_new(
            children[0].clone(),
            self.renamed_column_names.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        Ok(Box::pin(RenameColumnsStream::new(
            input,
            self.schema(),
            baseline_metrics,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.input.statistics()
    }
}

struct RenameColumnsStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    baseline_metrics: BaselineMetrics,
}

impl RenameColumnsStream {
    pub fn new(
        input: SendableRecordBatchStream,
        schema: SchemaRef,
        baseline_metrics: BaselineMetrics,
    ) -> RenameColumnsStream {
        RenameColumnsStream {
            input,
            schema,
            baseline_metrics,
        }
    }
}

impl RecordBatchStream for RenameColumnsStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for RenameColumnsStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx)? {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(batch)) => {
                let num_rows = batch.num_rows();
                self.baseline_metrics.record_poll(Poll::Ready(Some(Ok(
                    RecordBatch::try_new_with_options(
                        self.schema.clone(),
                        batch.columns().to_vec(),
                        &RecordBatchOptions::new().with_row_count(Some(num_rows)),
                    )?,
                ))))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.input.size_hint()
    }
}
