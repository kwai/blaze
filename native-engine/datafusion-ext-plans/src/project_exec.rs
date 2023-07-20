// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::common::cached_exprs_evaluator::CachedExprsEvaluator;
use crate::common::output::output_with_sender;
use crate::filter_exec::FilterExec;
use arrow::datatypes::{Field, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::{Result, Statistics};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{PhysicalExprRef, PhysicalSortExpr};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use datafusion_ext_commons::streams::coalesce_stream::CoalesceStream;
use futures::stream::once;
use futures::{FutureExt, StreamExt, TryStreamExt};
use itertools::Itertools;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ProjectExec {
    expr: Vec<(PhysicalExprRef, String)>,
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
}

impl ProjectExec {
    pub fn try_new(
        expr: Vec<(PhysicalExprRef, String)>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let input_schema = input.schema();
        let schema = Arc::new(Schema::new(
            expr.iter()
                .map(|(e, name)| {
                    Ok(Field::new(
                        name,
                        e.data_type(&input_schema)?,
                        e.nullable(&input_schema)?,
                    ))
                })
                .collect::<Result<Fields>>()?,
        ));

        Ok(Self {
            expr,
            input,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl ExecutionPlan for ProjectExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.input.output_partitioning().partition_count())
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
        Ok(Arc::new(Self::try_new(
            self.expr.clone(),
            children[0].clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let batch_size = context.session_config().batch_size();
        let metrics = BaselineMetrics::new(&self.metrics, partition);
        let elapsed_compute = metrics.elapsed_compute().clone();

        let exprs: Vec<PhysicalExprRef> = self.expr.iter().map(|(e, _name)| e.clone()).collect();

        let fut = if let Some(filter_exec) = self.input.as_any().downcast_ref::<FilterExec>() {
            let input = filter_exec.children()[0].execute(partition, context.clone())?;
            let filters = filter_exec.predicates().to_vec();
            execute_project_with_filtering(input, self.schema(), context, filters, exprs, metrics)
                .boxed()
        } else {
            let input = self.input.execute(partition, context.clone())?;
            execute_project_with_filtering(input, self.schema(), context, vec![], exprs, metrics)
                .boxed()
        };

        let output = Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            once(fut).try_flatten(),
        ));

        let coalesced = Box::pin(CoalesceStream::new(output, batch_size, elapsed_compute));
        Ok(coalesced)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "ProjectExec [{}]",
            self.expr
                .iter()
                .map(|(e, name)| format!("{e} AS {name}"))
                .join(", ")
        )
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}

async fn execute_project_with_filtering(
    mut input: SendableRecordBatchStream,
    output_schema: SchemaRef,
    context: Arc<TaskContext>,
    filters: Vec<PhysicalExprRef>,
    exprs: Vec<PhysicalExprRef>,
    metrics: BaselineMetrics,
) -> Result<SendableRecordBatchStream> {
    let cached_expr_evaluator = CachedExprsEvaluator::try_new(filters, exprs)?;

    output_with_sender(
        "Project",
        context,
        output_schema.clone(),
        move |sender| async move {
            while let Some(batch) = input.next().await.transpose()? {
                let mut timer = metrics.elapsed_compute().timer();
                let output_arrays = cached_expr_evaluator.filter_project(&batch)?;
                let output_batch = if !output_arrays.is_empty() {
                    RecordBatch::try_new(output_schema.clone(), output_arrays)?
                } else {
                    RecordBatch::new_empty(output_schema.clone())
                };
                metrics.record_output(output_batch.num_rows());
                sender.send(Ok(output_batch), Some(&mut timer)).await;
            }
            Ok(())
        },
    )
}
