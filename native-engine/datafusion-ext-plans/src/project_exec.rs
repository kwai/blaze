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

use crate::common::batch_statisitcs::{stat_input, InputBatchStatistics};
use crate::common::cached_exprs_evaluator::CachedExprsEvaluator;
use crate::common::output::output_with_sender;
use crate::filter_exec::FilterExec;
use arrow::datatypes::{Field, Fields, Schema, SchemaRef};
use datafusion::common::{Result, Statistics};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{PhysicalExprRef, PhysicalSortExpr};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use datafusion_ext_commons::streams::coalesce_stream::CoalesceStream;
use futures::stream::once;
use futures::{FutureExt, StreamExt, TryStreamExt};
use itertools::Itertools;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::common::column_pruning::{prune_columns, ExecuteWithColumnPruning};

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

impl DisplayAs for ProjectExec {
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
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let elapsed_compute = baseline_metrics.elapsed_compute().clone();

        let exprs: Vec<PhysicalExprRef> = self.expr.iter().map(|(e, _name)| e.clone()).collect();

        let fut = if let Some(filter_exec) = self.input.as_any().downcast_ref::<FilterExec>() {
            execute_project_with_filtering(
                filter_exec.children()[0].clone(),
                partition,
                context,
                self.schema(),
                filter_exec.predicates().to_vec(),
                exprs,
                self.metrics.clone(),
            )
            .boxed()
        } else {
            execute_project_with_filtering(
                self.input.clone(),
                partition,
                context,
                self.schema(),
                vec![],
                exprs,
                self.metrics.clone(),
            )
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

    fn statistics(&self) -> Statistics {
        todo!()
    }
}

impl ExecuteWithColumnPruning for ProjectExec {
    fn execute_projected(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
        projection: &[usize],
    ) -> Result<SendableRecordBatchStream> {
        let projected_project: Arc<dyn ExecutionPlan> = Arc::new(ProjectExec {
            input: self.input.clone(),
            expr: projection.iter().map(|&i| self.expr[i].clone()).collect(),
            schema: Arc::new(self.schema.project(projection)?),
            metrics: self.metrics.clone(),
        });
        projected_project.execute(partition, context)
    }
}

async fn execute_project_with_filtering(
    input: Arc<dyn ExecutionPlan>,
    partition: usize,
    context: Arc<TaskContext>,
    output_schema: SchemaRef,
    filters: Vec<PhysicalExprRef>,
    exprs: Vec<PhysicalExprRef>,
    metrics: ExecutionPlanMetricsSet,
) -> Result<SendableRecordBatchStream> {
    // execute input with pruning
    let baseline_metrics = BaselineMetrics::new(&metrics, partition);
    let num_exprs = exprs.len();
    let (pruned_exprs, projection) = prune_columns(&[exprs, filters].concat())?;
    let exprs = pruned_exprs
        .iter()
        .take(num_exprs)
        .cloned()
        .collect::<Vec<PhysicalExprRef>>();
    let filters = pruned_exprs
        .iter()
        .skip(num_exprs)
        .cloned()
        .collect::<Vec<PhysicalExprRef>>();
    let cached_expr_evaluator = CachedExprsEvaluator::try_new(filters, exprs)?;

    let mut input = stat_input(
        InputBatchStatistics::from_metrics_set_and_blaze_conf(&metrics, partition)?,
        input.execute_projected(partition, context.clone(), &projection)?,
    )?;

    output_with_sender(
        "Project",
        context,
        output_schema.clone(),
        move |sender| async move {
            while let Some(batch) = input.next().await.transpose()? {
                let mut timer = baseline_metrics.elapsed_compute().timer();
                let output_batch =
                    cached_expr_evaluator.filter_project(&batch, output_schema.clone())?;
                baseline_metrics.record_output(output_batch.num_rows());
                sender.send(Ok(output_batch), Some(&mut timer)).await;
            }
            Ok(())
        },
    )
}
