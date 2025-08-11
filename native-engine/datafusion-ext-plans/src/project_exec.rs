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

use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow::datatypes::{Field, Fields, Schema, SchemaRef};
use datafusion::{
    common::{Result, Statistics},
    execution::TaskContext,
    physical_expr::{EquivalenceProperties, PhysicalExprRef},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
    },
};
use datafusion_ext_commons::downcast_any;
use futures::StreamExt;
use itertools::Itertools;
use once_cell::sync::OnceCell;

use crate::{
    common::{
        cached_exprs_evaluator::CachedExprsEvaluator,
        column_pruning::{ExecuteWithColumnPruning, prune_columns},
        execution_context::ExecutionContext,
        timer_helper::TimerHelper,
    },
    filter_exec::FilterExec,
};

#[derive(Debug, Clone)]
pub struct ProjectExec {
    expr: Vec<(PhysicalExprRef, String)>,
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
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
            props: OnceCell::new(),
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
    fn name(&self) -> &str {
        "ProjectExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        self.props.get_or_init(|| {
            PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                self.input.output_partitioning().clone(),
                EmissionType::Both,
                Boundedness::Bounded,
            )
        })
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
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
        let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);
        let exprs: Vec<PhysicalExprRef> = self.expr.iter().map(|(e, _name)| e.clone()).collect();

        let output = if let Ok(filter_exec) = downcast_any!(self.input, FilterExec) {
            execute_project_with_filtering(
                filter_exec.children()[0].clone(),
                exec_ctx.clone(),
                filter_exec.predicates().to_vec(),
                exprs,
            )?
        } else {
            execute_project_with_filtering(self.input.clone(), exec_ctx.clone(), vec![], exprs)?
        };
        Ok(exec_ctx.coalesce_with_default_batch_size(output))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
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
            props: OnceCell::new(),
        });
        projected_project.execute(partition, context)
    }
}

fn execute_project_with_filtering(
    input: Arc<dyn ExecutionPlan>,
    exec_ctx: Arc<ExecutionContext>,
    filters: Vec<PhysicalExprRef>,
    exprs: Vec<PhysicalExprRef>,
) -> Result<SendableRecordBatchStream> {
    // execute input with pruning
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

    let cached_expr_evaluator = Arc::new(CachedExprsEvaluator::try_new(
        filters,
        exprs,
        exec_ctx.output_schema(),
    )?);

    let mut input = exec_ctx.execute_projected_with_input_stats(&input, &projection)?;
    Ok(exec_ctx
        .clone()
        .output_with_sender("Project", move |sender| async move {
            let elapsed_compute = exec_ctx.baseline_metrics().elapsed_compute().clone();
            let _timer = elapsed_compute.timer();
            sender.exclude_time(&elapsed_compute);

            while let Some(batch) = elapsed_compute
                .exclude_timer_async(input.next())
                .await
                .transpose()?
            {
                let output_batch = cached_expr_evaluator.filter_project(&batch)?;
                drop(batch);

                exec_ctx
                    .baseline_metrics()
                    .record_output(output_batch.num_rows());
                sender.send(output_batch).await;
            }
            Ok(())
        }))
}
