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

use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow::datatypes::{DataType, SchemaRef};
use datafusion::{
    common::{Result, Statistics},
    execution::context::TaskContext,
    physical_expr::{EquivalenceProperties, PhysicalExprRef, expressions::Column},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
    },
};
use datafusion_ext_commons::df_execution_err;
use futures::StreamExt;
use itertools::Itertools;
use once_cell::sync::OnceCell;

use crate::{
    common::{
        cached_exprs_evaluator::CachedExprsEvaluator, column_pruning::ExecuteWithColumnPruning,
        execution_context::ExecutionContext,
    },
    project_exec::ProjectExec,
};

#[derive(Debug, Clone)]
pub struct FilterExec {
    input: Arc<dyn ExecutionPlan>,
    predicates: Vec<PhysicalExprRef>,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}

impl FilterExec {
    pub fn try_new(
        predicates: Vec<PhysicalExprRef>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let schema = input.schema();

        if predicates.is_empty() {
            df_execution_err!("Filter requires at least one predicate")?;
        }
        if !predicates
            .iter()
            .all(|pred| matches!(pred.data_type(&schema), Ok(DataType::Boolean)))
        {
            df_execution_err!("Filter predicate must return boolean values")?;
        }
        Ok(Self {
            input,
            predicates,
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
        })
    }

    pub fn predicates(&self) -> &[PhysicalExprRef] {
        &self.predicates
    }
}

impl DisplayAs for FilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "FilterExec [{}]",
            self.predicates.iter().map(|e| format!("{e}")).join(", ")
        )
    }
}

impl ExecutionPlan for FilterExec {
    fn name(&self) -> &str {
        "FilterExec"
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
            self.predicates.clone(),
            children[0].clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let predicates = self.predicates.clone();
        let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);
        let input = exec_ctx.execute_with_input_stats(&self.input)?;
        let filtered = execute_filter(input, predicates, exec_ctx.clone())?;
        Ok(exec_ctx.coalesce_with_default_batch_size(filtered))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        todo!()
    }
}

impl ExecuteWithColumnPruning for FilterExec {
    fn execute_projected(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
        projection: &[usize],
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.schema();
        let project = Arc::new(ProjectExec::try_new(
            schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, field)| {
                    let name = field.name().to_owned();
                    let col: PhysicalExprRef = Arc::new(Column::new(&name, i));
                    (col, name)
                })
                .collect(),
            Arc::new(self.clone()),
        )?);
        project.execute_projected(partition, context, projection)
    }
}

fn execute_filter(
    mut input: SendableRecordBatchStream,
    predicates: Vec<PhysicalExprRef>,
    exec_ctx: Arc<ExecutionContext>,
) -> Result<SendableRecordBatchStream> {
    let input_schema = input.schema();
    let cached_exprs_evaluator =
        CachedExprsEvaluator::try_new(predicates, vec![], input_schema.clone())?;

    Ok(exec_ctx
        .clone()
        .output_with_sender("Filter", move |sender| async move {
            sender.exclude_time(exec_ctx.baseline_metrics().elapsed_compute());

            while let Some(batch) = input.next().await.transpose()? {
                let _timer = exec_ctx.baseline_metrics().elapsed_compute().timer();
                let filtered_batch = cached_exprs_evaluator.filter(&batch)?;
                exec_ctx
                    .baseline_metrics()
                    .record_output(filtered_batch.num_rows());
                sender.send(filtered_batch).await;
            }
            Ok(())
        }))
}
