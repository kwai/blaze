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

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow_schema::SchemaRef;
use datafusion::{
    common::{Result, Statistics},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::{EquivalenceProperties, Partitioning::UnknownPartitioning},
    physical_plan::{
        empty::EmptyExec,
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
        DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties,
    },
};
use datafusion_ext_commons::arrow::cast::cast;
use futures_util::StreamExt;
use once_cell::sync::OnceCell;

use crate::common::execution_context::ExecutionContext;

#[derive(Debug)]
pub struct UnionExec {
    num_partitions: usize,
    in_partition: usize,
    child: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    num_children: usize,
    current_child_index: usize,
    empty_child: Arc<dyn ExecutionPlan>,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}

impl UnionExec {
    pub fn new(
        num_partitions: usize,
        in_partition: usize,
        child: Arc<dyn ExecutionPlan>,
        num_children: usize,
        current_child_index: usize,
        schema: SchemaRef,
    ) -> Self {
        let empty_child = Arc::new(EmptyExec::new(schema.clone()));
        UnionExec {
            num_partitions,
            in_partition,
            child,
            schema,
            num_children,
            current_child_index,
            empty_child,
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
        }
    }
}

impl DisplayAs for UnionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "UnionExec")
    }
}

impl ExecutionPlan for UnionExec {
    fn name(&self) -> &str {
        "UnionExec"
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
                UnknownPartitioning(self.num_partitions),
                ExecutionMode::Bounded,
            )
        })
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        (0..self.num_children)
            .map(|i| {
                if i == self.current_child_index {
                    &self.child
                } else {
                    &self.empty_child
                }
            })
            .collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            self.num_partitions,
            self.in_partition,
            children[self.current_child_index].clone(),
            self.num_children,
            self.current_child_index,
            self.schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let exec_ctx =
            ExecutionContext::new(context.clone(), partition, self.schema(), &self.metrics);
        let in_exec_ctx = ExecutionContext::new(
            context.clone(),
            self.in_partition,
            self.child.schema(),
            &self.metrics,
        );
        let mut input = in_exec_ctx.execute_with_input_stats(&self.child)?;

        Ok(exec_ctx
            .clone()
            .output_with_sender("Union", move |sender| async move {
                let input_schema = input.schema();
                let output_schema = exec_ctx.output_schema();

                // cast data type if input schema not matching output schema
                let input_fields = input_schema.fields();
                let output_fields = output_schema.fields();
                let columns_need_cast = input_fields
                    .iter()
                    .zip(output_fields)
                    .map(|(input_field, output_field)| input_field != output_field)
                    .collect::<Vec<_>>();
                let need_cast = columns_need_cast.contains(&true);

                while let Some(mut batch) = input.next().await.transpose()? {
                    if need_cast {
                        let mut casted_cols = Vec::with_capacity(batch.num_columns());
                        for ((mut col, &col_need_cast), output_field) in batch
                            .columns()
                            .iter()
                            .cloned()
                            .zip(&columns_need_cast)
                            .zip(output_schema.fields())
                        {
                            if col_need_cast {
                                col = cast(&col, output_field.data_type())?;
                            }
                            casted_cols.push(col);
                        }
                        batch = RecordBatch::try_new_with_options(
                            output_schema.clone(),
                            casted_cols,
                            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
                        )?;
                    }
                    exec_ctx.baseline_metrics().record_output(batch.num_rows());
                    sender.send(batch).await;
                }
                Ok(())
            }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        todo!()
    }
}
