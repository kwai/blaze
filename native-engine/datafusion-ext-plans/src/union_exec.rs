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
        DisplayAs, DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
    },
};
use datafusion_ext_commons::arrow::cast::cast;
use futures_util::StreamExt;
use once_cell::sync::OnceCell;

use crate::common::execution_context::ExecutionContext;

#[derive(Debug)]
pub struct UnionExec {
    inputs: Vec<UnionInput>,
    schema: SchemaRef,
    num_partitions: usize,
    cur_partition: usize,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}

#[derive(Debug, Clone)]
pub struct UnionInput(pub Arc<dyn ExecutionPlan>, pub usize);

impl UnionExec {
    pub fn new(
        inputs: Vec<UnionInput>,
        schema: SchemaRef,
        num_partitions: usize,
        cur_partition: usize,
    ) -> Self {
        UnionExec {
            inputs,
            schema,
            num_partitions,
            cur_partition,
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
                EmissionType::Both,
                Boundedness::Bounded,
            )
        })
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().map(|input| &input.0).collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            children
                .into_iter()
                .map(|child| UnionInput(child, 0))
                .collect(),
            self.schema.clone(),
            self.num_partitions,
            self.cur_partition,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != self.cur_partition {
            return Ok(Box::pin(EmptyRecordBatchStream::new(self.schema())));
        }
        let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);
        let input_streams = self
            .inputs
            .iter()
            .map(|UnionInput(input, partition)| {
                let in_exec_ctx = ExecutionContext::new(
                    exec_ctx.task_ctx().clone(),
                    *partition,
                    input.schema(),
                    &self.metrics,
                );
                in_exec_ctx.execute_with_input_stats(input)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(exec_ctx
            .clone()
            .output_with_sender("Union", move |sender| async move {
                let output_schema = exec_ctx.output_schema();
                let output_fields = output_schema.fields();

                // iterate all input streams
                for mut input in input_streams {
                    // cast data type if input schema not matching output schema
                    let input_schema = input.schema();
                    let input_fields = input_schema.fields();
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
