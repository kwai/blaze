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

use arrow::{
    datatypes::{Field, Fields, Schema, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use async_trait::async_trait;
use datafusion::{
    error::Result,
    execution::context::TaskContext,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream, Statistics,
        execution_plan::{Boundedness, EmissionType},
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
        stream::RecordBatchStreamAdapter,
    },
};
use futures::StreamExt;
use once_cell::sync::OnceCell;

use crate::{agg::AGG_BUF_COLUMN_NAME, common::execution_context::ExecutionContext};

#[derive(Debug, Clone)]
pub struct RenameColumnsExec {
    input: Arc<dyn ExecutionPlan>,
    renamed_column_names: Vec<String>,
    renamed_schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
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
            props: OnceCell::new(),
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
    fn name(&self) -> &str {
        "RenameColumnsExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.renamed_schema.clone()
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
        let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);
        let input = exec_ctx.execute(&self.input)?;
        let output = input.map(move |batch| {
            let input_batch = batch?;
            let output_batch = RecordBatch::try_new_with_options(
                exec_ctx.output_schema(),
                input_batch.columns().to_vec(),
                &RecordBatchOptions::new().with_row_count(Some(input_batch.num_rows())),
            )?;
            exec_ctx
                .baseline_metrics()
                .record_output(output_batch.num_rows());
            Ok(output_batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        todo!()
    }
}
