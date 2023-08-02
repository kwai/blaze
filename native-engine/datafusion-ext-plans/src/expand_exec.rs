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
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::Partitioning::UnknownPartitioning;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream,
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

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
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

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use arrow::array::{BooleanArray, Float32Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::{assert_batches_eq};
    use datafusion::physical_plan::{common, ExecutionPlan,};
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::prelude::{SessionContext};
    use crate::common::memory_manager::MemManager;
    use crate::expand_exec::ExpandExec;
    use datafusion::common::{Result, ScalarValue};
    use datafusion::logical_expr::{Operator, UserDefinedLogicalNode};
    use datafusion::physical_expr::expressions::{binary, col, lit};

    //build i32 table
    fn build_table_i32(
        a: (&str, &Vec<i32>),
    ) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Int32, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(a.1.clone())),
            ],
        )
            .unwrap()
    }

    fn build_table_int(
        a: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a);
        let schema = batch.schema();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    //build f32 table
    fn build_table_f32(
        a: (&str, &Vec<f32>),
    ) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Float32, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Float32Array::from(a.1.clone())),
            ],
        )
            .unwrap()
    }

    fn build_table_float(
        a: (&str, &Vec<f32>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_f32(a);
        let schema = batch.schema();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    //build str table
    fn build_table_str(
        a: (&str, &Vec<String>),
    ) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Utf8, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(a.1.clone())),
            ],
        )
            .unwrap()
    }

    fn build_table_string(
        a: (&str, &Vec<String>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_str(a);
        let schema = batch.schema();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    //build boolean table
    fn build_table_bool(
        a: (&str, &Vec<bool>),
    ) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Boolean, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(BooleanArray::from(a.1.clone())),
            ],
        )
            .unwrap()
    }

    fn build_table_boolean(
        a: (&str, &Vec<bool>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_bool(a);
        let schema = batch.schema();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    #[tokio::test]
    async fn test_expand_exec_i32() -> Result<()> {
        MemManager::init(10000);

        let input = build_table_int(
            ("a", &vec![-1, -2, 0, 3]),
        );

        let schema = Schema::new(vec![
            Field::new("test_i32", DataType::Int32, false),
        ]);

        let test_i32 = vec![4, 3, 2, 1];

        let projections= vec![
            vec![binary(col("test_i32", &schema).unwrap(), Operator::Multiply, lit(ScalarValue::from(2)), &schema).unwrap()],
            vec![binary(col("test_i32", &schema).unwrap(), Operator::Plus, lit(ScalarValue::from(100)), &schema).unwrap()],
            vec![binary(col("test_i32", &schema).unwrap(), Operator::Divide, lit(ScalarValue::from(-2)), &schema).unwrap()],
            vec![binary(col("test_i32", &schema).unwrap(), Operator::Modulo, lit(ScalarValue::from(2)), &schema).unwrap()],
            vec![binary(col("test_i32", &schema).unwrap(), Operator::BitwiseShiftLeft, lit(ScalarValue::from(1)), &schema).unwrap()],
        ];

        let expand_exec = ExpandExec::try_new(
            input.schema(),
            projections,
            input
        )?;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let output = expand_exec.execute(0, task_ctx).unwrap();
        let batches = common::collect(output).await?;
        let expected = vec![
            "+-----+",
            "| a   |",
            "+-----+",
            "| -2  |",
            "| -4  |",
            "| 0   |",
            "| 6   |",
            "| 99  |",
            "| 98  |",
            "| 100 |",
            "| 103 |",
            "| 0   |",
            "| 1   |",
            "| 0   |",
            "| -1  |",
            "| -1  |",
            "| 0   |",
            "| 0   |",
            "| 1   |",
            "| -2  |",
            "| -4  |",
            "| 0   |",
            "| 6   |",
            "+-----+",
        ];
        assert_batches_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_expand_exec_f32() -> Result<()> {
        MemManager::init(10000);

        let input = build_table_float(
            ("a", &vec![-1.2, -2.3, 0.0, 3.4]),
        );

        let schema = Schema::new(vec![
            Field::new("test_f32", DataType::Float32, false),
        ]);

        let test_i32:Vec<f32> = vec![4.3, 3.2, 2.1, 1.0];
        let projections= vec![
            vec![binary(col("test_f32", &schema).unwrap(), Operator::Multiply, lit(ScalarValue::from(2.1_f32)), &schema).unwrap()],
            vec![binary(col("test_f32", &schema).unwrap(), Operator::Plus, lit(ScalarValue::from(100_f32)), &schema).unwrap()],
            vec![binary(col("test_f32", &schema).unwrap(), Operator::Divide, lit(ScalarValue::from(-2_f32)), &schema).unwrap()],
            vec![binary(col("test_f32", &schema).unwrap(), Operator::Modulo, lit(ScalarValue::from(-2_f32)), &schema).unwrap()],
        ];

        let expand_exec = ExpandExec::try_new(
            input.schema(),
            projections,
            input
        )?;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let output = expand_exec.execute(0, task_ctx).unwrap();
        let batches = common::collect(output).await?;
        let expected = vec![
            "+-------------+",
            "| a           |",
            "+-------------+",
            "| -2.52       |",
            "| -4.8299994  |",
            "| 0.0         |",
            "| 7.14        |",
            "| 98.8        |",
            "| 97.7        |",
            "| 100.0       |",
            "| 103.4       |",
            "| 0.6         |",
            "| 1.15        |",
            "| 0.0         |",
            "| -1.7        |",
            "| -1.2        |",
            "| -0.29999995 |",
            "| 0.0         |",
            "| 1.4000001   |",
            "+-------------+",
        ];
        assert_batches_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_expand_exec_str() -> Result<()> {
        MemManager::init(10000);

        let input = build_table_string(
            ("a", &vec!["hello".to_string(), ",".to_string(), "rust".to_string(), "!".to_string()]),
        );

        let schema = Schema::new(vec![
            Field::new("test_str", DataType::Utf8, false),
        ]);

        let test_str = vec!["test".to_string()];

        let projections= vec![
            vec![binary(col("test_str", &schema).unwrap(), Operator::StringConcat, lit(Some("app").unwrap()), &schema).unwrap()],
        ];

        let expand_exec = ExpandExec::try_new(
            input.schema(),
            projections,
            input
        )?;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let output = expand_exec.execute(0, task_ctx).unwrap();
        let batches = common::collect(output).await?;
        let expected = vec![
            "+----------+",
            "| a        |",
            "+----------+",
            "| helloapp |",
            "| ,app     |",
            "| rustapp  |",
            "| !app     |",
            "+----------+",
        ];
        assert_batches_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_expand_exec_bool() -> Result<()> {
        MemManager::init(10000);

        let input = build_table_boolean(
            ("a", &vec![true, false, true, false]),
        );

        let schema = Schema::new(vec![
            Field::new("test_bool", DataType::Boolean, false),
        ]);

        let test_bool = vec![true];

        let projections= vec![
            vec![binary(col("test_bool", &schema).unwrap(), Operator::And, lit(ScalarValue::Boolean(Some(true))), &schema).unwrap()],
            vec![binary(col("test_bool", &schema).unwrap(), Operator::Or, lit(ScalarValue::Boolean(Some(true))), &schema).unwrap()],
        ];

        let expand_exec = ExpandExec::try_new(
            input.schema(),
            projections,
            input
        )?;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let output = expand_exec.execute(0, task_ctx).unwrap();
        let batches = common::collect(output).await?;
        let expected = vec![
            "+-------+",
            "| a     |",
            "+-------+",
            "| true  |",
            "| false |",
            "| true  |",
            "| false |",
            "| true  |",
            "| true  |",
            "| true  |",
            "| true  |",
            "+-------+",
        ];
        assert_batches_eq!(expected, &batches);

        Ok(())
    }


}