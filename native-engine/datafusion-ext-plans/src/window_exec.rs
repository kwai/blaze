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
    array::{Array, ArrayRef, Int32Array},
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use datafusion::{
    common::{Result, Statistics},
    execution::context::TaskContext,
    physical_expr::{EquivalenceProperties, PhysicalExprRef, PhysicalSortExpr},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
    },
};
use datafusion_ext_commons::{arrow::cast::cast, downcast_any};
use futures::StreamExt;
use once_cell::sync::OnceCell;

use crate::{
    common::execution_context::ExecutionContext,
    window::{WindowExpr, window_context::WindowContext},
};

#[derive(Debug)]
pub struct WindowExec {
    input: Arc<dyn ExecutionPlan>,
    context: Arc<WindowContext>,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}

impl WindowExec {
    // NOTE:
    // WindowExec now supports spark's WindowExec and WindowGroupLimitExec:
    // for normal WindowExec:
    //   group_limit = None
    //   output_window_cols = true
    //
    // for partial WindowGroupLimitExec
    //   group_limit = Some(K)
    //   output_window_cols = false
    //
    // for final WindowGroupLimitExec:
    //   group_limit = Some(K)
    //   output_window_cols = true
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        window_exprs: Vec<WindowExpr>,
        partition_spec: Vec<PhysicalExprRef>,
        order_spec: Vec<PhysicalSortExpr>,
        group_limit: Option<usize>,
        output_window_cols: bool,
    ) -> Result<Self> {
        let context = Arc::new(WindowContext::try_new(
            input.schema(),
            window_exprs,
            partition_spec,
            order_spec,
            group_limit,
            output_window_cols,
        )?);
        Ok(Self {
            input,
            context,
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
        })
    }

    pub fn window_context(&self) -> &WindowContext {
        &self.context
    }

    pub fn with_output_window_cols(&self, output_window_cols: bool) -> Self {
        Self {
            input: self.input.clone(),
            context: Arc::new(
                WindowContext::try_new(
                    self.input.schema(),
                    self.context.window_exprs.clone(),
                    self.context.partition_spec.clone(),
                    self.context.order_spec.clone(),
                    self.context.group_limit,
                    output_window_cols,
                )
                .expect("failed to create window context"),
            ),
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
        }
    }
}

impl DisplayAs for WindowExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Window")
    }
}

impl ExecutionPlan for WindowExec {
    fn name(&self) -> &str {
        "WindowExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.context.output_schema.clone()
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
            children[0].clone(),
            self.context.window_exprs.clone(),
            self.context.partition_spec.clone(),
            self.context.order_spec.clone(),
            self.context.group_limit,
            self.context.output_window_cols,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // combine WindowGroupLimitExec -> WindowExec
        if let Ok(window_group_limit) = downcast_any!(&self.input, WindowExec)
            && window_group_limit.window_context().group_limit.is_some()
        {
            let combined = Arc::new(Self {
                input: window_group_limit.input.clone(),
                context: Arc::new(WindowContext::try_new(
                    self.input.schema(),
                    self.context.window_exprs.clone(),
                    self.context.partition_spec.clone(),
                    self.context.order_spec.clone(),
                    window_group_limit.context.group_limit,
                    true,
                )?),
                metrics: self.metrics.clone(),
                props: OnceCell::new(),
            });
            return combined.execute(partition, context);
        }

        // at this moment only supports ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);
        let input = exec_ctx.execute_with_input_stats(&self.input)?;
        let coalesced = exec_ctx.coalesce_with_default_batch_size(input);
        let window_ctx = self.context.clone();
        execute_window(coalesced, exec_ctx, window_ctx)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        todo!()
    }
}

fn execute_window(
    mut input: SendableRecordBatchStream,
    exec_ctx: Arc<ExecutionContext>,
    window_ctx: Arc<WindowContext>,
) -> Result<SendableRecordBatchStream> {
    // start processing input batches
    Ok(exec_ctx
        .clone()
        .output_with_sender("Window", |sender| async move {
            sender.exclude_time(exec_ctx.baseline_metrics().elapsed_compute());

            let mut processors = window_ctx
                .window_exprs
                .iter()
                .map(|expr: &WindowExpr| expr.create_processor(&window_ctx))
                .collect::<Result<Vec<_>>>()?;

            while let Some(mut batch) = input.next().await.transpose()? {
                let _timer = exec_ctx.baseline_metrics().elapsed_compute().timer();
                let mut window_cols: Vec<ArrayRef> = processors
                    .iter_mut()
                    .map(|processor| processor.process_batch(&window_ctx, &batch))
                    .collect::<Result<_>>()?;

                if let Some(group_limit) = window_ctx.group_limit {
                    assert_eq!(window_cols.len(), 1);
                    let limited = arrow::compute::kernels::cmp::lt_eq(
                        &window_cols[0],
                        &Int32Array::new_scalar(group_limit as i32),
                    )?;
                    window_cols[0] = arrow::compute::filter(&window_cols[0], &limited)?;
                    batch = arrow::compute::filter_record_batch(&batch, &limited)?;
                }

                let outputs: Vec<ArrayRef> = batch
                    .columns()
                    .iter()
                    .cloned()
                    .chain(if window_ctx.output_window_cols {
                        window_cols
                    } else {
                        vec![]
                    })
                    .zip(window_ctx.output_schema.fields())
                    .map(|(array, field)| {
                        if array.data_type() != field.data_type() {
                            return cast(&array, field.data_type());
                        }
                        Ok(array.clone())
                    })
                    .collect::<Result<_>>()?;
                let output_batch = RecordBatch::try_new_with_options(
                    window_ctx.output_schema.clone(),
                    outputs,
                    &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
                )?;
                exec_ctx
                    .baseline_metrics()
                    .record_output(output_batch.num_rows());
                sender.send(output_batch).await;
            }
            Ok(())
        }))
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{array::*, datatypes::*, record_batch::RecordBatch};
    use datafusion::{
        assert_batches_eq,
        physical_expr::{PhysicalSortExpr, expressions::Column},
        physical_plan::{ExecutionPlan, test::TestMemoryExec},
        prelude::SessionContext,
    };

    use crate::{
        agg::AggFunction,
        window::{WindowExpr, WindowFunction, WindowRankType},
        window_exec::WindowExec,
    };

    fn build_table_i32(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Int32, false),
            Field::new(b.0, DataType::Int32, false),
            Field::new(c.0, DataType::Int32, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(a.1.clone())),
                Arc::new(Int32Array::from(b.1.clone())),
                Arc::new(Int32Array::from(c.1.clone())),
            ],
        )
        .unwrap()
    }

    fn build_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a, b, c);
        let schema = batch.schema();
        Arc::new(TestMemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    #[tokio::test]
    async fn test_window() -> Result<(), Box<dyn std::error::Error>> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        // test window
        let input = build_table(
            ("a1", &vec![1, 1, 1, 1, 2, 3, 3]),
            ("b1", &vec![1, 2, 2, 3, 4, 1, 1]),
            ("c1", &vec![0, 0, 0, 0, 0, 0, 0]),
        );
        let window_exprs = vec![
            WindowExpr::new(
                WindowFunction::RankLike(WindowRankType::RowNumber),
                vec![],
                Arc::new(Field::new("b1_row_number", DataType::Int32, false)),
                DataType::Int32,
            ),
            WindowExpr::new(
                WindowFunction::RankLike(WindowRankType::Rank),
                vec![],
                Arc::new(Field::new("b1_rank", DataType::Int32, false)),
                DataType::Int32,
            ),
            WindowExpr::new(
                WindowFunction::RankLike(WindowRankType::DenseRank),
                vec![],
                Arc::new(Field::new("b1_dense_rank", DataType::Int32, false)),
                DataType::Int32,
            ),
            WindowExpr::new(
                WindowFunction::Agg(AggFunction::Sum),
                vec![Arc::new(Column::new("b1", 1))],
                Arc::new(Field::new("b1_sum", DataType::Int64, false)),
                DataType::Int64,
            ),
        ];
        let window = Arc::new(WindowExec::try_new(
            input.clone(),
            window_exprs.clone(),
            vec![Arc::new(Column::new("a1", 0))],
            vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("b1", 1)),
                options: Default::default(),
            }],
            None,
            true,
        )?);
        let stream = window.execute(0, task_ctx.clone())?;
        let batches = datafusion::physical_plan::common::collect(stream).await?;
        let expected = vec![
            "+----+----+----+---------------+---------+---------------+--------+",
            "| a1 | b1 | c1 | b1_row_number | b1_rank | b1_dense_rank | b1_sum |",
            "+----+----+----+---------------+---------+---------------+--------+",
            "| 1  | 1  | 0  | 1             | 1       | 1             | 1      |",
            "| 1  | 2  | 0  | 2             | 2       | 2             | 3      |",
            "| 1  | 2  | 0  | 3             | 2       | 2             | 5      |",
            "| 1  | 3  | 0  | 4             | 4       | 3             | 8      |",
            "| 2  | 4  | 0  | 1             | 1       | 1             | 4      |",
            "| 3  | 1  | 0  | 1             | 1       | 1             | 1      |",
            "| 3  | 1  | 0  | 2             | 1       | 1             | 2      |",
            "+----+----+----+---------------+---------+---------------+--------+",
        ];
        assert_batches_eq!(expected, &batches);

        // test window without partition by clause
        let input = build_table(
            ("a1", &vec![1, 3, 3, 1, 1, 1, 2]),
            ("b1", &vec![1, 1, 1, 2, 2, 3, 4]),
            ("c1", &vec![0, 0, 0, 0, 0, 0, 0]),
        );
        let window_exprs = vec![
            WindowExpr::new(
                WindowFunction::RankLike(WindowRankType::RowNumber),
                vec![],
                Arc::new(Field::new("b1_row_number", DataType::Int32, false)),
                DataType::Int32,
            ),
            WindowExpr::new(
                WindowFunction::RankLike(WindowRankType::Rank),
                vec![],
                Arc::new(Field::new("b1_rank", DataType::Int32, false)),
                DataType::Int32,
            ),
            WindowExpr::new(
                WindowFunction::RankLike(WindowRankType::DenseRank),
                vec![],
                Arc::new(Field::new("b1_dense_rank", DataType::Int32, false)),
                DataType::Int32,
            ),
            WindowExpr::new(
                WindowFunction::Agg(AggFunction::Sum),
                vec![Arc::new(Column::new("b1", 1))],
                Arc::new(Field::new("b1_sum", DataType::Int64, false)),
                DataType::Int64,
            ),
        ];
        let window = Arc::new(WindowExec::try_new(
            input.clone(),
            window_exprs.clone(),
            vec![],
            vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("b1", 1)),
                options: Default::default(),
            }],
            None,
            true,
        )?);
        let stream = window.execute(0, task_ctx.clone())?;
        let batches = datafusion::physical_plan::common::collect(stream).await?;
        let expected = vec![
            "+----+----+----+---------------+---------+---------------+--------+",
            "| a1 | b1 | c1 | b1_row_number | b1_rank | b1_dense_rank | b1_sum |",
            "+----+----+----+---------------+---------+---------------+--------+",
            "| 1  | 1  | 0  | 1             | 1       | 1             | 1      |",
            "| 3  | 1  | 0  | 2             | 1       | 1             | 2      |",
            "| 3  | 1  | 0  | 3             | 1       | 1             | 3      |",
            "| 1  | 2  | 0  | 4             | 4       | 2             | 5      |",
            "| 1  | 2  | 0  | 5             | 4       | 2             | 7      |",
            "| 1  | 3  | 0  | 6             | 6       | 3             | 10     |",
            "| 2  | 4  | 0  | 7             | 7       | 4             | 14     |",
            "+----+----+----+---------------+---------+---------------+--------+",
        ];
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn test_window_group_limit() -> Result<(), Box<dyn std::error::Error>> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        // test window
        let input = build_table(
            ("a1", &vec![1, 1, 1, 1, 2, 3, 3]),
            ("b1", &vec![1, 2, 2, 3, 4, 1, 1]),
            ("c1", &vec![0, 0, 0, 0, 0, 0, 0]),
        );
        let window_exprs = vec![WindowExpr::new(
            WindowFunction::RankLike(WindowRankType::RowNumber),
            vec![],
            Arc::new(Field::new("b1_row_number", DataType::Int32, false)),
            DataType::Int32,
        )];
        let window = Arc::new(WindowExec::try_new(
            input.clone(),
            window_exprs.clone(),
            vec![Arc::new(Column::new("a1", 0))],
            vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("b1", 1)),
                options: Default::default(),
            }],
            Some(2),
            true,
        )?);
        let stream = window.execute(0, task_ctx.clone())?;
        let batches = datafusion::physical_plan::common::collect(stream).await?;
        let expected = vec![
            "+----+----+----+---------------+",
            "| a1 | b1 | c1 | b1_row_number |",
            "+----+----+----+---------------+",
            "| 1  | 1  | 0  | 1             |",
            "| 1  | 2  | 0  | 2             |",
            "| 2  | 4  | 0  | 1             |",
            "| 3  | 1  | 0  | 1             |",
            "| 3  | 1  | 0  | 2             |",
            "+----+----+----+---------------+",
        ];
        assert_batches_eq!(expected, &batches);
        Ok(())
    }
}
