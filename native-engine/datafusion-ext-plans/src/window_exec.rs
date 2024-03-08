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

use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow::{
    array::{Array, ArrayRef},
    datatypes::SchemaRef,
    error::ArrowError,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use datafusion::{
    common::{Result, Statistics},
    execution::context::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr,
        SendableRecordBatchStream,
    },
};
use datafusion_ext_commons::{cast::cast, streams::coalesce_stream::CoalesceInput};
use futures::{stream::once, StreamExt, TryFutureExt, TryStreamExt};

use crate::{
    common::output::TaskOutputter,
    window::{window_context::WindowContext, WindowExpr, WindowFunctionProcessor},
};

#[derive(Debug)]
pub struct WindowExec {
    input: Arc<dyn ExecutionPlan>,
    context: Arc<WindowContext>,
    metrics: ExecutionPlanMetricsSet,
}

impl WindowExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        window_exprs: Vec<WindowExpr>,
        partition_spec: Vec<Arc<dyn PhysicalExpr>>,
        order_spec: Vec<PhysicalSortExpr>,
    ) -> Result<Self> {
        let context = Arc::new(WindowContext::try_new(
            input.schema(),
            window_exprs,
            partition_spec,
            order_spec,
        )?);
        Ok(Self {
            input,
            context,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for WindowExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Window")
    }
}

impl ExecutionPlan for WindowExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.context.output_schema.clone()
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
        Ok(Arc::new(Self::try_new(
            children[0].clone(),
            self.context.window_exprs.clone(),
            self.context.partition_spec.clone(),
            self.context.order_spec.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // at this moment only supports ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        let input = self.input.execute(partition, context.clone())?;
        let coalesced = context.coalesce_with_default_batch_size(
            input,
            &BaselineMetrics::new(&self.metrics, partition),
        )?;

        let stream = execute_window(
            coalesced,
            context.clone(),
            self.context.clone(),
            BaselineMetrics::new(&self.metrics, partition),
        )
        .map_err(|e| ArrowError::ExternalError(Box::new(e)));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            once(stream).try_flatten(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        todo!()
    }
}

async fn execute_window(
    mut input: SendableRecordBatchStream,
    task_context: Arc<TaskContext>,
    context: Arc<WindowContext>,
    metrics: BaselineMetrics,
) -> Result<SendableRecordBatchStream> {
    let mut processors: Vec<Box<dyn WindowFunctionProcessor>> = context
        .window_exprs
        .iter()
        .map(|expr: &WindowExpr| expr.create_processor(&context))
        .collect::<Result<_>>()?;

    // start processing input batches
    let output_schema = context.output_schema.clone();
    task_context.output_with_sender("Window", output_schema, |sender| async move {
        while let Some(batch) = input.next().await.transpose()? {
            let elapsed_time = metrics.elapsed_compute().clone();
            let mut timer = elapsed_time.timer();

            let window_cols: Vec<ArrayRef> = processors
                .iter_mut()
                .map(|processor| {
                    if context.partition_spec.is_empty() {
                        processor.process_batch_without_partitions(&context, &batch)
                    } else {
                        processor.process_batch(&context, &batch)
                    }
                })
                .collect::<Result<_>>()?;

            let outputs: Vec<ArrayRef> = batch
                .columns()
                .iter()
                .chain(&window_cols)
                .zip(context.output_schema.fields())
                .map(|(array, field)| {
                    if array.data_type() != field.data_type() {
                        return cast(&array, field.data_type());
                    }
                    Ok(array.clone())
                })
                .collect::<Result<_>>()?;
            let output_batch = RecordBatch::try_new_with_options(
                context.output_schema.clone(),
                outputs,
                &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
            )?;

            metrics.record_output(output_batch.num_rows());
            sender.send(Ok(output_batch), Some(&mut timer)).await;
        }
        Ok(())
    })
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{array::*, datatypes::*, record_batch::RecordBatch};
    use datafusion::{
        assert_batches_eq,
        physical_expr::{expressions::Column, PhysicalSortExpr},
        physical_plan::{memory::MemoryExec, ExecutionPlan},
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
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
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
        let window = Arc::new(WindowExec::try_new(
            input,
            vec![
                WindowExpr::new(
                    WindowFunction::RankLike(WindowRankType::RowNumber),
                    vec![],
                    Arc::new(Field::new("b1_row_number", DataType::Int32, false)),
                ),
                WindowExpr::new(
                    WindowFunction::RankLike(WindowRankType::Rank),
                    vec![],
                    Arc::new(Field::new("b1_rank", DataType::Int32, false)),
                ),
                WindowExpr::new(
                    WindowFunction::RankLike(WindowRankType::DenseRank),
                    vec![],
                    Arc::new(Field::new("b1_dense_rank", DataType::Int32, false)),
                ),
                WindowExpr::new(
                    WindowFunction::Agg(AggFunction::Sum),
                    vec![Arc::new(Column::new("b1", 1))],
                    Arc::new(Field::new("b1_sum", DataType::Int64, false)),
                ),
            ],
            vec![Arc::new(Column::new("a1", 0))],
            vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("b1", 1)),
                options: Default::default(),
            }],
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
        let window = Arc::new(WindowExec::try_new(
            input,
            vec![
                WindowExpr::new(
                    WindowFunction::RankLike(WindowRankType::RowNumber),
                    vec![],
                    Arc::new(Field::new("b1_row_number", DataType::Int32, false)),
                ),
                WindowExpr::new(
                    WindowFunction::RankLike(WindowRankType::Rank),
                    vec![],
                    Arc::new(Field::new("b1_rank", DataType::Int32, false)),
                ),
                WindowExpr::new(
                    WindowFunction::RankLike(WindowRankType::DenseRank),
                    vec![],
                    Arc::new(Field::new("b1_dense_rank", DataType::Int32, false)),
                ),
                WindowExpr::new(
                    WindowFunction::Agg(AggFunction::Sum),
                    vec![Arc::new(Column::new("b1", 1))],
                    Arc::new(Field::new("b1_sum", DataType::Int64, false)),
                ),
            ],
            vec![],
            vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("b1", 1)),
                options: Default::default(),
            }],
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
}
