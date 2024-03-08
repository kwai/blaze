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

use std::{any::Any, fmt::Formatter, sync::Arc, time::Duration};

use arrow::{compute::SortOptions, datatypes::SchemaRef};
use datafusion::{
    error::Result,
    execution::context::TaskContext,
    logical_expr::JoinType,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        joins::{
            utils::{JoinFilter, JoinOn},
            SortMergeJoinExec as DFSortMergeJoinExec,
        },
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
        Statistics,
    },
};
use datafusion_ext_commons::downcast_any;
use futures::{stream::once, StreamExt, TryStreamExt};

use crate::common::output::TaskOutputter;

#[derive(Debug)]
pub struct SortMergeJoinExec {
    delegated: Arc<dyn ExecutionPlan>,
    metrics: ExecutionPlanMetricsSet,
}

impl SortMergeJoinExec {
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
        join_filter: Option<JoinFilter>,
        sort_options: Vec<SortOptions>,
    ) -> Result<Self> {
        let delegated = Arc::new(DFSortMergeJoinExec::try_new(
            left,
            right,
            on,
            join_filter,
            join_type,
            sort_options,
            false,
        )?);

        Ok(Self {
            delegated,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn delegated(&self) -> &DFSortMergeJoinExec {
        downcast_any!(self.delegated, DFSortMergeJoinExec).unwrap()
    }
}

impl DisplayAs for SortMergeJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "SortMergeJoin: join_type={:?}, on={:?}, schema={:?}",
            self.delegated().join_type,
            self.delegated().on,
            self.delegated().schema(),
        )
    }
}

impl ExecutionPlan for SortMergeJoinExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.delegated().schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.delegated().output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.delegated().output_ordering() // same with spark's
                                           // SortMergeJoinExec implementation
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![
            self.delegated().left.clone(),
            self.delegated().right.clone(),
        ]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let delegated = self.delegated.clone().with_new_children(children)?;
        Ok(Arc::new(Self {
            delegated,
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let metrics = BaselineMetrics::new(&self.metrics, partition);
        let output = Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            once(execute_delegated(
                partition,
                context,
                self.delegated.clone(),
                metrics,
            ))
            .try_flatten(),
        ));
        Ok(output)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.delegated().statistics()
    }
}

async fn execute_delegated(
    partition: usize,
    context: Arc<TaskContext>,
    delegated: Arc<dyn ExecutionPlan>,
    metrics: BaselineMetrics,
) -> Result<SendableRecordBatchStream> {
    let mut output_stream = delegated.execute(partition, context.clone())?;
    context.output_with_sender(
        "SortMergeJoin",
        delegated.schema(),
        move |sender| async move {
            while let Some(batch) = output_stream.next().await.transpose()? {
                sender.send(Ok(batch), None).await;
            }

            // copy delegated metrics to plan
            if let Some(delegated_metrics) = delegated.metrics() {
                metrics.record_output(delegated_metrics.output_rows().unwrap_or_default());
                metrics.elapsed_compute().add_duration(Duration::from_nanos(
                    delegated_metrics.elapsed_compute().unwrap_or_default() as u64,
                ));
            }
            Ok(())
        },
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        self,
        array::*,
        compute::SortOptions,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::{
        assert_batches_sorted_eq,
        error::Result,
        logical_expr::{JoinType, JoinType::*},
        physical_expr::expressions::Column,
        physical_plan::{common, joins::utils::*, memory::MemoryExec, ExecutionPlan},
        prelude::SessionContext,
    };

    use crate::sort_merge_join_exec::SortMergeJoinExec;

    fn columns(schema: &Schema) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }

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

    fn build_table_from_batches(batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
        let schema = batches.first().unwrap().schema();
        Arc::new(MemoryExec::try_new(&[batches], schema, None).unwrap())
    }

    fn build_date_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Date32, false),
            Field::new(b.0, DataType::Date32, false),
            Field::new(c.0, DataType::Date32, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Date32Array::from(a.1.clone())),
                Arc::new(Date32Array::from(b.1.clone())),
                Arc::new(Date32Array::from(c.1.clone())),
            ],
        )
        .unwrap();

        let schema = batch.schema();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    fn build_date64_table(
        a: (&str, &Vec<i64>),
        b: (&str, &Vec<i64>),
        c: (&str, &Vec<i64>),
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Date64, false),
            Field::new(b.0, DataType::Date64, false),
            Field::new(c.0, DataType::Date64, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Date64Array::from(a.1.clone())),
                Arc::new(Date64Array::from(b.1.clone())),
                Arc::new(Date64Array::from(c.1.clone())),
            ],
        )
        .unwrap();

        let schema = batch.schema();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    /// returns a table with 3 columns of i32 in memory
    pub fn build_table_i32_nullable(
        a: (&str, &Vec<Option<i32>>),
        b: (&str, &Vec<Option<i32>>),
        c: (&str, &Vec<Option<i32>>),
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(a.0, DataType::Int32, true),
            Field::new(b.0, DataType::Int32, true),
            Field::new(c.0, DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(a.1.clone())),
                Arc::new(Int32Array::from(b.1.clone())),
                Arc::new(Int32Array::from(c.1.clone())),
            ],
        )
        .unwrap();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    fn join_with_options(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
        sort_options: Vec<SortOptions>,
    ) -> Result<SortMergeJoinExec> {
        SortMergeJoinExec::try_new(left, right, on, join_type, None, sort_options)
    }

    async fn join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let sort_options = vec![SortOptions::default(); on.len()];
        join_collect_with_options(left, right, on, join_type, sort_options).await
    }

    async fn join_collect_with_options(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
        sort_options: Vec<SortOptions>,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let join = join_with_options(left, right, on, join_type, sort_options)?;
        let columns = columns(&join.schema());

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;
        Ok((columns, batches))
    }

    #[tokio::test]
    async fn join_inner_one() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 5]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );

        let on: JoinOn = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?),
            Arc::new(Column::new_with_schema("b1", &right.schema())?),
        )];

        let (_, batches) = join_collect(left, right, on, Inner).await?;

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 5  | 9  | 20 | 5  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_two() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2]),
            ("b2", &vec![1, 2, 2]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b2", &vec![1, 2, 2]),
            ("c2", &vec![70, 80, 90]),
        );
        let on: JoinOn = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?),
                Arc::new(Column::new_with_schema("a1", &right.schema())?),
            ),
            (
                Arc::new(Column::new_with_schema("b2", &left.schema())?),
                Arc::new(Column::new_with_schema("b2", &right.schema())?),
            ),
        ];

        let (_columns, batches) = join_collect(left, right, on, Inner).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b2 | c1 | a1 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 1  | 7  | 1  | 1  | 70 |",
            "| 2  | 2  | 8  | 2  | 2  | 80 |",
            "| 2  | 2  | 9  | 2  | 2  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_two_two() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 1, 2]),
            ("b2", &vec![1, 1, 2]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a1", &vec![1, 1, 3]),
            ("b2", &vec![1, 1, 2]),
            ("c2", &vec![70, 80, 90]),
        );
        let on: JoinOn = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?),
                Arc::new(Column::new_with_schema("a1", &right.schema())?),
            ),
            (
                Arc::new(Column::new_with_schema("b2", &left.schema())?),
                Arc::new(Column::new_with_schema("b2", &right.schema())?),
            ),
        ];

        let (_columns, batches) = join_collect(left, right, on, Inner).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b2 | c1 | a1 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 1  | 7  | 1  | 1  | 70 |",
            "| 1  | 1  | 7  | 1  | 1  | 80 |",
            "| 1  | 1  | 8  | 1  | 1  | 70 |",
            "| 1  | 1  | 8  | 1  | 1  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_with_nulls() -> Result<()> {
        let left = build_table_i32_nullable(
            ("a1", &vec![Some(1), Some(1), Some(2), Some(2)]),
            ("b2", &vec![None, Some(1), Some(2), Some(2)]), // null in key field
            ("c1", &vec![Some(1), None, Some(8), Some(9)]), // null in non-key field
        );
        let right = build_table_i32_nullable(
            ("a1", &vec![Some(1), Some(1), Some(2), Some(3)]),
            ("b2", &vec![None, Some(1), Some(2), Some(2)]),
            ("c2", &vec![Some(10), Some(70), Some(80), Some(90)]),
        );
        let on: JoinOn = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?),
                Arc::new(Column::new_with_schema("a1", &right.schema())?),
            ),
            (
                Arc::new(Column::new_with_schema("b2", &left.schema())?),
                Arc::new(Column::new_with_schema("b2", &right.schema())?),
            ),
        ];

        let (_, batches) = join_collect(left, right, on, Inner).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b2 | c1 | a1 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 1  |    | 1  | 1  | 70 |",
            "| 2  | 2  | 8  | 2  | 2  | 80 |",
            "| 2  | 2  | 9  | 2  | 2  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_with_nulls_with_options() -> Result<()> {
        let left = build_table_i32_nullable(
            ("a1", &vec![Some(2), Some(2), Some(1), Some(1)]),
            ("b2", &vec![Some(2), Some(2), Some(1), None]), // null in key field
            ("c1", &vec![Some(9), Some(8), None, Some(1)]), // null in non-key field
        );
        let right = build_table_i32_nullable(
            ("a1", &vec![Some(3), Some(2), Some(1), Some(1)]),
            ("b2", &vec![Some(2), Some(2), Some(1), None]),
            ("c2", &vec![Some(90), Some(80), Some(70), Some(10)]),
        );
        let on: JoinOn = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?),
                Arc::new(Column::new_with_schema("a1", &right.schema())?),
            ),
            (
                Arc::new(Column::new_with_schema("b2", &left.schema())?),
                Arc::new(Column::new_with_schema("b2", &right.schema())?),
            ),
        ];
        let (_, batches) = join_collect_with_options(
            left,
            right,
            on,
            Inner,
            vec![
                SortOptions {
                    descending: true,
                    nulls_first: false
                };
                2
            ],
            // null_equals_null=false
        )
        .await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b2 | c1 | a1 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 2  | 2  | 9  | 2  | 2  | 80 |",
            "| 2  | 2  | 8  | 2  | 2  | 80 |",
            "| 1  | 1  |    | 1  | 1  | 70 |",
            "+----+----+----+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_left_one() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on: JoinOn = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?),
            Arc::new(Column::new_with_schema("b1", &right.schema())?),
        )];

        let (_, batches) = join_collect(left, right, on, Left).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_one() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]), // 6 does not exist on the left
            ("c2", &vec![70, 80, 90]),
        );
        let on: JoinOn = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?),
            Arc::new(Column::new_with_schema("b1", &right.schema())?),
        )];

        let (_, batches) = join_collect(left, right, on, Right).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "|    |    |    | 30 | 6  | 90 |",
            "+----+----+----+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_full_one() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b2", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on: JoinOn = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?),
            Arc::new(Column::new_with_schema("b2", &right.schema())?),
        )];

        let (_, batches) = join_collect(left, right, on, Full).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "|    |    |    | 30 | 6  | 90 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_anti() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2, 3, 5]),
            ("b1", &vec![4, 5, 5, 7, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 8, 9, 11]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on: JoinOn = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?),
            Arc::new(Column::new_with_schema("b1", &right.schema())?),
        )];

        let (_, batches) = join_collect(left, right, on, LeftAnti).await?;
        let expected = vec![
            "+----+----+----+",
            "| a1 | b1 | c1 |",
            "+----+----+----+",
            "| 3  | 7  | 9  |",
            "| 5  | 7  | 11 |",
            "+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_semi() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2, 3]),
            ("b1", &vec![4, 5, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]), // 5 is double on the right
            ("c2", &vec![70, 80, 90]),
        );
        let on: JoinOn = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?),
            Arc::new(Column::new_with_schema("b1", &right.schema())?),
        )];

        let (_, batches) = join_collect(left, right, on, LeftSemi).await?;
        let expected = vec![
            "+----+----+----+",
            "| a1 | b1 | c1 |",
            "+----+----+----+",
            "| 1  | 4  | 7  |",
            "| 2  | 5  | 8  |",
            "| 2  | 5  | 8  |",
            "+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_with_duplicated_column_names() -> Result<()> {
        let left = build_table(
            ("a", &vec![1, 2, 3]),
            ("b", &vec![4, 5, 7]),
            ("c", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a", &vec![10, 20, 30]),
            ("b", &vec![1, 2, 7]),
            ("c", &vec![70, 80, 90]),
        );
        let on: JoinOn = vec![(
            // join on a=b so there are duplicate column names on unjoined columns
            Arc::new(Column::new_with_schema("a", &left.schema())?),
            Arc::new(Column::new_with_schema("b", &right.schema())?),
        )];

        let (_, batches) = join_collect(left, right, on, Inner).await?;
        let expected = vec![
            "+---+---+---+----+---+----+",
            "| a | b | c | a  | b | c  |",
            "+---+---+---+----+---+----+",
            "| 1 | 4 | 7 | 10 | 1 | 70 |",
            "| 2 | 5 | 8 | 20 | 2 | 80 |",
            "+---+---+---+----+---+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_date32() -> Result<()> {
        let left = build_date_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![19107, 19108, 19108]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_date_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![19107, 19108, 19109]),
            ("c2", &vec![70, 80, 90]),
        );

        let on: JoinOn = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?),
            Arc::new(Column::new_with_schema("b1", &right.schema())?),
        )];

        let (_, batches) = join_collect(left, right, on, Inner).await?;

        let expected = vec![
            "+------------+------------+------------+------------+------------+------------+",
            "| a1         | b1         | c1         | a2         | b1         | c2         |",
            "+------------+------------+------------+------------+------------+------------+",
            "| 1970-01-02 | 2022-04-25 | 1970-01-08 | 1970-01-11 | 2022-04-25 | 1970-03-12 |",
            "| 1970-01-03 | 2022-04-26 | 1970-01-09 | 1970-01-21 | 2022-04-26 | 1970-03-22 |",
            "| 1970-01-04 | 2022-04-26 | 1970-01-10 | 1970-01-21 | 2022-04-26 | 1970-03-22 |",
            "+------------+------------+------------+------------+------------+------------+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_date64() -> Result<()> {
        let left = build_date64_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1650703441000, 1650903441000, 1650903441000]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_date64_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![1650703441000, 1650503441000, 1650903441000]),
            ("c2", &vec![70, 80, 90]),
        );

        let on: JoinOn = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?),
            Arc::new(Column::new_with_schema("b1", &right.schema())?),
        )];

        let (_, batches) = join_collect(left, right, on, Inner).await?;
        let expected = vec![
            "+-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+",
            "| a1                      | b1                  | c1                      | a2                      | b1                  | c2                      |",
            "+-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+",
            "| 1970-01-01T00:00:00.001 | 2022-04-23T08:44:01 | 1970-01-01T00:00:00.007 | 1970-01-01T00:00:00.010 | 2022-04-23T08:44:01 | 1970-01-01T00:00:00.070 |",
            "| 1970-01-01T00:00:00.002 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.008 | 1970-01-01T00:00:00.030 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.090 |",
            "| 1970-01-01T00:00:00.003 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.009 | 1970-01-01T00:00:00.030 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.090 |",
            "+-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+",
        ];

        // The output order is important as SMJ preserves sortedness
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_left_sort_order() -> Result<()> {
        let left = build_table(
            ("a1", &vec![0, 1, 2, 3, 4, 5]),
            ("b1", &vec![3, 4, 5, 6, 6, 7]),
            ("c1", &vec![4, 5, 6, 7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![0, 10, 20, 30, 40]),
            ("b2", &vec![2, 4, 6, 6, 8]),
            ("c2", &vec![50, 60, 70, 80, 90]),
        );
        let on: JoinOn = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?),
            Arc::new(Column::new_with_schema("b2", &right.schema())?),
        )];

        let (_, batches) = join_collect(left, right, on, Left).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 0  | 3  | 4  |    |    |    |",
            "| 1  | 4  | 5  | 10 | 4  | 60 |",
            "| 2  | 5  | 6  |    |    |    |",
            "| 3  | 6  | 7  | 20 | 6  | 70 |",
            "| 3  | 6  | 7  | 30 | 6  | 80 |",
            "| 4  | 6  | 8  | 20 | 6  | 70 |",
            "| 4  | 6  | 8  | 30 | 6  | 80 |",
            "| 5  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_sort_order() -> Result<()> {
        let left = build_table(
            ("a1", &vec![0, 1, 2, 3]),
            ("b1", &vec![3, 4, 5, 7]),
            ("c1", &vec![6, 7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![0, 10, 20, 30]),
            ("b2", &vec![2, 4, 5, 6]),
            ("c2", &vec![60, 70, 80, 90]),
        );
        let on: JoinOn = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?),
            Arc::new(Column::new_with_schema("b2", &right.schema())?),
        )];

        let (_, batches) = join_collect(left, right, on, Right).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "|    |    |    | 0  | 2  | 60 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "|    |    |    | 30 | 6  | 90 |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_left_multiple_batches() -> Result<()> {
        let left_batch_1 = build_table_i32(
            ("a1", &vec![0, 1, 2]),
            ("b1", &vec![3, 4, 5]),
            ("c1", &vec![4, 5, 6]),
        );
        let left_batch_2 = build_table_i32(
            ("a1", &vec![3, 4, 5, 6]),
            ("b1", &vec![6, 6, 7, 9]),
            ("c1", &vec![7, 8, 9, 9]),
        );
        let right_batch_1 = build_table_i32(
            ("a2", &vec![0, 10, 20]),
            ("b2", &vec![2, 4, 6]),
            ("c2", &vec![50, 60, 70]),
        );
        let right_batch_2 = build_table_i32(
            ("a2", &vec![30, 40]),
            ("b2", &vec![6, 8]),
            ("c2", &vec![80, 90]),
        );
        let left = build_table_from_batches(vec![left_batch_1, left_batch_2]);
        let right = build_table_from_batches(vec![right_batch_1, right_batch_2]);
        let on: JoinOn = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?),
            Arc::new(Column::new_with_schema("b2", &right.schema())?),
        )];

        let (_, batches) = join_collect(left, right, on, Left).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 0  | 3  | 4  |    |    |    |",
            "| 1  | 4  | 5  | 10 | 4  | 60 |",
            "| 2  | 5  | 6  |    |    |    |",
            "| 3  | 6  | 7  | 20 | 6  | 70 |",
            "| 3  | 6  | 7  | 30 | 6  | 80 |",
            "| 4  | 6  | 8  | 20 | 6  | 70 |",
            "| 4  | 6  | 8  | 30 | 6  | 80 |",
            "| 5  | 7  | 9  |    |    |    |",
            "| 6  | 9  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_multiple_batches() -> Result<()> {
        let right_batch_1 = build_table_i32(
            ("a2", &vec![0, 1, 2]),
            ("b2", &vec![3, 4, 5]),
            ("c2", &vec![4, 5, 6]),
        );
        let right_batch_2 = build_table_i32(
            ("a2", &vec![3, 4, 5, 6]),
            ("b2", &vec![6, 6, 7, 9]),
            ("c2", &vec![7, 8, 9, 9]),
        );
        let left_batch_1 = build_table_i32(
            ("a1", &vec![0, 10, 20]),
            ("b1", &vec![2, 4, 6]),
            ("c1", &vec![50, 60, 70]),
        );
        let left_batch_2 = build_table_i32(
            ("a1", &vec![30, 40]),
            ("b1", &vec![6, 8]),
            ("c1", &vec![80, 90]),
        );
        let left = build_table_from_batches(vec![left_batch_1, left_batch_2]);
        let right = build_table_from_batches(vec![right_batch_1, right_batch_2]);
        let on: JoinOn = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?),
            Arc::new(Column::new_with_schema("b2", &right.schema())?),
        )];

        let (_, batches) = join_collect(left, right, on, Right).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "|    |    |    | 0  | 3  | 4  |",
            "| 10 | 4  | 60 | 1  | 4  | 5  |",
            "|    |    |    | 2  | 5  | 6  |",
            "| 20 | 6  | 70 | 3  | 6  | 7  |",
            "| 30 | 6  | 80 | 3  | 6  | 7  |",
            "| 20 | 6  | 70 | 4  | 6  | 8  |",
            "| 30 | 6  | 80 | 4  | 6  | 8  |",
            "|    |    |    | 5  | 7  | 9  |",
            "|    |    |    | 6  | 9  | 9  |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_full_multiple_batches() -> Result<()> {
        let left_batch_1 = build_table_i32(
            ("a1", &vec![0, 1, 2]),
            ("b1", &vec![3, 4, 5]),
            ("c1", &vec![4, 5, 6]),
        );
        let left_batch_2 = build_table_i32(
            ("a1", &vec![3, 4, 5, 6]),
            ("b1", &vec![6, 6, 7, 9]),
            ("c1", &vec![7, 8, 9, 9]),
        );
        let right_batch_1 = build_table_i32(
            ("a2", &vec![0, 10, 20]),
            ("b2", &vec![2, 4, 6]),
            ("c2", &vec![50, 60, 70]),
        );
        let right_batch_2 = build_table_i32(
            ("a2", &vec![30, 40]),
            ("b2", &vec![6, 8]),
            ("c2", &vec![80, 90]),
        );
        let left = build_table_from_batches(vec![left_batch_1, left_batch_2]);
        let right = build_table_from_batches(vec![right_batch_1, right_batch_2]);
        let on: JoinOn = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?),
            Arc::new(Column::new_with_schema("b2", &right.schema())?),
        )];

        let (_, batches) = join_collect(left, right, on, Full).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "|    |    |    | 0  | 2  | 50 |",
            "|    |    |    | 40 | 8  | 90 |",
            "| 0  | 3  | 4  |    |    |    |",
            "| 1  | 4  | 5  | 10 | 4  | 60 |",
            "| 2  | 5  | 6  |    |    |    |",
            "| 3  | 6  | 7  | 20 | 6  | 70 |",
            "| 3  | 6  | 7  | 30 | 6  | 80 |",
            "| 4  | 6  | 8  | 20 | 6  | 70 |",
            "| 4  | 6  | 8  | 30 | 6  | 80 |",
            "| 5  | 7  | 9  |    |    |    |",
            "| 6  | 9  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }
}
