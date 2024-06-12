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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        self,
        array::*,
        compute::SortOptions,
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    };
    use datafusion::{
        assert_batches_sorted_eq,
        common::JoinSide,
        error::Result,
        physical_expr::expressions::Column,
        physical_plan::{common, joins::utils::*, memory::MemoryExec, ExecutionPlan},
        prelude::SessionContext,
    };
    use TestType::*;

    use crate::{
        broadcast_join_build_hash_map_exec::BroadcastJoinBuildHashMapExec,
        broadcast_join_exec::BroadcastJoinExec,
        joins::join_utils::{JoinType, JoinType::*},
        sort_merge_join_exec::SortMergeJoinExec,
    };

    #[derive(Clone, Copy)]
    enum TestType {
        SMJ,
        BHJLeftProbed,
        BHJRightProbed,
    }

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

    fn build_join_schema_for_test(
        left: &Schema,
        right: &Schema,
        join_type: JoinType,
    ) -> Result<SchemaRef> {
        if join_type == Existence {
            let exists_field = Arc::new(Field::new("exists#0", DataType::Boolean, false));
            return Ok(Arc::new(Schema::new(
                [left.fields().to_vec(), vec![exists_field]].concat(),
            )));
        }
        Ok(Arc::new(
            build_join_schema(left, right, &join_type.try_into()?).0,
        ))
    }

    async fn join_collect(
        test_type: TestType,
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let schema = build_join_schema_for_test(&left.schema(), &right.schema(), join_type)?;

        let join: Arc<dyn ExecutionPlan> = match test_type {
            SMJ => {
                let sort_options = vec![SortOptions::default(); on.len()];
                Arc::new(SortMergeJoinExec::try_new(
                    schema,
                    left,
                    right,
                    on,
                    join_type,
                    sort_options,
                )?)
            }
            BHJLeftProbed => {
                let right = Arc::new(BroadcastJoinBuildHashMapExec::new(
                    right,
                    on.iter().map(|(_, right_key)| right_key.clone()).collect(),
                ));
                Arc::new(BroadcastJoinExec::try_new(
                    schema,
                    left,
                    right,
                    on,
                    join_type,
                    JoinSide::Right,
                    None,
                )?)
            }
            BHJRightProbed => {
                let left = Arc::new(BroadcastJoinBuildHashMapExec::new(
                    left,
                    on.iter().map(|(left_key, _)| left_key.clone()).collect(),
                ));
                Arc::new(BroadcastJoinExec::try_new(
                    schema,
                    left,
                    right,
                    on,
                    join_type,
                    JoinSide::Left,
                    None,
                )?)
            }
        };
        let columns = columns(&join.schema());
        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;
        Ok((columns, batches))
    }

    #[tokio::test]
    async fn join_inner_one() -> Result<()> {
        for test_type in [SMJ, BHJLeftProbed, BHJRightProbed] {
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

            let (_, batches) = join_collect(test_type, left, right, on, Inner).await?;
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
        }
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_two() -> Result<()> {
        for test_type in [SMJ, BHJLeftProbed, BHJRightProbed] {
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

            let (_columns, batches) = join_collect(test_type, left, right, on, Inner).await?;
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
        }
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_two_two() -> Result<()> {
        for test_type in [SMJ, BHJLeftProbed, BHJRightProbed] {
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

            let (_columns, batches) = join_collect(test_type, left, right, on, Inner).await?;
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
        }
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_with_nulls() -> Result<()> {
        for test_type in [SMJ, BHJLeftProbed, BHJRightProbed] {
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

            let (_, batches) = join_collect(test_type, left, right, on, Inner).await?;
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
        }
        Ok(())
    }

    #[tokio::test]
    async fn join_left_one() -> Result<()> {
        for test_type in [SMJ, BHJLeftProbed, BHJRightProbed] {
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

            let (_, batches) = join_collect(test_type, left, right, on, Left).await?;
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
        }
        Ok(())
    }

    #[tokio::test]
    async fn join_right_one() -> Result<()> {
        for test_type in [SMJ, BHJLeftProbed, BHJRightProbed] {
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

            let (_, batches) = join_collect(test_type, left, right, on, Right).await?;
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
        }
        Ok(())
    }

    #[tokio::test]
    async fn join_full_one() -> Result<()> {
        for test_type in [SMJ, BHJLeftProbed, BHJRightProbed] {
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

            let (_, batches) = join_collect(test_type, left, right, on, Full).await?;
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
        }
        Ok(())
    }

    #[tokio::test]
    async fn join_anti() -> Result<()> {
        for test_type in [SMJ, BHJLeftProbed, BHJRightProbed] {
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

            let (_, batches) = join_collect(test_type, left, right, on, LeftAnti).await?;
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
        }
        Ok(())
    }

    #[tokio::test]
    async fn join_semi() -> Result<()> {
        for test_type in [SMJ, BHJLeftProbed, BHJRightProbed] {
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

            let (_, batches) = join_collect(test_type, left, right, on, LeftSemi).await?;
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
        }
        Ok(())
    }

    #[tokio::test]
    async fn join_with_duplicated_column_names() -> Result<()> {
        for test_type in [SMJ, BHJLeftProbed, BHJRightProbed] {
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

            let (_, batches) = join_collect(test_type, left, right, on, Inner).await?;
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
        }
        Ok(())
    }

    #[tokio::test]
    async fn join_date32() -> Result<()> {
        for test_type in [SMJ, BHJLeftProbed, BHJRightProbed] {
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

            let (_, batches) = join_collect(test_type, left, right, on, Inner).await?;

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
        }
        Ok(())
    }

    #[tokio::test]
    async fn join_date64() -> Result<()> {
        for test_type in [SMJ, BHJLeftProbed, BHJRightProbed] {
            let left = build_date64_table(
                ("a1", &vec![1, 2, 3]),
                ("b1", &vec![1650703441000, 1650903441000, 1650903441000]), /* this has a
                                                                             * repetition */
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

            let (_, batches) = join_collect(test_type, left, right, on, Inner).await?;
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
        }
        Ok(())
    }

    #[tokio::test]
    async fn join_left_sort_order() -> Result<()> {
        for test_type in [SMJ, BHJLeftProbed, BHJRightProbed] {
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

            let (_, batches) = join_collect(test_type, left, right, on, Left).await?;
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
        }
        Ok(())
    }

    #[tokio::test]
    async fn join_right_sort_order() -> Result<()> {
        for test_type in [SMJ, BHJLeftProbed, BHJRightProbed] {
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

            let (_, batches) = join_collect(test_type, left, right, on, Right).await?;
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
        }
        Ok(())
    }

    #[tokio::test]
    async fn join_left_multiple_batches() -> Result<()> {
        for test_type in [SMJ, BHJLeftProbed, BHJRightProbed] {
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

            let (_, batches) = join_collect(test_type, left, right, on, Left).await?;
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
        }
        Ok(())
    }

    #[tokio::test]
    async fn join_right_multiple_batches() -> Result<()> {
        for test_type in [SMJ, BHJLeftProbed, BHJRightProbed] {
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

            let (_, batches) = join_collect(test_type, left, right, on, Right).await?;
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
        }
        Ok(())
    }

    #[tokio::test]
    async fn join_full_multiple_batches() -> Result<()> {
        for test_type in [SMJ, BHJLeftProbed, BHJRightProbed] {
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

            let (_, batches) = join_collect(test_type, left, right, on, Full).await?;
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
        }
        Ok(())
    }

    #[tokio::test]
    async fn join_existence_multiple_batches() -> Result<()> {
        for test_type in [SMJ, BHJLeftProbed, BHJRightProbed] {
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

            let (_, batches) = join_collect(test_type, left, right, on, Existence).await?;
            let expected = vec![
                "+----+----+----+----------+",
                "| a1 | b1 | c1 | exists#0 |",
                "+----+----+----+----------+",
                "| 0  | 3  | 4  | false    |",
                "| 1  | 4  | 5  | true     |",
                "| 2  | 5  | 6  | false    |",
                "| 3  | 6  | 7  | true     |",
                "| 4  | 6  | 8  | true     |",
                "| 5  | 7  | 9  | false    |",
                "| 6  | 9  | 9  | false    |",
                "+----+----+----+----------+",
            ];
            assert_batches_sorted_eq!(expected, &batches);
        }
        Ok(())
    }
}
