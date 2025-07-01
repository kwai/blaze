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

use std::{any::Any, fmt::Formatter, pin::Pin, sync::Arc, sync::atomic::{AtomicUsize, Ordering}};

use arrow::{compute::SortOptions, datatypes::SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::{
    common::{DataFusionError, JoinSide},
    error::Result,
    execution::context::TaskContext,
    physical_expr::{EquivalenceProperties, PhysicalExprRef},
    physical_plan::{
        joins::utils::JoinOn,
        metrics::{ExecutionPlanMetricsSet, MetricsSet, Time},
        DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, ExecutionPlanProperties,
        PlanProperties, SendableRecordBatchStream, Statistics,
    },
};
use futures_util::task::SpawnExt;
use datafusion_ext_commons::{batch_size, df_execution_err};
use once_cell::sync::OnceCell;
use crate::{
    common::{
        column_pruning::ExecuteWithColumnPruning,
        execution_context::{ExecutionContext, WrappedRecordBatchSender},
        timer_helper::TimerHelper,
    },
    cur_forward,
    joins::{
        join_utils::{JoinType, JoinType::*},
        smj::{
            existence_join::ExistenceJoiner,
            full_join::{FullOuterJoiner, InnerJoiner, LeftOuterJoiner, RightOuterJoiner},
            semi_join::{LeftAntiJoiner, LeftSemiJoiner, RightAntiJoiner, RightSemiJoiner},
        },
        stream_cursor::StreamCursor,
        JoinParams, JoinProjection, StreamCursors,
    },
    sort_exec::KeyRowsOutput,
};
use crate::sort_exec::SortExec;

pub static SORT_MERGE_JOIN_EXEC_KEY_ROWS_CONVERT_COUNT: AtomicUsize = AtomicUsize::new(0);

trait RowConverterExt {
    fn convert_columns_with_metrics(&self, columns: &[arrow::array::ArrayRef], caller: &'static str) -> arrow::error::Result<arrow::row::Rows>;
}

impl RowConverterExt for arrow::row::RowConverter {
    fn convert_columns_with_metrics(&self, columns: &[arrow::array::ArrayRef], caller: &'static str) -> arrow::error::Result<arrow::row::Rows> {
        if caller == "SortMergeJoinExec" {
            SORT_MERGE_JOIN_EXEC_KEY_ROWS_CONVERT_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        self.convert_columns(columns)
    }
}

#[derive(Debug)]
pub struct SortMergeJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    join_type: JoinType,
    sort_options: Vec<SortOptions>,
    join_params: OnceCell<JoinParams>,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
    interleaver_time_metric: OnceCell<Time>,
}

impl SortMergeJoinExec {
    pub fn try_new(
        schema: SchemaRef,
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
        sort_options: Vec<SortOptions>,
    ) -> Result<Self> {
        Ok(Self {
            schema,
            left,
            right,
            on,
            join_type,
            sort_options,
            join_params: OnceCell::new(),
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
            interleaver_time_metric: OnceCell::new(),
        })
    }

    pub fn try_new_with_join_params(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_params: JoinParams,
    ) -> Result<Self> {
        let on = join_params
            .left_keys
            .iter()
            .zip(&join_params.right_keys)
            .map(|(l, r)| (l.clone(), r.clone()))
            .collect();

        Ok(Self {
            schema: join_params.output_schema.clone(),
            left,
            right,
            on,
            join_type: join_params.join_type,
            sort_options: join_params.sort_options.clone(),
            join_params: OnceCell::with_value(join_params),
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
            interleaver_time_metric: OnceCell::new(),
        })
    }

    fn create_join_params(&self, projection: &[usize]) -> Result<JoinParams> {
        let left_schema = self.left.schema();
        let right_schema = self.right.schema();
        let (left_keys, right_keys): (Vec<PhysicalExprRef>, Vec<PhysicalExprRef>) =
            self.on.iter().cloned().unzip();
        let key_data_types = self
            .on
            .iter()
            .map(|(left_key, right_key)| {
                Ok({
                    let left_dt = left_key.data_type(&left_schema)?;
                    let right_dt = right_key.data_type(&right_schema)?;
                    if left_dt != right_dt {
                        df_execution_err!(
                            "join key data type differs {left_dt:?} <-> {right_dt:?}"
                        )?;
                    }
                    left_dt
                })
            })
            .collect::<Result<_>>()?;

        let projection = JoinProjection::try_new(
            self.join_type,
            &self.schema,
            &left_schema,
            &right_schema,
            projection,
        )?;
        Ok(JoinParams {
            join_type: self.join_type,
            left_schema,
            right_schema,
            output_schema: self.schema(),
            left_keys,
            right_keys,
            key_data_types,
            sort_options: self.sort_options.clone(),
            projection,
            batch_size: batch_size(),
        })
    }

    pub fn interleaver_time_metric(&self) -> &OnceCell<Time> {
        &self.interleaver_time_metric
    }

    fn execute_with_projection(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
        projection: Vec<usize>,
    ) -> Result<SendableRecordBatchStream> {
        let join_params = self
            .join_params
            .get_or_try_init(|| self.create_join_params(&projection))?
            .clone();
        let exec_ctx = ExecutionContext::new(
            context,
            partition,
            join_params.projection.schema.clone(),
            &self.metrics,
        );
        let exec_ctx_cloned = exec_ctx.clone();
        // First determine whether left/right implements KeyRowsOutput and key_exprs is consistent with join key
        let left_key_rows = self.left.as_any().downcast_ref::<SortExec>();
        let right_key_rows = self.right.as_any().downcast_ref::<SortExec>();
        let left_keys_match = left_key_rows
            .map(|kro| {
                let kro_exprs = kro.key_exprs();
                kro_exprs.len() == join_params.left_keys.len()
                    && kro_exprs.iter().zip(&join_params.left_keys).all(|(a, b)| a.eq(b))
            })
            .unwrap_or(false);
        let right_keys_match = right_key_rows
            .map(|kro| {
                let kro_exprs = kro.key_exprs();
                kro_exprs.len() == join_params.right_keys.len()
                    && kro_exprs.iter().zip(&join_params.right_keys).all(|(a, b)| a.eq(b))
            })
            .unwrap_or(false);

        let left_stream = if left_key_rows.is_some() && left_keys_match {
            // Use output with key rows
            let kro = left_key_rows.unwrap();
            let stream = kro.execute_with_key_rows_output(partition, exec_ctx.task_ctx());
            Some(stream)
        } else {
            None
        };
        let right_stream = if right_key_rows.is_some() && right_keys_match {
            let kro = right_key_rows.unwrap();
            let stream = kro.execute_with_key_rows_output(partition, exec_ctx.task_ctx());
            Some(stream)
        } else {
            None
        };

        let (left_key_rows_batches, left_stream) = if let Some(mut s) = left_stream {
            // Collect All (RecordBatch, Rows)
            use futures::StreamExt;
            let mut batches = vec![];
            let mut s = s;
            while let Some(Ok((batch, rows))) = futures::executor::block_on(s.next()) {
                batches.push((batch, rows));
            }
            // Use an empty stream instead, the actual key rows are passed as parameters
            (Some(batches), exec_ctx.execute(&self.left)?)
        } else {
            (None, exec_ctx.execute(&self.left)?)
        };
        let (right_key_rows_batches, right_stream) = if let Some(mut s) = right_stream {
            use futures::StreamExt;
            let mut batches = vec![];
            let mut s = s;
            while let Some(Ok((batch, rows))) = futures::executor::block_on(s.next()) {
                batches.push((batch, rows));
            }
            (Some(batches), exec_ctx.execute(&self.right)?)
        } else {
            (None, exec_ctx.execute(&self.right)?)
        };

        let (left_key_rows_batches, left_stream) = (left_key_rows_batches, left_stream);
        let (right_key_rows_batches, right_stream) = (right_key_rows_batches, right_stream);
        let output = exec_ctx_cloned
            .clone()
            .output_with_sender("SortMergeJoin", move |sender| {
                execute_join(
                    left_stream,
                    right_stream,
                    join_params,
                    exec_ctx_cloned,
                    sender,
                    left_key_rows_batches,
                    right_key_rows_batches,
                )
            });
        Ok(exec_ctx.coalesce_with_default_batch_size(output))
    }
}

impl DisplayAs for SortMergeJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "SortMergeJoin: join_type={:?}, on={:?}, schema={:?}",
            self.join_type, self.on, self.schema,
        )
    }
}

impl ExecuteWithColumnPruning for SortMergeJoinExec {
    fn execute_projected(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
        projection: &[usize],
    ) -> Result<SendableRecordBatchStream> {
        self.execute_with_projection(partition, context, projection.to_vec())
    }
}

impl ExecutionPlan for SortMergeJoinExec {
    fn name(&self) -> &str {
        "SortMergeJoinExec"
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
                self.right.output_partitioning().clone(),
                ExecutionMode::Bounded,
            )
        })
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SortMergeJoinExec::try_new(
            self.schema(),
            children[0].clone(),
            children[1].clone(),
            self.on.clone(),
            self.join_type,
            self.sort_options.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let projection = (0..self.schema.fields().len()).collect();
        self.execute_with_projection(partition, context, projection)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        todo!()
    }
}

pub async fn execute_join(
    lstream: SendableRecordBatchStream,
    rstream: SendableRecordBatchStream,
    join_params: JoinParams,
    exec_ctx: Arc<ExecutionContext>,
    sender: Arc<WrappedRecordBatchSender>,
    left_key_rows_batches: Option<Vec<(RecordBatch, arrow::row::Rows)>>,
    right_key_rows_batches: Option<Vec<(RecordBatch, arrow::row::Rows)>>,
) -> Result<()> {
    let _timer = exec_ctx.baseline_metrics().elapsed_compute().timer();
    let poll_time = Time::new();
    let interleaver_time = exec_ctx.register_timer_metric("interleaver_time");

    let mut curs = (
        if let Some(batches) = left_key_rows_batches {
            StreamCursor::from_key_rows_batches(
                batches,
                join_params.left_keys.clone(),
                join_params.projection.left.clone(),
                join_params.left_schema.clone(),
            )
        } else {
            StreamCursor::try_new(
                lstream,
                poll_time.clone(),
                &join_params,
                JoinSide::Left,
                &join_params.projection.left,
            )?
        },
        if let Some(batches) = right_key_rows_batches {
            StreamCursor::from_key_rows_batches(
                batches,
                join_params.right_keys.clone(),
                join_params.projection.right.clone(),
                join_params.right_schema.clone(),
            )
        } else {
            StreamCursor::try_new(
                rstream,
                poll_time.clone(),
                &join_params,
                JoinSide::Right,
                &join_params.projection.right,
            )?
        },
    );

    // start first batches of both side asynchronously
    tokio::try_join!(
        async { Ok::<_, DataFusionError>(cur_forward!(curs.0)) },
        async { Ok::<_, DataFusionError>(cur_forward!(curs.1)) },
    )?;

    let join_type = join_params.join_type;
    let mut joiner: Pin<Box<dyn Joiner + Send>> = match join_type {
        Inner => Box::pin(InnerJoiner::new_with_metric(join_params, sender.clone(), interleaver_time.clone())),
        Left => Box::pin(LeftOuterJoiner::new_with_metric(join_params, sender.clone(), interleaver_time.clone())),
        Right => Box::pin(RightOuterJoiner::new_with_metric(join_params, sender.clone(), interleaver_time.clone())),
        Full => Box::pin(FullOuterJoiner::new_with_metric(join_params, sender.clone(), interleaver_time.clone())),
        LeftSemi => Box::pin(LeftSemiJoiner::new_with_metric(join_params, sender.clone(), interleaver_time.clone())),
        RightSemi => Box::pin(RightSemiJoiner::new_with_metric(join_params, sender.clone(), interleaver_time.clone())),
        LeftAnti => Box::pin(LeftAntiJoiner::new_with_metric(join_params, sender.clone(), interleaver_time.clone())),
        RightAnti => Box::pin(RightAntiJoiner::new_with_metric(join_params, sender.clone(), interleaver_time.clone())),
        Existence => Box::pin(ExistenceJoiner::new_with_metric(join_params, sender.clone(), interleaver_time.clone())),
    };
    joiner.as_mut().join(&mut curs).await?;
    exec_ctx
        .baseline_metrics()
        .record_output(joiner.num_output_rows());

    // discount poll time
    exec_ctx
        .baseline_metrics()
        .elapsed_compute()
        .sub_duration(poll_time.duration());

    // After join execution is completed, metrics are printed
    println!("[METRICS] SortExec key rows convert count: {}", crate::sort_exec::SORT_EXEC_KEY_ROWS_CONVERT_COUNT.load(Ordering::Relaxed));
    println!("[METRICS] SortMergeJoinExec key rows convert count: {}", SORT_MERGE_JOIN_EXEC_KEY_ROWS_CONVERT_COUNT.load(Ordering::Relaxed));
    Ok(())
}

#[macro_export]
macro_rules! compare_cursor {
    ($curs:expr) => {{
        match ($curs.0.cur_idx, $curs.1.cur_idx) {
            (lidx, _) if $curs.0.is_null_key(lidx) => Ordering::Less,
            (_, ridx) if $curs.1.is_null_key(ridx) => Ordering::Greater,
            (lidx, ridx) => $curs.0.key(lidx).cmp(&$curs.1.key(ridx)),
        }
    }};
}

#[async_trait]
pub trait Joiner {
    async fn join(self: Pin<&mut Self>, curs: &mut StreamCursors) -> Result<()>;
    fn num_output_rows(&self) -> usize;
}

#[cfg(test)]
mod test_interleaver_metric {
    use super::*;
    use std::sync::Arc;
    use arrow::{array::Int32Array, datatypes::{Field, DataType, Schema}, record_batch::RecordBatch};
    use datafusion::{physical_plan::{memory::MemoryExec, common}, prelude::SessionContext};
    use std::time::Instant;
    use arrow::array::{ArrayRef};

    fn build_table(a: (&str, &Vec<i32>), b: (&str, &Vec<i32>), c: (&str, &Vec<i32>)) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(a.0, DataType::Int32, false),
            Field::new(b.0, DataType::Int32, false),
            Field::new(c.0, DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(a.1.clone())),
                Arc::new(Int32Array::from(b.1.clone())),
                Arc::new(Int32Array::from(c.1.clone())),
            ],
        ).unwrap();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_interleaver_time_metric() {
        let left = build_table(
            ("a1", &(0..10000).collect()),
            ("b1", &(0..10000).collect()),
            ("c1", &(0..10000).collect()),
        );
        let right = build_table(
            ("a2", &(0..10000).collect()),
            ("b2", &(0..10000).collect()),
            ("c2", &(0..10000).collect()),
        );
        let on: JoinOn = vec![
            (
                Arc::new(datafusion::physical_expr::expressions::Column::new("a1", 0)),
                Arc::new(datafusion::physical_expr::expressions::Column::new("a2", 0)),
            ),
            (
                Arc::new(datafusion::physical_expr::expressions::Column::new("b1", 1)),
                Arc::new(datafusion::physical_expr::expressions::Column::new("b2", 1)),
            )
        ];
        let sort_options = vec![SortOptions::default(); on.len()];
        let schema = Arc::new(Schema::new(vec![
            Field::new("a1", DataType::Int32, false),
            Field::new("b1", DataType::Int32, false),
            Field::new("c1", DataType::Int32, false),
            Field::new("a2", DataType::Int32, false),
            Field::new("b2", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]));
        let smj = SortMergeJoinExec::try_new(
            schema,
            left,
            right,
            on,
            JoinType::Inner,
            sort_options,
        ).unwrap();
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let stream = smj.execute(0, task_ctx).unwrap();
        let _ = common::collect(stream).await.unwrap();
        let metrics = smj.metrics();
        let (elapsed_compute, interleaver_time) = metrics.map(|mset| {
            let elapsed_compute = mset.iter()
                .find(|m| m.value().name() == "elapsed_compute")
                .map(|m| m.value().as_usize());
            let interleaver_time = mset.iter()
                .find(|m| m.value().name() == "interleaver_time")
                .map(|m| m.value().as_usize());
            (elapsed_compute, interleaver_time)
        }).unwrap_or((None, None));

        println!("elapsed_compute(ns): {:?}", elapsed_compute);
        println!("interleaver_time(ns): {:?}", interleaver_time);
        if let (Some(ec), Some(it)) = (elapsed_compute, interleaver_time) {
            println!("interleaver_time 占比: {:.2}%", (it as f64) / (ec as f64) * 100.0);
        }
        assert!(interleaver_time.is_some(), "interleaver_time metric should be present");
    }

    #[test]
    fn benchmark_interleaver() {
        let row_counts = [10_000, 100_000, 1_000_000];
        let col_counts = [2, 4, 8, 16, 32];
        for &rows in &row_counts {
            for &cols in &col_counts {
                let schema = Schema::new((0..cols).map(|i| Field::new(&format!("col{}", i), DataType::Int32, false)).collect::<Vec<_>>());
                let columns: Vec<ArrayRef> = (0..cols)
                    .map(|i| Arc::new(Int32Array::from_iter_values((0..rows).map(|v| v + i))) as ArrayRef)
                    .collect();
                let batch = RecordBatch::try_new(Arc::new(schema), columns).unwrap();
                // 计时
                let start = Instant::now();
                let _ = interleave(&batch);
                let elapsed = start.elapsed();
                println!("interleaver: rows={rows}, cols={cols}, elapsed={:?}", elapsed);
            }
        }
    }

    fn interleave(batch: &RecordBatch) -> Vec<Vec<i32>> {
        let num_rows = batch.num_rows();
        let num_columns = batch.num_columns();
        let mut result = vec![vec![0; num_columns]; num_rows];
        for col_idx in 0..num_columns {
            let array = batch.column(col_idx).as_any().downcast_ref::<Int32Array>().unwrap();
            for row_idx in 0..num_rows {
                result[row_idx][col_idx] = array.value(row_idx);
            }
        }
        result
    }
}
