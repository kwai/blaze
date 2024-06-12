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

use std::{
    any::Any,
    fmt::Formatter,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::{compute::SortOptions, datatypes::SchemaRef};
use async_trait::async_trait;
use datafusion::{
    common::{DataFusionError, JoinSide},
    error::Result,
    execution::context::TaskContext,
    physical_expr::{PhysicalExprRef, PhysicalSortExpr},
    physical_plan::{
        joins::utils::JoinOn,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
        Statistics,
    },
};
use datafusion_ext_commons::{
    batch_size, df_execution_err, streams::coalesce_stream::CoalesceInput,
};
use futures::TryStreamExt;

use crate::{
    common::{
        column_pruning::ExecuteWithColumnPruning,
        output::{TaskOutputter, WrappedRecordBatchSender},
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
};

#[derive(Debug)]
pub struct SortMergeJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    join_type: JoinType,
    sort_options: Vec<SortOptions>,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
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
            metrics: ExecutionPlanMetricsSet::new(),
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

    fn execute_with_projection(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
        projection: Vec<usize>,
    ) -> Result<SendableRecordBatchStream> {
        let metrics = Arc::new(BaselineMetrics::new(&self.metrics, partition));
        let join_params = self.create_join_params(&projection)?;
        let left = self.left.execute(partition, context.clone())?;
        let right = self.right.execute(partition, context.clone())?;

        let metrics_cloned = metrics.clone();
        let context_cloned = context.clone();
        let output_stream = Box::pin(RecordBatchStreamAdapter::new(
            join_params.projection.schema.clone(),
            futures::stream::once(async move {
                context_cloned.output_with_sender(
                    "SortMergeJoin",
                    join_params.projection.schema.clone(),
                    move |sender| execute_join(left, right, join_params, metrics_cloned, sender),
                )
            })
            .try_flatten(),
        ));
        Ok(context.coalesce_with_default_batch_size(output_stream, &metrics)?)
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
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.right.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        match self.join_type {
            Left | LeftSemi | LeftAnti | Existence => self.left.output_ordering(),
            Right | RightSemi | RightAnti => self.right.output_ordering(),
            Inner => self.left.output_ordering(),
            Full => None,
        }
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
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
    metrics: Arc<BaselineMetrics>,
    sender: Arc<WrappedRecordBatchSender>,
) -> Result<()> {
    let start_time = Instant::now();

    let mut curs = (
        StreamCursor::try_new(
            lstream,
            &join_params,
            JoinSide::Left,
            &join_params.projection.left,
        )?,
        StreamCursor::try_new(
            rstream,
            &join_params,
            JoinSide::Right,
            &join_params.projection.right,
        )?,
    );

    // start first batches of both side asynchronously
    tokio::try_join!(
        async { Ok::<_, DataFusionError>(cur_forward!(curs.0)) },
        async { Ok::<_, DataFusionError>(cur_forward!(curs.1)) },
    )?;

    let join_type = join_params.join_type;
    let mut joiner: Pin<Box<dyn Joiner + Send>> = match join_type {
        Inner => Box::pin(InnerJoiner::new(join_params, sender)),
        Left => Box::pin(LeftOuterJoiner::new(join_params, sender)),
        Right => Box::pin(RightOuterJoiner::new(join_params, sender)),
        Full => Box::pin(FullOuterJoiner::new(join_params, sender)),
        LeftSemi => Box::pin(LeftSemiJoiner::new(join_params, sender)),
        RightSemi => Box::pin(RightSemiJoiner::new(join_params, sender)),
        LeftAnti => Box::pin(LeftAntiJoiner::new(join_params, sender)),
        RightAnti => Box::pin(RightAntiJoiner::new(join_params, sender)),
        Existence => Box::pin(ExistenceJoiner::new(join_params, sender)),
    };
    joiner.as_mut().join(&mut curs).await?;
    metrics.record_output(joiner.num_output_rows());

    // discount poll input and send output batch time
    let mut join_time_ns = (Instant::now() - start_time).as_nanos() as u64;
    join_time_ns -= joiner.total_send_output_time() as u64;
    join_time_ns -= curs.0.total_poll_time() as u64;
    join_time_ns -= curs.1.total_poll_time() as u64;
    metrics
        .elapsed_compute()
        .add_duration(Duration::from_nanos(join_time_ns));
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
    fn total_send_output_time(&self) -> usize;
    fn num_output_rows(&self) -> usize;
}
