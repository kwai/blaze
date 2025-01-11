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

use std::{any::Any, fmt::Formatter, pin::Pin, sync::Arc};

use arrow::{compute::SortOptions, datatypes::SchemaRef};
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
};

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
        let left = exec_ctx.execute(&self.left)?;
        let right = exec_ctx.execute(&self.right)?;
        let output = exec_ctx_cloned
            .clone()
            .output_with_sender("SortMergeJoin", move |sender| {
                execute_join(left, right, join_params, exec_ctx_cloned, sender)
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
) -> Result<()> {
    let _timer = exec_ctx.baseline_metrics().elapsed_compute().timer();
    let poll_time = Time::new();

    let mut curs = (
        StreamCursor::try_new(
            lstream,
            poll_time.clone(),
            &join_params,
            JoinSide::Left,
            &join_params.projection.left,
        )?,
        StreamCursor::try_new(
            rstream,
            poll_time.clone(),
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
    exec_ctx
        .baseline_metrics()
        .record_output(joiner.num_output_rows());

    // discount poll time
    exec_ctx
        .baseline_metrics()
        .elapsed_compute()
        .sub_duration(poll_time.duration());
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
