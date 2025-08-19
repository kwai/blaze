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

use std::{any::Any, fmt::Formatter, pin::Pin, sync::Arc};

use arrow::{compute::SortOptions, datatypes::SchemaRef};
use async_trait::async_trait;
use datafusion::{
    common::DataFusionError,
    error::Result,
    execution::context::TaskContext,
    physical_expr::{EquivalenceProperties, PhysicalExprRef, PhysicalSortExpr},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream, Statistics,
        execution_plan::{Boundedness, EmissionType},
        joins::utils::JoinOn,
        metrics::{ExecutionPlanMetricsSet, MetricsSet, Time},
    },
};
use datafusion_ext_commons::{batch_size, df_execution_err};
use futures::FutureExt;
use once_cell::sync::OnceCell;

use crate::{
    common::{
        column_pruning::ExecuteWithColumnPruning,
        execution_context::{ExecutionContext, WrappedRecordBatchSender},
        timer_helper::TimerHelper,
    },
    cur_forward,
    joins::{
        JoinParams, JoinProjection,
        join_utils::{JoinType, JoinType::*},
        smj::{
            existence_join::ExistenceJoiner,
            full_join::{FullOuterJoiner, InnerJoiner, LeftOuterJoiner, RightOuterJoiner},
            semi_join::{LeftAntiJoiner, LeftSemiJoiner, RightAntiJoiner, RightSemiJoiner},
        },
        stream_cursor::StreamCursor,
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
            &self.left.schema(),
            &self.right.schema(),
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

        let poll_time = Time::new();
        let left = exec_ctx.execute_projected_with_key_rows_output(
            &self.left,
            &join_params
                .left_keys
                .iter()
                .zip(&join_params.sort_options)
                .map(|(k, s)| PhysicalSortExpr::new(k.clone(), s.clone()))
                .collect::<Vec<_>>(),
            &join_params.projection.left,
        )?;
        let right = exec_ctx.execute_projected_with_key_rows_output(
            &self.right,
            &join_params
                .right_keys
                .iter()
                .zip(&join_params.sort_options)
                .map(|(k, s)| PhysicalSortExpr::new(k.clone(), s.clone()))
                .collect::<Vec<_>>(),
            &join_params.projection.right,
        )?;

        let left = StreamCursor::try_new(left, poll_time.clone(), &join_params.key_data_types)?;
        let right = StreamCursor::try_new(right, poll_time.clone(), &join_params.key_data_types)?;
        self.output_with_streams(exec_ctx, join_params, left, right, poll_time)
    }

    fn output_with_streams(
        &self,
        exec_ctx: Arc<ExecutionContext>,
        join_params: JoinParams,
        left: StreamCursor,
        right: StreamCursor,
        poll_time: Time,
    ) -> Result<SendableRecordBatchStream> {
        let exec_ctx_cloned = exec_ctx.clone();
        let output = exec_ctx
            .clone()
            .output_with_sender("SortMergeJoin", move |sender| {
                let exec_ctx = exec_ctx_cloned.clone();
                sender.exclude_time(exec_ctx.baseline_metrics().elapsed_compute());
                execute_join(
                    left,
                    right,
                    join_params,
                    exec_ctx,
                    poll_time.clone(),
                    sender,
                )
                .inspect(move |_| {
                    // discount poll time
                    exec_ctx_cloned
                        .baseline_metrics()
                        .elapsed_compute()
                        .sub_duration(poll_time.duration());
                })
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
                EmissionType::Both,
                Boundedness::Bounded,
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

async fn execute_join(
    mut left: StreamCursor,
    mut right: StreamCursor,
    join_params: JoinParams,
    exec_ctx: Arc<ExecutionContext>,
    poll_time: Time,
    sender: Arc<WrappedRecordBatchSender>,
) -> Result<()> {
    let _timer = exec_ctx.baseline_metrics().elapsed_compute().timer();

    // Start first batches of both sides asynchronously
    let concurrent_poll_time = Time::new();
    let concurrent_poll_timer = concurrent_poll_time.timer();
    tokio::try_join!(
        async { Ok::<_, DataFusionError>(cur_forward!(left)) },
        async { Ok::<_, DataFusionError>(cur_forward!(right)) },
    )?;
    drop(concurrent_poll_timer);
    poll_time.sub_duration(poll_time.duration());
    poll_time.add_duration(concurrent_poll_time.duration());

    macro_rules! join {
        ($joiner:ty) => {{
            let mut joiner = Box::pin(<$joiner>::new(join_params, sender));
            joiner.as_mut().join(&mut left, &mut right).await?;
            exec_ctx
                .baseline_metrics()
                .record_output(joiner.num_output_rows());
        }};
    }

    match join_params.join_type {
        Inner => {
            join!(InnerJoiner)
        }
        Left => {
            join!(LeftOuterJoiner)
        }
        Right => {
            join!(RightOuterJoiner)
        }
        Full => {
            join!(FullOuterJoiner)
        }
        LeftSemi => {
            join!(LeftSemiJoiner)
        }
        LeftAnti => {
            join!(LeftAntiJoiner)
        }
        RightSemi => {
            join!(RightSemiJoiner)
        }
        RightAnti => {
            join!(RightAntiJoiner)
        }
        Existence => {
            join!(ExistenceJoiner)
        }
    };
    Ok(())
}

#[macro_export]
macro_rules! compare_cursor {
    ($cur1:expr, $cur2:expr) => {{
        match ($cur1.cur_idx(), $cur2.cur_idx()) {
            (lidx, _) if $cur1.is_null_key(lidx) => Ordering::Less,
            (_, ridx) if $cur2.is_null_key(ridx) => Ordering::Greater,
            (lidx, ridx) => $cur1.key(lidx).cmp(&$cur2.key(ridx)),
        }
    }};
}

#[async_trait]
pub trait Joiner {
    async fn join(
        self: Pin<&mut Self>,
        cur1: &mut StreamCursor,
        cur2: &mut StreamCursor,
    ) -> Result<()>;
    fn num_output_rows(&self) -> usize;
}
