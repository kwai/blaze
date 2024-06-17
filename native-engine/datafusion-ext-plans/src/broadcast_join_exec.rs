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
    fmt::{Debug, Formatter},
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::{
    array::{Array, ArrayRef, RecordBatch},
    buffer::NullBuffer,
    compute::SortOptions,
    datatypes::{DataType, SchemaRef},
    row::{RowConverter, Rows, SortField},
};
use async_trait::async_trait;
use datafusion::{
    common::{JoinSide, Result, Statistics},
    execution::context::TaskContext,
    physical_expr::{PhysicalExprRef, PhysicalSortExpr},
    physical_plan::{
        joins::utils::JoinOn,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet, Time},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    },
};
use datafusion_ext_commons::{
    batch_size, df_execution_err, streams::coalesce_stream::CoalesceInput,
};
use futures::{StreamExt, TryStreamExt};

use crate::{
    common::output::{TaskOutputter, WrappedRecordBatchSender},
    joins::{
        bhj::{
            existence_join::{LeftProbedExistenceJoiner, RightProbedExistenceJoiner},
            full_join::{
                LeftProbedFullOuterJoiner, LeftProbedInnerJoiner, LeftProbedLeftJoiner,
                LeftProbedRightJoiner, RightProbedFullOuterJoiner, RightProbedInnerJoiner,
                RightProbedLeftJoiner, RightProbedRightJoiner,
            },
            semi_join::{
                LeftProbedLeftAntiJoiner, LeftProbedLeftSemiJoiner, LeftProbedRightAntiJoiner,
                LeftProbedRightSemiJoiner, RightProbedLeftAntiJoiner, RightProbedLeftSemiJoiner,
                RightProbedRightAntiJoiner, RightProbedRightSemiJoiner,
            },
        },
        join_hash_map::JoinHashMap,
        join_utils::{JoinType, JoinType::*},
        JoinParams,
    },
};

#[derive(Debug)]
pub struct BroadcastJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    join_type: JoinType,
    broadcast_side: JoinSide,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
}

impl BroadcastJoinExec {
    pub fn try_new(
        schema: SchemaRef,
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
        broadcast_side: JoinSide,
    ) -> Result<Self> {
        Ok(Self {
            left,
            right,
            on,
            join_type,
            broadcast_side,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn create_join_params(&self) -> Result<JoinParams> {
        let left_schema = self.left.schema();
        let right_schema = self.right.schema();
        let (left_keys, right_keys): (Vec<PhysicalExprRef>, Vec<PhysicalExprRef>) =
            self.on.iter().cloned().unzip();
        let key_data_types: Vec<DataType> = self
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

        // use smaller batch size and coalesce batches at the end, to avoid buffer
        // overflowing
        let batch_size = batch_size();
        let sub_batch_size = batch_size / batch_size.ilog2() as usize;

        Ok(JoinParams {
            join_type: self.join_type,
            left_schema,
            right_schema,
            output_schema: self.schema(),
            left_keys,
            right_keys,
            batch_size: sub_batch_size,
            sort_options: vec![SortOptions::default(); self.on.len()],
            key_data_types,
        })
    }
}

impl ExecutionPlan for BroadcastJoinExec {
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
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::try_new(
            self.schema.clone(),
            children[0].clone(),
            children[1].clone(),
            self.on.iter().cloned().collect(),
            self.join_type,
            self.broadcast_side,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let metrics = Arc::new(BaselineMetrics::new(&self.metrics, partition));
        let join_params = self.create_join_params()?;
        let left = self.left.execute(partition, context.clone())?;
        let right = self.right.execute(partition, context.clone())?;
        let broadcast_side = self.broadcast_side;
        let output_schema = self.schema();

        let metrics_cloned = metrics.clone();
        let context_cloned = context.clone();
        let output_stream = Box::pin(RecordBatchStreamAdapter::new(
            output_schema.clone(),
            futures::stream::once(async move {
                context_cloned.output_with_sender("BroadcastJoin", output_schema, move |sender| {
                    execute_join(
                        left,
                        right,
                        join_params,
                        broadcast_side,
                        metrics_cloned,
                        sender,
                    )
                })
            })
            .try_flatten(),
        ));
        Ok(context.coalesce_with_default_batch_size(output_stream, &metrics)?)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        unimplemented!()
    }
}

impl DisplayAs for BroadcastJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "BroadcastJoin")
    }
}

async fn execute_join(
    left: SendableRecordBatchStream,
    right: SendableRecordBatchStream,
    join_params: JoinParams,
    broadcast_side: JoinSide,
    metrics: Arc<BaselineMetrics>,
    sender: Arc<WrappedRecordBatchSender>,
) -> Result<()> {
    let start_time = Instant::now();
    let excluded_time_ns = 0;
    let poll_time = Time::new();

    let (mut probed, keys, mut joiner): (_, _, Pin<Box<dyn Joiner + Send>>) = match broadcast_side {
        JoinSide::Left => {
            let lmap = collect_join_hash_map(left, poll_time.clone()).await?;
            (
                right,
                join_params.right_keys.clone(),
                match join_params.join_type {
                    Inner => Box::pin(RightProbedInnerJoiner::new(join_params, lmap, sender)),
                    Left => Box::pin(RightProbedLeftJoiner::new(join_params, lmap, sender)),
                    Right => Box::pin(RightProbedRightJoiner::new(join_params, lmap, sender)),
                    Full => Box::pin(RightProbedFullOuterJoiner::new(join_params, lmap, sender)),
                    LeftSemi => Box::pin(RightProbedLeftSemiJoiner::new(join_params, lmap, sender)),
                    LeftAnti => Box::pin(RightProbedLeftAntiJoiner::new(join_params, lmap, sender)),
                    RightSemi => {
                        Box::pin(RightProbedRightSemiJoiner::new(join_params, lmap, sender))
                    }
                    RightAnti => {
                        Box::pin(RightProbedRightAntiJoiner::new(join_params, lmap, sender))
                    }
                    Existence => {
                        Box::pin(RightProbedExistenceJoiner::new(join_params, lmap, sender))
                    }
                },
            )
        }
        JoinSide::Right => {
            let rmap = collect_join_hash_map(right, poll_time.clone()).await?;
            (
                left,
                join_params.left_keys.clone(),
                match join_params.join_type {
                    Inner => Box::pin(LeftProbedInnerJoiner::new(join_params, rmap, sender)),
                    Left => Box::pin(LeftProbedLeftJoiner::new(join_params, rmap, sender)),
                    Right => Box::pin(LeftProbedRightJoiner::new(join_params, rmap, sender)),
                    Full => Box::pin(LeftProbedFullOuterJoiner::new(join_params, rmap, sender)),
                    LeftSemi => Box::pin(LeftProbedLeftSemiJoiner::new(join_params, rmap, sender)),
                    LeftAnti => Box::pin(LeftProbedLeftAntiJoiner::new(join_params, rmap, sender)),
                    RightSemi => {
                        Box::pin(LeftProbedRightSemiJoiner::new(join_params, rmap, sender))
                    }
                    RightAnti => {
                        Box::pin(LeftProbedRightAntiJoiner::new(join_params, rmap, sender))
                    }
                    Existence => {
                        Box::pin(LeftProbedExistenceJoiner::new(join_params, rmap, sender))
                    }
                },
            )
        }
    };

    let probed_schema = probed.schema();
    let key_converter = Box::pin(RowConverter::new(
        keys.iter()
            .map(|k| Ok(SortField::new(k.data_type(&probed_schema)?.clone())))
            .collect::<Result<_>>()?,
    )?);

    while let Some(batch) = {
        let timer = poll_time.timer();
        let batch = probed.next().await.transpose()?;
        drop(timer);
        batch
    } {
        let key_columns: Vec<ArrayRef> = keys
            .iter()
            .map(|key| Ok(key.evaluate(&batch)?.into_array(batch.num_rows())?))
            .collect::<Result<_>>()?;
        let key_rows = key_converter.convert_columns(&key_columns)?;
        let key_has_nulls = key_columns
            .iter()
            .map(|c| c.nulls().cloned())
            .reduce(|lhs, rhs| NullBuffer::union(lhs.as_ref(), rhs.as_ref()))
            .unwrap_or(None);
        joiner.as_mut().join(batch, key_rows, key_has_nulls).await?;
    }
    joiner.as_mut().finish().await?;
    metrics.record_output(joiner.num_output_rows());

    // discount poll input and send output batch time
    let mut join_time_ns = (Instant::now() - start_time).as_nanos() as u64;
    join_time_ns -= excluded_time_ns as u64;
    metrics
        .elapsed_compute()
        .add_duration(Duration::from_nanos(join_time_ns));
    Ok(())
}

async fn collect_join_hash_map(
    mut input: SendableRecordBatchStream,
    poll_time: Time,
) -> Result<JoinHashMap> {
    let mut hash_map_batches = vec![];
    while let Some(batch) = {
        let timer = poll_time.timer();
        let batch = input.next().await.transpose()?;
        drop(timer);
        batch
    } {
        hash_map_batches.push(batch);
    }
    Ok(JoinHashMap::try_new(hash_map_batches)?)
}

#[async_trait]
pub trait Joiner {
    async fn join(
        self: Pin<&mut Self>,
        probed_batch: RecordBatch,
        probed_key: Rows,
        probed_key_has_null: Option<NullBuffer>,
    ) -> Result<()>;

    async fn finish(self: Pin<&mut Self>) -> Result<()>;

    fn total_send_output_time(&self) -> usize;
    fn num_output_rows(&self) -> usize;
}
