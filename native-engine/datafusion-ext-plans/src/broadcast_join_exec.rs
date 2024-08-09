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
    future::Future,
    pin::Pin,
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use arrow::{
    array::RecordBatch,
    compute::{concat_batches, SortOptions},
    datatypes::{DataType, SchemaRef},
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
use hashbrown::HashMap;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;

use crate::{
    common::{
        batch_statisitcs::{stat_input, InputBatchStatistics},
        column_pruning::ExecuteWithColumnPruning,
        output::{TaskOutputter, WrappedRecordBatchSender},
    },
    joins::{
        bhj::{
            full_join::{
                LProbedFullOuterJoiner, LProbedInnerJoiner, LProbedLeftJoiner, LProbedRightJoiner,
                RProbedFullOuterJoiner, RProbedInnerJoiner, RProbedLeftJoiner, RProbedRightJoiner,
            },
            semi_join::{
                LProbedExistenceJoiner, LProbedLeftAntiJoiner, LProbedLeftSemiJoiner,
                LProbedRightAntiJoiner, LProbedRightSemiJoiner, RProbedExistenceJoiner,
                RProbedLeftAntiJoiner, RProbedLeftSemiJoiner, RProbedRightAntiJoiner,
                RProbedRightSemiJoiner,
            },
        },
        join_hash_map::{join_data_schema, join_hash_map_schema, JoinHashMap},
        join_utils::{JoinType, JoinType::*},
        JoinParams, JoinProjection,
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
    is_built: bool, // true for BroadcastHashJoin, false for ShuffledHashJoin
    cached_build_hash_map_id: Option<String>,
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
        is_built: bool,
        cached_build_hash_map_id: Option<String>,
    ) -> Result<Self> {
        Ok(Self {
            left,
            right,
            on,
            join_type,
            broadcast_side,
            schema,
            is_built,
            cached_build_hash_map_id,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    pub fn on(&self) -> &JoinOn {
        &self.on
    }

    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    pub fn broadcast_side(&self) -> JoinSide {
        self.broadcast_side
    }

    fn create_join_params(&self, projection: &[usize]) -> Result<JoinParams> {
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

        let projection = JoinProjection::try_new(
            self.join_type,
            &self.schema,
            &match self.broadcast_side {
                JoinSide::Left if self.is_built => join_data_schema(&left_schema),
                _ => left_schema.clone(),
            },
            &match self.broadcast_side {
                JoinSide::Right if self.is_built => join_data_schema(&right_schema),
                _ => right_schema.clone(),
            },
            projection,
        )?;

        Ok(JoinParams {
            join_type: self.join_type,
            left_schema,
            right_schema,
            output_schema: self.schema(),
            left_keys,
            right_keys,
            batch_size: batch_size(),
            sort_options: vec![SortOptions::default(); self.on.len()],
            projection,
            key_data_types,
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
        let broadcast_side = self.broadcast_side;
        let is_built = self.is_built;
        let cached_build_hash_map_id = self.cached_build_hash_map_id.clone();

        // stat probed side
        let input_batch_stat =
            InputBatchStatistics::from_metrics_set_and_blaze_conf(&self.metrics, partition)?;
        let left = stat_input(input_batch_stat.clone(), left)?;
        let right = stat_input(input_batch_stat.clone(), right)?;

        let metrics_cloned = metrics.clone();
        let context_cloned = context.clone();
        let output_stream = Box::pin(RecordBatchStreamAdapter::new(
            join_params.projection.schema.clone(),
            futures::stream::once(async move {
                context_cloned.output_with_sender(
                    if is_built {
                        "BroadcastJoin"
                    } else {
                        "HashJoin"
                    },
                    join_params.projection.schema.clone(),
                    move |sender| {
                        execute_join(
                            left,
                            right,
                            join_params,
                            broadcast_side,
                            cached_build_hash_map_id,
                            is_built,
                            metrics_cloned,
                            sender,
                        )
                    },
                )
            })
            .try_flatten(),
        ));
        Ok(context.coalesce_with_default_batch_size(output_stream, &metrics)?)
    }
}

impl ExecuteWithColumnPruning for BroadcastJoinExec {
    fn execute_projected(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
        projection: &[usize],
    ) -> Result<SendableRecordBatchStream> {
        self.execute_with_projection(partition, context, projection.to_vec())
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
        match self.broadcast_side {
            JoinSide::Left => self.right.output_partitioning(),
            JoinSide::Right => self.left.output_partitioning(),
        }
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
            self.is_built,
            None,
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
        unimplemented!()
    }
}

impl DisplayAs for BroadcastJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}",
            if self.is_built {
                "BroadcastJoin"
            } else {
                "HashJoin"
            }
        )
    }
}

async fn execute_join_with_map(
    mut probed: SendableRecordBatchStream,
    map: Arc<JoinHashMap>,
    join_params: JoinParams,
    broadcast_side: JoinSide,
    metrics: Arc<BaselineMetrics>,
    poll_time: Time,
    sender: Arc<WrappedRecordBatchSender>,
) -> Result<()> {
    let start_time = Instant::now();
    let mut excluded_time_ns = 0;

    let mut joiner: Pin<Box<dyn Joiner + Send>> = match broadcast_side {
        JoinSide::Left => match join_params.join_type {
            Inner => Box::pin(RProbedInnerJoiner::new(join_params, map, sender)),
            Left => Box::pin(RProbedLeftJoiner::new(join_params, map, sender)),
            Right => Box::pin(RProbedRightJoiner::new(join_params, map, sender)),
            Full => Box::pin(RProbedFullOuterJoiner::new(join_params, map, sender)),
            LeftSemi => Box::pin(RProbedLeftSemiJoiner::new(join_params, map, sender)),
            LeftAnti => Box::pin(RProbedLeftAntiJoiner::new(join_params, map, sender)),
            RightSemi => Box::pin(RProbedRightSemiJoiner::new(join_params, map, sender)),
            RightAnti => Box::pin(RProbedRightAntiJoiner::new(join_params, map, sender)),
            Existence => Box::pin(RProbedExistenceJoiner::new(join_params, map, sender)),
        },
        JoinSide::Right => match join_params.join_type {
            Inner => Box::pin(LProbedInnerJoiner::new(join_params, map, sender)),
            Left => Box::pin(LProbedLeftJoiner::new(join_params, map, sender)),
            Right => Box::pin(LProbedRightJoiner::new(join_params, map, sender)),
            Full => Box::pin(LProbedFullOuterJoiner::new(join_params, map, sender)),
            LeftSemi => Box::pin(LProbedLeftSemiJoiner::new(join_params, map, sender)),
            LeftAnti => Box::pin(LProbedLeftAntiJoiner::new(join_params, map, sender)),
            RightSemi => Box::pin(LProbedRightSemiJoiner::new(join_params, map, sender)),
            RightAnti => Box::pin(LProbedRightAntiJoiner::new(join_params, map, sender)),
            Existence => Box::pin(LProbedExistenceJoiner::new(join_params, map, sender)),
        },
    };

    while let Some(batch) = {
        let timer = poll_time.timer();
        let batch = probed.next().await.transpose()?;
        drop(timer);
        batch
    } {
        joiner.as_mut().join(batch).await?;
        if joiner.can_early_stop() {
            break;
        }
    }
    joiner.as_mut().finish().await?;
    metrics.record_output(joiner.num_output_rows());

    excluded_time_ns += poll_time.value();
    excluded_time_ns += joiner.total_send_output_time();

    // discount poll input and send output batch time
    let mut join_time_ns = (Instant::now() - start_time).as_nanos() as u64;
    join_time_ns -= excluded_time_ns as u64;
    metrics
        .elapsed_compute()
        .add_duration(Duration::from_nanos(join_time_ns));
    Ok(())
}

async fn execute_join(
    left: SendableRecordBatchStream,
    right: SendableRecordBatchStream,
    join_params: JoinParams,
    broadcast_side: JoinSide,
    cached_build_hash_map_id: Option<String>,
    is_built: bool,
    metrics: Arc<BaselineMetrics>,
    sender: Arc<WrappedRecordBatchSender>,
) -> Result<()> {
    println!("XXX broadcast_side: {broadcast_side}, is_built: {is_built}");
    let poll_time = Time::new();
    let (probed, map) = match broadcast_side {
        JoinSide::Left => {
            let right_schema = right.schema();
            let mut right_peeked = Box::pin(right.peekable());
            let lmap = futures::join!(
                // fetch two sides asynchronously
                async {
                    let timer = poll_time.timer();
                    right_peeked.as_mut().peek().await;
                    drop(timer);
                },
                async {
                    if is_built {
                        collect_join_hash_map(
                            cached_build_hash_map_id,
                            left,
                            &join_params.left_keys,
                            matches!(join_params.join_type, RightSemi | RightAnti),
                            poll_time.clone(),
                        )
                        .await
                    } else {
                        build_join_hash_map(
                            left,
                            &join_params.left_keys,
                            matches!(join_params.join_type, RightSemi | RightAnti),
                            poll_time.clone(),
                        )
                        .await
                    }
                }
            )
            .1?;
            (
                Box::pin(RecordBatchStreamAdapter::new(right_schema, right_peeked)),
                lmap,
            )
        }
        JoinSide::Right => {
            let left_schema = left.schema();
            let mut left_peeked = Box::pin(left.peekable());
            let rmap = futures::join!(
                // fetch two sides asynchronizely
                async {
                    let timer = poll_time.timer();
                    left_peeked.as_mut().peek().await;
                    drop(timer);
                },
                async {
                    if is_built {
                        collect_join_hash_map(
                            cached_build_hash_map_id,
                            right,
                            &join_params.right_keys,
                            matches!(join_params.join_type, LeftSemi | LeftAnti | Existence),
                            poll_time.clone(),
                        )
                        .await
                    } else {
                        build_join_hash_map(
                            right,
                            &join_params.right_keys,
                            matches!(join_params.join_type, LeftSemi | LeftAnti | Existence),
                            poll_time.clone(),
                        )
                        .await
                    }
                }
            )
            .1?;
            (
                Box::pin(RecordBatchStreamAdapter::new(left_schema, left_peeked)),
                rmap,
            )
        }
    };

    execute_join_with_map(
        probed,
        map,
        join_params,
        broadcast_side,
        metrics,
        poll_time,
        sender,
    )
    .await
}

async fn build_join_hash_map(
    input: SendableRecordBatchStream,
    key_exprs: &[PhysicalExprRef],
    distinct: bool,
    poll_time: Time,
) -> Result<Arc<JoinHashMap>> {
    let data_schema = input.schema();
    let hash_map_schema = join_hash_map_schema(&data_schema);

    let timer = poll_time.timer();
    let data_batches: Vec<RecordBatch> = input.try_collect().await?;
    drop(timer);

    let data_batch = concat_batches(&data_schema, data_batches.iter())?;
    if data_batch.num_rows() == 0 {
        return Ok(Arc::new(JoinHashMap::try_new_empty(
            hash_map_schema,
            key_exprs,
        )?));
    }

    let mut join_hash_map = JoinHashMap::try_from_data_batch(data_batch, key_exprs)?;
    if distinct {
        join_hash_map.distinct()?;
    }
    Ok(Arc::new(join_hash_map))
}

async fn collect_join_hash_map(
    cached_build_hash_map_id: Option<String>,
    input: SendableRecordBatchStream,
    key_exprs: &[PhysicalExprRef],
    distinct: bool,
    poll_time: Time,
) -> Result<Arc<JoinHashMap>> {
    Ok(match cached_build_hash_map_id {
        Some(cached_id) => {
            get_cached_join_hash_map(&cached_id, || async {
                collect_join_hash_map_without_caching(input, key_exprs, distinct, poll_time).await
            })
            .await?
        }
        None => {
            let map = collect_join_hash_map_without_caching(input, key_exprs, distinct, poll_time)
                .await?;
            Arc::new(map)
        }
    })
}

async fn collect_join_hash_map_without_caching(
    input: SendableRecordBatchStream,
    key_exprs: &[PhysicalExprRef],
    distinct: bool,
    poll_time: Time,
) -> Result<JoinHashMap> {
    let hash_map_schema = input.schema();
    let timer = poll_time.timer();
    let hash_map_batches: Vec<RecordBatch> = input.try_collect().await?;
    drop(timer);

    let mut join_hash_map = match hash_map_batches.len() {
        0 => JoinHashMap::try_new_empty(hash_map_schema, key_exprs)?,
        1 => {
            if hash_map_batches[0].num_rows() == 0 {
                JoinHashMap::try_new_empty(hash_map_schema, key_exprs)?
            } else {
                JoinHashMap::try_from_hash_map_batch(hash_map_batches[0].clone(), key_exprs)?
            }
        }
        n => return df_execution_err!("expect zero or one hash map batch, got {n}"),
    };
    if distinct {
        join_hash_map.distinct()?;
    }
    Ok(join_hash_map)
}

#[async_trait]
pub trait Joiner {
    async fn join(self: Pin<&mut Self>, probed_batch: RecordBatch) -> Result<()>;
    async fn finish(self: Pin<&mut Self>) -> Result<()>;

    fn can_early_stop(&self) -> bool {
        false
    }

    fn total_send_output_time(&self) -> usize;
    fn num_output_rows(&self) -> usize;
}

async fn get_cached_join_hash_map<Fut: Future<Output = Result<JoinHashMap>> + Send>(
    cached_id: &str,
    init: impl FnOnce() -> Fut,
) -> Result<Arc<JoinHashMap>> {
    type Slot = Arc<tokio::sync::Mutex<Weak<JoinHashMap>>>;
    static CACHED_JOIN_HASH_MAP: OnceCell<Arc<Mutex<HashMap<String, Slot>>>> = OnceCell::new();

    // TODO: remove expired keys from cached join hash map
    let cached_join_hash_map = CACHED_JOIN_HASH_MAP.get_or_init(|| Arc::default());
    let slot = cached_join_hash_map
        .lock()
        .entry(cached_id.to_string())
        .or_default()
        .clone();

    let mut slot = slot.lock().await;
    if let Some(cached) = slot.upgrade() {
        Ok(cached)
    } else {
        let new = Arc::new(init().await?);
        *slot = Arc::downgrade(&new);
        Ok(new)
    }
}
