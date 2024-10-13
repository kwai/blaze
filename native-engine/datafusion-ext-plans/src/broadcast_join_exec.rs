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
};

use arrow::{
    array::RecordBatch,
    compute::{concat_batches, SortOptions},
    datatypes::{DataType, SchemaRef},
};
use async_trait::async_trait;
use datafusion::{
    common::{DataFusionError, JoinSide, Result, Statistics},
    execution::context::TaskContext,
    physical_expr::{EquivalenceProperties, PhysicalExprRef},
    physical_plan::{
        joins::utils::JoinOn,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet, Time},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, ExecutionPlanProperties,
        PlanProperties, SendableRecordBatchStream,
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
        timer_helper::{RegisterTimer, TimerHelper},
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
    props: OnceCell<PlanProperties>,
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
            props: OnceCell::new(),
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
        let metrics = self.metrics.clone();
        let baseline_metrics = Arc::new(BaselineMetrics::new(&metrics, partition));
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

        let baseline_metrics_cloned = baseline_metrics.clone();
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
                        sender.exclude_time(baseline_metrics_cloned.elapsed_compute());
                        execute_join(
                            partition,
                            left,
                            right,
                            join_params,
                            broadcast_side,
                            cached_build_hash_map_id,
                            is_built,
                            metrics,
                            sender,
                        )
                    },
                )
            })
            .try_flatten(),
        ));
        Ok(context.coalesce_with_default_batch_size(output_stream, &baseline_metrics)?)
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
    fn name(&self) -> &str {
        "BroadcastJoin"
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
                match self.broadcast_side {
                    JoinSide::Left => self.right.output_partitioning().clone(),
                    JoinSide::Right => self.left.output_partitioning().clone(),
                },
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
    probed_side_hash_time: Time,
    probed_side_search_time: Time,
    probed_side_compare_time: Time,
    build_output_time: Time,
    sender: Arc<WrappedRecordBatchSender>,
) -> Result<()> {
    let _timer = metrics.elapsed_compute().timer();
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

    while let Some(batch) = metrics
        .elapsed_compute()
        .exclude_timer_async(probed.next())
        .await
        .transpose()?
    {
        joiner
            .as_mut()
            .join(
                batch,
                &probed_side_hash_time,
                &probed_side_search_time,
                &probed_side_compare_time,
                &build_output_time,
            )
            .await?;

        if joiner.can_early_stop() {
            break;
        }
    }
    joiner.as_mut().finish(&build_output_time).await?;
    metrics.record_output(joiner.num_output_rows());
    Ok(())
}

async fn execute_join(
    partition: usize,
    left: SendableRecordBatchStream,
    right: SendableRecordBatchStream,
    join_params: JoinParams,
    broadcast_side: JoinSide,
    cached_build_hash_map_id: Option<String>,
    is_built: bool,
    metrics: ExecutionPlanMetricsSet,
    sender: Arc<WrappedRecordBatchSender>,
) -> Result<()> {
    let baseline_metrics = Arc::new(BaselineMetrics::new(&metrics, partition));
    let build_time = metrics.register_timer("build_hash_map_time", partition);
    let probed_side_hash_time = metrics.register_timer("probed_side_hash_time", partition);
    let probed_side_search_time = metrics.register_timer("probed_side_search_time", partition);
    let probed_side_compare_time = metrics.register_timer("probed_side_compare_time", partition);
    let build_output_time = metrics.register_timer("build_output_time", partition);

    let (probed_input, built_input) = match broadcast_side {
        JoinSide::Left => (right, left),
        JoinSide::Right => (left, right),
    };
    let map_keys = match broadcast_side {
        JoinSide::Left => join_params.left_keys.clone(),
        JoinSide::Right => join_params.right_keys.clone(),
    };

    // fetch two sides asynchronously to eagerly fetch probed side
    let (probed, map) = futures::try_join!(
        async {
            let probed_schema = probed_input.schema();
            let mut probed_peeked = Box::pin(probed_input.peekable());
            probed_peeked.as_mut().peek().await;
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                probed_schema,
                probed_peeked,
            )))
        },
        async {
            if is_built {
                collect_join_hash_map(
                    cached_build_hash_map_id,
                    built_input,
                    &map_keys,
                    build_time.clone(),
                )
                .await
            } else {
                build_join_hash_map(built_input, &map_keys, build_time.clone()).await
            }
        }
    )?;

    baseline_metrics
        .elapsed_compute()
        .add_duration(build_time.duration());

    execute_join_with_map(
        probed,
        map,
        join_params,
        broadcast_side,
        baseline_metrics.clone(),
        probed_side_hash_time,
        probed_side_search_time,
        probed_side_compare_time,
        build_output_time,
        sender,
    )
    .await
}

async fn build_join_hash_map(
    input: SendableRecordBatchStream,
    key_exprs: &[PhysicalExprRef],
    build_time: Time,
) -> Result<Arc<JoinHashMap>> {
    let data_schema = input.schema();
    let hash_map_schema = join_hash_map_schema(&data_schema);
    let data_batches: Vec<RecordBatch> = input.try_collect().await?;

    let join_hash_map = build_time.with_timer(|| {
        let data_batch = concat_batches(&data_schema, data_batches.iter())?;
        if data_batch.num_rows() == 0 {
            return Ok(Arc::new(JoinHashMap::create_empty(
                hash_map_schema,
                key_exprs,
            )?));
        }

        let join_hash_map = JoinHashMap::create_from_data_batch(data_batch, key_exprs)?;
        Ok::<_, DataFusionError>(Arc::new(join_hash_map))
    })?;
    Ok(join_hash_map)
}

async fn collect_join_hash_map(
    cached_build_hash_map_id: Option<String>,
    input: SendableRecordBatchStream,
    key_exprs: &[PhysicalExprRef],
    build_time: Time,
) -> Result<Arc<JoinHashMap>> {
    Ok(match cached_build_hash_map_id {
        Some(cached_id) => {
            get_cached_join_hash_map(&cached_id, || async {
                collect_join_hash_map_without_caching(input, key_exprs, build_time).await
            })
            .await?
        }
        None => {
            let map = collect_join_hash_map_without_caching(input, key_exprs, build_time).await?;
            Arc::new(map)
        }
    })
}

async fn collect_join_hash_map_without_caching(
    input: SendableRecordBatchStream,
    key_exprs: &[PhysicalExprRef],
    build_time: Time,
) -> Result<JoinHashMap> {
    let hash_map_schema = input.schema();
    let hash_map_batches: Vec<RecordBatch> = input.try_collect().await?;

    build_time.with_timer(|| {
        let join_hash_map = match hash_map_batches.len() {
            0 => JoinHashMap::create_empty(hash_map_schema, key_exprs)?,
            1 => {
                if hash_map_batches[0].num_rows() == 0 {
                    JoinHashMap::create_empty(hash_map_schema, key_exprs)?
                } else {
                    JoinHashMap::load_from_hash_map_batch(hash_map_batches[0].clone(), key_exprs)?
                }
            }
            n => return df_execution_err!("expect zero or one hash map batch, got {n}"),
        };
        Ok(join_hash_map)
    })
}

#[async_trait]
pub trait Joiner {
    async fn join(
        self: Pin<&mut Self>,
        probed_batch: RecordBatch,
        probed_side_hash_time: &Time,
        probed_side_search_time: &Time,
        probed_side_compare_time: &Time,
        build_output_time: &Time,
    ) -> Result<()>;

    async fn finish(self: Pin<&mut Self>, build_output_time: &Time) -> Result<()>;

    fn can_early_stop(&self) -> bool {
        false
    }

    fn num_output_rows(&self) -> usize;
}

async fn get_cached_join_hash_map<Fut: Future<Output = Result<JoinHashMap>> + Send>(
    cached_id: &str,
    init: impl FnOnce() -> Fut,
) -> Result<Arc<JoinHashMap>> {
    type Slot = Arc<tokio::sync::Mutex<Weak<JoinHashMap>>>;
    static CACHED_JOIN_HASH_MAP: OnceCell<Arc<Mutex<HashMap<String, Slot>>>> = OnceCell::new();

    // remove expire keys and insert new key
    let slot = {
        let cached_join_hash_map = CACHED_JOIN_HASH_MAP.get_or_init(|| Arc::default());
        let mut cached_join_hash_map = cached_join_hash_map.lock();

        cached_join_hash_map.retain(|_, v| Arc::strong_count(v) > 0);
        cached_join_hash_map
            .entry(cached_id.to_string())
            .or_default()
            .clone()
    };

    let mut slot = slot.lock().await;
    if let Some(cached) = slot.upgrade() {
        log::info!("got cached broadcast join hash map: ${cached_id}");
        Ok(cached)
    } else {
        log::info!("collecting broadcast join hash map: ${cached_id}");
        let new = Arc::new(init().await?);
        *slot = Arc::downgrade(&new);
        Ok(new)
    }
}
