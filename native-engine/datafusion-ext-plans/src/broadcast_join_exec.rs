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

use std::{
    any::Any,
    fmt::{Debug, Formatter},
    future::Future,
    pin::Pin,
    sync::{Arc, Weak},
    time::Duration,
};

use arrow::{
    array::RecordBatch,
    compute::SortOptions,
    datatypes::{DataType, SchemaRef},
};
use arrow_schema::Schema;
use async_trait::async_trait;
use datafusion::{
    common::{JoinSide, Result, Statistics},
    execution::context::TaskContext,
    physical_expr::{EquivalenceProperties, PhysicalExprRef},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
        joins::utils::JoinOn,
        metrics::{ExecutionPlanMetricsSet, MetricsSet, Time},
        stream::RecordBatchStreamAdapter,
    },
};
use datafusion_ext_commons::{batch_size, df_execution_err};
use futures::{StreamExt, TryStreamExt};
use futures_util::stream::Peekable;
use hashbrown::HashMap;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;

use crate::{
    broadcast_join_build_hash_map_exec::execute_build_hash_map,
    common::{
        column_pruning::ExecuteWithColumnPruning,
        execution_context::{ExecutionContext, WrappedRecordBatchSender},
        stream_exec::create_record_batch_stream_exec,
        timer_helper::TimerHelper,
    },
    joins::{
        JoinParams, JoinProjection,
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
        join_hash_map::{JoinHashMap, join_data_schema, join_hash_map_schema},
        join_utils::{JoinType, JoinType::*},
    },
    sort_exec::create_default_ascending_sort_exec,
    sort_merge_join_exec::SortMergeJoinExec,
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
        let join_params = self.create_join_params(&projection)?;
        let exec_ctx = ExecutionContext::new(
            context,
            partition,
            join_params.projection.schema.clone(),
            &self.metrics,
        );
        let left = self.left.clone();
        let right = self.right.clone();
        let broadcast_side = self.broadcast_side;
        let is_built = self.is_built;
        let cached_build_hash_map_id = self.cached_build_hash_map_id.clone();

        let exec_ctx_cloned = exec_ctx.clone();
        let output_stream = exec_ctx_cloned.clone().output_with_sender(
            if is_built {
                "BroadcastJoin"
            } else {
                "HashJoin"
            },
            move |sender| {
                sender.exclude_time(exec_ctx_cloned.baseline_metrics().elapsed_compute());
                execute_join(
                    left,
                    right,
                    join_params,
                    broadcast_side,
                    cached_build_hash_map_id,
                    is_built,
                    exec_ctx_cloned,
                    sender,
                )
            },
        );
        Ok(exec_ctx.coalesce_with_default_batch_size(output_stream))
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
                    JoinSide::None => unreachable!(),
                },
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
    probed_plan: Arc<dyn ExecutionPlan>,
    map: Arc<JoinHashMap>,
    join_params: JoinParams,
    broadcast_side: JoinSide,
    exec_ctx: Arc<ExecutionContext>,
    probed_side_hash_time: Time,
    probed_side_search_time: Time,
    probed_side_compare_time: Time,
    build_output_time: Time,
    sender: Arc<WrappedRecordBatchSender>,
) -> Result<()> {
    let elapsed_compute = exec_ctx.baseline_metrics().elapsed_compute().clone();
    let _timer = elapsed_compute.timer();

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
        JoinSide::None => unreachable!(),
    };

    if !joiner.can_early_stop() {
        let mut probed = exec_ctx.stat_input(exec_ctx.execute(&probed_plan)?);
        while !joiner.can_early_stop()
            && let Some(batch) = exec_ctx
                .baseline_metrics()
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
        }
    }
    joiner.as_mut().finish(&build_output_time).await?;
    exec_ctx
        .baseline_metrics()
        .record_output(joiner.num_output_rows());
    Ok(())
}

async fn execute_join_with_smj_fallback(
    probed_plan: Arc<dyn ExecutionPlan>,
    built: SendableRecordBatchStream,
    join_params: JoinParams,
    broadcast_side: JoinSide,
    exec_ctx: Arc<ExecutionContext>,
    sender: Arc<WrappedRecordBatchSender>,
) -> Result<()> {
    // remove the table data column from the built stream
    let built_schema = built.schema();
    let built_sorted: Arc<dyn ExecutionPlan> = {
        let removed_schema = {
            let mut fields = built_schema.fields().to_vec();
            fields.pop();
            Arc::new(Schema::new(fields))
        };
        let remoted_stream = Box::pin(RecordBatchStreamAdapter::new(
            removed_schema.clone(),
            built.map(|batch| {
                Ok({
                    let mut batch = batch?;
                    batch.remove_column(batch.num_columns() - 1);
                    batch
                })
            }),
        ));
        create_record_batch_stream_exec(remoted_stream, exec_ctx.partition_id())?
    };

    // create sorted streams, build side is already sorted
    let (left_exec, right_exec) = match broadcast_side {
        JoinSide::Left => (
            built_sorted,
            create_default_ascending_sort_exec(
                probed_plan,
                &join_params.right_keys,
                Some(exec_ctx.execution_plan_metrics().clone()),
                false, // do not record output metric
            ),
        ),
        JoinSide::Right => (
            create_default_ascending_sort_exec(
                probed_plan,
                &join_params.left_keys,
                Some(exec_ctx.execution_plan_metrics().clone()),
                false, // do not record output metric
            ),
            built_sorted,
        ),
        JoinSide::None => unreachable!(),
    };

    // run sort merge join
    let smj_exec = Arc::new(SortMergeJoinExec::try_new(
        join_params.output_schema,
        left_exec.clone(),
        right_exec.clone(),
        join_params
            .left_keys
            .to_vec()
            .into_iter()
            .zip(join_params.right_keys.to_vec())
            .collect(),
        join_params.join_type,
        vec![SortOptions::default(); join_params.left_keys.len()],
    )?);
    let mut join_output = smj_exec.execute(exec_ctx.partition_id(), exec_ctx.task_ctx())?;

    // send all outputs
    while let Some(batch) = join_output.next().await.transpose()? {
        sender.send(batch).await;
    }

    // elapsed_compute = sort time + merge time
    let smj_time = exec_ctx.register_timer_metric("fallback_sort_merge_join_time");
    smj_time.add_duration(Duration::from_nanos(
        left_exec
            .metrics()
            .and_then(|m| m.elapsed_compute())
            .unwrap_or(0) as u64,
    ));
    smj_time.add_duration(Duration::from_nanos(
        right_exec
            .metrics()
            .and_then(|m| m.elapsed_compute())
            .unwrap_or(0) as u64,
    ));
    smj_time.add_duration(Duration::from_nanos(
        smj_exec
            .metrics()
            .and_then(|m| m.elapsed_compute())
            .unwrap_or(0) as u64,
    ));
    exec_ctx
        .baseline_metrics()
        .elapsed_compute()
        .add_duration(smj_time.duration());
    Ok(())
}

async fn execute_join(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_params: JoinParams,
    broadcast_side: JoinSide,
    cached_build_hash_map_id: Option<String>,
    is_built: bool,
    exec_ctx: Arc<ExecutionContext>,
    sender: Arc<WrappedRecordBatchSender>,
) -> Result<()> {
    let build_time = exec_ctx.register_timer_metric("build_hash_map_time");
    let probed_side_hash_time = exec_ctx.register_timer_metric("probed_side_hash_time");
    let probed_side_search_time = exec_ctx.register_timer_metric("probed_side_search_time");
    let probed_side_compare_time = exec_ctx.register_timer_metric("probed_side_compare_time");
    let build_output_time = exec_ctx.register_timer_metric("build_output_time");

    let (probed_plan, built_plan) = match broadcast_side {
        JoinSide::Left => (right, left),
        JoinSide::Right => (left, right),
        JoinSide::None => unreachable!(),
    };
    let map_keys = match broadcast_side {
        JoinSide::Left => join_params.left_keys.clone(),
        JoinSide::Right => join_params.right_keys.clone(),
        JoinSide::None => unreachable!(),
    };

    let built_input = if is_built {
        exec_ctx.stat_input(exec_ctx.execute(&built_plan)?)
    } else {
        let data_input = exec_ctx.stat_input(exec_ctx.execute(&built_plan)?);
        let built_schema = join_hash_map_schema(&data_input.schema());
        execute_build_hash_map(
            data_input,
            map_keys.clone(),
            exec_ctx.with_new_output_schema(built_schema),
            build_time.clone(),
        )?
    };
    let built_collected = collect_join_hash_map(
        Box::pin(built_input.peekable()),
        cached_build_hash_map_id.filter(|_| is_built),
        &map_keys,
        build_time,
    )
    .await?;

    match built_collected {
        CollectJoinHashMapResult::Map(map) => {
            let join_with_map = execute_join_with_map(
                probed_plan,
                map,
                join_params,
                broadcast_side,
                exec_ctx,
                probed_side_hash_time,
                probed_side_search_time,
                probed_side_compare_time,
                build_output_time,
                sender,
            );
            join_with_map.await?;
        }
        CollectJoinHashMapResult::SortedStream(stream) => {
            let built_input = Box::pin(RecordBatchStreamAdapter::new(
                stream.get_ref().schema(),
                stream,
            ));
            let join_with_smj_fallback = execute_join_with_smj_fallback(
                probed_plan,
                built_input,
                join_params,
                broadcast_side,
                exec_ctx,
                sender,
            );
            join_with_smj_fallback.await?;
        }
    }
    Ok(())
}

enum CollectJoinHashMapResult {
    Map(Arc<JoinHashMap>),
    SortedStream(Pin<Box<Peekable<SendableRecordBatchStream>>>),
}

async fn collect_join_hash_map(
    input: Pin<Box<Peekable<SendableRecordBatchStream>>>,
    cached_build_hash_map_id: Option<String>,
    key_exprs: &[PhysicalExprRef],
    build_time: Time,
) -> Result<CollectJoinHashMapResult> {
    Ok(match cached_build_hash_map_id {
        Some(cached_id) => {
            get_cached_join_hash_map(&cached_id, || async {
                collect_join_hash_map_without_caching(input, key_exprs, build_time).await
            })
            .await?
        }
        None => collect_join_hash_map_without_caching(input, key_exprs, build_time).await?,
    })
}

async fn collect_join_hash_map_without_caching(
    mut input: Pin<Box<Peekable<SendableRecordBatchStream>>>,
    key_exprs: &[PhysicalExprRef],
    build_time: Time,
) -> Result<CollectJoinHashMapResult> {
    let hash_map_schema = input.get_ref().schema();
    let is_smj_fallback_join = matches!(
        input.as_mut().peek().await,
        Some(Ok(batch)) if !JoinHashMap::record_batch_contains_hash_map(batch),
    );
    if is_smj_fallback_join {
        return Ok(CollectJoinHashMapResult::SortedStream(input));
    }

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
        Ok(CollectJoinHashMapResult::Map(Arc::new(join_hash_map)))
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

async fn get_cached_join_hash_map<Fut: Future<Output = Result<CollectJoinHashMapResult>> + Send>(
    cached_id: &str,
    init: impl FnOnce() -> Fut,
) -> Result<CollectJoinHashMapResult> {
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
        Ok(CollectJoinHashMapResult::Map(cached))
    } else {
        log::info!("collecting broadcast join hash map: ${cached_id}");
        match init().await? {
            CollectJoinHashMapResult::Map(map) => {
                *slot = Arc::downgrade(&map);
                Ok(CollectJoinHashMapResult::Map(map))
            }
            CollectJoinHashMapResult::SortedStream(sorted_stream) => {
                Ok(CollectJoinHashMapResult::SortedStream(sorted_stream))
            }
        }
    }
}
