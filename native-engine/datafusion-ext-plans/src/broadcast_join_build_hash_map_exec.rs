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
    sync::Arc,
};

use arrow::{
    array::{RecordBatch, new_null_array},
    datatypes::SchemaRef,
};
use arrow_schema::DataType;
use auron_jni_bridge::{
    conf,
    conf::{BooleanConf, IntConf},
};
use datafusion::{
    common::Result,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::{EquivalenceProperties, Partitioning, PhysicalExprRef},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        metrics::{ExecutionPlanMetricsSet, MetricsSet, Time},
        stream::RecordBatchStreamAdapter,
    },
};
use datafusion_ext_commons::arrow::{array_size::BatchSize, coalesce::coalesce_batches_unchecked};
use futures::StreamExt;
use once_cell::sync::OnceCell;

use crate::{
    common::{
        execution_context::ExecutionContext, stream_exec::create_record_batch_stream_exec,
        timer_helper::TimerHelper,
    },
    joins::join_hash_map::{JoinHashMap, join_hash_map_schema},
    sort_exec::create_default_ascending_sort_exec,
};

pub struct BroadcastJoinBuildHashMapExec {
    input: Arc<dyn ExecutionPlan>,
    keys: Vec<PhysicalExprRef>,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}

impl BroadcastJoinBuildHashMapExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, keys: Vec<PhysicalExprRef>) -> Self {
        Self {
            input,
            keys,
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
        }
    }
}

impl Debug for BroadcastJoinBuildHashMapExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BroadcastJoinBuildHashMap [{:?}]", self.keys)
    }
}

impl DisplayAs for BroadcastJoinBuildHashMapExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "BroadcastJoinBuildHashMapExec [{:?}]", self.keys)
    }
}

impl ExecutionPlan for BroadcastJoinBuildHashMapExec {
    fn name(&self) -> &str {
        "BroadcastJoinBuildHashMapExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        join_hash_map_schema(&self.input.schema())
    }

    fn properties(&self) -> &PlanProperties {
        self.props.get_or_init(|| {
            PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                Partitioning::UnknownPartitioning(
                    self.input.output_partitioning().partition_count(),
                ),
                EmissionType::Both,
                Boundedness::Bounded,
            )
        })
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children[0].clone(), self.keys.clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);
        let input = exec_ctx.execute(&self.input)?;
        let build_time = exec_ctx.register_timer_metric("build_time");
        execute_build_hash_map(input, self.keys.clone(), exec_ctx, build_time)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

pub fn execute_build_hash_map(
    mut input: SendableRecordBatchStream,
    keys: Vec<PhysicalExprRef>,
    exec_ctx: Arc<ExecutionContext>,
    build_time: Time,
) -> Result<SendableRecordBatchStream> {
    // output hash map batches as stream
    Ok(exec_ctx
        .clone()
        .output_with_sender("BuildHashMap", move |sender| async move {
            sender.exclude_time(&build_time);
            let _timer = build_time.timer();

            let smj_fallback_enabled = conf::SMJ_FALLBACK_ENABLE.value().unwrap_or(false);
            let smj_fallback_rows_threshold = conf::SMJ_FALLBACK_ROWS_THRESHOLD
                .value()
                .unwrap_or(i32::MAX) as usize;
            let smj_fallback_mem_threshold = conf::SMJ_FALLBACK_MEM_SIZE_THRESHOLD
                .value()
                .unwrap_or(i32::MAX) as usize;

            let data_schema = input.schema();
            let mut staging_batches: Vec<RecordBatch> = vec![];
            let mut staging_num_rows = 0;
            let mut stating_mem_size = 0;
            let mut fallback_to_sorted = false;

            while let Some(batch) = build_time
                .exclude_timer_async(input.next())
                .await
                .transpose()?
            {
                staging_batches.push(batch.clone());
                if smj_fallback_enabled {
                    staging_num_rows += batch.num_rows();
                    stating_mem_size += batch.get_batch_mem_size();

                    // fallback if staging data is too large
                    if staging_num_rows > smj_fallback_rows_threshold
                        || stating_mem_size > smj_fallback_mem_threshold
                    {
                        fallback_to_sorted = true;
                        break;
                    }
                }
            }

            // no fallbacks - generate one hashmap batch
            if !fallback_to_sorted {
                let data_batch =
                    coalesce_batches_unchecked(data_schema, &std::mem::take(&mut staging_batches));
                let hash_map = JoinHashMap::create_from_data_batch(data_batch, &keys)?;
                sender.send(hash_map.into_hash_map_batch()?).await;
                exec_ctx
                    .baseline_metrics()
                    .elapsed_compute()
                    .add_duration(build_time.duration());
                return Ok(());
            }

            // fallback to sort-merge join
            // sort all input data
            let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
                data_schema,
                futures::stream::iter(staging_batches.into_iter().map(|batch| Ok(batch)))
                    .chain(input),
            ));
            let input_exec = create_record_batch_stream_exec(input, exec_ctx.partition_id())?;
            let sort_exec = create_default_ascending_sort_exec(
                input_exec,
                &keys,
                Some(exec_ctx.execution_plan_metrics().clone()),
                false, // do not record output metric
            );
            let mut sorted_stream =
                sort_exec.execute(exec_ctx.partition_id(), exec_ctx.task_ctx())?;

            // append a null table data column
            let hash_map_batch_schema = join_hash_map_schema(&sorted_stream.schema());
            while let Some(batch) = sorted_stream.next().await.transpose()? {
                let null_table_data_column = new_null_array(&DataType::Binary, batch.num_rows());
                let sorted_hash_map_batch = RecordBatch::try_new(
                    hash_map_batch_schema.clone(),
                    batch
                        .columns()
                        .iter()
                        .cloned()
                        .chain(Some(null_table_data_column))
                        .collect(),
                )?;
                sender.send(sorted_hash_map_batch).await;
            }
            exec_ctx
                .baseline_metrics()
                .elapsed_compute()
                .add_duration(build_time.duration());
            Ok(())
        }))
}
