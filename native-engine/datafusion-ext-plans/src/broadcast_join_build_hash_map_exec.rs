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
    sync::Arc,
};

use arrow::{array::RecordBatch, compute::concat_batches, datatypes::SchemaRef};
use datafusion::{
    common::Result,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr},
    physical_plan::{
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
        DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, ExecutionPlanProperties,
        PlanProperties,
    },
};
use futures::StreamExt;
use once_cell::sync::OnceCell;

use crate::{
    common::{execution_context::ExecutionContext, timer_helper::TimerHelper},
    joins::join_hash_map::{join_hash_map_schema, JoinHashMap},
};

pub struct BroadcastJoinBuildHashMapExec {
    input: Arc<dyn ExecutionPlan>,
    keys: Vec<Arc<dyn PhysicalExpr>>,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}

impl BroadcastJoinBuildHashMapExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, keys: Vec<Arc<dyn PhysicalExpr>>) -> Self {
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
                ExecutionMode::Bounded,
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
        execute_build_hash_map(input, self.keys.clone(), exec_ctx)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

pub fn collect_hash_map(
    data_schema: SchemaRef,
    data_batches: Vec<RecordBatch>,
    keys: Vec<Arc<dyn PhysicalExpr>>,
) -> Result<JoinHashMap> {
    let data_batch = concat_batches(&data_schema, data_batches.iter())?;
    let hash_map = JoinHashMap::create_from_data_batch(data_batch, &keys)?;
    Ok(hash_map)
}

fn execute_build_hash_map(
    mut input: SendableRecordBatchStream,
    keys: Vec<Arc<dyn PhysicalExpr>>,
    exec_ctx: Arc<ExecutionContext>,
) -> Result<SendableRecordBatchStream> {
    // output hash map batches as stream
    Ok(exec_ctx
        .clone()
        .output_with_sender("BuildHashMap", move |sender| async move {
            let elapsed_compute = exec_ctx.baseline_metrics().elapsed_compute().clone();
            let _timer = elapsed_compute.timer();

            // collect all input batches
            let mut data_batches = vec![];
            while let Some(batch) = elapsed_compute
                .exclude_timer_async(input.next())
                .await
                .transpose()?
            {
                data_batches.push(batch);
            }

            // build hash map
            let data_schema = input.schema();
            let hash_map = collect_hash_map(data_schema, data_batches, keys)?;
            sender.send(hash_map.into_hash_map_batch()?).await;
            Ok(())
        }))
}
