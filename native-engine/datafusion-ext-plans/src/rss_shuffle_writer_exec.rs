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

//! Defines the External shuffle repartition plan

use std::{any::Any, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use blaze_jni_bridge::{jni_call_static, jni_new_global_ref, jni_new_string};
use datafusion::{
    arrow::datatypes::SchemaRef,
    error::{DataFusionError, Result},
    execution::context::TaskContext,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
        DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream, Statistics,
    },
};
use once_cell::sync::OnceCell;

use crate::{
    common::execution_context::ExecutionContext,
    memmgr::MemManager,
    shuffle::{
        rss_single_repartitioner::RssSingleShuffleRepartitioner,
        rss_sort_repartitioner::RssSortShuffleRepartitioner, ShuffleRepartitioner,
    },
};

/// The rss shuffle writer operator maps each input partition to M output
/// partitions based on a partitioning scheme. No guarantees are made about the
/// order of the resulting partitions.
#[derive(Debug)]
pub struct RssShuffleWriterExec {
    input: Arc<dyn ExecutionPlan>,
    partitioning: Partitioning,
    pub rss_partition_writer_resource_id: String,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}

impl DisplayAs for RssShuffleWriterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "RssShuffleWriterExec: partitioning={:?}",
            self.partitioning
        )
    }
}

#[async_trait]
impl ExecutionPlan for RssShuffleWriterExec {
    fn name(&self) -> &str {
        "RssShuffleWriterExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        self.props.get_or_init(|| {
            PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                self.partitioning.clone(),
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
        match children.len() {
            1 => Ok(Arc::new(RssShuffleWriterExec::try_new(
                children[0].clone(),
                self.partitioning.clone(),
                self.rss_partition_writer_resource_id.clone(),
            )?)),
            _ => Err(DataFusionError::Internal(
                "RssShuffleWriterExec wrong number of children".to_string(),
            )),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);
        let resource_id = jni_new_string!(&self.rss_partition_writer_resource_id)?;
        let rss_partition_writer_local = jni_call_static!(
            JniBridge.getResource(resource_id.as_obj()) -> JObject
        )?;
        let rss_partition_writer = jni_new_global_ref!(rss_partition_writer_local.as_obj())?;

        let repartitioner: Arc<dyn ShuffleRepartitioner> = match &self.partitioning {
            p if p.partition_count() == 1 => {
                Arc::new(RssSingleShuffleRepartitioner::new(rss_partition_writer))
            }
            Partitioning::Hash(..) | Partitioning::RoundRobinBatch(..) => {
                let sort_time = exec_ctx.register_timer_metric("sort_time");
                let partitioner = Arc::new(RssSortShuffleRepartitioner::new(
                    partition,
                    rss_partition_writer,
                    self.partitioning.clone(),
                    sort_time,
                ));
                MemManager::register_consumer(partitioner.clone(), true);
                partitioner
            }
            p => unreachable!("unsupported partitioning: {:?}", p),
        };
        let input = exec_ctx.execute_with_input_stats(&self.input)?;
        repartitioner.execute(exec_ctx, input)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.input.statistics()
    }
}

impl RssShuffleWriterExec {
    /// Create a new RssShuffleWriterExec
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
        rss_partition_writer_resource_id: String,
    ) -> Result<Self> {
        Ok(RssShuffleWriterExec {
            input,
            partitioning,
            rss_partition_writer_resource_id,
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
        })
    }
}
