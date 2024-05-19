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
    physical_plan::{
        expressions::PhysicalSortExpr,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
        Statistics,
    },
};
use futures::{stream::once, TryStreamExt};

use crate::{
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
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// Partitioning scheme to use
    partitioning: Partitioning,
    /// scala rssShuffleWriter
    pub rss_partition_writer_resource_id: String,
    /// Metrics
    metrics: ExecutionPlanMetricsSet,
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
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.partitioning.clone()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
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
        let resource_id = jni_new_string!(&self.rss_partition_writer_resource_id)?;
        let rss_partition_writer_local = jni_call_static!(
            JniBridge.getResource(resource_id.as_obj()) -> JObject
        )?;
        let rss_partition_writer = jni_new_global_ref!(rss_partition_writer_local.as_obj())?;

        // record uncompressed data size
        let data_size_metric = MetricBuilder::new(&self.metrics).counter("data_size", partition);

        let input = self.input.execute(partition, context.clone())?;
        let repartitioner: Arc<dyn ShuffleRepartitioner> = match &self.partitioning {
            p if p.partition_count() == 1 => {
                Arc::new(RssSingleShuffleRepartitioner::new(rss_partition_writer))
            }
            Partitioning::Hash(..) => {
                let partitioner = Arc::new(RssSortShuffleRepartitioner::new(
                    partition,
                    rss_partition_writer,
                    self.partitioning.clone(),
                ));
                MemManager::register_consumer(partitioner.clone(), true);
                partitioner
            }
            p => unreachable!("unsupported partitioning: {:?}", p),
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            once(repartitioner.execute(
                context.clone(),
                partition,
                input,
                BaselineMetrics::new(&self.metrics, partition),
                data_size_metric,
            ))
            .try_flatten(),
        )))
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
        })
    }
}
