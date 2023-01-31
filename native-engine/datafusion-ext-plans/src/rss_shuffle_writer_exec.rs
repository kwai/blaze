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

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::datasource::TableType::Base;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;

use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{Count, MetricsSet, MetricValue, Time};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayFormatType, Metric};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::Statistics;
use futures::{TryFutureExt, TryStreamExt};
use futures::stream::once;
use jni::objects::JObject;
use blaze_commons::{jni_call_static, jni_new_global_ref, jni_new_string};
use crate::shuffle::bucket_rss_repartitioner::BucketRssShuffleRepartitioner;
use crate::shuffle::rss_single_repartitioner::RssSingleShuffleRepartitioner;
use crate::shuffle::ShuffleRepartitioner;


/// The rss shuffle writer operator maps each input partition to M output partitions based on a
/// partitioning scheme. No guarantees are made about the order of the resulting partitions.
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
                self.rss_partition_writer_resource_id.clone()
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
        let input = self.input.execute(partition, context.clone())?;


        let rss_partition_writer = jni_new_global_ref!(
            jni_call_static!(
            JniBridge.getResource(
                jni_new_string!(&self.rss_partition_writer_resource_id)?
            ) -> JObject
        )?)?;

        let repartitioner: Arc<dyn ShuffleRepartitioner> =
            match &self.partitioning {
                p if p.partition_count() == 1 =>
                    Arc::new(RssSingleShuffleRepartitioner::new(
                        rss_partition_writer,
                        BaselineMetrics::new(&self.metrics, partition),
                    )),
                Partitioning::Hash(_, _) =>
                    Arc::new(BucketRssShuffleRepartitioner::new(
                        partition,
                        rss_partition_writer,
                        self.schema(),
                        self.partitioning.clone(),
                        BaselineMetrics::new(&self.metrics, partition),
                        context.clone(),
                    )),
                p => unreachable!("unsupported partitioning: {:?}", p),
            };

        let stream = repartitioner.execute(
            input,
            context.session_config().batch_size(),
            BaselineMetrics::new(&self.metrics, partition),
        ).map_err(|e| {
            ArrowError::ExternalError(Box::new(e))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            once(stream).try_flatten()
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "RssShuffleWriterExec: partitioning={:?}", self.partitioning)
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.input.statistics()
    }
}

impl RssShuffleWriterExec {
    /// Create a new RssShuffleWriterExec
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
        rss_partition_writer_resource_id: String
    ) -> Result<Self> {
        Ok(RssShuffleWriterExec {
            input,
            partitioning,
            rss_partition_writer_resource_id,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}
