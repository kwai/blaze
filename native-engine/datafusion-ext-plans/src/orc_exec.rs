// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{any::Any, fmt, fmt::Formatter, sync::Arc};

use arrow::{datatypes::SchemaRef, error::ArrowError};
use blaze_jni_bridge::{jni_call_static, jni_new_global_ref, jni_new_string};
use bytes::Bytes;
use datafusion::{
    datasource::physical_plan::{
        FileMeta, FileOpenFuture, FileOpener, FileScanConfig, FileStream, SchemaAdapter,
    },
    error::Result,
    execution::context::TaskContext,
    physical_plan::{
        expressions::PhysicalSortExpr,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricValue, MetricsSet, Time},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, Metric, Partitioning, PhysicalExpr,
        RecordBatchStream, SendableRecordBatchStream, Statistics,
    },
};
use datafusion_ext_commons::{batch_size, hadoop_fs::FsProvider};
use futures::{future::BoxFuture, FutureExt, StreamExt};
use futures_util::{stream::once, TryStreamExt};
use orc_rust::{
    arrow_reader::ArrowReaderBuilder, projection::ProjectionMask, reader::AsyncChunkReader,
};

use crate::common::{internal_file_reader::InternalFileReader, output::TaskOutputter};

/// Execution plan for scanning one or more Orc partitions
#[derive(Debug, Clone)]
pub struct OrcExec {
    fs_resource_id: String,
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    projected_output_ordering: Vec<Vec<PhysicalSortExpr>>,
    metrics: ExecutionPlanMetricsSet,
    _predicate: Option<Arc<dyn PhysicalExpr>>,
}

impl OrcExec {
    /// Create a new Orc reader execution plan provided file list and
    /// schema.
    pub fn new(
        base_config: FileScanConfig,
        fs_resource_id: String,
        _predicate: Option<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        let metrics = ExecutionPlanMetricsSet::new();

        let (projected_schema, projected_statistics, projected_output_ordering) =
            base_config.project();

        Self {
            fs_resource_id,
            base_config,
            projected_statistics,
            projected_schema,
            projected_output_ordering,
            metrics,
            _predicate,
        }
    }
}

impl DisplayAs for OrcExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        let limit = self.base_config.limit;
        let projection = self.base_config.projection.clone();
        let file_group = self
            .base_config
            .file_groups
            .iter()
            .flatten()
            .cloned()
            .collect::<Vec<_>>();

        write!(
            f,
            "OrcExec: file_group={:?}, limit={:?}, projection={:?}",
            file_group, limit, projection
        )
    }
}

impl ExecutionPlan for OrcExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.base_config.file_groups.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.projected_output_ordering
            .first()
            .map(|ordering| ordering.as_slice())
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition_index: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition_index);
        let io_time = Time::default();
        let io_time_metric = Arc::new(Metric::new(
            MetricValue::Time {
                name: "io_time".into(),
                time: io_time.clone(),
            },
            Some(partition_index),
        ));
        self.metrics.register(io_time_metric);

        // get fs object from jni bridge resource
        let resource_id = jni_new_string!(&self.fs_resource_id)?;
        let fs = jni_call_static!(JniBridge.getResource(resource_id.as_obj()) -> JObject)?;
        let fs_provider = Arc::new(FsProvider::new(jni_new_global_ref!(fs.as_obj())?, &io_time));

        let projection = match self.base_config.file_column_projection_indices() {
            Some(proj) => proj,
            None => (0..self.base_config.file_schema.fields().len()).collect(),
        };

        let opener = OrcOpener {
            projection,
            batch_size: batch_size(),
            _limit: self.base_config.limit,
            table_schema: self.base_config.file_schema.clone(),
            _metrics: self.metrics.clone(),
            fs_provider,
        };

        let baseline_metrics_cloned = baseline_metrics.clone();
        let file_stream =
            FileStream::new(&self.base_config, partition_index, opener, &self.metrics)?;
        let mut stream = Box::pin(file_stream);
        let context_cloned = context.clone();
        let timed_stream = Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            once(async move {
                context_cloned.output_with_sender(
                    "OrcScan",
                    stream.schema(),
                    move |sender| async move {
                        let mut timer = baseline_metrics_cloned.elapsed_compute().timer();
                        while let Some(batch) = stream.next().await.transpose()? {
                            sender.send(Ok(batch), Some(&mut timer)).await;
                        }
                        Ok(())
                    },
                )
            })
            .try_flatten(),
        ));
        Ok(timed_stream)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self.projected_statistics.clone())
    }
}

struct OrcOpener {
    projection: Vec<usize>,
    batch_size: usize,
    _limit: Option<usize>,
    table_schema: SchemaRef,
    _metrics: ExecutionPlanMetricsSet,
    fs_provider: Arc<FsProvider>,
}

impl FileOpener for OrcOpener {
    fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture> {
        let reader = OrcFileReaderRef(Arc::new(InternalFileReader::new(
            self.fs_provider.clone(),
            file_meta.object_meta.clone(),
        )));
        let batch_size = self.batch_size;
        let projection = self.projection.clone();
        let projected_schema = SchemaRef::from(self.table_schema.project(&projection)?);
        let schema_adapter = SchemaAdapter::new(projected_schema);

        Ok(Box::pin(async move {
            let builder = ArrowReaderBuilder::try_new_async(reader)
                .await
                .map_err(ArrowError::from)?;
            let file_schema = builder.schema();
            let (schema_mapping, adapted_projections) = schema_adapter.map_schema(&file_schema)?;
            // Offset by 1 since index 0 is the root
            let projection = adapted_projections
                .iter()
                .map(|i| i + 1)
                .collect::<Vec<_>>();
            let projection_mask =
                ProjectionMask::roots(builder.file_metadata().root_data_type(), projection);
            let stream = builder
                .with_batch_size(batch_size)
                .with_projection(projection_mask)
                .build_async();

            let adapted = stream
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))
                .map(move |maybe_batch| {
                    maybe_batch.and_then(|b| schema_mapping.map_batch(b).map_err(Into::into))
                });

            Ok(adapted.boxed())
        }))
    }
}

#[derive(Clone)]
struct OrcFileReaderRef(Arc<InternalFileReader>);

impl AsyncChunkReader for OrcFileReaderRef {
    fn len(&mut self) -> BoxFuture<'_, std::io::Result<u64>> {
        async move { Ok(self.0.get_meta().size as u64) }.boxed()
    }

    fn get_bytes(
        &mut self,
        offset_from_start: u64,
        length: u64,
    ) -> BoxFuture<'_, std::io::Result<Bytes>> {
        let offset_from_start = offset_from_start as usize;
        let length = length as usize;
        let range = offset_from_start..(offset_from_start + length);
        async move { self.0.read_fully(range).map_err(|e| e.into()) }.boxed()
    }
}
