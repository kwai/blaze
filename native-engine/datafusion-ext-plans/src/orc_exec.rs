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
    datasource::{
        physical_plan::{FileMeta, FileOpenFuture, FileOpener, FileScanConfig, FileStream},
        schema_adapter::SchemaAdapter,
    },
    error::Result,
    execution::context::TaskContext,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
        DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PhysicalExpr,
        PlanProperties, SendableRecordBatchStream, Statistics,
    },
};
use datafusion::datasource::schema_adapter::SchemaMapper;
use datafusion_ext_commons::{batch_size, df_execution_err, hadoop_fs::FsProvider};
use futures::{future::BoxFuture, FutureExt, StreamExt};
use futures_util::TryStreamExt;
use once_cell::sync::OnceCell;
use orc_rust::{
    arrow_reader::ArrowReaderBuilder, projection::ProjectionMask, reader::AsyncChunkReader,
    reader::metadata::FileMetadata,
};

use crate::{
    common::execution_context::ExecutionContext,
    scan::{internal_file_reader::InternalFileReader, BlazeSchemaMapping},
};

/// Execution plan for scanning one or more Orc partitions
#[derive(Debug, Clone)]
pub struct OrcExec {
    fs_resource_id: String,
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    _predicate: Option<Arc<dyn PhysicalExpr>>,
    props: OnceCell<PlanProperties>,
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

        let (projected_schema, projected_statistics, _projected_output_ordering) =
            base_config.project();

        Self {
            fs_resource_id,
            base_config,
            projected_statistics,
            projected_schema,
            metrics,
            _predicate,
            props: OnceCell::new(),
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
    fn name(&self) -> &str {
        "OrcExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn properties(&self) -> &PlanProperties {
        self.props.get_or_init(|| {
            PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                Partitioning::UnknownPartitioning(self.base_config.file_groups.len()),
                ExecutionMode::Bounded,
            )
        })
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
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
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);
        let io_time = exec_ctx.register_timer_metric("io_time");

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
            table_schema: self.base_config.file_schema.clone(),
            fs_provider,
        };

        let mut file_stream = Box::pin(FileStream::new(
            &self.base_config,
            partition,
            opener,
            exec_ctx.execution_plan_metrics(),
        )?);
        let timed_stream =
            exec_ctx
                .clone()
                .output_with_sender("OrcScan", move |sender| async move {
                    sender.exclude_time(exec_ctx.baseline_metrics().elapsed_compute());
                    let _timer = exec_ctx.baseline_metrics().elapsed_compute().timer();
                    while let Some(batch) = file_stream.next().await.transpose()? {
                        sender.send(batch).await;
                    }
                    Ok(())
                });
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
    table_schema: SchemaRef,
    fs_provider: Arc<FsProvider>,
}

impl OrcOpener {
    fn map_schema(&self, orc_file_meta: &FileMetadata) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        let projection = self.projection.clone();
        let projected_schema = SchemaRef::from(self.table_schema.project(&projection)?);

        let mut projection = Vec::with_capacity(projected_schema.fields().len());
        let mut field_mappings = vec![None; self.table_schema.fields().len()];

        for nameColumn in orc_file_meta.root_data_type().children() {
            if let Some((table_idx, _table_field)) =
            projected_schema.fields().find(nameColumn.name()) {
                field_mappings[table_idx] = Some(projection.len());
                projection.push(nameColumn.data_type().column_index());
            }
        }

        Ok((
            Arc::new(BlazeSchemaMapping::new(self.table_schema.clone(),
                                             field_mappings,
            )),
            projection,
        ))
    }
}

impl FileOpener for OrcOpener {
    fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture> {
        let reader = OrcFileReaderRef(Arc::new(InternalFileReader::try_new(
            self.fs_provider.clone(),
            file_meta.object_meta.clone(),
        )?));
        let batch_size = self.batch_size;


        Ok(Box::pin(async move {
            let mut builder = ArrowReaderBuilder::try_new_async(reader)
                .await
                .or_else(|err| df_execution_err!("create orc reader error: {err}"))?;
            if let Some(range) = file_meta.range.clone() {
                let range = range.start as usize..range.end as usize;
                builder = builder.with_file_byte_range(range);
            }

            let (schema_mapping, projection) =
                self.map_schema(builder.file_metadata())?;

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
