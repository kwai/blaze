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

//! Execution plan for reading Parquet files

use std::{any::Any, fmt, fmt::Formatter, ops::Range, pin::Pin, sync::Arc};

use arrow::datatypes::SchemaRef;
use arrow_schema::{DataType, TimeUnit};
use auron_jni_bridge::{
    conf,
    conf::{BooleanConf, IntConf},
    jni_call_static, jni_new_global_ref, jni_new_string,
};
use bytes::Bytes;
use datafusion::{
    datasource::physical_plan::{
        FileMeta, FileOpener, FileScanConfig, FileStream, OnError, ParquetFileMetrics,
        ParquetFileReaderFactory,
    },
    error::{DataFusionError, Result},
    execution::context::TaskContext,
    parquet::{
        arrow::{
            arrow_reader::ArrowReaderOptions,
            async_reader::{AsyncFileReader, fetch_parquet_metadata},
        },
        errors::ParquetError,
        file::metadata::ParquetMetaData,
    },
    physical_expr::{EquivalenceProperties, PhysicalExprRef},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream, Statistics,
        execution_plan::{Boundedness, EmissionType},
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
    },
};
use datafusion_datasource_parquet::opener::ParquetOpener;
use datafusion_ext_commons::{batch_size, hadoop_fs::FsProvider};
use fmt::Debug;
use futures::{FutureExt, StreamExt, future::BoxFuture};
use itertools::Itertools;
use object_store::ObjectMeta;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;

use crate::{
    common::execution_context::ExecutionContext,
    scan::{AuronSchemaAdapterFactory, internal_file_reader::InternalFileReader},
};

/// Execution plan for scanning one or more Parquet partitions
#[derive(Debug, Clone)]
pub struct ParquetExec {
    fs_resource_id: String,
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    predicate: Option<PhysicalExprRef>,
    props: OnceCell<PlanProperties>,
}

impl ParquetExec {
    /// Create a new Parquet reader execution plan provided file list and
    /// schema.
    pub fn new(
        base_config: FileScanConfig,
        fs_resource_id: String,
        predicate: Option<PhysicalExprRef>,
    ) -> Self {
        let metrics = ExecutionPlanMetricsSet::new();
        let (projected_schema, _constraints, projected_statistics, _projected_output_ordering) =
            base_config.project();

        Self {
            fs_resource_id,
            base_config,
            projected_schema,
            projected_statistics,
            metrics,
            predicate,
            props: OnceCell::new(),
        }
    }
}

impl DisplayAs for ParquetExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        let limit = self.base_config.limit;
        let file_group = &self.base_config.file_groups;
        let pred = &self.predicate;
        write!(
            f,
            "ParquetExec: limit={limit:?}, file_group={file_group:?}, predicate={pred:?}"
        )
    }
}

impl ExecutionPlan for ParquetExec {
    fn name(&self) -> &str {
        "ParquetExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn properties(&self) -> &PlanProperties {
        self.props.get_or_init(|| {
            PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                Partitioning::UnknownPartitioning(self.base_config.file_groups.len()),
                EmissionType::Both,
                Boundedness::Bounded,
            )
        })
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
        let elapsed_compute = exec_ctx.baseline_metrics().elapsed_compute().clone();
        let _timer = elapsed_compute.timer();
        let io_time = exec_ctx.register_timer_metric("io_time");

        // get fs object from jni bridge resource
        let resource_id = jni_new_string!(&self.fs_resource_id)?;
        let fs = jni_call_static!(JniBridge.getResource(resource_id.as_obj()) -> JObject)?;
        let fs_provider = Arc::new(FsProvider::new(jni_new_global_ref!(fs.as_obj())?, &io_time));

        let schema_adapter_factory = Arc::new(AuronSchemaAdapterFactory);
        let projection = match self.base_config.file_column_projection_indices() {
            Some(proj) => proj,
            None => (0..self.base_config.file_schema.fields().len()).collect(),
        };

        let page_filtering_enabled = conf::PARQUET_ENABLE_PAGE_FILTERING.value()?;
        let bloom_filter_enabled = conf::PARQUET_ENABLE_BLOOM_FILTER.value()?;

        let opener: Arc<dyn FileOpener> = Arc::new(ParquetOpener {
            partition_index: partition,
            projection: Arc::from(projection),
            batch_size: batch_size(),
            limit: self.base_config.limit,
            predicate: self.predicate.clone(),
            logical_file_schema: self.base_config.file_schema.clone(),
            partition_fields: self.base_config.table_partition_cols.to_vec(),
            metadata_size_hint: None,
            metrics: self.metrics.clone(),
            parquet_file_reader_factory: Arc::new(FsReaderFactory::new(fs_provider)),
            pushdown_filters: page_filtering_enabled,
            reorder_filters: page_filtering_enabled,
            enable_page_index: page_filtering_enabled,
            enable_bloom_filter: bloom_filter_enabled,
            schema_adapter_factory,
            enable_row_group_stats_pruning: true,
            coerce_int96: Some(TimeUnit::Millisecond),
            file_decryption_properties: None,
            expr_adapter_factory: None,
        });

        let mut file_stream = FileStream::new(&self.base_config, partition, opener, &self.metrics)?;
        if conf::IGNORE_CORRUPTED_FILES.value()? {
            file_stream = file_stream.with_on_error(OnError::Skip);
        }

        let timed_stream = execute_parquet_scan(Box::pin(file_stream), exec_ctx.clone())?;
        Ok(exec_ctx.coalesce_with_default_batch_size(timed_stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self.projected_statistics.clone())
    }
}

fn execute_parquet_scan(
    mut stream: Pin<Box<FileStream>>,
    exec_ctx: Arc<ExecutionContext>,
) -> Result<SendableRecordBatchStream> {
    Ok(exec_ctx
        .clone()
        .output_with_sender("ParquetScan", move |sender| async move {
            sender.exclude_time(exec_ctx.baseline_metrics().elapsed_compute());
            let _timer = exec_ctx.baseline_metrics().elapsed_compute().timer();
            while let Some(batch) = stream.next().await.transpose()? {
                sender.send(batch).await;
            }
            Ok(())
        }))
}

#[derive(Clone)]
pub struct FsReaderFactory {
    fs_provider: Arc<FsProvider>,
}

impl FsReaderFactory {
    pub fn new(fs_provider: Arc<FsProvider>) -> Self {
        Self { fs_provider }
    }
}

impl Debug for FsReaderFactory {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "FsReaderFactory")
    }
}

impl ParquetFileReaderFactory for FsReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        _metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
        let internal_reader = Arc::new(InternalFileReader::try_new(
            self.fs_provider.clone(),
            file_meta.object_meta.clone(),
        )?);
        let reader = ParquetFileReaderRef(Arc::new(ParquetFileReader {
            internal_reader,
            metrics: ParquetFileMetrics::new(
                partition_index,
                file_meta
                    .object_meta
                    .location
                    .filename()
                    .unwrap_or("__default_filename__"),
                metrics,
            ),
        }));
        Ok(Box::new(reader))
    }
}

struct ParquetFileReader {
    internal_reader: Arc<InternalFileReader>,
    metrics: ParquetFileMetrics,
}

#[derive(Clone)]
struct ParquetFileReaderRef(Arc<ParquetFileReader>);

impl ParquetFileReader {
    fn get_meta(&self) -> ObjectMeta {
        self.internal_reader.get_meta()
    }

    fn get_internal_reader(&self) -> Arc<InternalFileReader> {
        self.internal_reader.clone()
    }
}

impl AsyncFileReader for ParquetFileReaderRef {
    fn get_bytes(
        &mut self,
        range: Range<u64>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Bytes>> {
        let inner = self.0.clone();
        inner
            .metrics
            .bytes_scanned
            .add((range.end - range.start) as usize);
        async move {
            tokio::task::spawn_blocking(move || {
                inner
                    .get_internal_reader()
                    .read_fully(range)
                    .map_err(|e| ParquetError::External(Box::new(e)))
            })
            .await
            .expect("tokio spawn_blocking error")
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Vec<Bytes>>> {
        let max_over_read_size = conf::PARQUET_MAX_OVER_READ_SIZE.value().unwrap_or(16384) as usize;
        let num_ranges = ranges.len();
        let (sorted_range_indices, sorted_ranges): (Vec<usize>, Vec<Range<u64>>) = ranges
            .into_iter()
            .enumerate()
            .sorted_unstable_by_key(|(_, r)| r.start)
            .unzip();
        let mut merged_ranges = vec![];

        for range in sorted_ranges.iter().cloned() {
            if merged_ranges.is_empty() {
                merged_ranges.push(range);
                continue;
            }

            let last_merged_range = merged_ranges.last_mut().unwrap();
            if range.start <= last_merged_range.end + max_over_read_size as u64 {
                last_merged_range.end = range.end.max(last_merged_range.end);
            } else {
                merged_ranges.push(range);
            }
        }

        async move {
            let inner = self.0.clone();
            let merged_bytes = Arc::new(Mutex::new(Vec::with_capacity(merged_ranges.len())));
            let merged_bytes_cloned = merged_bytes.clone();
            let merged_ranges_cloned = merged_ranges.clone();
            tokio::task::spawn_blocking(move || {
                let merged_bytes = &mut *merged_bytes_cloned.lock();
                for range in merged_ranges_cloned {
                    inner
                        .metrics
                        .bytes_scanned
                        .add((range.end - range.start) as usize);
                    if range.is_empty() {
                        merged_bytes.push(Bytes::new());
                        continue;
                    }
                    let bytes = inner.get_internal_reader().read_fully(range)?;
                    merged_bytes.push(bytes);
                }
                Ok::<_, DataFusionError>(())
            })
            .await
            .expect("tokio spawn_blocking error")
            .map_err(|e| ParquetError::External(Box::new(e)))?;

            let merged_bytes = &*merged_bytes.lock();
            let mut sorted_range_bytes = Vec::with_capacity(num_ranges);
            let mut m = 0;
            for range in sorted_ranges {
                if range.is_empty() {
                    sorted_range_bytes.push(Bytes::new());
                    continue;
                }
                while merged_ranges[m].end <= range.start {
                    m += 1;
                }
                let len = range.end - range.start;
                if len < merged_ranges[m].end - merged_ranges[m].start {
                    let offset = range.start - merged_ranges[m].start;
                    sorted_range_bytes
                        .push(merged_bytes[m].slice(offset as usize..(offset + len) as usize));
                } else {
                    sorted_range_bytes.push(merged_bytes[m].clone());
                }
            }

            let mut range_bytes = Vec::with_capacity(num_ranges);
            for i in 0..num_ranges {
                range_bytes.push(sorted_range_bytes[sorted_range_indices[i]].clone());
            }
            Ok(range_bytes)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, datafusion::parquet::errors::Result<Arc<ParquetMetaData>>> {
        let metadata_cache_size = conf::PARQUET_METADATA_CACHE_SIZE.value().unwrap_or(5) as usize;
        type ParquetMetaDataSlot = tokio::sync::OnceCell<Arc<ParquetMetaData>>;
        type ParquetMetaDataCacheTable = Vec<(ObjectMeta, ParquetMetaDataSlot)>;
        static METADATA_CACHE: OnceCell<Mutex<ParquetMetaDataCacheTable>> = OnceCell::new();

        let inner = self.0.clone();
        let meta_size = inner.get_meta().size as usize;
        let size_hint = None;
        let cache_slot = (move || {
            let mut metadata_cache = METADATA_CACHE.get_or_init(|| Mutex::new(Vec::new())).lock();

            // find existed cache slot
            for (cache_meta, cache_slot) in metadata_cache.iter() {
                if cache_meta.location == self.0.get_meta().location {
                    return cache_slot.clone();
                }
            }

            // reserve a new cache slot
            if metadata_cache.len() >= metadata_cache_size {
                metadata_cache.remove(0); // remove eldest
            }
            let cache_slot = ParquetMetaDataSlot::default();
            metadata_cache.push((self.0.get_meta().clone(), cache_slot.clone()));
            cache_slot
        })();

        // fetch metadata from file and update to cache
        async move {
            cache_slot
                .get_or_try_init(move || async move {
                    fetch_parquet_metadata(
                        move |range| {
                            let inner = inner.clone();
                            inner.metrics.bytes_scanned.add(range.end - range.start);
                            async move {
                                tokio::task::spawn_blocking(move || {
                                    inner
                                        .get_internal_reader()
                                        .read_fully(range.start as u64..range.end as u64)
                                        .map_err(|e| ParquetError::External(Box::new(e)))
                                })
                                .await
                                .expect("tokio spawn_blocking error")
                            }
                        },
                        meta_size,
                        size_hint,
                    )
                    .await
                    .map(|parquet_metadata| Arc::new(parquet_metadata))
                })
                .map(|parquet_metadata| parquet_metadata.cloned())
                .await
        }
        .boxed()
    }

    fn get_bytes_sync(&mut self, range: Range<u64>) -> datafusion::parquet::errors::Result<Bytes> {
        self.0
            .get_internal_reader()
            .read_fully(range)
            .map_err(|e| ParquetError::External(Box::new(e)))
    }
}

fn expr_contains_decimal_type(expr: &PhysicalExprRef, schema: &SchemaRef) -> Result<bool> {
    if matches!(expr.data_type(schema)?, DataType::Decimal128(..)) {
        return Ok(true);
    }
    for child_expr in expr.children().iter() {
        if expr_contains_decimal_type(&child_expr, schema)? {
            return Ok(true);
        }
    }
    Ok(false)
}
