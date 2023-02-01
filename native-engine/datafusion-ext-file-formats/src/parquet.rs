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

use fmt::Debug;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::{any::Any, convert::TryInto};

use arrow::{
    array::ArrayRef,
    datatypes::{Schema, SchemaRef},
    error::ArrowError,
};
use bytes::Bytes;
use datafusion::common::Column;
use datafusion::datasource::listing::FileRange;
use datafusion::logical_expr::Expr;
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::{
    metadata::{ParquetMetaData, RowGroupMetaData},
    statistics::Statistics as ParquetStatistics,
};
use datafusion::physical_plan::metrics::{BaselineMetrics, MetricValue, Time};
use datafusion::physical_plan::Metric;
use datafusion::{
    error::Result,
    execution::context::TaskContext,
    physical_plan::{
        expressions::PhysicalSortExpr,
        metrics::{self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
        DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
        Statistics,
    },
    scalar::ScalarValue,
};
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryStreamExt};
use jni::objects::JObject;
use log::{debug, warn};
use once_cell::sync::OnceCell;

use crate::file_stream::{FileStream, FormatReader, ReaderFuture};
use crate::fs::{FsDataInputStream, FsProvider};
use crate::parquet_file_format::fetch_parquet_metadata;
use crate::pruning::{PruningPredicate, PruningStatistics};
use crate::{FileScanConfig, ObjectMeta, SchemaAdapter};
use blaze_commons::{jni_call_static, jni_new_global_ref, jni_new_string};

/// Execution plan for scanning one or more Parquet partitions
#[derive(Debug, Clone)]
pub struct ParquetExec {
    base_config: FileScanConfig,
    fs_resource_id: String,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Optional predicate for pruning row groups
    pruning_predicate: Option<PruningPredicate>,
}

/// Stores metrics about the parquet execution for a particular parquet file
#[derive(Debug, Clone)]
struct ParquetFileMetrics {
    /// Number of times the predicate could not be evaluated
    pub predicate_evaluation_errors: metrics::Count,
    /// Number of row groups pruned using
    pub row_groups_pruned: metrics::Count,
    /// Total number of bytes scanned
    pub bytes_scanned: metrics::Count,
}

impl ParquetExec {
    /// Create a new Parquet reader execution plan provided file list and schema.
    pub fn new(
        base_config: FileScanConfig,
        fs_resource_id: String,
        predicate: Option<Expr>,
    ) -> Self {
        debug!("Creating ParquetExec, files: {:?}, projection {:?}, predicate: {:?}, limit: {:?}",
            base_config.file_groups, base_config.projection, predicate, base_config.limit);

        let metrics = ExecutionPlanMetricsSet::new();
        let predicate_creation_errors =
            MetricBuilder::new(&metrics).global_counter("num_predicate_creation_errors");

        let pruning_predicate = predicate.and_then(|predicate_expr| {
            match PruningPredicate::try_new(
                predicate_expr,
                base_config.file_schema.clone(),
            ) {
                Ok(pruning_predicate) => Some(pruning_predicate),
                Err(e) => {
                    debug!("Could not create pruning predicate for: {}", e);
                    predicate_creation_errors.add(1);
                    None
                }
            }
        });

        let (projected_schema, projected_statistics) = base_config.project();

        Self {
            base_config,
            fs_resource_id,
            projected_schema,
            projected_statistics,
            metrics,
            pruning_predicate,
        }
    }

    /// Ref to the base configs
    pub fn base_config(&self) -> &FileScanConfig {
        &self.base_config
    }

    /// Optional reference to this parquet scan's pruning predicate
    pub fn pruning_predicate(&self) -> Option<&PruningPredicate> {
        self.pruning_predicate.as_ref()
    }
}

impl ParquetFileMetrics {
    /// Create new metrics
    pub fn new(
        partition: usize,
        filename: &str,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        let predicate_evaluation_errors = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("predicate_evaluation_errors", partition);

        let row_groups_pruned = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("row_groups_pruned", partition);

        let bytes_scanned = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("bytes_scanned", partition);

        Self {
            predicate_evaluation_errors,
            row_groups_pruned,
            bytes_scanned,
        }
    }
}

impl ExecutionPlan for ParquetExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.base_config.file_groups.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn relies_on_input_order(&self) -> bool {
        false
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
        let timer = baseline_metrics.elapsed_compute().timer();

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
        let fs_provider = Arc::new(FsProvider::new(
            jni_new_global_ref!(jni_call_static!(
                JniBridge.getResource(
                    jni_new_string!(&self.fs_resource_id)?
                ) -> JObject
            )?)?,
            &io_time,
        ));

        let projection = match self.base_config.file_column_projection_indices() {
            Some(proj) => proj,
            None => (0..self.base_config.file_schema.fields().len()).collect(),
        };

        let opener = ParquetOpener {
            partition_index,
            projection: Arc::from(projection),
            batch_size: context.session_config().batch_size(),
            pruning_predicate: self.pruning_predicate.clone(),
            table_schema: self.base_config.file_schema.clone(),
            metrics: self.metrics.clone(),
        };
        drop(timer);

        let stream = FileStream::new(
            fs_provider,
            &self.base_config,
            partition_index,
            context,
            opener,
            baseline_metrics,
        )?;
        Ok(Box::pin(stream))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                // NOTE:
                //  partitions is too long and slow to format in some cases, so
                //  disable the partitions field in logging.
                //
                // super::FileGroupsDisplay(&self.base_config.file_groups),
                //
                if let Some(pre) = &self.pruning_predicate {
                    write!(
                        f,
                        "ParquetExec: limit={:?}, partitions=..., predicate={}, projection={}",
                        self.base_config.limit,
                        pre.predicate_expr(),
                        super::ProjectSchemaDisplay(&self.projected_schema),
                    )
                } else {
                    write!(
                        f,
                        "ParquetExec: limit={:?}, partitions=..., projection={}",
                        self.base_config.limit,
                        super::ProjectSchemaDisplay(&self.projected_schema),
                    )
                }
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        self.projected_statistics.clone()
    }
}

/// Implements [`FormatReader`] for a parquet file
struct ParquetOpener {
    partition_index: usize,
    projection: Arc<[usize]>,
    batch_size: usize,
    pruning_predicate: Option<PruningPredicate>,
    table_schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
}

impl FormatReader for ParquetOpener {
    fn open(
        &self,
        fs_provider: Arc<FsProvider>,
        meta: ObjectMeta,
        range: Option<FileRange>,
    ) -> ReaderFuture {
        let metrics = ParquetFileMetrics::new(
            self.partition_index,
            meta.location.as_ref(),
            &self.metrics,
        );

        let reader = ParquetFileReader {
            fs_provider,
            input: OnceCell::new(),
            meta,
            metrics: metrics.clone(),
        };

        let schema_adapter = SchemaAdapter::new(self.table_schema.clone());
        let batch_size = self.batch_size;
        let projection = self.projection.clone();
        let pruning_predicate = self.pruning_predicate.clone();

        Box::pin(async move {
            let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
            let adapted_projections =
                schema_adapter.map_projections(builder.schema(), &projection)?;

            let mask = ProjectionMask::roots(
                builder.parquet_schema(),
                adapted_projections.iter().cloned(),
            );

            let groups = builder.metadata().row_groups();
            let row_groups = prune_row_groups(groups, range, pruning_predicate, &metrics);

            let stream = builder
                .with_projection(mask)
                .with_batch_size(batch_size)
                .with_row_groups(row_groups)
                .build()?;

            let adapted = stream
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))
                .map(move |maybe_batch| {
                    maybe_batch.and_then(|b| {
                        schema_adapter
                            .adapt_batch(b, &projection)
                            .map_err(Into::into)
                    })
                });

            Ok(adapted.boxed())
        })
    }
}

/// Implements [`AsyncFileReader`] for a parquet file in object storage
struct ParquetFileReader {
    fs_provider: Arc<FsProvider>,
    input: OnceCell<Arc<FsDataInputStream>>,
    meta: ObjectMeta,
    metrics: ParquetFileMetrics,
}

impl ParquetFileReader {
    fn get_input(&self) -> datafusion::parquet::errors::Result<Arc<FsDataInputStream>> {
        let input = self
            .input
            .get_or_try_init(|| -> Result<Arc<FsDataInputStream>> {
                let fs = self.fs_provider.provide(&self.meta.location)?;
                Ok(Arc::new(fs.open(&self.meta.location)?))
            })
            .map_err(|e| {
                ParquetError::General(format!(
                    "ParquetFileReader: get FSDataInputStream error: {}",
                    e
                ))
            })?;
        Ok(input.clone())
    }
}
impl AsyncFileReader for ParquetFileReader {
    fn get_bytes(
        &mut self,
        range: Range<usize>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Bytes>> {
        self.metrics.bytes_scanned.add(range.end - range.start);

        futures::future::ready(())
            .map(move |_| -> datafusion::parquet::errors::Result<Bytes> {
                let mut bytes = vec![0u8; range.len()];
                self.get_input()?
                    .read_fully(range.start as u64, &mut bytes)
                    .map_err(|e| {
                        ParquetError::General(format!(
                            "parquetFileReader::get_bytes error: {}",
                            e
                        ))
                    })?;
                Ok(Bytes::from(bytes))
            })
            .boxed()
    }

    fn get_metadata(
        &mut self,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let metadata = fetch_parquet_metadata(self.get_input()?, &self.meta)
                .await
                .map_err(|e| {
                    ParquetError::General(format!(
                        "parquetFileReader::get_metadata error: {}",
                        e
                    ))
                })?;
            Ok(Arc::new(metadata))
        })
    }
}

/// Wraps parquet statistics in a way
/// that implements [`PruningStatistics`]
struct RowGroupPruningStatistics<'a> {
    row_group_metadata: &'a RowGroupMetaData,
    parquet_schema: &'a Schema,
}

/// Extract the min/max statistics from a `ParquetStatistics` object
macro_rules! get_statistic {
    ($column_statistics:expr, $func:ident, $bytes_func:ident) => {{
        if !$column_statistics.has_min_max_set() {
            return None;
        }
        match $column_statistics {
            ParquetStatistics::Boolean(s) => Some(ScalarValue::Boolean(Some(*s.$func()))),
            ParquetStatistics::Int32(s) => Some(ScalarValue::Int32(Some(*s.$func()))),
            ParquetStatistics::Int64(s) => Some(ScalarValue::Int64(Some(*s.$func()))),
            // 96 bit ints not supported
            ParquetStatistics::Int96(_) => None,
            ParquetStatistics::Float(s) => Some(ScalarValue::Float32(Some(*s.$func()))),
            ParquetStatistics::Double(s) => Some(ScalarValue::Float64(Some(*s.$func()))),
            ParquetStatistics::ByteArray(s) => {
                let s = std::str::from_utf8(s.$bytes_func())
                    .map(|s| s.to_string())
                    .ok();
                Some(ScalarValue::Utf8(s))
            }
            // type not supported yet
            ParquetStatistics::FixedLenByteArray(_) => None,
        }
    }};
}

// Extract the min or max value calling `func` or `bytes_func` on the ParquetStatistics as appropriate
macro_rules! get_min_max_values {
    ($self:expr, $column:expr, $func:ident, $bytes_func:ident) => {{
        let (_column_index, field) =
            if let Some((v, f)) = $self.parquet_schema.column_with_name(&$column.name) {
                (v, f)
            } else {
                // Named column was not present
                return None;
            };

        let data_type = field.data_type();
        // The result may be None, because DataFusion doesn't have support for ScalarValues of the column type
        let null_scalar: ScalarValue = data_type.try_into().ok()?;

        $self.row_group_metadata
            .columns()
            .iter()
            .find(|c| c.column_descr().name() == &$column.name)
            .and_then(|c| c.statistics())
            .map(|stats| get_statistic!(stats, $func, $bytes_func))
            .flatten()
            // column either didn't have statistics at all or didn't have min/max values
            .or_else(|| Some(null_scalar.clone()))
            .map(|s| s.to_array())
    }}
}

// Extract the null count value on the ParquetStatistics
macro_rules! get_null_count_values {
    ($self:expr, $column:expr) => {{
        let value = ScalarValue::UInt64(
            if let Some(col) = $self
                .row_group_metadata
                .columns()
                .iter()
                .find(|c| c.column_descr().name() == &$column.name)
            {
                col.statistics().map(|s| s.null_count())
            } else {
                Some($self.row_group_metadata.num_rows() as u64)
            },
        );

        Some(value.to_array())
    }};
}

impl<'a> PruningStatistics for RowGroupPruningStatistics<'a> {
    fn num_rows(&self, _column: &Column) -> Option<ArrayRef> {
        let num_rows = self.row_group_metadata.num_rows();
        Some(ScalarValue::UInt64(Some(num_rows as u64)).to_array())
    }

    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        get_min_max_values!(self, column, min, min_bytes)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        get_min_max_values!(self, column, max, max_bytes)
    }

    fn num_containers(&self) -> usize {
        1
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        get_null_count_values!(self, column)
    }
}

fn prune_row_groups(
    groups: &[RowGroupMetaData],
    range: Option<FileRange>,
    predicate: Option<PruningPredicate>,
    metrics: &ParquetFileMetrics,
) -> Vec<usize> {
    // TODO: Columnar pruning
    let mut filtered = Vec::with_capacity(groups.len());
    for (idx, metadata) in groups.iter().enumerate() {
        if let Some(range) = &range {
            let offset = metadata.column(0).file_offset();
            if offset < range.start || offset >= range.end {
                continue;
            }
        }

        if let Some(predicate) = &predicate {
            let pruning_stats = RowGroupPruningStatistics {
                row_group_metadata: metadata,
                parquet_schema: predicate.schema().as_ref(),
            };
            match predicate.prune(&pruning_stats) {
                Ok(values) => {
                    // NB: false means don't scan row group
                    if !values[0] {
                        metrics.row_groups_pruned.add(1);
                        continue;
                    }
                }
                // stats filter array could not be built
                // return a closure which will not filter out any row groups
                Err(e) => {
                    warn!("Error evaluating row group predicate values {}", e);
                    metrics.predicate_evaluation_errors.add(1);
                }
            }
        }

        filtered.push(idx)
    }
    filtered
}
