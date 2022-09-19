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

//! Execution plans that read file formats
//!
//! Copied from datafusion v10.0.0, modified not to use dictionary for
//! partition cols.

use std::any::Any;

use std::{
    collections::HashMap,
    fmt::{Display, Formatter, Result as FmtResult},
    sync::Arc,
    vec,
};

use async_trait::async_trait;
use datafusion::arrow::array::new_null_array;
use datafusion::arrow::compute::cast;
use datafusion::arrow::record_batch::RecordBatchOptions;
use datafusion::arrow::{
    array::ArrayRef,
    datatypes::{Field, Schema, SchemaRef},
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use datafusion::datasource::{listing::PartitionedFile, object_store::ObjectStoreUrl};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::{ColumnStatistics, ExecutionPlan, Statistics};
use datafusion::{error::Result, scalar::ScalarValue};
use object_store::{ObjectMeta, ObjectStore};

pub use self::parquet::ParquetExec;

mod file_stream;
mod parquet;
mod parquet_file_format;

/// The base configurations to provide when creating a physical plan for
/// any given file format.
#[derive(Debug, Clone)]
pub struct FileScanConfig {
    /// Object store URL
    pub object_store_url: ObjectStoreUrl,
    /// Schema before projection. It contains the columns that are expected
    /// to be in the files without the table partition columns.
    pub file_schema: SchemaRef,
    /// List of files to be processed, grouped into partitions
    pub file_groups: Vec<Vec<PartitionedFile>>,
    /// Estimated overall statistics of the files, taking `filters` into account.
    pub statistics: Statistics,
    /// Columns on which to project the data. Indexes that are higher than the
    /// number of columns of `file_schema` refer to `table_partition_cols`.
    pub projection: Option<Vec<usize>>,
    /// The minimum number of records required from this source plan
    pub limit: Option<usize>,
    /// The partitioning column names
    pub table_partition_cols: Vec<String>,
    /// The partitioning columns schema
    pub partition_schema: SchemaRef,
}

impl FileScanConfig {
    /// Project the schema and the statistics on the given column indices
    fn project(&self) -> (SchemaRef, Statistics) {
        if self.projection.is_none() && self.table_partition_cols.is_empty() {
            return (Arc::clone(&self.file_schema), self.statistics.clone());
        }

        let proj_iter: Box<dyn Iterator<Item = usize>> = match &self.projection {
            Some(proj) => Box::new(proj.iter().copied()),
            None => Box::new(
                0..(self.file_schema.fields().len() + self.table_partition_cols.len()),
            ),
        };

        let mut table_fields = vec![];
        let mut table_cols_stats = vec![];
        for idx in proj_iter {
            if idx < self.file_schema.fields().len() {
                table_fields.push(self.file_schema.field(idx).clone());
                if let Some(file_cols_stats) = &self.statistics.column_statistics {
                    table_cols_stats.push(file_cols_stats[idx].clone())
                } else {
                    table_cols_stats.push(ColumnStatistics::default())
                }
            } else {
                let partition_idx = idx - self.file_schema.fields().len();
                table_fields.push(Field::new(
                    &self.table_partition_cols[partition_idx],
                    self.partition_schema
                        .field(partition_idx)
                        .data_type()
                        .clone(),
                    false,
                ));
                // TODO provide accurate stat for partition column (#1186)
                table_cols_stats.push(ColumnStatistics::default())
            }
        }

        let table_stats = Statistics {
            num_rows: self.statistics.num_rows,
            is_exact: self.statistics.is_exact,
            // TODO correct byte size?
            total_byte_size: None,
            column_statistics: Some(table_cols_stats),
        };

        let table_schema = Arc::new(Schema::new(table_fields));

        (table_schema, table_stats)
    }

    fn file_column_projection_indices(&self) -> Option<Vec<usize>> {
        self.projection.as_ref().map(|p| {
            p.iter()
                .filter(|col_idx| **col_idx < self.file_schema.fields().len())
                .copied()
                .collect()
        })
    }
}

/// A wrapper to customize partitioned file display
#[derive(Debug)]
struct FileGroupsDisplay<'a>(&'a [Vec<PartitionedFile>]);

impl<'a> Display for FileGroupsDisplay<'a> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let parts: Vec<_> = self
            .0
            .iter()
            .map(|pp| {
                pp.iter()
                    .map(|pf| pf.object_meta.location.as_ref())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .collect();
        write!(f, "[{}]", parts.join(", "))
    }
}

/// A wrapper to customize partitioned file display
#[derive(Debug)]
struct ProjectSchemaDisplay<'a>(&'a SchemaRef);

impl<'a> Display for ProjectSchemaDisplay<'a> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let parts: Vec<_> = self
            .0
            .fields()
            .iter()
            .map(|x| x.name().to_owned())
            .collect::<Vec<String>>();
        write!(f, "[{}]", parts.join(", "))
    }
}

/// A utility which can adapt file-level record batches to a table schema which may have a schema
/// obtained from merging multiple file-level schemas.
///
/// This is useful for enabling schema evolution in partitioned datasets.
///
/// This has to be done in two stages.
///
/// 1. Before reading the file, we have to map projected column indexes from the table schema to
///    the file schema.
///
/// 2. After reading a record batch we need to map the read columns back to the expected columns
///    indexes and insert null-valued columns wherever the file schema was missing a colum present
///    in the table schema.
#[derive(Clone, Debug)]
pub(crate) struct SchemaAdapter {
    /// Schema for the table
    table_schema: SchemaRef,
}

impl SchemaAdapter {
    pub(crate) fn new(table_schema: SchemaRef) -> SchemaAdapter {
        Self { table_schema }
    }

    /// Map projected column indexes to the file schema. This will fail if the table schema
    /// and the file schema contain a field with the same name and different types.
    pub fn map_projections(
        &self,
        file_schema: &Schema,
        projections: &[usize],
    ) -> Result<Vec<usize>> {
        let mut mapped: Vec<usize> = vec![];
        for idx in projections {
            let field = self.table_schema.field(*idx);
            if let Ok(mapped_idx) = file_schema.index_of(field.name().as_str()) {
                // NOTE: blaze data type is not checked here because we will
                // do some cast in adapt_batch (like binary -> string)
                mapped.push(mapped_idx)
            }
        }
        Ok(mapped)
    }

    /// Re-order projected columns by index in record batch to match table schema column ordering. If the record
    /// batch does not contain a column for an expected field, insert a null-valued column at the
    /// required column index.
    pub fn adapt_batch(
        &self,
        batch: RecordBatch,
        projections: &[usize],
    ) -> Result<RecordBatch> {
        let batch_rows = batch.num_rows();

        let batch_schema = batch.schema();

        let mut cols: Vec<ArrayRef> = Vec::with_capacity(batch.columns().len());
        let batch_cols = batch.columns().to_vec();

        for field_idx in projections {
            let table_field = &self.table_schema.fields()[*field_idx];
            if let Some((batch_idx, _name)) =
                batch_schema.column_with_name(table_field.name().as_str())
            {
                // blaze: try to cast if column type does not match table field type
                cols.push(cast(&batch_cols[batch_idx], table_field.data_type())?);
            } else {
                cols.push(new_null_array(table_field.data_type(), batch_rows))
            }
        }

        let projected_schema = Arc::new(self.table_schema.clone().project(projections)?);

        // Necessary to handle empty batches
        let mut options = RecordBatchOptions::default();
        options.row_count = Some(batch.num_rows());

        Ok(RecordBatch::try_new_with_options(
            projected_schema,
            cols,
            &options,
        )?)
    }
}

/// A helper that projects partition columns into the file record batches.
///
/// One interesting trick is the usage of a cache for the key buffers of the partition column
/// dictionaries. Indeed, the partition columns are constant, so the dictionaries that represent them
/// have all their keys equal to 0. This enables us to re-use the same "all-zero" buffer across batches,
/// which makes the space consumption of the partition columns O(batch_size) instead of O(record_count).
struct PartitionColumnProjector {
    /// Mapping between the indexes in the list of partition columns and the target
    /// schema. Sorted by index in the target schema so that we can iterate on it to
    /// insert the partition columns in the target record batch.
    projected_partition_indexes: Vec<(usize, usize)>,
    /// The schema of the table once the projection was applied.
    projected_schema: SchemaRef,
}

impl PartitionColumnProjector {
    // Create a projector to insert the partitioning columns into batches read from files
    // - projected_schema: the target schema with both file and partitioning columns
    // - table_partition_cols: all the partitioning column names
    fn new(projected_schema: SchemaRef, table_partition_cols: &[String]) -> Self {
        let mut idx_map = HashMap::new();
        for (partition_idx, partition_name) in table_partition_cols.iter().enumerate() {
            if let Ok(schema_idx) = projected_schema.index_of(partition_name) {
                idx_map.insert(partition_idx, schema_idx);
            }
        }

        let mut projected_partition_indexes: Vec<_> = idx_map.into_iter().collect();
        projected_partition_indexes.sort_by(|(_, a), (_, b)| a.cmp(b));

        Self {
            projected_partition_indexes,
            projected_schema,
        }
    }

    // Transform the batch read from the file by inserting the partitioning columns
    // to the right positions as deduced from `projected_schema`
    // - file_batch: batch read from the file, with internal projection applied
    // - partition_values: the list of partition values, one for each partition column
    fn project(
        &mut self,
        file_batch: RecordBatch,
        partition_values: &[ScalarValue],
    ) -> ArrowResult<RecordBatch> {
        let expected_cols =
            self.projected_schema.fields().len() - self.projected_partition_indexes.len();

        if file_batch.columns().len() != expected_cols {
            return Err(ArrowError::SchemaError(format!(
                "Unexpected batch schema from file, expected {} cols but got {}",
                expected_cols,
                file_batch.columns().len()
            )));
        }
        let mut cols = file_batch.columns().to_vec();
        for &(pidx, sidx) in &self.projected_partition_indexes {
            cols.insert(
                sidx,
                partition_values[pidx].to_array_of_size(file_batch.num_rows()),
            );
        }
        RecordBatch::try_new(Arc::clone(&self.projected_schema), cols)
    }
}

/// This trait abstracts all the file format specific implementations
/// from the `TableProvider`. This helps code re-utilization across
/// providers that support the the same file formats.
#[async_trait]
pub trait FileFormat: Send + Sync + std::fmt::Debug {
    /// Returns the table provider as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Infer the common schema of the provided objects. The objects will usually
    /// be analysed up to a given number of records or files (as specified in the
    /// format config) then give the estimated common schema. This might fail if
    /// the files have schemas that cannot be merged.
    async fn infer_schema(
        &self,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef>;

    /// Infer the statistics for the provided object. The cost and accuracy of the
    /// estimated statistics might vary greatly between file formats.
    ///
    /// `table_schema` is the (combined) schema of the overall table
    /// and may be a superset of the schema contained in this file.
    ///
    /// TODO: should the file source return statistics for only columns referred to in the table schema?
    async fn infer_stats(
        &self,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics>;

    /// Take a list of files and convert it to the appropriate executor
    /// according to this file format.
    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
        filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>>;
}
