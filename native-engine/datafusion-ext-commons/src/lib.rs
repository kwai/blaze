#![feature(new_uninit)]
#![feature(io_error_other)]
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

use arrow::compute::concat;
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use log::trace;

pub mod array_builder;
pub mod cast;
pub mod ffi;
pub mod hadoop_fs;
pub mod io;
pub mod loser_tree;
pub mod spark_hash;
pub mod streams;
pub mod uda;

/// Concatenates an array of `RecordBatch` into one batch
pub fn concat_batches(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    row_count: usize,
) -> ArrowResult<RecordBatch> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }
    let mut arrays = Vec::with_capacity(schema.fields().len());
    for i in 0..schema.fields().len() {
        let array = concat(
            &batches
                .iter()
                .map(|batch| batch.column(i).as_ref())
                .collect::<Vec<_>>(),
        )?;
        arrays.push(array);
    }
    trace!(
        "Combined {} batches containing {} rows",
        batches.len(),
        row_count
    );

    let options = RecordBatchOptions::new().with_row_count(Some(row_count));

    RecordBatch::try_new_with_options(schema.clone(), arrays, &options)
}
