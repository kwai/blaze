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

use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::Result;

pub mod bytes_arena;
pub mod cached_exprs_evaluator;
pub mod memory_manager;
pub mod onheap_spill;
pub mod output;
pub mod rdxsort;

pub struct BatchesInterleaver {
    schema: SchemaRef,
    batches_arrays: Vec<Vec<ArrayRef>>,
}

impl BatchesInterleaver {
    pub fn new(schema: SchemaRef, batches: &[RecordBatch]) -> Self {
        let mut batches_arrays: Vec<Vec<ArrayRef>> = schema
            .fields()
            .iter()
            .map(|_| Vec::with_capacity(batches.len()))
            .collect();
        for batch in batches {
            for (col_idx, column) in batch.columns().iter().enumerate() {
                batches_arrays[col_idx].push(column.clone());
            }
        }

        Self {
            schema,
            batches_arrays,
        }
    }

    pub fn interleave(&self, indices: &[(usize, usize)]) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new_with_options(
            self.schema.clone(),
            self.batches_arrays
                .iter()
                .map(|arrays| {
                    arrow::compute::interleave(
                        &arrays
                            .iter()
                            .map(|array| array.as_ref())
                            .collect::<Vec<_>>(),
                        indices,
                    )
                })
                .collect::<ArrowResult<Vec<_>>>()?,
            &RecordBatchOptions::new().with_row_count(Some(indices.len())),
        )?)
    }
}
