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

use arrow::array::{ArrayRef, PrimitiveArray, UInt32Array};
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::Result;

pub mod batch_statisitcs;
pub mod bytes_arena;
pub mod cached_exprs_evaluator;
pub mod column_pruning;
pub mod memory_manager;
pub mod onheap_spill;
pub mod output;
pub mod rdxsort;
pub mod slim_bytes;

pub struct BatchTaker<'a>(pub &'a RecordBatch);

impl BatchTaker<'_> {
    pub fn take<T: num::PrimInt>(
        &self,
        indices: impl IntoIterator<Item = T>,
    ) -> Result<RecordBatch> {
        let indices: UInt32Array =
            PrimitiveArray::from_iter(indices.into_iter().map(|idx| idx.to_u32().unwrap()));
        self.take_impl(&indices)
    }

    pub fn take_opt<T: num::PrimInt>(
        &self,
        indices: impl IntoIterator<Item = Option<T>>,
    ) -> Result<RecordBatch> {
        let indices: UInt32Array = PrimitiveArray::from_iter(
            indices
                .into_iter()
                .map(|opt| opt.map(|idx| idx.to_u32().unwrap())),
        );
        self.take_impl(&indices)
    }

    fn take_impl(&self, indices: &UInt32Array) -> Result<RecordBatch> {
        let cols = self
            .0
            .columns()
            .iter()
            .map(|c| Ok(arrow::compute::take(c, indices, None)?))
            .collect::<Result<_>>()?;
        let taken = RecordBatch::try_new_with_options(
            self.0.schema(),
            cols,
            &RecordBatchOptions::new().with_row_count(Some(indices.len())),
        )?;
        Ok(taken)
    }
}

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
