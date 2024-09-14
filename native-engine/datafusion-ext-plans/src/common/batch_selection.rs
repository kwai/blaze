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

use arrow::{
    array::{ArrayRef, ArrowPrimitiveType, PrimitiveArray},
    datatypes::SchemaRef,
    error::Result as ArrowResult,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use datafusion::common::Result;

pub fn take_batch<T: ArrowPrimitiveType>(
    batch: RecordBatch,
    indices: impl Into<PrimitiveArray<T>>,
) -> Result<RecordBatch> {
    take_batch_internal(batch, indices.into())
}

pub fn take_cols<T: ArrowPrimitiveType>(
    cols: &[ArrayRef],
    indices: impl Into<PrimitiveArray<T>>,
) -> Result<Vec<ArrayRef>> {
    take_cols_internal(cols, &indices.into())
}

fn take_batch_internal<T: ArrowPrimitiveType>(
    batch: RecordBatch,
    indices: impl Into<PrimitiveArray<T>>,
) -> Result<RecordBatch> {
    let indices = indices.into();
    let taken_num_batch_rows = indices.len();
    let schema = batch.schema();
    let cols = batch.columns();

    let cols = take_cols_internal(cols, &indices)?;
    drop(indices);

    let taken = RecordBatch::try_new_with_options(
        schema,
        cols,
        &RecordBatchOptions::new().with_row_count(Some(taken_num_batch_rows)),
    )?;
    Ok(taken)
}

fn take_cols_internal<T: ArrowPrimitiveType>(
    cols: &[ArrayRef],
    indices: &PrimitiveArray<T>,
) -> Result<Vec<ArrayRef>> {
    let cols = cols
        .into_iter()
        .map(|c| Ok(arrow::compute::take(&c, indices, None)?))
        .collect::<Result<_>>()?;
    Ok(cols)
}

pub fn interleave_batches(
    schema: SchemaRef,
    batches: &[RecordBatch],
    indices: &[(usize, usize)],
) -> Result<RecordBatch> {
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

    Ok(RecordBatch::try_new_with_options(
        schema.clone(),
        batches_arrays
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
