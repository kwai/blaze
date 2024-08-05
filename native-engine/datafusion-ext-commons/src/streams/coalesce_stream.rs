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

use std::{
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};

use arrow::{
    array::{make_array, new_empty_array, Array, ArrayRef, AsArray, Capacities, MutableArrayData},
    datatypes::{
        ArrowNativeType, BinaryType, ByteArrayType, LargeBinaryType, LargeUtf8Type, SchemaRef,
        Utf8Type,
    },
    record_batch::{RecordBatch, RecordBatchOptions},
};
use arrow_schema::DataType;
use datafusion::{
    common::Result,
    execution::TaskContext,
    physical_plan::{
        metrics::{BaselineMetrics, Time},
        RecordBatchStream, SendableRecordBatchStream,
    },
};
use futures::{Stream, StreamExt};

use crate::{array_size::ArraySize, batch_size, suggested_output_batch_mem_size};

pub trait CoalesceInput {
    fn coalesce_input(
        &self,
        input: SendableRecordBatchStream,
        batch_size: usize,
        metrics: &BaselineMetrics,
    ) -> Result<SendableRecordBatchStream>;

    fn coalesce_with_default_batch_size(
        &self,
        input: SendableRecordBatchStream,
        metrics: &BaselineMetrics,
    ) -> Result<SendableRecordBatchStream>;
}

impl CoalesceInput for Arc<TaskContext> {
    fn coalesce_input(
        &self,
        input: SendableRecordBatchStream,
        batch_size: usize,
        metrics: &BaselineMetrics,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(CoalesceStream::new(
            input,
            batch_size,
            metrics.elapsed_compute().clone(),
        )))
    }

    fn coalesce_with_default_batch_size(
        &self,
        input: SendableRecordBatchStream,
        metrics: &BaselineMetrics,
    ) -> Result<SendableRecordBatchStream> {
        self.coalesce_input(input, batch_size(), metrics)
    }
}

pub struct CoalesceStream {
    input: SendableRecordBatchStream,
    staging_batches: Vec<RecordBatch>,
    staging_rows: usize,
    staging_batches_mem_size: usize,
    batch_size: usize,
    elapsed_compute: Time,
}

impl CoalesceStream {
    pub fn new(input: SendableRecordBatchStream, batch_size: usize, elapsed_compute: Time) -> Self {
        Self {
            input,
            staging_batches: vec![],
            staging_rows: 0,
            staging_batches_mem_size: 0,
            batch_size,
            elapsed_compute,
        }
    }

    fn coalesce(&mut self) -> Result<RecordBatch> {
        // better concat_batches() implementation that releases old batch columns asap.
        let schema = self.input.schema();

        // collect all columns
        let mut all_cols = schema.fields().iter().map(|_| vec![]).collect::<Vec<_>>();
        for batch in std::mem::take(&mut self.staging_batches) {
            for i in 0..all_cols.len() {
                all_cols[i].push(batch.column(i).clone());
            }
        }

        // coalesce each column
        let mut coalesced_cols = vec![];
        for (cols, field) in all_cols.into_iter().zip(schema.fields()) {
            let dt = field.data_type();
            coalesced_cols.push(coalesce_arrays_unchecked(dt, &cols));
        }
        let coalesced_batch = RecordBatch::try_new_with_options(
            schema,
            coalesced_cols,
            &RecordBatchOptions::new().with_row_count(Some(self.staging_rows)),
        )?;
        self.staging_rows = 0;
        self.staging_batches_mem_size = 0;
        Ok(coalesced_batch)
    }

    fn should_flush(&self) -> bool {
        let size_limit = suggested_output_batch_mem_size();
        let (batch_size_limit, mem_size_limit) = if self.staging_batches.len() > 1 {
            (self.batch_size, size_limit)
        } else {
            (self.batch_size / 2, size_limit / 2)
        };
        self.staging_rows >= batch_size_limit || self.staging_batches_mem_size > mem_size_limit
    }
}

impl RecordBatchStream for CoalesceStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

impl Stream for CoalesceStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let elapsed_time = self.elapsed_compute.clone();
        loop {
            match ready!(self.input.poll_next_unpin(cx)).transpose()? {
                Some(batch) => {
                    let _timer = elapsed_time.timer();
                    let num_rows = batch.num_rows();
                    if num_rows > 0 {
                        self.staging_rows += batch.num_rows();
                        self.staging_batches_mem_size += batch.get_array_mem_size();
                        self.staging_batches.push(batch);
                        if self.should_flush() {
                            let coalesced = self.coalesce()?;
                            return Poll::Ready(Some(Ok(coalesced)));
                        }
                        continue;
                    }
                }
                None if !self.staging_batches.is_empty() => {
                    let _timer = elapsed_time.timer();
                    let coalesced = self.coalesce()?;
                    return Poll::Ready(Some(Ok(coalesced)));
                }
                None => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

/// coalesce arrays without checking there data types, invokers must make
/// sure all arrays have the same data type
pub fn coalesce_arrays_unchecked(data_type: &DataType, arrays: &[ArrayRef]) -> ArrayRef {
    if arrays.is_empty() {
        return new_empty_array(data_type);
    }
    if arrays.len() == 1 {
        return arrays[0].clone();
    }

    fn binary_capacity<T: ByteArrayType>(arrays: &[ArrayRef]) -> Capacities {
        let mut item_capacity = 0;
        let mut bytes_capacity = 0;
        for array in arrays {
            let a = array.as_bytes::<T>();

            // Guaranteed to always have at least one element
            let offsets = a.value_offsets();
            bytes_capacity += offsets[offsets.len() - 1].as_usize() - offsets[0].as_usize();
            item_capacity += a.len();
        }
        Capacities::Binary(item_capacity, Some(bytes_capacity))
    }

    let capacity = match data_type {
        DataType::Utf8 => binary_capacity::<Utf8Type>(arrays),
        DataType::LargeUtf8 => binary_capacity::<LargeUtf8Type>(arrays),
        DataType::Binary => binary_capacity::<BinaryType>(arrays),
        DataType::LargeBinary => binary_capacity::<LargeBinaryType>(arrays),
        _ => Capacities::Array(arrays.iter().map(|a| a.len()).sum()),
    };

    // Concatenates arrays using MutableArrayData
    let array_data: Vec<_> = arrays.iter().map(|a| a.to_data()).collect::<Vec<_>>();
    let array_data_refs = array_data.iter().collect();
    let mut mutable = MutableArrayData::with_capacities(array_data_refs, false, capacity);

    for (i, a) in arrays.iter().enumerate() {
        mutable.extend(i, 0, a.len())
    }
    make_array(mutable.freeze())
}
