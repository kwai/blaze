// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{ptr::NonNull, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, ArrowPrimitiveType, BinaryArray, BooleanBufferBuilder, GenericByteArray,
        PrimitiveArray, StringArray, downcast_primitive,
    },
    buffer::{MutableBuffer, NullBuffer, OffsetBuffer, ScalarBuffer},
    datatypes::{ArrowNativeType, ByteArrayType},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use arrow_schema::DataType;
use datafusion::common::Result;

use crate::{downcast_any, prefetch_read_data, unchecked};

pub fn take_batch<T: ArrowPrimitiveType>(
    batch: RecordBatch,
    indices: impl Into<PrimitiveArray<T>>,
) -> Result<RecordBatch> {
    let indices = indices.into();
    let taken_num_batch_rows = indices.len();
    Ok(RecordBatch::try_new_with_options(
        batch.schema(),
        take_cols_internal(batch.columns(), &indices)?,
        &RecordBatchOptions::new().with_row_count(Some(taken_num_batch_rows)),
    )?)
}

pub fn take_cols<T: ArrowPrimitiveType>(
    cols: &[ArrayRef],
    indices: impl Into<PrimitiveArray<T>>,
) -> Result<Vec<ArrayRef>> {
    take_cols_internal(cols, &indices.into())
}

fn take_cols_internal<T: ArrowPrimitiveType>(
    cols: &[ArrayRef],
    indices: &PrimitiveArray<T>,
) -> Result<Vec<ArrayRef>> {
    cols.into_iter()
        .map(|c| Ok(arrow::compute::take(&c, indices, None)?))
        .collect::<Result<_>>()
}

pub type ArrayInterleaver = Box<dyn Fn(&[(usize, usize)]) -> Result<ArrayRef> + Send>;
pub type BatchInterleaver = Box<dyn Fn(&[(usize, usize)]) -> Result<RecordBatch> + Send>;

#[inline]
pub fn create_batch_interleaver(
    batches: &[RecordBatch],
    with_prefetching: bool,
) -> Result<BatchInterleaver> {
    let batch_schema = batches[0].schema();
    let mut col_arrays = vec![vec![]; batches[0].num_columns()];
    for batch in batches {
        for (col_idx, col) in batch.columns().iter().enumerate() {
            col_arrays[col_idx].push(col.clone());
        }
    }
    let col_interleavers = col_arrays
        .iter()
        .map(|arrays| create_array_interleaver(arrays, with_prefetching))
        .collect::<Result<Vec<_>>>()?;
    Ok(Box::new(move |indices| {
        let cols = col_interleavers
            .iter()
            .map(|col_interleaver| col_interleaver(indices))
            .collect::<Result<Vec<_>>>();
        let batch = RecordBatch::try_new_with_options(
            batch_schema.clone(),
            cols?,
            &RecordBatchOptions::new().with_row_count(Some(indices.len())),
        )?;
        Ok(batch)
    }))
}

#[inline]
pub fn create_array_interleaver(
    values: &[ArrayRef],
    with_prefetching: bool,
) -> Result<ArrayInterleaver> {
    struct Interleave<T> {
        arrays: Vec<T>,
        has_nulls: bool,
    }

    impl<T: Array> Interleave<T> {
        fn new(arrays: Vec<T>) -> Self {
            let has_nulls = arrays.iter().any(|v| v.null_count() > 0);
            Self { arrays, has_nulls }
        }

        fn nulls(&self, indices: &[(usize, usize)]) -> Option<NullBuffer> {
            let nulls = match self.has_nulls {
                true => {
                    let mut builder = BooleanBufferBuilder::new(indices.len());
                    for (a, b) in indices {
                        let v = self.arrays[*a].is_valid(*b);
                        builder.append(v)
                    }
                    Some(NullBuffer::new(builder.finish()))
                }
                false => None,
            };
            nulls
        }
    }

    #[inline]
    fn interleave_primitive<T: ArrowPrimitiveType, const WITH_PREFETCHING: bool>(
        interleaver: &Interleave<PrimitiveArray<T>>,
        indices: &[(usize, usize)],
        dt: &DataType,
    ) -> Result<ArrayRef> {
        let nulls = interleaver.nulls(indices);
        let mut values = Vec::with_capacity(indices.len());

        for (i, &(array_idx, value_idx)) in indices.iter().enumerate() {
            if WITH_PREFETCHING {
                const PREFETCH_AHEAD: usize = 4;
                if i + PREFETCH_AHEAD < indices.len() {
                    let (prefetch_array_idx, prefetch_value_idx) = indices[i + PREFETCH_AHEAD];
                    prefetch_read_data!({
                        let array = interleaver.arrays.get_unchecked(prefetch_array_idx);
                        let ptr = array
                            .values()
                            .get_unchecked(array.offset() + prefetch_value_idx);
                        ptr
                    });
                }
            }
            let array = &interleaver.arrays[array_idx];
            if array.is_valid(value_idx) {
                values.push(array.value(value_idx));
            } else {
                values.push(Default::default());
            }
        }

        let array = PrimitiveArray::<T>::new(values.into(), nulls);
        Ok(Arc::new(array.with_data_type(dt.clone())))
    }

    #[inline]
    fn interleave_bytes<T: ByteArrayType, const WITH_PREFETCHING: bool>(
        interleaver: &Interleave<GenericByteArray<T>>,
        indices: &[(usize, usize)],
    ) -> Result<ArrayRef> {
        let nulls = interleaver.nulls(indices);
        let mut offsets = Vec::with_capacity(indices.len() + 1);
        let mut take_value_ptrs = Vec::with_capacity(indices.len());
        let mut capacity = 0;
        let zero = T::Offset::default();

        offsets.push(zero);
        for &(array_idx, value_idx) in indices {
            let array = &interleaver.arrays[array_idx];
            if array.is_valid(value_idx) {
                let value_offsets = unchecked!(array.value_offsets());
                let value_start = value_offsets[value_idx];
                let value_end = value_offsets[value_idx + 1];
                let value_len = value_end - value_start;
                capacity += value_len.as_usize();
                if value_len > zero {
                    let value_ptr = array.values().as_ptr().wrapping_add(value_start.as_usize());
                    take_value_ptrs.push(value_ptr);
                }
            }
            offsets.push(T::Offset::from_usize(capacity).expect("overflow"));
        }

        let array = unsafe {
            let mut values = MutableBuffer::new(capacity);
            let mut dest_ptr = NonNull::new_unchecked(values.as_mut_ptr());
            values.set_len(capacity);

            let mut src_start_ptr = NonNull::dangling(); // initial value is unused
            let mut src_end_ptr = src_start_ptr;

            macro_rules! apply_copy {
                () => {{
                    if src_end_ptr != src_start_ptr {
                        let src_len = src_end_ptr.offset_from(src_start_ptr) as usize;
                        dest_ptr.copy_from_nonoverlapping(src_start_ptr, src_len);
                        dest_ptr = dest_ptr.add(src_len);
                    }
                }};
            }

            let mut take_value_ptr_idx = 0;
            for i in 0..indices.len() {
                let take_value_ptrs = unchecked!(&take_value_ptrs);
                let offsets = unchecked!(&offsets);
                let value_len = (offsets[i + 1] - offsets[i]).as_usize();
                if value_len == 0 {
                    continue;
                }
                let value_ptr =
                    NonNull::new_unchecked(take_value_ptrs[take_value_ptr_idx] as *mut u8);
                take_value_ptr_idx += 1;

                // for continous elements, just extend the area to copy
                // otherwise, copy current area end move to the next area
                if src_end_ptr != value_ptr {
                    prefetch_read_data!(value_ptr.as_ptr()); // prefetch next while copying current
                    apply_copy!();
                    src_start_ptr = value_ptr;
                }
                src_end_ptr = value_ptr.add(value_len);
            }

            // copy last area
            apply_copy!();
            assert_eq!(
                dest_ptr.as_ptr(),
                values.as_mut_ptr().wrapping_add(capacity)
            );

            // build array
            let offsets = OffsetBuffer::new_unchecked(ScalarBuffer::from(offsets));
            GenericByteArray::<T>::new_unchecked(offsets, values.into(), nulls)
        };
        Ok(Arc::new(array))
    }

    if !values.is_empty() {
        let dt = values[0].data_type();

        macro_rules! primitive_helper {
            ($t:ty, $dt:ident) => {{
                let interleaver = Interleave::new(
                    values
                        .iter()
                        .map(|v| downcast_any!(v, PrimitiveArray<$t>).unwrap().clone())
                        .collect::<Vec<_>>(),
                );
                let dt = $dt.clone();
                return Ok(Box::new(move |indices| {
                    if with_prefetching {
                        interleave_primitive::<_, true>(&interleaver, indices, &dt)
                    } else {
                        interleave_primitive::<_, false>(&interleaver, indices, &dt)
                    }
                }));
            }};
        }
        downcast_primitive! {
            dt => (primitive_helper, dt),
            DataType::Utf8 => {
                let interleaver = Interleave::new(values
                    .iter()
                    .map(|v| downcast_any!(v, StringArray).unwrap().clone())
                    .collect::<Vec<_>>(),
                );
                return Ok(Box::new(move |indices| if with_prefetching {
                    interleave_bytes::<_, true>(&interleaver, indices)
                } else {
                    interleave_bytes::<_, false>(&interleaver, indices)
                }));
            }
            DataType::Binary => {
                let interleaver = Interleave::new(values
                    .iter()
                    .map(|v| downcast_any!(v, BinaryArray).unwrap().clone())
                    .collect::<Vec<_>>(),
                );
                return Ok(Box::new(move |indices| if with_prefetching {
                    interleave_bytes::<_, true>(&interleaver, indices)
                } else {
                    interleave_bytes::<_, false>(&interleaver, indices)
                }));
            }
            _ => {},
        }
    }

    // fallback to arrow's implementation
    let values_cloned = values.to_vec();
    Ok(Box::new(move |indices| {
        let value_refs = values_cloned.iter().map(|v| v.as_ref()).collect::<Vec<_>>();
        Ok(arrow::compute::interleave(&value_refs, indices)?)
    }))
}
