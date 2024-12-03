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

use std::sync::Arc;

use arrow::{
    array::{
        downcast_primitive, Array, ArrayRef, ArrowPrimitiveType, BinaryArray, BooleanBufferBuilder,
        BufferBuilder, GenericByteArray, PrimitiveArray, StringArray,
    },
    buffer::{MutableBuffer, NullBuffer, OffsetBuffer},
    datatypes::{ArrowNativeType, ByteArrayType},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use arrow_schema::DataType;
use datafusion::common::Result;

use crate::{downcast_any, prefetch_read_data};

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

        for (i, (a, b)) in indices.iter().enumerate() {
            if WITH_PREFETCHING {
                const PREFETCH_AHEAD: usize = 4;
                if i + PREFETCH_AHEAD < indices.len() {
                    let (pa, pb) = indices[i + PREFETCH_AHEAD];
                    prefetch_read_data!({
                        let array = interleaver.arrays.get_unchecked(pa);
                        let ptr = array.values().as_ptr().wrapping_add(array.offset() + pb);
                        ptr
                    });
                }
            }
            let v = interleaver.arrays[*a].value(*b);
            values.push(v)
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
        let mut capacity = 0;
        let mut offsets = BufferBuilder::<T::Offset>::new(indices.len() + 1);

        offsets.append(T::Offset::from_usize(0).unwrap());
        for (i, (a, b)) in indices.iter().enumerate() {
            if WITH_PREFETCHING {
                const PREFETCH_AHEAD: usize = 4;
                if i + PREFETCH_AHEAD < indices.len() {
                    let (pa, pb) = indices[i + PREFETCH_AHEAD];
                    prefetch_read_data!({
                        let array = interleaver.arrays.get_unchecked(pa);
                        array.value_offsets().get_unchecked(pb)
                    });
                }
            }
            let o = interleaver.arrays[*a].value_offsets();
            let element_len = o[*b + 1].as_usize() - o[*b].as_usize();
            capacity += element_len;
            offsets.append(T::Offset::from_usize(capacity).expect("overflow"));
        }

        let mut values = MutableBuffer::new(capacity);
        for (i, (a, b)) in indices.iter().enumerate() {
            if WITH_PREFETCHING {
                const PREFETCH_AHEAD: usize = 4;
                if i + PREFETCH_AHEAD < indices.len() {
                    let (pa, pb) = indices[i + PREFETCH_AHEAD];
                    prefetch_read_data!({
                        let array = interleaver.arrays.get_unchecked(pa);
                        let start = *array.value_offsets().get_unchecked(pb);
                        let ptr = array.values().as_ptr().wrapping_add(start.as_usize());
                        ptr
                    });
                }
            }
            values.extend_from_slice(interleaver.arrays[*a].value(*b).as_ref());
        }

        // Safety: safe by construction
        let array = unsafe {
            let offsets = OffsetBuffer::new_unchecked(offsets.finish().into());
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
