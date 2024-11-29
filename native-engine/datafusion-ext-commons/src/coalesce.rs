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
    array::{
        make_array, new_empty_array, Array, ArrayRef, AsArray, Capacities, MutableArrayData,
        RecordBatch, RecordBatchOptions,
    },
    datatypes::{
        ArrowNativeType, BinaryType, ByteArrayType, LargeBinaryType, LargeUtf8Type, Utf8Type,
    },
};
use arrow_schema::{DataType, SchemaRef};

/// coalesce batches without checking there schemas, invokers must make
/// sure all arrays have the same schema
pub fn coalesce_batches_unchecked(schema: SchemaRef, batches: &[RecordBatch]) -> RecordBatch {
    match batches.len() {
        0 => return RecordBatch::new_empty(schema),
        1 => return batches[0].clone(),
        _ => {}
    }
    let num_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
    let num_fields = schema.fields().len();
    let mut coalesced_cols = vec![];

    for i in 0..num_fields {
        let data_type = schema.field(i).data_type();
        let mut cols = Vec::with_capacity(batches.len());
        for j in 0..batches.len() {
            cols.push(batches[j].column(i).clone());
        }
        coalesced_cols.push(coalesce_arrays_unchecked(data_type, &cols));
    }

    RecordBatch::try_new_with_options(
        schema,
        coalesced_cols,
        &RecordBatchOptions::new().with_row_count(Some(num_rows)),
    )
    .expect("error coalescing record batch")
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
