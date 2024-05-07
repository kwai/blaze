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
    array::{Array, ArrayData, BooleanArray, RecordBatch, StructArray, UInt32Array},
    compute::take,
    ffi::FFI_ArrowArray,
};
use arrow_schema::DataType;

pub fn batch_to_ffi(batch: RecordBatch) -> FFI_ArrowArray {
    let struct_array = StructArray::from(batch);
    FFI_ArrowArray::new(&walkaround_sliced_boolean_array_issue(
        struct_array.to_data(),
    ))
}

/// TODO: we found that FFI with sliced boolean array will cause a data
/// inconsistency bug.  here we force to copy the sliced boolean array to make
/// it zero-offset based  to avoid this issue
pub fn walkaround_sliced_boolean_array_issue(data: ArrayData) -> ArrayData {
    if data.data_type() == &DataType::Boolean && data.offset() > 0 {
        let boolean_array = BooleanArray::from(data);
        return take(
            &boolean_array,
            &UInt32Array::from_iter_values(0..boolean_array.len() as u32),
            None,
        )
        .expect("take error")
        .to_data();
    }
    if !data.child_data().is_empty() {
        let new_child_data = data
            .child_data()
            .iter()
            .map(|child_data| walkaround_sliced_boolean_array_issue(child_data.clone()))
            .collect::<Vec<_>>();
        if data
            .child_data()
            .iter()
            .zip(&new_child_data)
            .any(|(old, new)| !new.ptr_eq(old))
        {
            return unsafe {
                // safety: no need to check, since all fields are the same except child_data
                ArrayData::new_unchecked(
                    data.data_type().clone(),
                    data.len(),
                    Some(data.null_count()),
                    data.nulls().map(|nb| nb.buffer()).cloned(),
                    data.offset(),
                    data.buffers().to_vec(),
                    new_child_data,
                )
            };
        }
        return data;
    }
    data
}
