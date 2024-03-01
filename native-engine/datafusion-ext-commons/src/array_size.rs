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
    array::{Array, ArrayData},
    record_batch::RecordBatch,
};

// NOTE:
// the official Array::get_array_memory_size() use buffer.capacity()
// which does not work on ffi arrays. we would like to use .len()
// instead for more precise memory statistics.
pub trait ArraySize {
    fn get_array_mem_size(&self) -> usize;
}

impl ArraySize for dyn Array {
    fn get_array_mem_size(&self) -> usize {
        get_array_data_mem_size(&self.to_data())
    }
}

impl ArraySize for RecordBatch {
    fn get_array_mem_size(&self) -> usize {
        self.columns()
            .iter()
            .map(|array| array.get_array_mem_size())
            .sum::<usize>()
    }
}

fn get_array_data_mem_size(array_data: &ArrayData) -> usize {
    let mut mem_size = 0;

    for buffer in array_data.buffers() {
        mem_size += buffer.len();
    }
    mem_size += array_data.nulls().map(|nb| nb.len()).unwrap_or_default();

    // summing child data size
    for child in array_data.child_data() {
        mem_size += get_array_data_mem_size(child);
    }
    mem_size
}
