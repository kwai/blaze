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

use arrow::{
    array::{Array, ArrayData, StructArray},
    buffer::Buffer,
    record_batch::RecordBatch,
};

// NOTE:
// the official Array::get_array_memory_size() use buffer.capacity()
// which does not work on ffi arrays. we would like to use .len()
// instead for more precise memory statistics.
pub trait ArraySize {
    fn get_array_mem_size(&self) -> usize;
}

impl<T: ?Sized + Array> ArraySize for T {
    fn get_array_mem_size(&self) -> usize {
        get_array_data_mem_size(&self.to_data())
    }
}

pub trait BatchSize {
    fn get_batch_mem_size(&self) -> usize;
}

impl BatchSize for RecordBatch {
    fn get_batch_mem_size(&self) -> usize {
        let as_struct = StructArray::from(self.clone());
        let as_dyn_array: &dyn Array = &as_struct;
        as_dyn_array.get_array_mem_size()
    }
}

fn get_array_data_mem_size(array_data: &ArrayData) -> usize {
    let mut mem_size = 0;

    for buffer in array_data.buffers() {
        mem_size += size_of::<Buffer>() + buffer.len().max(buffer.capacity());
    }

    mem_size += size_of::<Option<Buffer>>();
    mem_size += array_data
        .nulls()
        .map(|nb| nb.buffer().len().max(nb.buffer().capacity()))
        .unwrap_or_default();

    // summing child data size
    for child in array_data.child_data() {
        mem_size += size_of::<ArrayData>() + get_array_data_mem_size(child);
    }
    mem_size
}
