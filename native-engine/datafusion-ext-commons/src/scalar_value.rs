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

use arrow::array::{Array, UInt64Array};
use datafusion::{common, common::ScalarValue};

use crate::arrow::array_size::ArraySize;

pub fn compacted_scalar_value_from_array(
    array: &dyn Array,
    i: usize,
) -> common::Result<ScalarValue> {
    if array.data_type().is_nested() {
        // avoid using sliced nested array for imprecise memory usage
        let taken =
            arrow::compute::take(array, &UInt64Array::new_scalar(i as u64).into_inner(), None)?;
        ScalarValue::try_from_array(&taken, 0)
    } else {
        ScalarValue::try_from_array(array, i)
    }
}

pub fn scalar_value_heap_mem_size(value: &ScalarValue) -> usize {
    match value {
        ScalarValue::List(list) => list.as_ref().get_array_mem_size(),
        ScalarValue::Map(map) => map.get_array_mem_size(),
        ScalarValue::Struct(struct_) => struct_.get_array_mem_size(),
        _ => value.size() - size_of::<ScalarValue>(),
    }
}
