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

use arrow::array::*;
use datafusion::common::Result;
use datafusion::physical_plan::ColumnarValue;
use datafusion_ext_commons::spark_hash::create_hashes;
use std::sync::Arc;

/// implements org.apache.spark.sql.catalyst.expressions.UnscaledValue
pub fn spark_murmur3_hash(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let len = args
        .iter()
        .map(|arg| match arg {
            ColumnarValue::Array(array) => array.len(),
            ColumnarValue::Scalar(_) => 1,
        })
        .max()
        .unwrap_or(0);

    let arrays = args
        .iter()
        .map(|arg| match arg {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(len),
        })
        .collect::<Vec<_>>();

    // use identical seed as spark hash partition
    let spark_murmur3_default_seed = 42u32;
    let mut hash_buffer = vec![spark_murmur3_default_seed; len];
    create_hashes(&arrays, &mut hash_buffer)?;

    Ok(ColumnarValue::Array(Arc::new(
        Int32Array::from_iter_values(hash_buffer.into_iter().map(|hash| hash as i32)),
    )))
}
