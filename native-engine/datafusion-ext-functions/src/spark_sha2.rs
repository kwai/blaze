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

use std::{fmt::Write, sync::Arc};

use arrow::array::StringArray;
use datafusion::{
    common::{cast::as_binary_array, Result, ScalarValue},
    functions::crypto::{sha224, sha256, sha384, sha512},
    logical_expr::ScalarUDF,
    physical_plan::ColumnarValue,
};
use datafusion_ext_commons::df_execution_err;

/// `sha224` function that simulates Spark's `sha2` expression with bit width
/// 224
pub fn spark_sha224(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    wrap_digest_result_as_hex_string(args, sha224())
}

/// `sha256` function that simulates Spark's `sha2` expression with bit width 0
/// or 256
pub fn spark_sha256(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    wrap_digest_result_as_hex_string(args, sha256())
}

/// `sha384` function that simulates Spark's `sha2` expression with bit width
/// 384
pub fn spark_sha384(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    wrap_digest_result_as_hex_string(args, sha384())
}

/// `sha512` function that simulates Spark's `sha2` expression with bit width
/// 512
pub fn spark_sha512(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    wrap_digest_result_as_hex_string(args, sha512())
}

/// Spark requires hex string as the result of sha2 functions, we have to wrap
/// the result of digest functions as hex string
fn wrap_digest_result_as_hex_string(
    args: &[ColumnarValue],
    digest: Arc<ScalarUDF>,
) -> Result<ColumnarValue> {
    let value = digest.invoke(args)?;
    Ok(match value {
        ColumnarValue::Array(array) => {
            let binary_array = as_binary_array(&array)?;
            let string_array: StringArray = binary_array
                .iter()
                .map(|opt| opt.map(hex_encode::<_>))
                .collect();
            ColumnarValue::Array(Arc::new(string_array))
        }
        ColumnarValue::Scalar(ScalarValue::Binary(opt)) => {
            ColumnarValue::Scalar(ScalarValue::Utf8(opt.map(hex_encode::<_>)))
        }
        _ => {
            return df_execution_err!(
                "digest function should return binary value, but got: {:?}",
                value.data_type()
            )
        }
    })
}

#[inline]
fn hex_encode<T: AsRef<[u8]>>(data: T) -> String {
    let mut s = String::with_capacity(data.as_ref().len() * 2);
    for b in data.as_ref() {
        // Writing to a string never errors, so we can unwrap here.
        write!(&mut s, "{b:02x}").unwrap();
    }
    s
}
