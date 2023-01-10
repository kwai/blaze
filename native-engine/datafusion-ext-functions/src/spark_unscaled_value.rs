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
use datafusion::common::ScalarValue;
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

/// implements org.apache.spark.sql.catalyst.expressions.UnscaledValue
pub fn spark_unscaled_value(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    Ok(match &args[0] {
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Decimal128(Some(v), _, _) => {
                ColumnarValue::Scalar(ScalarValue::Int64(Some(*v as i64)))
            }
            _ => ColumnarValue::Scalar(ScalarValue::Int64(None)),
        },
        ColumnarValue::Array(array) => {
            let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let mut output = Int64Builder::new();

            for v in array.into_iter() {
                output.append_option(v.map(|v| v as i64));
            }
            ColumnarValue::Array(Arc::new(output.finish()))
        }
    })
}
