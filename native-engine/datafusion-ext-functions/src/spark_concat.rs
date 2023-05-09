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
use arrow::array::{Array, StringArray};
use datafusion::common::cast::as_string_array;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::physical_plan::ColumnarValue;

/// concat() function compatible with spark (returns null if any param is null)
/// concat('abcde', 2, 22) = 'abcde222
/// concat('abcde', 2, NULL, 22) = NULL
pub fn concat(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // do not accept 0 arguments.
    if args.is_empty() {
        return Err(DataFusionError::Internal(format!(
            "concat was called with {} arguments. It requires at least 1.",
            args.len()
        )));
    }

    // first, decide whether to return a scalar or a vector.
    let mut return_array = args.iter().filter_map(|x| match x {
        ColumnarValue::Array(array) => Some(array.len()),
        _ => None,
    });
    if let Some(size) = return_array.next() {
        let result = (0..size)
            .map(|index| {
                let mut owned_string: String = "".to_owned();
                let mut is_not_null = true;
                for arg in args {
                    #[allow(clippy::collapsible_match)]
                    match arg {
                        ColumnarValue::Scalar(ScalarValue::Utf8(maybe_value)) => {
                            if let Some(value) = maybe_value {
                                owned_string.push_str(value);
                            } else {
                                is_not_null = false;
                                break;
                            }
                        }
                        ColumnarValue::Array(v) => {
                            if v.is_valid(index) {
                                let v = as_string_array(v).unwrap();
                                owned_string.push_str(v.value(index));
                            } else {
                                is_not_null = false;
                                break;
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                is_not_null.then_some(owned_string)
            })
            .collect::<StringArray>();

        Ok(ColumnarValue::Array(Arc::new(result)))

    } else {
        // short avenue with only scalars
        // returns null if args contains null
        let is_not_null = args.iter().all(|arg| match arg {
            ColumnarValue::Scalar(scalar) if scalar.is_null() => false,
            _ => true,
        });
        if !is_not_null {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
        }

        // concat
        let initial = Some("".to_string());
        let result = args.iter().fold(initial, |mut acc, rhs| {
            if let Some(ref mut inner) = acc {
                match rhs {
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) => {
                        inner.push_str(v);
                    }
                    _ => unreachable!(""),
                };
            };
            acc
        });
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
    }
}