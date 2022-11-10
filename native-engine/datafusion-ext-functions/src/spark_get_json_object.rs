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

use std::str::FromStr;
use std::sync::Arc;
use arrow::array::{Array, new_null_array};
use arrow::datatypes::DataType;
use datafusion::arrow::array::StringArray;
use datafusion::common::{Result, ScalarValue};
use datafusion::physical_plan::ColumnarValue;
use jsonpath_rust::{JsonPathFinder, JsonPathInst};

pub fn spark_get_json_object(args: &[ColumnarValue]) ->Result<ColumnarValue> {

    let len = args
        .iter()
        .map(|arg| match arg {
            ColumnarValue::Array(array) => array.len(),
            ColumnarValue::Scalar(_) => 1,
        })
        .max()
        .unwrap_or(0);
    let mut json_buffer:Vec<Option<String>> = vec![];
    let json_string_array= match &args[0] {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Utf8 => {
                array.clone()
            }
            _ => {
                return Ok(ColumnarValue::Array(new_null_array(&DataType::Utf8, array.len())))
            }
        }
        ColumnarValue::Scalar(scalar) => {
            scalar.to_array_of_size(1)
        }
    };
    let path_string = match &args[1] {
        ColumnarValue::Scalar(ScalarValue::Utf8(str)) => match str {
            None => {
                return Ok(ColumnarValue::Array(new_null_array(&DataType::Utf8,len)))
            }
            Some(path) => path,
        }
        _ => unreachable!("path should be ScalarValue"),
    };

        let string_array = json_string_array.as_any().downcast_ref::<StringArray>().unwrap();
        let path = JsonPathInst::from_str(path_string);
        match path {
            //build JsonPathInst failed
            Err(_str) => {
                return Ok(ColumnarValue::Array(new_null_array(&DataType::Utf8,len)))
            }
            Ok(correct_path) => {
                let mut finder = JsonPathFinder::new(Box::new(serde_json::Value::Null), Box::new(correct_path));
                for i in 0..len {
                    if string_array.is_valid(i) {
                        match finder.set_json_str(string_array.value(i)) {
                            Ok(_) => {
                                let finder_value = finder.find();
                                let values_in_array = finder_value.as_array().unwrap().clone();
                                match values_in_array.len() {
                                    0 => json_buffer.push(None),
                                    1 => {
                                        let first_value = values_in_array.first().unwrap();
                                        match first_value {
                                            serde_json::Value::String(str) => json_buffer.push(Some(str.clone())),
                                            serde_json::Value::Null => json_buffer.push(None),
                                            _ => json_buffer.push(Some(first_value.to_string())),
                                        }
                                    },
                                    _ => json_buffer.push(Some(finder_value.to_string())),
                                }
                            }
                            Err(_str) => {
                                // finder json_string build failed, return Null Value
                                json_buffer.push(None);
                            }
                        }
                    }
                    else {
                        json_buffer.push(None);
                    }
                }
            }
        }

    Ok(ColumnarValue::Array(
        Arc::new(StringArray::from_iter(json_buffer.iter())))
    )

}