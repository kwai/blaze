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

use std::any::Any;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use arrow::array::{Array, BooleanArray, StringArray};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::PhysicalExpr;

#[derive(Debug)]
pub struct StringStartsWithExpr {
    arg1: Arc<dyn PhysicalExpr>,
    arg2: String,
}

impl Display for StringStartsWithExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!("StartsWith({}, {})", self.arg1, self.arg2)
    }
}

impl PhysicalExpr for StringStartsWithExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let arg1 = self.arg1.evaluate(batch)?;

        match arg1 {
            ColumnarValue::Array(array) => {
                let string_array = array.as_any().downcast_ref::<StringArray>()?;
                let ret_array = Arc::new(
                    BooleanArray::from(
                        string_array.iter().map(|maybe_string| {
                            maybe_string.map(|string| {
                                string.starts_with(&self.arg2)
                            })
                        })));
                Ok(ColumnarValue::Array(ret_array))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(maybe_string)) => {
                let ret = maybe_string.map(|string| {
                    string.starts_with(&self.arg2)
                });
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(ret)))
            }
            arg1 => {
                Err(DataFusionError::Plan(format!("starts_with: invalid arg1: {:?}", arg1)))
            }
        }
    }
}