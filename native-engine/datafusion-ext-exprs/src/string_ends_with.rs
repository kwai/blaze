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

use crate::down_cast_any_ref;
use arrow::array::{Array, BooleanArray, StringArray};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::PhysicalExpr;
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug)]
pub struct StringEndsWithExpr {
    expr: Arc<dyn PhysicalExpr>,
    suffix: String,
}

impl PartialEq<dyn Any> for StringEndsWithExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.expr.eq(&x.expr) && self.suffix == x.suffix)
            .unwrap_or(false)
    }
}

impl StringEndsWithExpr {
    pub fn new(expr: Arc<dyn PhysicalExpr>, suffix: String) -> Self {
        Self { expr, suffix }
    }

    pub fn suffix(&self) -> &str {
        &self.suffix
    }

    pub fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }
}

impl Display for StringEndsWithExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "EndsWith({}, {})", self.expr, self.suffix)
    }
}

impl PhysicalExpr for StringEndsWithExpr {
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
        let expr = self.expr.evaluate(batch)?;

        match expr {
            ColumnarValue::Array(array) => {
                let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                let ret_array = Arc::new(BooleanArray::from_iter(string_array.iter().map(
                    |maybe_string| maybe_string.map(|string| string.ends_with(&self.suffix)),
                )));
                Ok(ColumnarValue::Array(ret_array))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(maybe_string)) => {
                let ret = maybe_string.map(|string| string.ends_with(&self.suffix));
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(ret)))
            }
            expr => Err(DataFusionError::Plan(format!(
                "ends_with: invalid expr: {:?}",
                expr
            ))),
        }
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.suffix.clone(),
        )))
    }
}
