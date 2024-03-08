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

use std::{
    any::Any,
    fmt::{Display, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow::{
    array::{Array, BooleanArray, StringArray},
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    common::{Result, ScalarValue},
    logical_expr::ColumnarValue,
    physical_plan::PhysicalExpr,
};
use datafusion_ext_commons::df_execution_err;

use crate::down_cast_any_ref;

#[derive(Debug, Hash)]
pub struct StringStartsWithExpr {
    expr: Arc<dyn PhysicalExpr>,
    prefix: String,
}

impl PartialEq<dyn Any> for StringStartsWithExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.expr.eq(&x.expr) && self.prefix == x.prefix)
            .unwrap_or(false)
    }
}

impl StringStartsWithExpr {
    pub fn new(expr: Arc<dyn PhysicalExpr>, prefix: String) -> Self {
        Self { expr, prefix }
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }
}

impl Display for StringStartsWithExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "StartsWith({}, {})", self.expr, self.prefix)
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
        let expr = self.expr.evaluate(batch)?;

        match expr {
            ColumnarValue::Array(array) => {
                let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                let ret_array = Arc::new(BooleanArray::from_iter(string_array.iter().map(
                    |maybe_string| maybe_string.map(|string| string.starts_with(&self.prefix)),
                )));
                Ok(ColumnarValue::Array(ret_array))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(maybe_string)) => {
                let ret = maybe_string.map(|string| string.starts_with(&self.prefix));
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(ret)))
            }
            expr => df_execution_err!("starts_with: invalid expr: {expr:?}"),
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
            self.prefix.clone(),
        )))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, BooleanArray, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::physical_expr::{expressions as phys_expr, PhysicalExpr};

    use crate::string_starts_with::StringStartsWithExpr;

    #[test]
    fn test_ok() {
        let string_array: ArrayRef = Arc::new(StringArray::from(vec![
            None,
            Some("rabaok".to_string()),
            Some("rraara".to_string()),
            Some("s_skdo[]ra.,?';,{}\ra".to_string()),
            Some(" raefuwidn".to_string()),
        ]));
        let schema = Arc::new(Schema::new(vec![Field::new("col3", DataType::Utf8, true)]));

        let batch =
            RecordBatch::try_new(schema, vec![string_array]).expect("Error creating RecordBatch");

        let pattern = "ra".to_string();
        let expr = Arc::new(StringStartsWithExpr::new(
            phys_expr::col("col3", &batch.schema()).unwrap(),
            pattern,
        ));
        let ret = expr
            .evaluate(&batch)
            .expect("Error evaluating expr")
            .into_array(batch.num_rows())
            .unwrap();

        let expected: ArrayRef = Arc::new(BooleanArray::from(vec![
            None,
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            // None
        ]));
        assert_eq!(&ret, &expected);
    }

    #[test]
    fn test_scalar_string() {
        let string_array: ArrayRef = Arc::new(StringArray::from(vec![
            Some("Hello, Rust".to_string()),
            Some("Hello, He".to_string()),
            None,
            Some("RustHe".to_string()),
            Some("HellHe".to_string()),
        ]));
        let schema = Arc::new(Schema::new(vec![Field::new("col4", DataType::Utf8, true)]));

        let batch =
            RecordBatch::try_new(schema, vec![string_array]).expect("Error creating RecordBatch");

        let pattern = "ra".to_string();
        let expr = Arc::new(StringStartsWithExpr::new(phys_expr::lit("rarrr"), pattern));
        let ret = expr
            .evaluate(&batch)
            .expect("Error evaluating expr")
            .into_array(batch.num_rows())
            .unwrap();

        let expected: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(true),
            Some(true),
            Some(true),
        ]));
        assert_eq!(&ret, &expected);
    }
}
