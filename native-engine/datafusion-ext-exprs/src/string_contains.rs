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

use std::{
    any::Any,
    fmt::{Display, Formatter},
    hash::Hash,
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
    physical_expr::PhysicalExprRef,
    physical_plan::PhysicalExpr,
};
use datafusion_ext_commons::df_execution_err;

#[derive(Debug, Eq, Hash)]
pub struct StringContainsExpr {
    expr: PhysicalExprRef,
    infix: String,
}

impl PartialEq for StringContainsExpr {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr) && self.infix == other.infix
    }
}

impl StringContainsExpr {
    pub fn new(expr: PhysicalExprRef, infix: String) -> Self {
        Self { expr, infix }
    }

    pub fn infix(&self) -> &str {
        &self.infix
    }

    pub fn expr(&self) -> &PhysicalExprRef {
        &self.expr
    }
}

impl Display for StringContainsExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Contains({}, {})", self.expr, self.infix)
    }
}

impl PhysicalExpr for StringContainsExpr {
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
                let ret_array =
                    Arc::new(BooleanArray::from_iter(string_array.iter().map(
                        |maybe_string| maybe_string.map(|string| string.contains(&self.infix)),
                    )));
                Ok(ColumnarValue::Array(ret_array))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(maybe_string)) => {
                let ret = maybe_string.map(|string| string.contains(&self.infix));
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(ret)))
            }
            expr => df_execution_err!("contains: invalid expr: {expr:?}")?,
        }
    }

    fn children(&self) -> Vec<&PhysicalExprRef> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalExprRef>,
    ) -> Result<PhysicalExprRef> {
        Ok(Arc::new(Self::new(children[0].clone(), self.infix.clone())))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "fmt_sql not used")
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
    use datafusion::physical_expr::{PhysicalExpr, expressions as phys_expr};

    use crate::string_contains::StringContainsExpr;

    #[test]
    fn test_ok() {
        // create a StringArray from the vector
        let string_array: ArrayRef = Arc::new(StringArray::from(vec![
            Some("abrr".to_string()),
            Some("barr".to_string()),
            Some("rnba".to_string()),
            Some("nbar".to_string()),
            None, // null
        ]));

        // create a schema with the field
        let schema = Arc::new(Schema::new(vec![Field::new("col1", DataType::Utf8, true)]));

        // create a RecordBatch with the schema and StringArray
        let batch =
            RecordBatch::try_new(schema, vec![string_array]).expect("Error creating RecordBatch");

        // test: col1 like 'ba%'
        let pattern = "ba".to_string();
        let expr = Arc::new(StringContainsExpr::new(
            phys_expr::col("col1", &batch.schema()).unwrap(),
            pattern,
        ));
        let ret = expr
            .evaluate(&batch)
            .expect("Error evaluating expr")
            .into_array(batch.num_rows())
            .unwrap();

        // verify result
        let expected: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(false),
            Some(true),
            Some(true),
            Some(true),
            None,
        ]));
        assert_eq!(&ret, &expected);
    }

    #[test]
    fn test_scalar_string() {
        // create a StringArray from the vector
        let string_array: ArrayRef = Arc::new(StringArray::from(vec![
            Some("abrr".to_string()),
            Some("barr".to_string()),
            Some("rnba".to_string()),
            Some("nbar".to_string()),
            None, // null
        ]));

        // create a schema with the field
        let schema = Arc::new(Schema::new(vec![Field::new("col2", DataType::Utf8, true)]));

        // create a RecordBatch with the schema and StringArray
        let batch =
            RecordBatch::try_new(schema, vec![string_array]).expect("Error creating RecordBatch");

        // test: literal like '%ba%'
        let pattern = "ba".to_string();
        let expr = Arc::new(StringContainsExpr::new(phys_expr::lit("abab"), pattern));
        let ret = expr
            .evaluate(&batch)
            .expect("Error evaluating expr")
            .into_array(batch.num_rows())
            .unwrap();

        // verify result
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
