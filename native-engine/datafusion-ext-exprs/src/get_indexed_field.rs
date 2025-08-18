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
    convert::TryInto,
    fmt::{Debug, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow::{array::*, compute::*, datatypes::*, record_batch::RecordBatch};
use datafusion::{
    common::{
        Result, ScalarValue,
        cast::{as_list_array, as_struct_array},
    },
    logical_expr::ColumnarValue,
    physical_expr::{PhysicalExpr, PhysicalExprRef},
};
use datafusion_ext_commons::df_execution_err;

/// expression to get a field of a list array.
#[derive(Debug, Eq, Hash)]
pub struct GetIndexedFieldExpr {
    arg: PhysicalExprRef,
    key: ScalarValue,
}

impl GetIndexedFieldExpr {
    /// Create new get field expression
    pub fn new(arg: PhysicalExprRef, key: ScalarValue) -> Self {
        Self { arg, key }
    }

    /// Get the input key
    pub fn key(&self) -> &ScalarValue {
        &self.key
    }

    /// Get the input expression
    pub fn arg(&self) -> &PhysicalExprRef {
        &self.arg
    }
}

impl std::fmt::Display for GetIndexedFieldExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "({}).[{}]", self.arg, self.key)
    }
}

impl PhysicalExpr for GetIndexedFieldExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        let data_type = self.arg.data_type(input_schema)?;
        let field = get_indexed_field(&data_type, &self.key)?;
        Ok(field.data_type().clone())
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        let data_type = self.arg.data_type(input_schema)?;
        let nullable = self.arg.nullable(input_schema)?;
        let field = get_indexed_field(&data_type, &self.key)?;
        Ok(nullable || field.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let array_value = self.arg.evaluate(batch)?;
        let array_is_scalar = matches!(array_value, ColumnarValue::Scalar(_));
        let array = array_value.into_array(1)?;

        match (array.data_type(), &self.key) {
            (DataType::List(_) | DataType::Struct(_), _) if self.key.is_null() => {
                let scalar_null: ScalarValue = array.data_type().try_into()?;
                Ok(ColumnarValue::Scalar(scalar_null))
            }
            (DataType::List(lst), &ScalarValue::Int64(Some(idx))) => {
                let as_list_array = as_list_array(&array)?;

                if idx < 1 || as_list_array.is_empty() {
                    let scalar_null: ScalarValue = lst.data_type().try_into()?;
                    return Ok(ColumnarValue::Scalar(scalar_null));
                }

                let list_len = as_list_array.len();
                let mut take_indices_builder = Int32Builder::with_capacity(list_len);
                for (i, array) in as_list_array.iter().enumerate() {
                    match array {
                        Some(array) if idx <= array.len() as i64 => {
                            let base_offset = as_list_array.value_offsets()[i];
                            let take_offset = base_offset + idx as i32 - 1;
                            take_indices_builder.append_value(take_offset);
                        }
                        _ => take_indices_builder.append_null(),
                    }
                }
                let taken = take(
                    &as_list_array.values(),
                    &take_indices_builder.finish(),
                    None,
                )?;
                if array_is_scalar {
                    return Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                        &taken, 0,
                    )?));
                }
                Ok(ColumnarValue::Array(taken))
            }
            (DataType::Struct(_), ScalarValue::Int32(Some(k))) => {
                let as_struct_array = as_struct_array(&array)?;
                let taken = as_struct_array.column(*k as usize).clone();
                if array_is_scalar {
                    return Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                        &taken, 0,
                    )?));
                }
                Ok(ColumnarValue::Array(taken))
            }
            (dt, key) => {
                let key_dt = key.data_type();
                df_execution_err!("unsupported data types for GetIndexedField: ({dt}, {key_dt})")
            }
        }
    }

    fn children(&self) -> Vec<&PhysicalExprRef> {
        vec![&self.arg]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalExprRef>,
    ) -> Result<PhysicalExprRef> {
        Ok(Arc::new(GetIndexedFieldExpr::new(
            children[0].clone(),
            self.key.clone(),
        )))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "fmt_sql not used")
    }
}

impl PartialEq for GetIndexedFieldExpr {
    fn eq(&self, other: &Self) -> bool {
        self.arg.eq(&other.arg) && self.key == other.key
    }
}

fn get_indexed_field(data_type: &DataType, key: &ScalarValue) -> Result<Arc<Field>> {
    match (data_type, key) {
        (DataType::List(lt), ScalarValue::Int64(Some(i))) => Ok(Arc::new(Field::new(
            i.to_string(),
            lt.data_type().clone(),
            true,
        ))),
        (DataType::Struct(fields), ScalarValue::Int32(Some(k))) => {
            let field: Option<&Arc<Field>> = fields.get(*k as usize);
            match field {
                None => df_execution_err!("Field {k} not found in struct"),
                Some(f) => Ok(f.clone()),
            }
        }
        (dt, key) => {
            let key_dt = key.data_type();
            df_execution_err!("unsupported data types for GetIndexedField: ({dt}, {key_dt})")
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{array::*, datatypes::*, record_batch::RecordBatch};
    use datafusion::{
        assert_batches_eq,
        physical_plan::{PhysicalExpr, expressions::Column},
        scalar::ScalarValue,
    };

    use super::GetIndexedFieldExpr;

    #[test]
    fn test_list() -> Result<(), Box<dyn std::error::Error>> {
        let array: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(100), Some(101), Some(102)]),
            Some(vec![Some(200), Some(201)]),
            None,
            Some(vec![Some(300)]),
            Some(vec![Some(400), Some(401), None, Some(403)]),
        ]));
        let input_batch = RecordBatch::try_from_iter_with_nullable(vec![("cccccc1", array, true)])?;

        let get_indexed = Arc::new(GetIndexedFieldExpr::new(
            Arc::new(Column::new("cccccc1", 0)),
            ScalarValue::from(2_i64),
        ));
        let output_array = get_indexed.evaluate(&input_batch)?.into_array(0)?;
        let output_batch =
            RecordBatch::try_from_iter_with_nullable(vec![("cccccc1", output_array, true)])?;

        let expected = vec![
            "+---------+",
            "| cccccc1 |",
            "+---------+",
            "| 101     |",
            "| 201     |",
            "|         |",
            "|         |",
            "| 401     |",
            "+---------+",
        ];
        assert_batches_eq!(expected, &[output_batch]);

        // test with sliced batch
        let input_batch = input_batch.slice(1, 3);
        let output_array = get_indexed.evaluate(&input_batch)?.into_array(0)?;
        let output_batch =
            RecordBatch::try_from_iter_with_nullable(vec![("cccccc1", output_array, true)])?;
        let expected = vec![
            "+---------+",
            "| cccccc1 |",
            "+---------+",
            "| 201     |",
            "|         |",
            "|         |",
            "+---------+",
        ];
        assert_batches_eq!(expected, &[output_batch]);
        Ok(())
    }
}
