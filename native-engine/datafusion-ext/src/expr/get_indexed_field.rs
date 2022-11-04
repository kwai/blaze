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

use datafusion::arrow::array::{Array, FixedSizeListArray};
use datafusion::arrow::compute::concat;
use datafusion::arrow::datatypes::Field;
use datafusion::arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion::common::DataFusionError;
use datafusion::common::Result;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::convert::TryInto;
use std::fmt::Debug;
use std::{any::Any, sync::Arc};
use crate::expr::down_cast_any_ref;

/// expression to get a field of a struct array.
#[derive(Debug)]
pub struct FixedSizeListGetIndexedFieldExpr {
    arg: Arc<dyn PhysicalExpr>,
    key: ScalarValue,
}

impl FixedSizeListGetIndexedFieldExpr {
    pub fn new(arg: Arc<dyn PhysicalExpr>, key: ScalarValue) -> Self {
        Self { arg, key }
    }

    pub fn key(&self) -> &ScalarValue {
        &self.key
    }

    pub fn arg(&self) -> &Arc<dyn PhysicalExpr> {
        &self.arg
    }
}

impl PartialEq<dyn Any> for FixedSizeListGetIndexedFieldExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.arg.eq(&x.arg) && self.key == x.key)
            .unwrap_or(false)
    }
}

impl std::fmt::Display for FixedSizeListGetIndexedFieldExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "({}).[{}]", self.arg, self.key)
    }
}

impl PhysicalExpr for FixedSizeListGetIndexedFieldExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        let data_type = self.arg.data_type(input_schema)?;
        get_data_type_field(&data_type, &self.key).map(|f| f.data_type().clone())
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        let data_type = self.arg.data_type(input_schema)?;
        get_data_type_field(&data_type, &self.key).map(|f| f.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let array = self.arg.evaluate(batch)?.into_array(1);
        match (array.data_type(), &self.key) {
            (DataType::FixedSizeList(_, _), _) if self.key.is_null() => {
                let scalar_null: ScalarValue = array.data_type().try_into()?;
                Ok(ColumnarValue::Scalar(scalar_null))
            }
            (DataType::FixedSizeList(_, l), ScalarValue::Int64(Some(i))) => {
                let as_list_array =
                    array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();

                if *i < 1 || *i >= *l as i64 || as_list_array.is_empty() {
                    let scalar_null: ScalarValue = array.data_type().try_into()?;
                    return Ok(ColumnarValue::Scalar(scalar_null))
                }

                let sliced_array: Vec<Arc<dyn Array>> = (0..as_list_array.len())
                    .filter_map(|idx| if as_list_array.is_valid(idx) {
                        Some(as_list_array.value(idx).slice(*i as usize - 1, 1))
                    } else {
                        None
                    })
                    .collect();

                // concat requires input of at least one array
                if sliced_array.is_empty() {
                    let scalar_null: ScalarValue = array.data_type().try_into()?;
                    Ok(ColumnarValue::Scalar(scalar_null))
                } else {
                    let vec = sliced_array.iter().map(|a| a.as_ref()).collect::<Vec<&dyn Array>>();
                    let iter = concat(vec.as_slice()).unwrap();

                    Ok(ColumnarValue::Array(iter))
                }
            }
            (dt, key) => Err(DataFusionError::Execution(format!("get indexed field (FixedSizeList) is only possible on lists with int64 indexes. Tried {:?} with {:?} index", dt, key))),
        }
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.arg.clone()]
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn PhysicalExpr>>) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self::new(children[0].clone(), self.key.clone())))
    }
}

fn get_data_type_field(data_type: &DataType, key: &ScalarValue) -> Result<Field> {
    match (data_type, key) {
        (DataType::FixedSizeList(lt, _), ScalarValue::Int64(Some(i))) => {
            Ok(Field::new(&i.to_string(), lt.data_type().clone(), true))
        }
        _ => Err(DataFusionError::Plan(
            "The expression to get an indexed field is only valid for `FixedSizeList` types"
                .to_string(),
        )),
    }
}
