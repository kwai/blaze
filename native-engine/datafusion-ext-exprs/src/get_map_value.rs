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
use arrow::array::*;
use arrow::compute::{eq_dyn_binary_scalar, eq_dyn_bool_scalar, eq_dyn_scalar, eq_dyn_utf8_scalar};
use arrow::datatypes::Field;
use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion::common::DataFusionError;
use datafusion::common::Result;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::{any::Any, sync::Arc};

/// expression to get value of a key in map array.
#[derive(Debug, Hash)]
pub struct GetMapValueExpr {
    arg: Arc<dyn PhysicalExpr>,
    key: ScalarValue,
}

impl GetMapValueExpr {
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

impl PartialEq<dyn Any> for GetMapValueExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.arg.eq(&x.arg) && self.key == x.key)
            .unwrap_or(false)
    }
}

impl std::fmt::Display for GetMapValueExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "({}).[{}]", self.arg, self.key)
    }
}

impl PhysicalExpr for GetMapValueExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        let data_type = self.arg.data_type(input_schema)?;
        get_data_type_field(&data_type).map(|f| f.data_type().clone())
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        let data_type = self.arg.data_type(input_schema)?;
        get_data_type_field(&data_type).map(|f| f.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let array = self.arg.evaluate(batch)?.into_array(1);
        match (array.data_type(), &self.key) {
            (DataType::Map(_, _), _) if self.key.is_null() => {
                Err(DataFusionError::NotImplemented("map key not support Null Type".to_string()))
            }
            (DataType::Map(_, _), _) => {
                let as_map_array = array.as_any().downcast_ref::<MapArray>().unwrap();
                if !as_map_array.key_type().equals_datatype(&self.key.get_datatype()) {
                    return Err(DataFusionError::Execution("MapArray key type must equal to GetMapValue key type".to_string()))
                }

                macro_rules! get_boolean_value {
                    ($keyarrowty:ident, $scalar:expr) => {{
                        type A = paste::paste! {[< $keyarrowty Array >]};
                        let key_array =  as_map_array.keys().as_any().downcast_ref::<A>().unwrap();
                        let ans_boolean = eq_dyn_bool_scalar(key_array, $scalar)?;
                        let ans_index = ans_boolean.iter().enumerate()
                            .filter(|(_, ans)| if let Some(res) = ans { res.clone() } else { false })
                            .map(|(idx, _)|idx as i32)
                            .collect::<Vec<_>>();
                        let mut indices = vec![];
                        if ans_index.len() == 0 {
                            for _i in 0..as_map_array.len() {
                                indices.push(None);
                            }
                        } else {
                            let mut cur_offset = 0;
                            for &idx in as_map_array.value_offsets().into_iter().skip(1) {
                                if cur_offset >= ans_index.len() {
                                    indices.push(None);
                                } else if idx <= ans_index[cur_offset] {
                                    indices.push(None);
                                } else {
                                    indices.push(Some(ans_index[cur_offset] as u32));
                                    cur_offset += 1;
                                }
                            }
                        }
                        let indice_array = UInt32Array::from(indices);
                        let ans_array = arrow::compute::take(as_map_array.values(), &indice_array, None)?;
                        Ok(ColumnarValue::Array(ans_array))
                    }};
                }

                macro_rules! get_prim_value {
                    ($keyarrowty:ident, $scalar:expr) => {{
                        type A = paste::paste! {[< $keyarrowty Array >]};
                        let key_array =  as_map_array.keys().as_any().downcast_ref::<A>().unwrap();
                        let ans_boolean = eq_dyn_scalar(key_array, $scalar)?;
                        let ans_index = ans_boolean.iter().enumerate()
                            .filter(|(_, ans)| if let Some(res) = ans { res.clone() } else { false })
                            .map(|(idx, _)|idx as i32)
                            .collect::<Vec<_>>();
                        let mut indices = vec![];
                        if ans_index.len() == 0 {
                            for _i in 0..as_map_array.len() {
                                indices.push(None);
                            }
                        } else {
                            let mut cur_offset = 0;
                            for &idx in as_map_array.value_offsets().into_iter().skip(1) {
                                if cur_offset >= ans_index.len() {
                                    indices.push(None);
                                } else if idx <= ans_index[cur_offset] {
                                    indices.push(None);
                                } else {
                                    indices.push(Some(ans_index[cur_offset] as u32));
                                    cur_offset += 1;
                                }
                            }
                        }
                        let indice_array = UInt32Array::from(indices);
                        let ans_array = arrow::compute::take(as_map_array.values(), &indice_array, None)?;
                        Ok(ColumnarValue::Array(ans_array))
                    }};
                }

                macro_rules! get_str_value {
                    ($keyarrowty:ident, $scalar:expr) => {{
                        type A = paste::paste! {[< $keyarrowty Array >]};
                        let key_array =  as_map_array.keys().as_any().downcast_ref::<A>().unwrap();
                        let ans_boolean = eq_dyn_utf8_scalar(key_array, $scalar)?;
                        let ans_index = ans_boolean.iter().enumerate()
                            .filter(|(_, ans)| if let Some(res) = ans { res.clone() } else { false })
                            .map(|(idx, _)|idx as i32)
                            .collect::<Vec<_>>();
                        let mut indices = vec![];
                        if ans_index.len() == 0 {
                            for _i in 0..as_map_array.len() {
                                indices.push(None);
                            }
                        } else {
                            let mut cur_offset = 0;
                            for &idx in as_map_array.value_offsets().into_iter().skip(1) {
                                if cur_offset >= ans_index.len() {
                                    indices.push(None);
                                } else if idx <= ans_index[cur_offset] {
                                    indices.push(None);
                                } else {
                                    indices.push(Some(ans_index[cur_offset] as u32));
                                    cur_offset += 1;
                                }
                            }
                        }
                        let indice_array = UInt32Array::from(indices);
                        let ans_array = arrow::compute::take(as_map_array.values(), &indice_array, None)?;
                        Ok(ColumnarValue::Array(ans_array))
                    }};
                }

                macro_rules! get_binary_value {
                    ($keyarrowty:ident, $scalar:expr) => {{
                        type A = paste::paste! {[< $keyarrowty Array >]};
                        let key_array =  as_map_array.keys().as_any().downcast_ref::<A>().unwrap();
                        let ans_boolean = eq_dyn_binary_scalar(key_array, $scalar)?;
                        let ans_index = ans_boolean.iter().enumerate()
                            .filter(|(_, ans)| if let Some(res) = ans { res.clone() } else { false })
                            .map(|(idx, _)|idx as i32)
                            .collect::<Vec<_>>();
                        let mut indices = vec![];
                        if ans_index.len() == 0 {
                            for _i in 0..as_map_array.len() {
                                indices.push(None);
                            }
                        } else {
                            let mut cur_offset = 0;
                            for &idx in as_map_array.value_offsets().into_iter().skip(1) {
                                if cur_offset >= ans_index.len() {
                                    indices.push(None);
                                } else if idx <= ans_index[cur_offset] {
                                    indices.push(None);
                                } else {
                                    indices.push(Some(ans_index[cur_offset] as u32));
                                    cur_offset += 1;
                                }
                            }
                        }
                        let indice_array = UInt32Array::from(indices);
                        let ans_array = arrow::compute::take(as_map_array.values(), &indice_array, None)?;
                        Ok(ColumnarValue::Array(ans_array))
                    }};
                }

                match &self.key {
                    ScalarValue::Boolean(Some(i)) => get_boolean_value!(Boolean, *i),
                    ScalarValue::Float32(Some(i)) => get_prim_value!(Float32, *i),
                    ScalarValue::Float64(Some(i)) => get_prim_value!(Float64, *i),
                    ScalarValue::Int8(Some(i)) => get_prim_value!(Int8, *i),
                    ScalarValue::Int16(Some(i)) => get_prim_value!(Int16, *i),
                    ScalarValue::Int32(Some(i)) => get_prim_value!(Int32, *i),
                    ScalarValue::Int64(Some(i)) => get_prim_value!(Int64, *i),
                    ScalarValue::UInt8(Some(i)) => get_prim_value!(UInt8, *i),
                    ScalarValue::UInt16(Some(i)) => get_prim_value!(UInt16, *i),
                    ScalarValue::UInt32(Some(i)) => get_prim_value!(UInt32, *i),
                    ScalarValue::UInt64(Some(i)) => get_prim_value!(UInt64, *i),
                    ScalarValue::Utf8(Some(i)) => get_str_value!(String, i.as_str()),
                    ScalarValue::LargeUtf8(Some(i)) => get_str_value!(LargeString, i.as_str()),
                    ScalarValue::Binary(Some(i)) => get_binary_value!(Binary, i.as_slice()),
                    ScalarValue::LargeBinary(Some(i)) => get_binary_value!(LargeBinary, i.as_slice()),
                    t => {
                        Err(DataFusionError::Execution(
                            format!("get map value (Map) not support {} as key type", t)))
                    },
                }
            }
            (dt, key) => {
                Err(DataFusionError::Execution(format!("get map value (Map) is only possible on map with no-null key. Tried {:?} with {:?} key", dt, key)))
            },
        }
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.arg.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self::new(children[0].clone(), self.key.clone())))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }
}

fn get_data_type_field(data_type: &DataType) -> Result<Field> {
    match data_type {
        DataType::Map(field, _) => {
            if let DataType::Struct(fields) = field.data_type() {
                Ok(fields[1].as_ref().clone()) // values field
            } else {
                Err(DataFusionError::NotImplemented(
                    "Map field only support Struct".to_string(),
                ))
            }
        }
        _ => Err(DataFusionError::Plan(
            "The expression to get map value is only valid for `Map` types".to_string(),
        )),
    }
}

#[cfg(test)]
mod test {
    use super::GetMapValueExpr;
    use arrow::array::*;
    use arrow::buffer::Buffer;
    use arrow::datatypes::{DataType, Field, ToByteSlice};
    use arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_eq;
    use datafusion::common::ScalarValue;
    use datafusion::physical_plan::expressions::Column;
    use datafusion::physical_plan::PhysicalExpr;
    use std::sync::Arc;

    #[test]
    fn test_map_1() -> Result<(), Box<dyn std::error::Error>> {
        //Construct key and values
        let key_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from(&[0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice()))
            .build()
            .unwrap();
        let value_data = ArrayData::builder(DataType::UInt32)
            .len(8)
            .add_buffer(Buffer::from(
                &[0u32, 10, 20, 0, 40, 0, 60, 70].to_byte_slice(),
            ))
            .null_bit_buffer(Some(Buffer::from(&[0b11010110])))
            .build()
            .unwrap();

        let entry_offsets = Buffer::from(&[0, 3, 6, 8].to_byte_slice());

        let keys_field = Arc::new(Field::new("keys", DataType::Int32, false));
        let values_field = Arc::new(Field::new("values", DataType::UInt32, true));
        let entry_struct = StructArray::from(vec![
            (keys_field.clone(), make_array(key_data)),
            (values_field.clone(), make_array(value_data.clone())),
        ]);

        // Construct a map array from the above two
        let map_data_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                entry_struct.data_type().clone(),
                true,
            )),
            false,
        );

        let map_data = ArrayData::builder(map_data_type)
            .len(3)
            .add_buffer(entry_offsets)
            .add_child_data(entry_struct.into_data())
            .build()
            .unwrap();
        let map_array: ArrayRef = Arc::new(MapArray::from(map_data));
        let input_batch = RecordBatch::try_from_iter_with_nullable(vec![("col", map_array, true)])?;
        let get_indexed = Arc::new(GetMapValueExpr::new(
            Arc::new(Column::new("col", 0)),
            ScalarValue::from(7_i32),
        ));
        let output_array = get_indexed.evaluate(&input_batch)?.into_array(0);
        let output_batch =
            RecordBatch::try_from_iter_with_nullable(vec![("col", output_array, true)])?;

        let expected =
            vec!["+-----+", "| col |", "+-----+", "|     |", "|     |", "| 70  |", "+-----+"];
        assert_batches_eq!(expected, &[output_batch]);
        Ok(())
    }

    #[test]
    fn test_map_2() -> Result<(), Box<dyn std::error::Error>> {
        let keys = vec!["a", "b", "c", "d", "e", "f", "g", "h"];
        let values_data = UInt32Array::from(vec![0u32, 10, 20, 30, 40, 50, 60, 70]);

        // Construct a buffer for value offsets, for the nested array:
        //  [[a, b, c], [d, e, f], [g, h]]
        let entry_offsets = [0, 3, 6, 8];

        let map_array: ArrayRef = Arc::new(
            MapArray::new_from_strings(keys.clone().into_iter(), &values_data, &entry_offsets)
                .unwrap(),
        );
        let input_batch = RecordBatch::try_from_iter_with_nullable(vec![("col", map_array, true)])?;
        let get_indexed = Arc::new(GetMapValueExpr::new(
            Arc::new(Column::new("col", 0)),
            ScalarValue::from("e"),
        ));
        let output_array = get_indexed.evaluate(&input_batch)?.into_array(0);
        let output_batch =
            RecordBatch::try_from_iter_with_nullable(vec![("col", output_array, true)])?;

        let expected =
            vec!["+-----+", "| col |", "+-----+", "|     |", "| 40  |", "|     |", "+-----+"];
        assert_batches_eq!(expected, &[output_batch]);
        Ok(())
    }
}
