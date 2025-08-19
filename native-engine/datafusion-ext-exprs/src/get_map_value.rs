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
    fmt::{Debug, Formatter},
    hash::Hash,
    sync::Arc,
};

use arrow::{
    array::*,
    compute::SortOptions,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    common::{Result, ScalarValue},
    logical_expr::ColumnarValue,
    physical_expr::{PhysicalExpr, PhysicalExprRef},
};
use datafusion_ext_commons::{df_execution_err, df_unimplemented_err};
use itertools::Itertools;

/// expression to get value of a key in map array.
#[derive(Debug, Eq, Hash)]
pub struct GetMapValueExpr {
    arg: PhysicalExprRef,
    key: ScalarValue,
}

impl GetMapValueExpr {
    pub fn new(arg: PhysicalExprRef, key: ScalarValue) -> Self {
        Self { arg, key }
    }

    pub fn key(&self) -> &ScalarValue {
        &self.key
    }

    pub fn arg(&self) -> &PhysicalExprRef {
        &self.arg
    }
}

impl PartialEq for GetMapValueExpr {
    fn eq(&self, other: &Self) -> bool {
        self.arg.eq(&other.arg) && self.key == other.key
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
        let array = self.arg.evaluate(batch)?.into_array(1)?;
        match (array.data_type(), &self.key) {
            (DataType::Map(..), _) if self.key.is_null() => {
                df_unimplemented_err!("map key not support Null Type")
            }
            (DataType::Map(..), _) => {
                let as_map_array = array.as_map();
                let value_data = as_map_array.values().to_data();
                let key = self.key.to_array()?;
                let comparator =
                    make_comparator(as_map_array.keys(), &key, SortOptions::default())?;
                let mut mutable =
                    MutableArrayData::new(vec![&value_data], true, as_map_array.len());

                for (start, end) in as_map_array
                    .value_offsets()
                    .iter()
                    .map(|offset| *offset as usize)
                    .tuple_windows()
                {
                    let mut found = false;
                    for key_idx in start..end {
                        if comparator(key_idx, 0).is_eq() {
                            found = true;
                            mutable.extend(0, key_idx, key_idx + 1);
                            break;
                        }
                    }
                    if !found {
                        mutable.extend_nulls(1);
                    }
                }
                Ok(ColumnarValue::Array(make_array(mutable.freeze())))
            }
            (dt, key) => {
                df_execution_err!(
                    "get map value (Map) is only possible on map with no-null key. Tried {dt:?} with {key:?} key"
                )
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
        Ok(Arc::new(Self::new(children[0].clone(), self.key.clone())))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "fmt_sql not used")
    }
}

fn get_data_type_field(data_type: &DataType) -> Result<Field> {
    match data_type {
        DataType::Map(field, _) => {
            if let DataType::Struct(fields) = field.data_type() {
                Ok(fields[1].as_ref().clone()) // values field
            } else {
                df_unimplemented_err!("Map field only support Struct")
            }
        }
        _ => df_execution_err!("The expression to get map value is only valid for `Map` types"),
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{
        array::*,
        buffer::Buffer,
        datatypes::{DataType, Field, ToByteSlice},
        record_batch::RecordBatch,
    };
    use datafusion::{
        assert_batches_eq,
        common::ScalarValue,
        physical_plan::{PhysicalExpr, expressions::Column},
    };

    use super::GetMapValueExpr;

    #[test]
    fn test_map_1() -> Result<(), Box<dyn std::error::Error>> {
        // Construct key and values
        let key_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref(
                &[0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice(),
            ))
            .build()
            .unwrap();
        let value_data = ArrayData::builder(DataType::UInt32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref(
                &[0u32, 10, 20, 0, 40, 0, 60, 70].to_byte_slice(),
            ))
            .null_bit_buffer(Some(Buffer::from_slice_ref(&[0b11010110])))
            .build()
            .unwrap();

        let entry_offsets = Buffer::from_slice_ref(&[0, 3, 6, 8].to_byte_slice());

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
        let input_batch =
            RecordBatch::try_from_iter_with_nullable(vec![("test col", map_array, true)])?;
        let get_indexed = Arc::new(GetMapValueExpr::new(
            Arc::new(Column::new("test col", 0)),
            ScalarValue::from(7_i32),
        ));
        let output_array = get_indexed.evaluate(&input_batch)?.into_array(0)?;
        let output_batch =
            RecordBatch::try_from_iter_with_nullable(vec![("test col", output_array, true)])?;
        let expected = vec![
            "+----------+",
            "| test col |",
            "+----------+",
            "|          |",
            "|          |",
            "| 70       |",
            "+----------+",
        ];
        assert_batches_eq!(expected, &[output_batch]);

        // test with sliced batch
        let input_batch = input_batch.slice(2, 1);
        let output_array = get_indexed.evaluate(&input_batch)?.into_array(0)?;
        let output_batch =
            RecordBatch::try_from_iter_with_nullable(vec![("test col", output_array, true)])?;
        let expected = vec![
            "+----------+",
            "| test col |",
            "+----------+",
            "| 70       |",
            "+----------+",
        ];
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
        let input_batch =
            RecordBatch::try_from_iter_with_nullable(vec![("test col", map_array, true)])?;
        let get_indexed = Arc::new(GetMapValueExpr::new(
            Arc::new(Column::new("test col", 0)),
            ScalarValue::from("e"),
        ));
        let output_array = get_indexed.evaluate(&input_batch)?.into_array(0)?;
        let output_batch =
            RecordBatch::try_from_iter_with_nullable(vec![("test col", output_array, true)])?;

        let expected = vec![
            "+----------+",
            "| test col |",
            "+----------+",
            "|          |",
            "| 40       |",
            "|          |",
            "+----------+",
        ];
        assert_batches_eq!(expected, &[output_batch]);

        // test with sliced batch
        let input_batch = input_batch.slice(1, 2);
        let output_array = get_indexed.evaluate(&input_batch)?.into_array(0)?;
        let output_batch =
            RecordBatch::try_from_iter_with_nullable(vec![("test col", output_array, true)])?;
        let expected = vec![
            "+----------+",
            "| test col |",
            "+----------+",
            "| 40       |",
            "|          |",
            "+----------+",
        ];
        assert_batches_eq!(expected, &[output_batch]);
        Ok(())
    }
}
