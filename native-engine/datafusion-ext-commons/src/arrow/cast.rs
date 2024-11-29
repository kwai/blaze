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

use arrow::{array::*, datatypes::*};
use datafusion::common::Result;

use crate::df_execution_err;

pub fn cast(array: &dyn Array, cast_type: &DataType) -> Result<ArrayRef> {
    return cast_impl(array, cast_type, false);
}

pub fn cast_scan_input_array(array: &dyn Array, cast_type: &DataType) -> Result<ArrayRef> {
    return cast_impl(array, cast_type, true);
}

pub fn cast_impl(
    array: &dyn Array,
    cast_type: &DataType,
    match_struct_fields: bool,
) -> Result<ArrayRef> {
    Ok(match (&array.data_type(), cast_type) {
        (&t1, t2) if t1 == t2 => make_array(array.to_data()),

        (_, &DataType::Null) => Arc::new(NullArray::new(array.len())),

        // float to int
        // use unchecked casting, which is compatible with spark
        (&DataType::Float32, &DataType::Int8) => {
            let input = array.as_primitive::<Float32Type>();
            let output: Int8Array = arrow::compute::unary(input, |v| v as i8);
            Arc::new(output)
        }
        (&DataType::Float32, &DataType::Int16) => {
            let input = array.as_primitive::<Float32Type>();
            let output: Int16Array = arrow::compute::unary(input, |v| v as i16);
            Arc::new(output)
        }
        (&DataType::Float32, &DataType::Int32) => {
            let input = array.as_primitive::<Float32Type>();
            let output: Int32Array = arrow::compute::unary(input, |v| v as i32);
            Arc::new(output)
        }
        (&DataType::Float32, &DataType::Int64) => {
            let input = array.as_primitive::<Float32Type>();
            let output: Int64Array = arrow::compute::unary(input, |v| v as i64);
            Arc::new(output)
        }
        (&DataType::Float64, &DataType::Int8) => {
            let input = array.as_primitive::<Float64Type>();
            let output: Int8Array = arrow::compute::unary(input, |v| v as i8);
            Arc::new(output)
        }
        (&DataType::Float64, &DataType::Int16) => {
            let input = array.as_primitive::<Float64Type>();
            let output: Int16Array = arrow::compute::unary(input, |v| v as i16);
            Arc::new(output)
        }
        (&DataType::Float64, &DataType::Int32) => {
            let input = array.as_primitive::<Float64Type>();
            let output: Int32Array = arrow::compute::unary(input, |v| v as i32);
            Arc::new(output)
        }
        (&DataType::Float64, &DataType::Int64) => {
            let input = array.as_primitive::<Float64Type>();
            let output: Int64Array = arrow::compute::unary(input, |v| v as i64);
            Arc::new(output)
        }

        (&DataType::Timestamp(..), DataType::Float64) => {
            // timestamp to f64 = timestamp to i64 to f64, only used in agg.sum()
            arrow::compute::cast(
                &arrow::compute::cast(array, &DataType::Int64)?,
                &DataType::Float64,
            )?
        }
        (&DataType::Boolean, DataType::Utf8) => {
            // spark compatible boolean to string cast
            Arc::new(
                array
                    .as_boolean()
                    .iter()
                    .map(|value| value.map(|value| if value { "true" } else { "false" }))
                    .collect::<StringArray>(),
            )
        }
        (&DataType::List(_), DataType::List(to_field)) => {
            let list = as_list_array(array);
            let items = cast_impl(list.values(), to_field.data_type(), match_struct_fields)?;
            make_array(
                list.to_data()
                    .into_builder()
                    .data_type(DataType::List(to_field.clone()))
                    .child_data(vec![items.into_data()])
                    .build()?,
            )
        }
        (&DataType::Struct(_), DataType::Struct(to_fields)) => {
            let struct_ = as_struct_array(array);

            if !match_struct_fields {
                if to_fields.len() != struct_.num_columns() {
                    df_execution_err!("cannot cast structs with different numbers of fields")?;
                }

                let casted_arrays = struct_
                    .columns()
                    .iter()
                    .zip(to_fields)
                    .map(|(column, to_field)| {
                        cast_impl(column, to_field.data_type(), match_struct_fields)
                    })
                    .collect::<Result<Vec<_>>>()?;

                make_array(
                    struct_
                        .to_data()
                        .into_builder()
                        .data_type(DataType::Struct(to_fields.clone()))
                        .child_data(
                            casted_arrays
                                .into_iter()
                                .map(|array| array.into_data())
                                .collect(),
                        )
                        .build()?,
                )
            } else {
                let mut null_column_name = vec![];
                let casted_arrays = to_fields
                    .iter()
                    .map(|field| {
                        let col = struct_.column_by_name(field.name().as_str());
                        if col.is_some() {
                            cast_impl(col.unwrap(), field.data_type(), match_struct_fields)
                        } else {
                            null_column_name.push(field.name().clone());
                            Ok(new_null_array(field.data_type(), struct_.len()))
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;

                let casted_fields = to_fields
                    .iter()
                    .map(|field: &FieldRef| {
                        if null_column_name.contains(field.name()) {
                            Arc::new(Field::new(field.name(), field.data_type().clone(), true))
                        } else {
                            field.clone()
                        }
                    })
                    .collect::<Vec<_>>();

                make_array(
                    struct_
                        .to_data()
                        .into_builder()
                        .data_type(DataType::Struct(Fields::from(casted_fields)))
                        .child_data(
                            casted_arrays
                                .into_iter()
                                .map(|array| array.into_data())
                                .collect(),
                        )
                        .build()?,
                )
            }
        }
        (&DataType::Map(..), &DataType::Map(ref to_entries_field, to_sorted)) => {
            let map = as_map_array(array);
            let entries = cast_impl(
                map.entries(),
                to_entries_field.data_type(),
                match_struct_fields,
            )?;
            make_array(
                map.to_data()
                    .into_builder()
                    .data_type(DataType::Map(to_entries_field.clone(), to_sorted))
                    .child_data(vec![entries.into_data()])
                    .build()?,
            )
        }
        _ => {
            // default cast
            arrow::compute::kernels::cast::cast(array, cast_type)?
        }
    })
}

#[cfg(test)]
mod test {
    use datafusion::common::cast::{as_decimal128_array, as_float64_array, as_int32_array};

    use super::*;

    #[test]
    fn test_boolean_to_string() {
        let bool_array: ArrayRef =
            Arc::new(BooleanArray::from_iter(vec![None, Some(true), Some(false)]));
        let casted = cast(&bool_array, &DataType::Utf8).unwrap();
        assert_eq!(
            as_string_array(&casted),
            &StringArray::from_iter(vec![None, Some("true"), Some("false")])
        );
    }

    #[test]
    fn test_float_to_int() {
        let f64_array: ArrayRef = Arc::new(Float64Array::from_iter(vec![
            None,
            Some(123.456),
            Some(987.654),
            Some(i32::MAX as f64 + 10000.0),
            Some(i32::MIN as f64 - 10000.0),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(f64::NAN),
        ]));
        let casted = cast(&f64_array, &DataType::Int32).unwrap();
        assert_eq!(
            as_int32_array(&casted).unwrap(),
            &Int32Array::from_iter(vec![
                None,
                Some(123),
                Some(987),
                Some(i32::MAX),
                Some(i32::MIN),
                Some(i32::MAX),
                Some(i32::MIN),
                Some(0),
            ])
        );
    }

    #[test]
    fn test_int_to_float() {
        let i32_array: ArrayRef = Arc::new(Int32Array::from_iter(vec![
            None,
            Some(123),
            Some(987),
            Some(i32::MAX),
            Some(i32::MIN),
        ]));
        let casted = cast(&i32_array, &DataType::Float64).unwrap();
        assert_eq!(
            as_float64_array(&casted).unwrap(),
            &Float64Array::from_iter(vec![
                None,
                Some(123.0),
                Some(987.0),
                Some(i32::MAX as f64),
                Some(i32::MIN as f64),
            ])
        );
    }

    #[test]
    fn test_int_to_decimal() {
        let i32_array: ArrayRef = Arc::new(Int32Array::from_iter(vec![
            None,
            Some(123),
            Some(987),
            Some(i32::MAX),
            Some(i32::MIN),
        ]));
        let casted = cast(&i32_array, &DataType::Decimal128(38, 18)).unwrap();
        assert_eq!(
            as_decimal128_array(&casted).unwrap(),
            &Decimal128Array::from_iter(vec![
                None,
                Some(123000000000000000000),
                Some(987000000000000000000),
                Some(i32::MAX as i128 * 1000000000000000000),
                Some(i32::MIN as i128 * 1000000000000000000),
            ])
            .with_precision_and_scale(38, 18)
            .unwrap()
        );
    }

    #[test]
    fn test_string_to_decimal() {
        let string_array: ArrayRef = Arc::new(StringArray::from_iter(vec![
            None,
            Some("123.456"),
            Some("987.654"),
            Some("123456789012345.678901234567890"),
            Some("-123456789012345.678901234567890"),
        ]));
        let casted = cast(&string_array, &DataType::Decimal128(38, 18)).unwrap();
        assert_eq!(
            as_decimal128_array(&casted).unwrap(),
            &Decimal128Array::from_iter(vec![
                None,
                Some(123456000000000000000i128),
                Some(987654000000000000000i128),
                Some(123456789012345678901234567890000i128),
                Some(-123456789012345678901234567890000i128),
            ])
            .with_precision_and_scale(38, 18)
            .unwrap()
        );
    }

    #[test]
    fn test_decimal_to_string() {
        let decimal_array: ArrayRef = Arc::new(
            Decimal128Array::from_iter(vec![
                None,
                Some(123000000000000000000),
                Some(987000000000000000000),
                Some(987654321000000000000),
                Some(i32::MAX as i128 * 1000000000000000000),
                Some(i32::MIN as i128 * 1000000000000000000),
            ])
            .with_precision_and_scale(38, 18)
            .unwrap(),
        );
        let casted = cast(&decimal_array, &DataType::Utf8).unwrap();
        assert_eq!(
            casted.as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from_iter(vec![
                None,
                Some("123.000000000000000000"),
                Some("987.000000000000000000"),
                Some("987.654321000000000000"),
                Some("2147483647.000000000000000000"),
                Some("-2147483648.000000000000000000"),
            ])
        );
    }
}
