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
use num::{Bounded, FromPrimitive, Integer, Signed};

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

        // spark compatible str to int
        (&DataType::Utf8, to_dt) if to_dt.is_signed_integer() => {
            return try_cast_string_array_to_integer(array, to_dt);
        }

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
                        let origin = field.name().as_str();
                        let mut col = struct_.column_by_name(origin);
                        // correct orc map entries field name from "keys" to "key", "values" to
                        // "value"
                        if col.is_none() && (origin.eq("key") || origin.eq("value")) {
                            let adjust = format!("{}s", origin);
                            log::info!("adjust map entries field name: {} -> {}", origin, adjust);
                            col = struct_.column_by_name(adjust.as_str());
                        }
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

fn try_cast_string_array_to_integer(array: &dyn Array, cast_type: &DataType) -> Result<ArrayRef> {
    macro_rules! cast {
        ($target_type:ident) => {{
            type B = paste::paste! {[<$target_type Builder>]};
            let array = array.as_any().downcast_ref::<StringArray>().unwrap();
            let mut builder = B::new();

            for v in array.iter() {
                match v {
                    Some(s) => builder.append_option(to_integer(s)),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }};
    }

    Ok(match cast_type {
        DataType::Int8 => cast!(Int8),
        DataType::Int16 => cast!(Int16),
        DataType::Int32 => cast!(Int32),
        DataType::Int64 => cast!(Int64),
        _ => arrow::compute::cast(array, cast_type)?,
    })
}

// this implementation is original copied from spark UTF8String.scala
fn to_integer<T: Bounded + FromPrimitive + Integer + Signed + Copy>(input: &str) -> Option<T> {
    let bytes = input.as_bytes();

    if bytes.is_empty() {
        return None;
    }

    let b = bytes[0];
    let negative = b == b'-';
    let mut offset = 0;

    if negative || b == b'+' {
        offset += 1;
        if bytes.len() == 1 {
            return None;
        }
    }

    let separator = b'.';
    let radix = T::from_usize(10).unwrap();
    let stop_value = T::min_value() / radix;
    let mut result = T::zero();

    while offset < bytes.len() {
        let b = bytes[offset];
        offset += 1;
        if b == separator {
            // We allow decimals and will return a truncated integral in that case.
            // Therefore we won't throw an exception here (checking the fractional
            // part happens below.)
            break;
        }

        let digit = if b.is_ascii_digit() {
            b - b'0'
        } else {
            return None;
        };

        // We are going to process the new digit and accumulate the result. However,
        // before doing this, if the result is already smaller than the
        // stopValue(Long.MIN_VALUE / radix), then result * 10 will definitely
        // be smaller than minValue, and we can stop.
        if result < stop_value {
            return None;
        }

        result = result * radix - T::from_u8(digit).unwrap();
        // Since the previous result is less than or equal to stopValue(Long.MIN_VALUE /
        // radix), we can just use `result > 0` to check overflow. If result
        // overflows, we should stop.
        if result > T::zero() {
            return None;
        }
    }

    // This is the case when we've encountered a decimal separator. The fractional
    // part will not change the number, but we will verify that the fractional part
    // is well formed.
    while offset < bytes.len() {
        let current_byte = bytes[offset];
        if !current_byte.is_ascii_digit() {
            return None;
        }
        offset += 1;
    }

    if !negative {
        result = -result;
        if result < T::zero() {
            return None;
        }
    }
    Some(result)
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

    #[test]
    fn test_string_to_bigint() {
        let string_array: ArrayRef = Arc::new(StringArray::from_iter(vec![
            None,
            Some("123"),
            Some("987"),
            Some("987.654"),
            Some("123456789012345"),
            Some("-123456789012345"),
            Some("999999999999999999999999999999999"),
        ]));
        let casted = cast(&string_array, &DataType::Int64).unwrap();
        assert_eq!(
            casted.as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from_iter(vec![
                None,
                Some(123),
                Some(987),
                Some(987),
                Some(123456789012345),
                Some(-123456789012345),
                None,
            ])
        );
    }
}
