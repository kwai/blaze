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

use std::{str::FromStr, sync::Arc};

use arrow::{array::*, datatypes::*};
use bigdecimal::{FromPrimitive, ToPrimitive};
use datafusion::common::{
    cast::{as_float32_array, as_float64_array},
    Result,
};
use num::{cast::AsPrimitive, Bounded, Integer, Signed};
use paste::paste;

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
        (&DataType::Float32, &DataType::Int8) => Arc::new(cast_float_to_integer::<_, Int8Type>(
            as_float32_array(array)?,
        )),
        (&DataType::Float32, &DataType::Int16) => Arc::new(cast_float_to_integer::<_, Int16Type>(
            as_float32_array(array)?,
        )),
        (&DataType::Float32, &DataType::Int32) => Arc::new(cast_float_to_integer::<_, Int32Type>(
            as_float32_array(array)?,
        )),
        (&DataType::Float32, &DataType::Int64) => Arc::new(cast_float_to_integer::<_, Int64Type>(
            as_float32_array(array)?,
        )),
        (&DataType::Float64, &DataType::Int8) => Arc::new(cast_float_to_integer::<_, Int8Type>(
            as_float64_array(array)?,
        )),
        (&DataType::Float64, &DataType::Int16) => Arc::new(cast_float_to_integer::<_, Int16Type>(
            as_float64_array(array)?,
        )),
        (&DataType::Float64, &DataType::Int32) => Arc::new(cast_float_to_integer::<_, Int32Type>(
            as_float64_array(array)?,
        )),
        (&DataType::Float64, &DataType::Int64) => Arc::new(cast_float_to_integer::<_, Int64Type>(
            as_float64_array(array)?,
        )),

        (&DataType::Utf8, &DataType::Int8)
        | (&DataType::Utf8, &DataType::Int16)
        | (&DataType::Utf8, &DataType::Int32)
        | (&DataType::Utf8, &DataType::Int64) => {
            // spark compatible string to integer cast
            try_cast_string_array_to_integer(array, cast_type)?
        }
        (&DataType::Utf8, &DataType::Decimal128(..)) => {
            // spark compatible string to decimal cast
            try_cast_string_array_to_decimal(array, cast_type)?
        }
        (&DataType::Decimal128(..), DataType::Utf8) => {
            // spark compatible decimal to string cast
            try_cast_decimal_array_to_string(array, cast_type)?
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
            try_cast_boolean_array_to_string(array, cast_type)?
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

fn try_cast_string_array_to_integer(array: &dyn Array, cast_type: &DataType) -> Result<ArrayRef> {
    macro_rules! cast {
        ($target_type:ident) => {{
            type B = paste! {[<$target_type Builder>]};
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

fn try_cast_string_array_to_decimal(array: &dyn Array, cast_type: &DataType) -> Result<ArrayRef> {
    if let &DataType::Decimal128(precision, scale) = cast_type {
        let array = array.as_any().downcast_ref::<StringArray>().unwrap();
        let mut builder = Decimal128Builder::new();

        for v in array.iter() {
            match v {
                Some(s) => match to_decimal(s, precision, scale) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                },
                None => builder.append_null(),
            }
        }
        return Ok(Arc::new(
            builder
                .finish()
                .with_precision_and_scale(precision, scale)?,
        ));
    }
    unreachable!("cast_type must be DataType::Decimal")
}

fn try_cast_decimal_array_to_string(array: &dyn Array, cast_type: &DataType) -> Result<ArrayRef> {
    if let &DataType::Utf8 = cast_type {
        let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
        let mut builder = StringBuilder::new();
        for v in 0..array.len() {
            if array.is_valid(v) {
                builder.append_value(array.value_as_string(v))
            } else {
                builder.append_null()
            }
        }
        return Ok(Arc::new(builder.finish()));
    }
    unreachable!("cast_type must be DataType::Utf8")
}

fn try_cast_boolean_array_to_string(array: &dyn Array, cast_type: &DataType) -> Result<ArrayRef> {
    if let &DataType::Utf8 = cast_type {
        let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        return Ok(Arc::new(
            array
                .iter()
                .map(|value| value.map(|value| if value { "true" } else { "false" }))
                .collect::<StringArray>(),
        ));
    }
    unreachable!("cast_type must be DataType::Utf8")
}

fn cast_float_to_integer<F: ArrowPrimitiveType, T: ArrowPrimitiveType>(
    array: &PrimitiveArray<F>,
) -> PrimitiveArray<T>
where
    F::Native: AsPrimitive<T::Native>,
{
    arrow::compute::unary(array, |v| v.as_())
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

fn to_decimal(input: &str, precision: u8, scale: i8) -> Option<i128> {
    let precision = precision as u64;
    let scale = scale as i64;
    bigdecimal::BigDecimal::from_str(input)
        .ok()
        .map(|decimal| decimal.with_prec(precision).with_scale(scale))
        .and_then(|decimal| {
            let (bigint, _exp) = decimal.as_bigint_and_exponent();
            bigint.to_i128()
        })
}

#[cfg(test)]
mod test {
    use datafusion::common::cast::as_int32_array;

    use crate::cast::*;

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
        let i32_array = as_int32_array(&casted).unwrap();

        assert_eq!(
            i32_array,
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
}
