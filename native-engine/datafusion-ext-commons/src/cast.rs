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

use arrow::array::*;
use arrow::datatypes::*;
use datafusion::common::{DataFusionError, Result};
use num::{Bounded, FromPrimitive, Integer, Signed, ToPrimitive};
use paste::paste;
use std::str::FromStr;
use std::sync::Arc;

pub fn cast(array: &dyn Array, cast_type: &DataType) -> Result<ArrayRef> {
    Ok(match (&array.data_type(), cast_type) {
        (_, &DataType::Null) => Arc::new(NullArray::new(array.len())),
        (&DataType::Utf8, &DataType::Int8)
        | (&DataType::Utf8, &DataType::Int16)
        | (&DataType::Utf8, &DataType::Int32)
        | (&DataType::Utf8, &DataType::Int64) => {
            // spark compatible string to integer cast
            try_cast_string_array_to_integer(array, cast_type)?
        }
        (&DataType::Utf8, &DataType::Decimal128(_, _)) => {
            // spark compatible string to decimal cast
            try_cast_string_array_to_decimal(array, cast_type)?
        }
        (&DataType::Decimal128(_, _), DataType::Utf8) => {
            // spark compatible decimal to string cast
            try_cast_decimal_array_to_string(array, cast_type)?
        }
        (&DataType::Timestamp(_, _), DataType::Float64) => {
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
            let casted_items = cast(list.values(), to_field.data_type())?;
            make_array(ArrayData::try_new(
                DataType::List(to_field.clone()),
                list.len(),
                list.nulls().map(|nb| nb.buffer().clone()),
                list.offset(),
                list.to_data().buffers().to_vec(),
                vec![casted_items.into_data()],
            )?)
        }
        (&DataType::Struct(_), DataType::Struct(to_fields)) => {
            let struct_ = as_struct_array(array);
            if to_fields.len() != struct_.num_columns() {
                return Err(DataFusionError::Execution(
                    "cannot cast structs with different numbers of fields".to_string(),
                ));
            }
            let casted_arrays = struct_
                .columns()
                .iter()
                .zip(to_fields)
                .map(|(column, to_field)| cast(column, to_field.data_type()))
                .collect::<Result<Vec<_>>>()?;

            make_array(ArrayData::try_new(
                DataType::Struct(to_fields.clone()),
                struct_.len(),
                struct_.nulls().map(|nb| nb.buffer().clone()),
                struct_.offset(),
                struct_.to_data().buffers().to_vec(),
                casted_arrays
                    .into_iter()
                    .map(|array| array.into_data())
                    .collect(),
            )?)
        }
        (&DataType::Map(_, _), &DataType::Map(ref to_entries_field, to_sorted)) => {
            let map = as_map_array(array);
            let casted_entries = cast(map.entries(), to_entries_field.data_type())?;

            make_array(ArrayData::try_new(
                DataType::Map(to_entries_field.clone(), to_sorted),
                map.len(),
                map.nulls().map(|nb| nb.buffer().clone()),
                map.offset(),
                map.to_data().buffers().to_vec(),
                vec![casted_entries.into_data()],
            )?)
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

        // We are going to process the new digit and accumulate the result. However, before doing
        // this, if the result is already smaller than the stopValue(Long.MIN_VALUE / radix), then
        // result * 10 will definitely be smaller than minValue, and we can stop.
        if result < stop_value {
            return None;
        }

        result = result * radix - T::from_u8(digit).unwrap();
        // Since the previous result is less than or equal to stopValue(Long.MIN_VALUE / radix), we
        // can just use `result > 0` to check overflow. If result overflows, we should stop.
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
