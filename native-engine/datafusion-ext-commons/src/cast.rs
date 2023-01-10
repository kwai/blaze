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
use datafusion::common::{Result, ScalarValue};
use datafusion::physical_plan::ColumnarValue;
use num::{Bounded, FromPrimitive, Integer, Signed, ToPrimitive};
use paste::paste;
use std::str::FromStr;
use std::sync::Arc;

pub fn cast(value: ColumnarValue, cast_type: &DataType) -> Result<ColumnarValue> {
    match (&value.data_type(), cast_type) {
        (_, &DataType::Null) => match value {
            ColumnarValue::Array(array) => {
                Ok(ColumnarValue::Array(Arc::new(NullArray::new(array.len()))))
            }
            ColumnarValue::Scalar(_) => Ok(ColumnarValue::Scalar(ScalarValue::Null)),
        },
        (&DataType::Utf8, &DataType::Int8)
        | (&DataType::Utf8, &DataType::Int16)
        | (&DataType::Utf8, &DataType::Int32)
        | (&DataType::Utf8, &DataType::Int64) => {
            // spark compatible string to integer cast
            match value {
                ColumnarValue::Array(array) => Ok(ColumnarValue::Array(
                    try_cast_string_array_to_integer(&array, cast_type)?,
                )),
                ColumnarValue::Scalar(scalar) => {
                    let scalar_array = scalar.to_array();
                    let cast_array =
                        try_cast_string_array_to_integer(&scalar_array, cast_type)?;
                    let cast_scalar = ScalarValue::try_from_array(&cast_array, 0)?;
                    Ok(ColumnarValue::Scalar(cast_scalar))
                }
            }
        }
        (&DataType::Utf8, &DataType::Decimal128(_, _)) => {
            // spark compatible string to decimal cast
            match value {
                ColumnarValue::Array(array) => Ok(ColumnarValue::Array(
                    try_cast_string_array_to_decimal(&array, cast_type)?,
                )),
                ColumnarValue::Scalar(scalar) => {
                    let scalar_array = scalar.to_array();
                    let cast_array =
                        try_cast_string_array_to_decimal(&scalar_array, cast_type)?;
                    let cast_scalar = ScalarValue::try_from_array(&cast_array, 0)?;
                    Ok(ColumnarValue::Scalar(cast_scalar))
                }
            }
        }
        (&DataType::Decimal128(_, _), DataType::Utf8) => {
            // spark compatible decimal to string cast
            match value {
                ColumnarValue::Array(array) => Ok(ColumnarValue::Array(
                    try_cast_decimal_array_to_string(&array, cast_type)?,
                )),
                ColumnarValue::Scalar(scalar) => {
                    let scalar_array = scalar.to_array();
                    let cast_array =
                        try_cast_decimal_array_to_string(&scalar_array, cast_type)?;
                    let cast_scalar = ScalarValue::try_from_array(&cast_array, 0)?;
                    Ok(ColumnarValue::Scalar(cast_scalar))
                }
            }
        }
        _ => {
            // default cast
            match value {
                ColumnarValue::Array(array) => Ok(ColumnarValue::Array(
                    arrow::compute::kernels::cast::cast(&array, cast_type)?,
                )),
                ColumnarValue::Scalar(scalar) => {
                    let scalar_array = scalar.to_array();
                    let cast_array =
                        arrow::compute::kernels::cast::cast(&scalar_array, cast_type)?;
                    let cast_scalar = ScalarValue::try_from_array(&cast_array, 0)?;
                    Ok(ColumnarValue::Scalar(cast_scalar))
                }
            }
        }
    }
}

fn try_cast_string_array_to_integer(
    array: &ArrayRef,
    cast_type: &DataType,
) -> Result<ArrayRef> {
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

fn try_cast_string_array_to_decimal(
    array: &ArrayRef,
    cast_type: &DataType,
) -> Result<ArrayRef> {
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

fn try_cast_decimal_array_to_string(
    array: &ArrayRef,
    cast_type: &DataType,
) -> Result<ArrayRef> {
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

// this implementation is original copied from spark UTF8String.scala
fn to_integer<T: Bounded + FromPrimitive + Integer + Signed + Copy>(
    input: &str,
) -> Option<T> {
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

        let digit;
        if (b'0'..=b'9').contains(&b) {
            digit = b - b'0';
        } else {
            return None;
        }

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
        if !(b'0'..=b'9').contains(&current_byte) {
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
