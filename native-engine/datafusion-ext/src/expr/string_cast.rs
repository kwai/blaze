use std::any::Any;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use num::{Bounded, cast, FromPrimitive, Integer, Signed};
use paste::paste;

/// expression to cast a string into numeric types (as the original
/// implementation has different behavior than spark)
#[derive(Debug)]
pub struct StringTryCastExpr {
    pub arg: Arc<dyn PhysicalExpr>,
    pub cast_type: DataType,
}

impl StringTryCastExpr {
    pub fn new(arg: Arc<dyn PhysicalExpr>, cast_type: DataType) -> Self {
        Self {
            arg,
            cast_type,
        }
    }
}

impl Display for StringTryCastExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "StringCast({} AS {:?})", self.arg, self.cast_type)
    }
}

impl PhysicalExpr for StringTryCastExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.cast_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let value = self.arg.evaluate(batch)?;
        match value {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(
                try_cast_string_array_to_integer(&array, &self.cast_type)?
            )),
            ColumnarValue::Scalar(scalar) => {
                let scalar_array = scalar.to_array();
                let cast_array =
                    try_cast_string_array_to_integer(&scalar_array, &self.cast_type)?;
                let cast_scalar = ScalarValue::try_from_array(&cast_array, 0)?;
                Ok(ColumnarValue::Scalar(cast_scalar))
            }
        }
    }
}

fn try_cast_string_array_to_integer(array: &ArrayRef, cast_type: &DataType) -> Result<ArrayRef> {
    macro_rules! cast {
        ($target_type:ident) => {{
            type B = paste! {[<$target_type Builder>]};
            let array = array.as_any().downcast_ref::<StringArray>().unwrap();
            let mut builder = B::new(array.len());

            for v in array.iter() {
                match v {
                    Some(s) => builder.append_option(to_integer(s))?,
                    None => builder.append_null()?,
                }
            }
            std::sync::Arc::new(builder.finish())
        }}
    }

    Ok(match cast_type {
        DataType::Int8 => cast!(Int8),
        DataType::Int16 => cast!(Int16),
        DataType::Int32 => cast!(Int32),
        DataType::Int64 => cast!(Int64),
        _ => datafusion::arrow::compute::cast(array, cast_type)?,
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

        let digit;
        if b >= b'0' && b <= b'9' {
            digit = b - b'0';
        } else {
            return None;
        }

        // We are going to process the new digit and accumulate the result. However, before doing
        // this, if the result is already smaller than the stopValue(Long.MIN_VALUE / radix), then
        // result * 10 will definitely be smaller than minValue, and we can stop.
        if result < stop_value {
            return None
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
        if current_byte < b'0' || current_byte > b'9' {
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
    return Some(result);
}

