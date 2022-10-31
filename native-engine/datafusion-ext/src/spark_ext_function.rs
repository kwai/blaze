use arrow::compute::{eq_scalar, nullif};
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

pub fn placeholder(_args: &[ColumnarValue]) -> Result<ColumnarValue> {
    panic!("placeholder() should never be called")
}

/// used to avoid DivideByZero error in divide/modulo
pub fn spark_null_if_zero(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    Ok(match &args[0] {
        ColumnarValue::Scalar(scalar) => {
            spark_null_if_zero(&[ColumnarValue::Array(scalar.to_array())])?
        },
        ColumnarValue::Array(array) => {
            macro_rules! handle {
                ($dt:ident) => {{
                    type T = paste::paste! {datafusion::arrow::datatypes::[<$dt Type>]};
                    let array = as_primitive_array::<T>(array);
                    let eq_zeros = eq_scalar(array, T::default_value())?;
                    Arc::new(nullif(array, &eq_zeros)?) as ArrayRef
                }}
            }
            macro_rules! handle_decimal {
                ($dt:ident, $precision:expr, $scale:expr) => {{
                    type T = paste::paste! {datafusion::arrow::datatypes::[<$dt Type>]};
                    let array = array.as_any().downcast_ref::<DecimalArray<T>>().unwrap();
                    let filtered = array
                        .iter()
                        .map(|v| v.filter(|v| *v.raw_value() != [0u8; T::BYTE_LENGTH]));
                    Arc::new(DecimalArray::<T>::from_iter(filtered)
                        .with_precision_and_scale($precision, $scale)?
                    )
                }}
            }
            ColumnarValue::Array(match array.data_type() {
                DataType::Int8 => handle!(Int8),
                DataType::Int16 => handle!(Int16),
                DataType::Int32 => handle!(Int32),
                DataType::Int64 => handle!(Int64),
                DataType::UInt8 => handle!(UInt8),
                DataType::UInt16 => handle!(UInt16),
                DataType::UInt32 => handle!(UInt32),
                DataType::UInt64 => handle!(UInt64),
                DataType::Float32 => handle!(Float32),
                DataType::Float64 => handle!(Float64),
                DataType::Decimal128(precision, scale) =>
                    handle_decimal!(Decimal128, *precision, *scale),
                DataType::Decimal256(precision, scale) =>
                    handle_decimal!(Decimal256, *precision, *scale),
                dt => {
                    return Err(DataFusionError::Execution(
                        format!("Unsupported data type: {:?}", dt))
                    );
                }
            })
        }
    })
}

/// implements org.apache.spark.sql.catalyst.expressions.UnscaledValue
pub fn spark_unscaled_value(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    Ok(match &args[0] {
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Decimal128(Some(v), _, _) => {
                ColumnarValue::Scalar(ScalarValue::Int64(Some(*v as i64)))
            }
            _ => ColumnarValue::Scalar(ScalarValue::Int64(None)),
        },
        ColumnarValue::Array(array) => {
            let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let mut output = Int64Builder::new();

            for v in array.into_iter() {
                output.append_option(v.map(|v| v.as_i128() as i64));
            }
            ColumnarValue::Array(Arc::new(output.finish()))
        }
    })
}

/// implements org.apache.spark.sql.catalyst.expressions.MakeDecimal
pub fn spark_make_decimal(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let precision = match &args[1] {
        &ColumnarValue::Scalar(ScalarValue::Int32(Some(precision))) => precision as u8,
        _ => unreachable!("make_decimal.precision is not int32 value"),
    };
    let scale = match &args[2] {
        &ColumnarValue::Scalar(ScalarValue::Int32(Some(scale))) => scale as u8,
        _ => unreachable!("make_decimal.scale is not int32 value"),
    };
    assert!(precision >= 1, "make_decimal: illegal precision: {}", precision);

    Ok(match &args[0] {
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Int64(Some(v)) => {
                ColumnarValue::Scalar(
                    ScalarValue::Decimal128(Some(*v as i128), precision, scale),
                )
            },
            _ => {
                ColumnarValue::Scalar(ScalarValue::Decimal128(None, precision, scale))
            },
        },
        ColumnarValue::Array(array) => {
            let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            let mut output = Decimal128Builder::with_capacity(array.len(), precision, scale);

            for v in array.into_iter() {
                match v {
                    Some(v) => output.append_value(v as i128)?,
                    None => output.append_null(),
                }
            }
            ColumnarValue::Array(Arc::new(output.finish()))
        }
    })
}

/// implements org.apache.spark.sql.catalyst.expressions.MakeDecimal
pub fn spark_check_overflow(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let to_precision = match &args[1] {
        &ColumnarValue::Scalar(ScalarValue::Int32(Some(precision))) => precision as u8,
        _ => unreachable!("check_overflow.precision is not int32 value"),
    };
    let to_scale = match &args[2] {
        &ColumnarValue::Scalar(ScalarValue::Int32(Some(scale))) => scale as u8,
        _ => unreachable!("check_overflow.scale is not int32 value"),
    };
    assert!(to_precision >= 1, "check_overflow: illegal precision: {}", to_precision);

    Ok(match &args[0] {
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Decimal128(Some(i128_val), precision, scale) => {
                ColumnarValue::Scalar(
                    ScalarValue::Decimal128(
                        change_precision_round_half_up(
                            *i128_val,
                            *precision,
                            *scale,
                            to_precision,
                            to_scale),
                        to_precision,
                        to_scale
                    ))
            }
            _ => {
                ColumnarValue::Scalar(ScalarValue::Decimal128(None, to_precision, to_scale))
            },
        },
        ColumnarValue::Array(array) => {
            let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let mut output = Decimal128Builder::with_capacity(array.len(), to_precision, to_scale);

            for v in array.into_iter() {
                match v {
                    Some(v) => {
                        output.append_option(
                            change_precision_round_half_up(
                                v.as_i128(),
                                array.precision(),
                                array.scale(),
                                to_precision,
                                to_scale))?;
                    }
                    None => output.append_null(),
                }
            }
            ColumnarValue::Array(Arc::new(output.finish()))
        }
    })
}

/// implements org.apache.spark.sql.types.Decimal.changePrecision
fn change_precision_round_half_up(
    mut i128_val: i128,
    precision: u8,
    scale: u8,
    to_precision: u8,
    to_scale: u8
) -> Option<i128> {
    let max_spark_precision = 38;

    if to_precision == precision && to_scale == scale {
        return Some(i128_val);
    }
    if to_scale < scale {
        // Easier case: we just need to divide our scale down
        let diff = scale - to_scale;
        let pow10diff = i128::pow(10, diff as u32);
        // % and / always round to 0
        let dropped_digits = i128_val % pow10diff;
        i128_val /= pow10diff;
        if dropped_digits.abs() * 2 >= pow10diff {
            i128_val += if dropped_digits < 0 {
                -1
            } else {
                1
            };
        }
    } else if to_scale > scale {
        // We might be able to multiply i128_val by a power of 10 and not overflow, but if not,
        // switch to using a BigDecimal
        let diff = to_scale - scale;
        // Multiplying i128_val by POW_10(diff) will still keep it below max_long_digits
        i128_val *= i128::pow(10, diff as u32);
    }

    // check whether the i128_val overflows s max precision supported in spark
    let p = i128::pow(10, u32::min(to_precision as u32, max_spark_precision));
    if i128_val <= -p || i128_val >= p {
        return None;
    }
    Some(i128_val)
}