use datafusion::arrow::array::*;
use datafusion::common::ScalarValue;
use datafusion::error::Result;
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

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

    Ok(match &args[0] {
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Int64(Some(v)) => ColumnarValue::Scalar(
                ScalarValue::Decimal128(Some(*v as i128), precision, scale),
            ),
            _ => ColumnarValue::Scalar(ScalarValue::Decimal128(None, precision, scale)),
        },
        ColumnarValue::Array(array) => {
            let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            let mut output = Decimal128Builder::new(precision, scale);

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
