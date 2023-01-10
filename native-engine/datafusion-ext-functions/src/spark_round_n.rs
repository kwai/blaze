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
use arrow::datatypes::{DataType, Float32Type, Float64Type};
use bigdecimal::num_bigint::BigInt;
use bigdecimal::{BigDecimal, ToPrimitive};
use datafusion::common::Result;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

pub fn spark_round_n(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let n = match &args[1] {
        &ColumnarValue::Scalar(ScalarValue::Int32(Some(precision))) => precision,
        _ => unreachable!("round.n is not int32 value"),
    };

    Ok(match &args[0] {
        ColumnarValue::Scalar(scalar) => {
            spark_round_n(&[ColumnarValue::Array(scalar.to_array()), args[1].clone()])?
        }
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Float32 => {
                let array = as_primitive_array::<Float32Type>(array);
                ColumnarValue::Array(Arc::new(
                    array
                        .iter()
                        .map(|v| v.map(|v| round_f32(v, n)))
                        .collect::<Float32Array>(),
                ))
            }
            DataType::Float64 => {
                let array = as_primitive_array::<Float64Type>(array);
                ColumnarValue::Array(Arc::new(
                    array
                        .iter()
                        .map(|v| v.map(|v| round_f64(v, n)))
                        .collect::<Float64Array>(),
                ))
            }
            DataType::Decimal128(precision, scale) => {
                let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
                let mut output = Decimal128Builder::with_capacity(array.len());

                for v in array.into_iter() {
                    match v {
                        Some(v) => {
                            let i128_val = round_decimal(v, *scale, n);
                            output.append_option(i128_val);
                        }
                        None => output.append_null(),
                    }
                }
                ColumnarValue::Array(Arc::new(
                    output
                        .finish()
                        .with_precision_and_scale(*precision, n as i8)?,
                ))
            }
            dt => {
                return Err(DataFusionError::Plan(format!(
                    "round: unsupported data type {:?}",
                    dt
                )));
            }
        },
    })
}

fn round_f32(v: f32, n: i32) -> f32 {
    let scale: f32 = 10_f32.powi(n);
    (v * scale).round() / scale
}

fn round_f64(v: f64, n: i32) -> f64 {
    let scale: f64 = 10_f64.powi(n);
    (v * scale).round() / scale
}

fn round_decimal(i128_val: i128, scale: i8, n: i32) -> Option<i128> {
    let decimal = BigDecimal::new(BigInt::from(i128_val), scale as i64);
    let rounded = decimal.round(n as i64);
    rounded.as_bigint_and_exponent().0.to_i128()
}

#[cfg(test)]
mod test {
    use crate::spark_round_n::spark_round_n;
    use arrow::array::*;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::ColumnarValue;
    use std::sync::Arc;

    #[test]
    fn test_float() {
        let input: ArrayRef = Arc::new(Float64Array::from_iter(&[
            Some(123.000000),
            Some(123.456000),
            Some(123.456789),
            None,
        ]));

        // round with n >= 0
        let output = spark_round_n(&[
            ColumnarValue::Array(input.clone()),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(5))),
        ])
        .unwrap()
        .into_array(4);

        assert_eq!(
            output
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
            vec![Some(123.00000), Some(123.45600), Some(123.45679), None,]
        );
    }
    #[test]
    fn test_decimal() {
        let input: ArrayRef = Arc::new(
            Decimal128Array::from_iter(&[
                Some(123000000),
                Some(123456000),
                Some(123456789),
                None,
            ])
            .with_precision_and_scale(9, 6)
            .unwrap(),
        );

        // round with n >= 0
        let output = spark_round_n(&[
            ColumnarValue::Array(input.clone()),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(5))),
        ])
        .unwrap()
        .into_array(4);

        assert_eq!(
            output
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
            vec![Some(12300000), Some(12345600), Some(12345679), None,]
        );
    }
}
