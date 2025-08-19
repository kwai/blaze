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

use std::sync::Arc;

use arrow::{
    array::*,
    compute::kernels::{cmp::eq, nullif::nullif},
    datatypes::*,
};
use datafusion::{
    common::{Result, ScalarValue},
    physical_plan::ColumnarValue,
};
use datafusion_ext_commons::{df_execution_err, df_unimplemented_err};

pub fn spark_null_if(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // copied from https://docs.rs/datafusion-functions/36.0.0/src/datafusion_functions/core/nullif.rs.html
    // will use ScalarUDF in the future
    if args.len() != 2 {
        return df_execution_err!(
            "{:?} args were supplied but NULLIF takes exactly two args",
            args.len()
        );
    }

    let (lhs, rhs) = (&args[0], &args[1]);

    match (lhs, rhs) {
        (ColumnarValue::Array(lhs), ColumnarValue::Scalar(rhs)) => {
            let rhs = rhs.to_scalar()?;
            let array = nullif(lhs, &eq(&lhs, &rhs)?)?;

            Ok(ColumnarValue::Array(array))
        }
        (ColumnarValue::Array(lhs), ColumnarValue::Array(rhs)) => {
            let array = nullif(lhs, &eq(&lhs, &rhs)?)?;
            Ok(ColumnarValue::Array(array))
        }
        (ColumnarValue::Scalar(lhs), ColumnarValue::Array(rhs)) => {
            let lhs = lhs.to_array_of_size(rhs.len())?;
            let array = nullif(&lhs, &eq(&lhs, &rhs)?)?;
            Ok(ColumnarValue::Array(array))
        }
        (ColumnarValue::Scalar(lhs), ColumnarValue::Scalar(rhs)) => {
            let val: ScalarValue = match lhs.eq(rhs) {
                true => lhs.data_type().try_into()?,
                false => lhs.clone(),
            };

            Ok(ColumnarValue::Scalar(val))
        }
    }
}

/// used to avoid DivideByZero error in divide/modulo
pub fn spark_null_if_zero(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    Ok(match &args[0] {
        ColumnarValue::Scalar(scalar) => {
            let data_type = scalar.data_type();
            let zero = match &data_type {
                &DataType::Decimal128(prec, scale) => ScalarValue::Decimal128(Some(0), prec, scale),
                _other => ScalarValue::new_zero(&data_type)?,
            };
            if scalar.eq(&zero) {
                ColumnarValue::Scalar(ScalarValue::try_from(data_type)?)
            } else {
                ColumnarValue::Scalar(scalar.clone())
            }
        }
        ColumnarValue::Array(array) => {
            macro_rules! handle {
                ($dt:ident) => {{
                    type T = paste::paste! {arrow::datatypes::[<$dt Type>]};
                    let array = as_primitive_array::<T>(array);
                    let _0 = PrimitiveArray::<T>::new_scalar(Default::default());
                    let eq_zeros = eq(array, &_0)?;
                    Arc::new(nullif(array, &eq_zeros)?) as ArrayRef
                }};
            }
            macro_rules! handle_decimal {
                ($dt:ident, $precision:expr, $scale:expr) => {{
                    type T = paste::paste! {arrow::datatypes::[<$dt Type>]};
                    let array = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
                    let _0 = <T as ArrowPrimitiveType>::Native::from_le_bytes([0; T::BYTE_LENGTH]);
                    let filtered = array.iter().map(|v| v.filter(|v| *v != _0));
                    Arc::new(
                        PrimitiveArray::<T>::from_iter(filtered)
                            .with_precision_and_scale($precision, $scale)?,
                    )
                }};
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
                DataType::Decimal128(precision, scale) => {
                    handle_decimal!(Decimal128, *precision, *scale)
                }
                DataType::Decimal256(precision, scale) => {
                    handle_decimal!(Decimal256, *precision, *scale)
                }
                dt => {
                    return df_unimplemented_err!("Unsupported data type: {dt:?}");
                }
            })
        }
    })
}

#[cfg(test)]
mod test {
    use std::{error::Error, sync::Arc};

    use arrow::array::{ArrayRef, Decimal128Array, Float32Array, Int32Array};
    use datafusion::{common::ScalarValue, logical_expr::ColumnarValue};

    use crate::spark_null_if::spark_null_if_zero;

    #[test]
    fn test_null_if_zero_int() -> Result<(), Box<dyn Error>> {
        let result = spark_null_if_zero(&vec![ColumnarValue::Array(Arc::new(Int32Array::from(
            vec![Some(1), None, Some(-1), Some(0)],
        )))])?
        .into_array(4)?;

        let expected = Int32Array::from(vec![Some(1), None, Some(-1), None]);
        let expected: ArrayRef = Arc::new(expected);

        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn test_null_if_zero_decimal() -> Result<(), Box<dyn Error>> {
        let result = spark_null_if_zero(&vec![ColumnarValue::Scalar(ScalarValue::Decimal128(
            Some(1230427389124691),
            20,
            2,
        ))])?
        .into_array(1)?;

        let expected = Decimal128Array::from(vec![Some(1230427389124691)])
            .with_precision_and_scale(20, 2)
            .unwrap();
        let expected: ArrayRef = Arc::new(expected);

        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn test_null_if_zero_float() -> Result<(), Box<dyn Error>> {
        let result = spark_null_if_zero(&vec![ColumnarValue::Scalar(ScalarValue::Float32(Some(
            0.0,
        )))])?
        .into_array(1)?;

        let expected = Float32Array::from(vec![None]);
        let expected: ArrayRef = Arc::new(expected);

        assert_eq!(&result, &expected);
        Ok(())
    }
}
