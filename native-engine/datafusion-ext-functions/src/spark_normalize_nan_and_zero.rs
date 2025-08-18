// Copyright 2022 The Auron Authors
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

use arrow::{
    array::{Float32Array, Float64Array},
    datatypes::DataType,
};
use datafusion::{
    common::{
        Result, ScalarValue,
        cast::{as_float32_array, as_float64_array},
    },
    logical_expr::ColumnarValue,
};
use datafusion_ext_commons::df_execution_err;
use num::Float;

pub fn spark_normalize_nan_and_zero(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    match &args[0] {
        ColumnarValue::Scalar(ScalarValue::Float32(Some(v))) => Ok(ColumnarValue::Scalar(
            ScalarValue::Float32(Some(normalize_float(*v))),
        )),
        ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => Ok(ColumnarValue::Scalar(
            ScalarValue::Float64(Some(normalize_float(*v))),
        )),
        ColumnarValue::Array(array) => match array.as_ref().data_type() {
            DataType::Float32 => Ok(ColumnarValue::Array(Arc::new(Float32Array::from_iter(
                as_float32_array(array)?
                    .into_iter()
                    .map(|s| s.map(normalize_float)),
            )))),
            DataType::Float64 => Ok(ColumnarValue::Array(Arc::new(Float64Array::from_iter(
                as_float64_array(array)?
                    .into_iter()
                    .map(|s| s.map(normalize_float)),
            )))),
            other_dt => df_execution_err!(
                "spark_normalize_nan_and_zero for Array type only supports Float32/Float64, not {}",
                other_dt
            ),
        },
        other_dt => df_execution_err!(
            "spark_normalize_nan_and_zero only supports non-null Float32/Float64 scalars or Float32/Float64 arrays, not: {:?}",
            other_dt.data_type()
        ),
    }
}

fn normalize_float<T: Float>(v: T) -> T {
    if v.is_nan() {
        T::nan()
    } else if v == T::neg_zero() {
        T::zero()
    } else {
        v
    }
}

#[cfg(test)]
mod test {
    use std::{error::Error, sync::Arc};

    use arrow::array::{ArrayRef, Float32Array, Float64Array};
    use datafusion::{common::ScalarValue, logical_expr::ColumnarValue};

    use crate::spark_normalize_nan_and_zero::spark_normalize_nan_and_zero;

    macro_rules! test_normalize_nan_and_zero_array_type {
        ($test_name:ident, $array_type:ty, $nan_literal:expr) => {
            #[test]
            fn $test_name() -> Result<(), Box<dyn Error>> {
                let input_data = vec![
                    Some(12345678.0),
                    Some(-12345678.0),
                    Some(-0.0),
                    Some(0.0),
                    None,
                    Some($nan_literal),
                ];
                let input_columnar_value =
                    ColumnarValue::Array(Arc::new(<$array_type>::from(input_data)));

                let result =
                    spark_normalize_nan_and_zero(&vec![input_columnar_value])?.into_array(6)?;

                let expected_data = vec![
                    Some(12345678.0),
                    Some(-12345678.0),
                    Some(0.0),
                    Some(0.0),
                    None,
                    Some($nan_literal),
                ];
                let expected_array_obj = <$array_type>::from(expected_data);
                let expected: ArrayRef = Arc::new(expected_array_obj);
                assert_eq!(&result, &expected);
                Ok(())
            }
        };
    }

    test_normalize_nan_and_zero_array_type!(
        test_normalize_nan_and_zero_array_f64,
        Float64Array,
        f64::NAN
    );

    test_normalize_nan_and_zero_array_type!(
        test_normalize_nan_and_zero_array_f32,
        Float32Array,
        f32::NAN
    );

    macro_rules! test_normalize_nan_and_zero_scalar_type {
        ($test_name:ident, $scalar_variant:ident, $array_type:ty) => {
            #[test]
            fn $test_name() -> Result<(), Box<dyn Error>> {
                let input_columnar_value =
                    ColumnarValue::Scalar(ScalarValue::$scalar_variant(Some(-0.0)));
                let result =
                    spark_normalize_nan_and_zero(&vec![input_columnar_value])?.into_array(1)?;
                let expected_array = <$array_type>::from(vec![Some(0.0)]);
                let expected: ArrayRef = Arc::new(expected_array);
                assert_eq!(&result, &expected);
                Ok(())
            }
        };
    }

    test_normalize_nan_and_zero_scalar_type!(
        test_normalize_nan_and_zero_scalar_f64,
        Float64,
        Float64Array
    );

    test_normalize_nan_and_zero_scalar_type!(
        test_normalize_nan_and_zero_scalar_f32,
        Float32,
        Float32Array
    );
}
