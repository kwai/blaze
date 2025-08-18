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

use arrow::array::*;
use datafusion::{
    common::{Result, ScalarValue},
    physical_plan::ColumnarValue,
};

/// implements org.apache.spark.sql.catalyst.expressions.UnscaledValue
pub fn spark_unscaled_value(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    Ok(match &args[0] {
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Decimal128(Some(v), ..) => {
                ColumnarValue::Scalar(ScalarValue::Int64(Some(*v as i64)))
            }
            _ => ColumnarValue::Scalar(ScalarValue::Int64(None)),
        },
        ColumnarValue::Array(array) => {
            let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let mut output = Int64Builder::new();

            for v in array.into_iter() {
                output.append_option(v.map(|v| v as i64));
            }
            ColumnarValue::Array(Arc::new(output.finish()))
        }
    })
}
#[cfg(test)]
mod test {
    use std::{error::Error, sync::Arc};

    use arrow::array::{ArrayRef, Decimal128Array, Int64Array};
    use datafusion::{common::ScalarValue, logical_expr::ColumnarValue};

    use crate::spark_unscaled_value::spark_unscaled_value;

    #[test]
    fn test_unscaled_value_array() -> Result<(), Box<dyn Error>> {
        let result = spark_unscaled_value(&vec![ColumnarValue::Array(Arc::new(
            Decimal128Array::from(vec![
                Some(1234567890987654321),
                Some(9876543210),
                Some(135792468109),
                None,
                Some(67898),
            ])
            .with_precision_and_scale(10, 8)?,
        ))])?
        .into_array(5)?;

        let expected = Int64Array::from(vec![
            Some(1234567890987654321),
            Some(9876543210),
            Some(135792468109),
            None,
            Some(67898),
        ]);
        let expected: ArrayRef = Arc::new(expected);
        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn test_unscaled_value_scalar() -> Result<(), Box<dyn Error>> {
        let result = spark_unscaled_value(&vec![ColumnarValue::Scalar(ScalarValue::Decimal128(
            Some(123),
            3,
            2,
        ))])?
        .into_array(1)?;
        let expected = Int64Array::from(vec![Some(123)]);
        let expected: ArrayRef = Arc::new(expected);
        assert_eq!(&result, &expected);
        Ok(())
    }
}
