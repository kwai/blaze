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

use arrow::{
    compute::{DatePart, date_part},
    datatypes::DataType,
};
use datafusion::{common::Result, physical_plan::ColumnarValue};
use datafusion_ext_commons::arrow::cast::cast;

pub fn spark_year(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let input = cast(&args[0].clone().into_array(1)?, &DataType::Date32)?;
    Ok(ColumnarValue::Array(date_part(&input, DatePart::Year)?))
}

pub fn spark_month(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let input = cast(&args[0].clone().into_array(1)?, &DataType::Date32)?;
    Ok(ColumnarValue::Array(date_part(&input, DatePart::Month)?))
}

pub fn spark_day(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let input = cast(&args[0].clone().into_array(1)?, &DataType::Date32)?;
    Ok(ColumnarValue::Array(date_part(&input, DatePart::Day)?))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Date32Array, Int32Array};

    use super::*;

    #[test]
    fn test_spark_year() {
        let input = Arc::new(Date32Array::from(vec![
            Some(0),
            Some(1000),
            Some(2000),
            None,
        ]));
        let args = vec![ColumnarValue::Array(input)];
        let expected_ret: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1970),
            Some(1972),
            Some(1975),
            None,
        ]));
        assert_eq!(
            &spark_year(&args).unwrap().into_array(1).unwrap(),
            &expected_ret
        );
    }

    #[test]
    fn test_spark_month() {
        let input = Arc::new(Date32Array::from(vec![Some(0), Some(35), Some(65), None]));
        let args = vec![ColumnarValue::Array(input)];
        let expected_ret: ArrayRef =
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3), None]));
        assert_eq!(
            &spark_month(&args).unwrap().into_array(1).unwrap(),
            &expected_ret
        );
    }

    #[test]
    fn test_spark_day() {
        let input = Arc::new(Date32Array::from(vec![
            Some(0),
            Some(10),
            Some(20),
            Some(30),
            Some(40),
            None,
        ]));
        let args = vec![ColumnarValue::Array(input)];
        let expected_ret: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(11),
            Some(21),
            Some(31),
            Some(10),
            None,
        ]));
        assert_eq!(
            &spark_day(&args).unwrap().into_array(1).unwrap(),
            &expected_ret
        );
    }
}
