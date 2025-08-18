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
use datafusion::{common::Result, physical_plan::ColumnarValue};
use datafusion_ext_commons::spark_hash::create_murmur3_hashes;

/// implements org.apache.spark.sql.catalyst.expressions.Murmur3Hash
pub fn spark_murmur3_hash(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let len = args
        .iter()
        .map(|arg| match arg {
            ColumnarValue::Array(array) => array.len(),
            ColumnarValue::Scalar(_) => 1,
        })
        .max()
        .unwrap_or(0);

    let arrays = args
        .iter()
        .map(|arg| {
            Ok(match arg {
                ColumnarValue::Array(array) => array.clone(),
                ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(len)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    // use identical seed as spark hash partition
    let spark_murmur3_default_seed = 42i32;
    let hashes = create_murmur3_hashes(len, &arrays, spark_murmur3_default_seed);

    Ok(ColumnarValue::Array(Arc::new(Int32Array::from(hashes))))
}

#[cfg(test)]
mod test {
    use std::{error::Error, sync::Arc};

    use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
    use datafusion::logical_expr::ColumnarValue;

    use crate::spark_murmur3_hash::spark_murmur3_hash;

    #[test]
    fn test_murmur3_hash_int64() -> Result<(), Box<dyn Error>> {
        let result = spark_murmur3_hash(&vec![ColumnarValue::Array(Arc::new(Int64Array::from(
            vec![Some(1), Some(0), Some(-1), Some(i64::MAX), Some(i64::MIN)],
        )))])?
        .into_array(5)?;

        let expected = Int32Array::from(vec![
            Some(-1712319331),
            Some(-1670924195),
            Some(-939490007),
            Some(-1604625029),
            Some(-853646085),
        ]);
        let expected: ArrayRef = Arc::new(expected);

        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn test_murmur3_hash_string() -> Result<(), Box<dyn Error>> {
        let result = spark_murmur3_hash(&vec![ColumnarValue::Array(Arc::new(
            StringArray::from_iter_values(["hello", "bar", "", "😁", "天地"]),
        ))])?
        .into_array(5)?;

        let expected = Int32Array::from(vec![
            Some(-1008564952),
            Some(-1808790533),
            Some(142593372),
            Some(885025535),
            Some(-1899966402),
        ]);
        let expected: ArrayRef = Arc::new(expected);

        assert_eq!(&result, &expected);
        Ok(())
    }
}
