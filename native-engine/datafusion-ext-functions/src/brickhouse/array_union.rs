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

use std::{cmp::Ordering, collections::HashSet, sync::Arc};

use arrow::{
    array::{Array, ListArray},
    buffer::{OffsetBuffer, ScalarBuffer},
    datatypes::{DataType, Field},
};
use datafusion::{
    common::{Result, ScalarValue},
    logical_expr::ColumnarValue,
};
use datafusion_ext_commons::{
    df_execution_err, downcast_any, scalar_value::compacted_scalar_value_from_array,
};
use itertools::Itertools;

/// Return a list of unique entries, for a given set of lists.
/// reference: https://gitee.com/mirrors_klout/brickhouse/blob/master/src/main/java/brickhouse/udf/collect/ArrayUnionUDF.java
///
/// {1, 2} U {1, 2} = {1, 2}
/// {1, 2} U {2, 3} = {1, 2, 3}
/// {1, 2, 3} U {3, 4, 5} = {1, 2, 3, 4, 5}
/// {1, 2, 3, null} U {3, 4, 5, null} = {1, 2, 3, 4, 5, null}
/// {1, 2, 3} U null = {1, 2, 3}
/// null U null = {}
pub fn array_union(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let num_rows = args
        .iter()
        .filter_map(|arg| match arg {
            ColumnarValue::Array(array) => Some(array.len()),
            ColumnarValue::Scalar(_) => None,
        })
        .max()
        .unwrap_or(1);

    let inner_dt = args
        .iter()
        .filter_map(|arg| match arg.data_type() {
            DataType::List(field) => Some(Ok(field.data_type().clone())),
            DataType::Null => None,
            _ => Some(df_execution_err!(
                "brickhouse.array_union args must be array"
            )),
        })
        .next()
        .unwrap_or(Ok(DataType::Null))?;

    let arg_arrays: Vec<ListArray> = args
        .iter()
        .map(|arg| {
            Ok(match arg {
                ColumnarValue::Array(array) => downcast_any!(array, ListArray)?.clone(),
                ColumnarValue::Scalar(scalar) if scalar.is_null() => {
                    ListArray::new_null(Arc::new(Field::new_list_field(DataType::Null, true)), 1)
                }
                ColumnarValue::Scalar(scalar) => {
                    let array = scalar.to_array()?;
                    downcast_any!(&array, ListArray)?.clone()
                }
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let mut offset_buffer = Vec::with_capacity(num_rows + 1);
    let mut offset = 0i32;
    offset_buffer.push(offset);

    let scalars = (0..num_rows)
        .into_iter()
        .map(|row_idx| {
            let mut set = HashSet::new();
            let mut valid = true;

            for (arg, arg_array) in args.iter().zip(&arg_arrays) {
                if matches!(arg, ColumnarValue::Array(..)) {
                    valid = valid && arg_array.is_valid(row_idx);
                    update_set(&mut set, arg_array, row_idx)?;
                } else {
                    valid = valid && arg_array.is_valid(0);
                    update_set(&mut set, arg_array, 0)?;
                }
            }
            offset += set.len() as i32;
            offset_buffer.push(offset);
            Ok(set
                .into_iter()
                .sorted_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal)))
        })
        .collect::<Result<Vec<_>>>()?;

    let offsets = OffsetBuffer::new(ScalarBuffer::from(offset_buffer));
    let values = match scalars.into_iter().flatten().collect::<Vec<_>>() {
        scalars if !scalars.is_empty() => ScalarValue::iter_to_array(scalars.into_iter())?,
        _empty => Arc::new(ListArray::new_null(
            Arc::new(Field::new_list_field(inner_dt.clone(), true)),
            0,
        )),
    };

    Ok(ColumnarValue::Array(Arc::new(ListArray::try_new(
        Arc::new(Field::new_list_field(values.data_type().clone(), true)),
        offsets,
        values,
        None,
    )?)))
}

fn update_set(set: &mut HashSet<ScalarValue>, array: &ListArray, row_idx: usize) -> Result<()> {
    if array.is_valid(row_idx) {
        let values = array.value(row_idx);
        for i in 0..values.len() {
            let scalar = compacted_scalar_value_from_array(&values, i)?;
            set.insert(scalar);
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use arrow::array::*;
    use datafusion::{assert_batches_eq, common::Result};

    use super::*;

    #[test]
    fn test_array_union() -> Result<()> {
        let list_123n_234n: ArrayRef = {
            let mut builder = ListBuilder::new(Int32Builder::new());
            builder.values().append_value(1);
            builder.values().append_value(2);
            builder.values().append_value(3);
            builder.values().append_null();
            builder.append(true);
            builder.values().append_value(2);
            builder.values().append_value(3);
            builder.values().append_value(4);
            builder.values().append_null();
            builder.append(true);
            Arc::new(builder.finish())
        };
        let list_234n_345n: ArrayRef = {
            let mut builder = ListBuilder::new(Int32Builder::new());
            builder.values().append_value(2);
            builder.values().append_value(3);
            builder.values().append_value(4);
            builder.values().append_null();
            builder.append(true);
            builder.values().append_value(3);
            builder.values().append_value(4);
            builder.values().append_value(5);
            builder.values().append_null();
            builder.append(true);
            Arc::new(builder.finish())
        };

        let ret = array_union(&[
            ColumnarValue::Array(list_123n_234n.clone()),
            ColumnarValue::Array(list_234n_345n.clone()),
        ])?;
        assert_batches_eq!(
            vec![
                "+------------------------+",
                "| array_union_actual_ret |",
                "+------------------------+",
                "| [, 1, 2, 3, 4]         |",
                "| [, 2, 3, 4, 5]         |",
                "+------------------------+",
            ],
            &[RecordBatch::try_from_iter_with_nullable([(
                "array_union_actual_ret",
                ret.into_array(0)?,
                true
            )])?]
        );

        let ret = array_union(&[
            ColumnarValue::Array(list_123n_234n.clone()),
            ColumnarValue::Scalar(ScalarValue::Null),
        ])?;
        assert_batches_eq!(
            vec![
                "+------------------------+",
                "| array_union_actual_ret |",
                "+------------------------+",
                "| [, 1, 2, 3]            |",
                "| [, 2, 3, 4]            |",
                "+------------------------+",
            ],
            &[RecordBatch::try_from_iter_with_nullable([(
                "array_union_actual_ret",
                ret.into_array(0)?,
                true
            )])?]
        );

        let ret = array_union(&[
            ColumnarValue::Scalar(ScalarValue::Null),
            ColumnarValue::Scalar(ScalarValue::Null),
        ])?;
        assert_batches_eq!(
            vec![
                "+------------------------+",
                "| array_union_actual_ret |",
                "+------------------------+",
                "| []                     |",
                "+------------------------+",
            ],
            &[RecordBatch::try_from_iter_with_nullable([(
                "array_union_actual_ret",
                ret.into_array(0)?,
                true
            )])?]
        );
        Ok(())
    }
}
