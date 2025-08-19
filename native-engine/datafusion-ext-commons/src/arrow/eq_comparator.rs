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

//! make_eq_comparator is derived from arrow-ord/50.0.0/src/arrow_ord/ord.rs

use arrow::{
    array::{cast::AsArray, types::*, *},
    datatypes::ArrowNativeType,
    error::ArrowError,
};
use arrow_schema::DataType;
use datafusion::common::Result;

use crate::{df_execution_err, downcast_any};

// inlines most common cases with single column
pub enum EqComparator {
    Int8(Int8Array, Int8Array),
    Int16(Int16Array, Int16Array),
    Int32(Int32Array, Int32Array),
    Int64(Int64Array, Int64Array),
    Date32(Date32Array, Date32Array),
    Date64(Date64Array, Date64Array),
    String(StringArray, StringArray),
    Binary(BinaryArray, BinaryArray),
    Other(DynEqComparator),
}

impl EqComparator {
    pub fn try_new(cols1: &[ArrayRef], cols2: &[ArrayRef]) -> Result<Self> {
        let mut it = cols1
            .iter()
            .zip(cols2)
            .map(|(col1, col2)| (col1.data_type(), col2.data_type()));

        Ok(match (it.next(), it.next()) {
            (Some((DataType::Int8, DataType::Int8)), None) => EqComparator::Int8(
                downcast_any!(&cols1[0], Int8Array)?.clone(),
                downcast_any!(&cols2[0], Int8Array)?.clone(),
            ),
            (Some((DataType::Int16, DataType::Int16)), None) => EqComparator::Int16(
                downcast_any!(&cols1[0], Int16Array)?.clone(),
                downcast_any!(&cols2[0], Int16Array)?.clone(),
            ),
            (Some((DataType::Int32, DataType::Int32)), None) => EqComparator::Int32(
                downcast_any!(&cols1[0], Int32Array)?.clone(),
                downcast_any!(&cols2[0], Int32Array)?.clone(),
            ),
            (Some((DataType::Int64, DataType::Int64)), None) => EqComparator::Int64(
                downcast_any!(&cols1[0], Int64Array)?.clone(),
                downcast_any!(&cols2[0], Int64Array)?.clone(),
            ),
            (Some((DataType::Date32, DataType::Date32)), None) => EqComparator::Date32(
                downcast_any!(&cols1[0], Date32Array)?.clone(),
                downcast_any!(&cols2[0], Date32Array)?.clone(),
            ),
            (Some((DataType::Date64, DataType::Date64)), None) => EqComparator::Date64(
                downcast_any!(&cols1[0], Date64Array)?.clone(),
                downcast_any!(&cols2[0], Date64Array)?.clone(),
            ),
            (Some((DataType::Utf8, DataType::Utf8)), None) => EqComparator::String(
                downcast_any!(&cols1[0], StringArray)?.clone(),
                downcast_any!(&cols2[0], StringArray)?.clone(),
            ),
            (Some((DataType::Binary, DataType::Binary)), None) => EqComparator::Binary(
                downcast_any!(&cols1[0], BinaryArray)?.clone(),
                downcast_any!(&cols2[0], BinaryArray)?.clone(),
            ),
            _ => EqComparator::Other(Self::make_eq_comparator_multiple_arrays(cols1, cols2)?),
        })
    }

    #[inline]
    pub fn eq(&self, i: usize, j: usize) -> bool {
        unsafe {
            // safety: performance critical path, use value_unchecked to avoid bounds check
            match self {
                EqComparator::Int8(c1, c2) => c1.value_unchecked(i) == c2.value_unchecked(j),
                EqComparator::Int16(c1, c2) => c1.value_unchecked(i) == c2.value_unchecked(j),
                EqComparator::Int32(c1, c2) => c1.value_unchecked(i) == c2.value_unchecked(j),
                EqComparator::Int64(c1, c2) => c1.value_unchecked(i) == c2.value_unchecked(j),
                EqComparator::Date32(c1, c2) => c1.value_unchecked(i) == c2.value_unchecked(j),
                EqComparator::Date64(c1, c2) => c1.value_unchecked(i) == c2.value_unchecked(j),
                EqComparator::String(c1, c2) => c1.value_unchecked(i) == c2.value_unchecked(j),
                EqComparator::Binary(c1, c2) => c1.value_unchecked(i) == c2.value_unchecked(j),
                EqComparator::Other(eq) => eq(i, j),
            }
        }
    }

    fn make_eq_comparator_multiple_arrays(
        cols1: &[ArrayRef],
        cols2: &[ArrayRef],
    ) -> Result<DynEqComparator> {
        if cols1.len() != cols2.len() {
            return df_execution_err!(
                "make_eq_comparator_multiple_arrays: cols1.len ({}) != cols2.len ({})",
                cols1.len(),
                cols2.len(),
            );
        }

        let eqs = cols1
            .iter()
            .zip(cols2)
            .map(|(col1, col2)| Ok(make_eq_comparator(col1, col2, true)?))
            .collect::<Result<Vec<_>>>()?;
        Ok(Box::new(move |i, j| eqs.iter().all(|eq| eq(i, j))))
    }
}

/// Compare the values at two arbitrary indices in two arrays.
pub type DynEqComparator = Box<dyn Fn(usize, usize) -> bool + Send + Sync>;

fn eq_impl<A, F>(l: &A, r: &A, ignores_null: bool, eq: F) -> DynEqComparator
where
    A: Array + Clone,
    F: Fn(usize, usize) -> bool + Send + Sync + 'static,
{
    if ignores_null {
        return Box::new(eq);
    }
    let l = l.logical_nulls().filter(|x| x.null_count() > 0);
    let r = r.logical_nulls().filter(|x| x.null_count() > 0);

    match (l, r) {
        (None, None) => Box::new(eq),
        (Some(l), None) => Box::new(move |i, j| {
            if l.is_null(i) {
                return false;
            }
            eq(i, j)
        }),
        (None, Some(r)) => Box::new(move |i, j| {
            if r.is_null(j) {
                return false;
            }
            eq(i, j)
        }),
        (Some(l), Some(r)) => Box::new(move |i, j| {
            if l.is_null(i) || r.is_null(j) {
                return false;
            }
            eq(i, j)
        }),
    }
}

fn eq_primitive<T: ArrowPrimitiveType>(
    left: &dyn Array,
    right: &dyn Array,
    ignores_null: bool,
) -> DynEqComparator
where
    T::Native: ArrowNativeTypeOp,
{
    let left = left.as_primitive::<T>();
    let right = right.as_primitive::<T>();
    let l_values = left.values().clone();
    let r_values = right.values().clone();
    eq_impl(&left, &right, ignores_null, move |i, j| {
        l_values[i] == r_values[j]
    })
}

fn eq_boolean(left: &dyn Array, right: &dyn Array, ignores_null: bool) -> DynEqComparator {
    let left = left.as_boolean();
    let right = right.as_boolean();

    let l_values = left.values().clone();
    let r_values = right.values().clone();

    eq_impl(left, right, ignores_null, move |i, j| {
        l_values.value(i) == r_values.value(j)
    })
}

fn eq_bytes<T: ByteArrayType>(
    left: &dyn Array,
    right: &dyn Array,
    ignores_null: bool,
) -> DynEqComparator {
    let left = left.as_bytes::<T>();
    let right = right.as_bytes::<T>();

    let l = left.clone();
    let r = right.clone();
    eq_impl(left, right, ignores_null, move |i, j| {
        let l: &[u8] = l.value(i).as_ref();
        let r: &[u8] = r.value(j).as_ref();
        l == r
    })
}

fn compare_dict<K: ArrowDictionaryKeyType>(
    left: &dyn Array,
    right: &dyn Array,
    ignores_null: bool,
) -> Result<DynEqComparator, ArrowError> {
    let left = left.as_dictionary::<K>();
    let right = right.as_dictionary::<K>();

    let eq = make_eq_comparator(
        left.values().as_ref(),
        right.values().as_ref(),
        ignores_null,
    )?;
    let left_keys = left.keys().values().clone();
    let right_keys = right.keys().values().clone();

    let f = eq_impl(left, right, ignores_null, move |i, j| {
        let l = left_keys[i].as_usize();
        let r = right_keys[j].as_usize();
        eq(l, r)
    });
    Ok(f)
}

fn eq_list<O: OffsetSizeTrait>(
    left: &dyn Array,
    right: &dyn Array,
    ignores_null: bool,
) -> Result<DynEqComparator, ArrowError> {
    let left = left.as_list::<O>();
    let right = right.as_list::<O>();

    let eq = make_eq_comparator(
        left.values().as_ref(),
        right.values().as_ref(),
        ignores_null,
    )?;

    let l_o = left.offsets().clone();
    let r_o = right.offsets().clone();
    let f = eq_impl(left, right, ignores_null, move |i, j| {
        let l_end = l_o[i + 1].as_usize();
        let l_start = l_o[i].as_usize();

        let r_end = r_o[j + 1].as_usize();
        let r_start = r_o[j].as_usize();

        for (i, j) in (l_start..l_end).zip(r_start..r_end) {
            if eq(i, j) {
                continue;
            }
            return false;
        }
        (l_end - l_start) == (r_end - r_start)
    });
    Ok(f)
}

fn eq_fixed_list(
    left: &dyn Array,
    right: &dyn Array,
    ignores_null: bool,
) -> Result<DynEqComparator, ArrowError> {
    let left = left.as_fixed_size_list();
    let right = right.as_fixed_size_list();
    let eq = make_eq_comparator(
        left.values().as_ref(),
        right.values().as_ref(),
        ignores_null,
    )?;

    let l_size = left.value_length().to_usize().unwrap();
    let r_size = right.value_length().to_usize().unwrap();
    let size_eq = l_size == r_size;

    let f = eq_impl(left, right, ignores_null, move |i, j| {
        let l_start = i * l_size;
        let l_end = l_start + l_size;
        let r_start = j * r_size;
        let r_end = r_start + r_size;
        for (i, j) in (l_start..l_end).zip(r_start..r_end) {
            if eq(i, j) {
                continue;
            }
            return false;
        }
        size_eq
    });
    Ok(f)
}

fn eq_struct(
    left: &dyn Array,
    right: &dyn Array,
    ignores_null: bool,
) -> Result<DynEqComparator, ArrowError> {
    let left = left.as_struct();
    let right = right.as_struct();

    if left.columns().len() != right.columns().len() {
        return Err(ArrowError::InvalidArgumentError(
            "Cannot compare StructArray with different number of columns".to_string(),
        ));
    }

    let columns = left.columns().iter().zip(right.columns());
    let comparators = columns
        .map(|(l, r)| make_eq_comparator(l, r, ignores_null))
        .collect::<Result<Vec<_>, _>>()?;

    let f = eq_impl(left, right, ignores_null, move |i, j| {
        for eq in &comparators {
            if eq(i, j) {
                continue;
            }
            return false;
        }
        return true;
    });
    Ok(f)
}

pub fn make_eq_comparator(
    left: &dyn Array,
    right: &dyn Array,
    ignores_null: bool,
) -> Result<DynEqComparator, ArrowError> {
    use arrow::datatypes::DataType::*;

    macro_rules! primitive_helper {
        ($t:ty, $left:expr, $right:expr) => {
            Ok(eq_primitive::<$t>($left, $right, ignores_null))
        };
    }
    downcast_primitive! {
        left.data_type(), right.data_type() => (primitive_helper, left, right),
        (Boolean, Boolean) => Ok(eq_boolean(left, right, ignores_null)),
        (Utf8, Utf8) => Ok(eq_bytes::<Utf8Type>(left, right, ignores_null)),
        (LargeUtf8, LargeUtf8) => Ok(eq_bytes::<LargeUtf8Type>(left, right, ignores_null)),
        (Binary, Binary) => Ok(eq_bytes::<BinaryType>(left, right, ignores_null)),
        (LargeBinary, LargeBinary) => Ok(eq_bytes::<LargeBinaryType>(left, right, ignores_null)),
        (FixedSizeBinary(_), FixedSizeBinary(_)) => {
            let left = left.as_fixed_size_binary();
            let right = right.as_fixed_size_binary();

            let l = left.clone();
            let r = right.clone();
            Ok(eq_impl(left, right, ignores_null, move |i, j| {
                l.value(i).eq(r.value(j))
            }))
        },
        (List(_), List(_)) => eq_list::<i32>(left, right, ignores_null),
        (LargeList(_), LargeList(_)) => eq_list::<i64>(left, right, ignores_null),
        (FixedSizeList(_, _), FixedSizeList(_, _)) => eq_fixed_list(left, right, ignores_null),
        (Struct(_), Struct(_)) => eq_struct(left, right, ignores_null),
        (Dictionary(l_key, _), Dictionary(r_key, _)) => {
             macro_rules! dict_helper {
                ($t:ty, $left:expr, $right:expr) => {
                     compare_dict::<$t>($left, $right, ignores_null)
                 };
             }
            downcast_integer! {
                 l_key.as_ref(), r_key.as_ref() => (dict_helper, left, right),
                 _ => unreachable!()
             }
        },
        (lhs, rhs) => Err(ArrowError::InvalidArgumentError(match lhs == rhs {
            true => format!("The data type type {lhs:?} has no natural order"),
            false => "Can't compare arrays of different types".to_string(),
        }))
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use arrow::{
        array::builder::{Int32Builder, ListBuilder},
        buffer::{NullBuffer, OffsetBuffer},
        datatypes::{DataType, Field, Fields, i256},
    };

    use super::*;

    #[test]
    fn test_fixed_size_binary() {
        let items = vec![vec![1u8], vec![2u8]];
        let array = FixedSizeBinaryArray::try_from_iter(items.into_iter()).unwrap();

        let eq = make_eq_comparator(&array, &array, false).unwrap();

        assert_eq!(false, eq(0, 1));
    }

    #[test]
    fn test_fixed_size_binary_fixed_size_binary() {
        let items = vec![vec![1u8]];
        let array1 = FixedSizeBinaryArray::try_from_iter(items.into_iter()).unwrap();
        let items = vec![vec![2u8]];
        let array2 = FixedSizeBinaryArray::try_from_iter(items.into_iter()).unwrap();

        let eq = make_eq_comparator(&array1, &array2, false).unwrap();

        assert_eq!(false, eq(0, 0));
    }

    #[test]
    fn test_i32() {
        let array = Int32Array::from(vec![1, 2]);

        let eq = make_eq_comparator(&array, &array, false).unwrap();

        assert_eq!(false, (eq)(0, 1));
    }

    #[test]
    fn test_i32_i32() {
        let array1 = Int32Array::from(vec![1]);
        let array2 = Int32Array::from(vec![2]);

        let eq = make_eq_comparator(&array1, &array2, false).unwrap();

        assert_eq!(false, eq(0, 0));
    }

    #[test]
    fn test_f64() {
        let array = Float64Array::from(vec![1.0, 2.0]);

        let eq = make_eq_comparator(&array, &array, false).unwrap();

        assert_eq!(false, eq(0, 1));
    }

    #[test]
    fn test_f64_nan() {
        let array = Float64Array::from(vec![1.0, f64::NAN]);

        let eq = make_eq_comparator(&array, &array, false).unwrap();

        assert_eq!(true, eq(0, 0));
        assert_eq!(false, eq(0, 1));
        assert_eq!(false, eq(1, 1)); // NaN != NaN
    }

    #[test]
    fn test_f64_zeros() {
        let array = Float64Array::from(vec![-0.0, 0.0]);

        let eq = make_eq_comparator(&array, &array, false).unwrap();

        assert_eq!(true, eq(0, 1)); // -0.0 == 0.0
        assert_eq!(true, eq(1, 0));
    }

    #[test]
    fn test_interval_day_time() {
        let array = IntervalDayTimeArray::from(vec![
            // 0 days, 1 second
            IntervalDayTimeType::make_value(0, 1000),
            // 1 day, 2 milliseconds
            IntervalDayTimeType::make_value(1, 2),
            // 90M milliseconds (which is more than is in 1 day)
            IntervalDayTimeType::make_value(0, 90_000_000),
        ]);

        let eq = make_eq_comparator(&array, &array, false).unwrap();

        assert_eq!(false, eq(0, 1));
        assert_eq!(false, eq(1, 0));

        // somewhat confusingly, while 90M milliseconds is more than 1 day,
        // it will compare less as the comparison is done on the underlying
        // values not field by field
        assert_eq!(false, eq(1, 2));
        assert_eq!(false, eq(2, 1));
    }

    #[test]
    fn test_interval_year_month() {
        let array = IntervalYearMonthArray::from(vec![
            // 1 year, 0 months
            IntervalYearMonthType::make_value(1, 0),
            // 0 years, 13 months
            IntervalYearMonthType::make_value(0, 13),
            // 1 year, 1 month
            IntervalYearMonthType::make_value(1, 1),
        ]);

        let eq = make_eq_comparator(&array, &array, false).unwrap();

        assert_eq!(false, eq(0, 1));
        assert_eq!(false, eq(1, 0));

        // the underlying representation is months, so both quantities are the same
        assert_eq!(true, eq(1, 2));
        assert_eq!(true, eq(2, 1));
    }

    #[test]
    fn test_interval_month_day_nano() {
        let array = IntervalMonthDayNanoArray::from(vec![
            // 100 days
            IntervalMonthDayNanoType::make_value(0, 100, 0),
            // 1 month
            IntervalMonthDayNanoType::make_value(1, 0, 0),
            // 100 day, 1 nanoseconds
            IntervalMonthDayNanoType::make_value(0, 100, 2),
        ]);

        let eq = make_eq_comparator(&array, &array, false).unwrap();

        assert_eq!(false, eq(0, 1));
        assert_eq!(false, eq(1, 0));

        // somewhat confusingly, while 100 days is more than 1 month in all cases
        // it will compare less as the comparison is done on the underlying
        // values not field by field
        assert_eq!(false, eq(1, 2));
        assert_eq!(false, eq(2, 1));
    }

    #[test]
    fn test_decimal() {
        let array = vec![Some(5_i128), Some(2_i128), Some(3_i128)]
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(23, 6)
            .unwrap();

        let eq = make_eq_comparator(&array, &array, false).unwrap();
        assert_eq!(false, eq(1, 0));
        assert_eq!(false, eq(0, 2));
    }

    #[test]
    fn test_decimali256() {
        let array = vec![
            Some(i256::from_i128(5_i128)),
            Some(i256::from_i128(2_i128)),
            Some(i256::from_i128(3_i128)),
        ]
        .into_iter()
        .collect::<Decimal256Array>()
        .with_precision_and_scale(53, 6)
        .unwrap();

        let eq = make_eq_comparator(&array, &array, false).unwrap();
        assert_eq!(false, eq(1, 0));
        assert_eq!(false, eq(0, 2));
    }

    #[test]
    fn test_dict() {
        let data = vec!["a", "b", "c", "a", "a", "c", "c"];
        let array = data.into_iter().collect::<DictionaryArray<Int16Type>>();

        let eq = make_eq_comparator(&array, &array, false).unwrap();

        assert_eq!(false, eq(0, 1));
        assert_eq!(true, eq(3, 4));
        assert_eq!(false, eq(2, 3));
    }

    #[test]
    fn test_multiple_dict() {
        let d1 = vec!["a", "b", "c", "d"];
        let a1 = d1.into_iter().collect::<DictionaryArray<Int16Type>>();
        let d2 = vec!["e", "f", "g", "a"];
        let a2 = d2.into_iter().collect::<DictionaryArray<Int16Type>>();

        let eq = make_eq_comparator(&a1, &a2, false).unwrap();

        assert_eq!(false, eq(0, 0));
        assert_eq!(true, eq(0, 3));
        assert_eq!(false, eq(1, 3));
    }

    #[test]
    fn test_primitive_dict() {
        let values = Int32Array::from(vec![1_i32, 0, 2, 5]);
        let keys = Int8Array::from_iter_values([0, 0, 1, 3]);
        let array1 = DictionaryArray::new(keys, Arc::new(values));

        let values = Int32Array::from(vec![2_i32, 3, 4, 5]);
        let keys = Int8Array::from_iter_values([0, 1, 1, 3]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let eq = make_eq_comparator(&array1, &array2, false).unwrap();

        assert_eq!(false, eq(0, 0));
        assert_eq!(false, eq(0, 3));
        assert_eq!(true, eq(3, 3));
        assert_eq!(false, eq(3, 1));
        assert_eq!(false, eq(3, 2));
    }

    #[test]
    fn test_float_dict() {
        let values = Float32Array::from(vec![1.0, 0.5, 2.1, 5.5]);
        let keys = Int8Array::from_iter_values([0, 0, 1, 3]);
        let array1 = DictionaryArray::try_new(keys, Arc::new(values)).unwrap();

        let values = Float32Array::from(vec![1.2, 3.2, 4.0, 5.5]);
        let keys = Int8Array::from_iter_values([0, 1, 1, 3]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let eq = make_eq_comparator(&array1, &array2, false).unwrap();

        assert_eq!(false, eq(0, 0));
        assert_eq!(false, eq(0, 3));
        assert_eq!(true, eq(3, 3));
        assert_eq!(false, eq(3, 1));
        assert_eq!(false, eq(3, 2));
    }

    #[test]
    fn test_timestamp_dict() {
        let values = TimestampSecondArray::from(vec![1, 0, 2, 5]);
        let keys = Int8Array::from_iter_values([0, 0, 1, 3]);
        let array1 = DictionaryArray::new(keys, Arc::new(values));

        let values = TimestampSecondArray::from(vec![2, 3, 4, 5]);
        let keys = Int8Array::from_iter_values([0, 1, 1, 3]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let eq = make_eq_comparator(&array1, &array2, false).unwrap();

        assert_eq!(false, eq(0, 0));
        assert_eq!(false, eq(0, 3));
        assert_eq!(true, eq(3, 3));
        assert_eq!(false, eq(3, 1));
        assert_eq!(false, eq(3, 2));
    }

    #[test]
    fn test_duration_dict() {
        let values = DurationSecondArray::from(vec![1, 0, 2, 5]);
        let keys = Int8Array::from_iter_values([0, 0, 1, 3]);
        let array1 = DictionaryArray::new(keys, Arc::new(values));

        let values = DurationSecondArray::from(vec![2, 3, 4, 5]);
        let keys = Int8Array::from_iter_values([0, 1, 1, 3]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let eq = make_eq_comparator(&array1, &array2, false).unwrap();

        assert_eq!(false, eq(0, 0));
        assert_eq!(false, eq(0, 3));
        assert_eq!(true, eq(3, 3));
        assert_eq!(false, eq(3, 1));
        assert_eq!(false, eq(3, 2));
    }

    #[test]
    fn test_decimal_dict() {
        let values = Decimal128Array::from(vec![1, 0, 2, 5]);
        let keys = Int8Array::from_iter_values([0, 0, 1, 3]);
        let array1 = DictionaryArray::new(keys, Arc::new(values));

        let values = Decimal128Array::from(vec![2, 3, 4, 5]);
        let keys = Int8Array::from_iter_values([0, 1, 1, 3]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let eq = make_eq_comparator(&array1, &array2, false).unwrap();

        assert_eq!(false, eq(0, 0));
        assert_eq!(false, eq(0, 3));
        assert_eq!(true, eq(3, 3));
        assert_eq!(false, eq(3, 1));
        assert_eq!(false, eq(3, 2));
    }

    #[test]
    fn test_decimal256_dict() {
        let values = Decimal256Array::from(vec![
            i256::from_i128(1),
            i256::from_i128(0),
            i256::from_i128(2),
            i256::from_i128(5),
        ]);
        let keys = Int8Array::from_iter_values([0, 0, 1, 3]);
        let array1 = DictionaryArray::new(keys, Arc::new(values));

        let values = Decimal256Array::from(vec![
            i256::from_i128(2),
            i256::from_i128(3),
            i256::from_i128(4),
            i256::from_i128(5),
        ]);
        let keys = Int8Array::from_iter_values([0, 1, 1, 3]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let eq = make_eq_comparator(&array1, &array2, false).unwrap();

        assert_eq!(false, eq(0, 0));
        assert_eq!(false, eq(0, 3));
        assert_eq!(true, eq(3, 3));
        assert_eq!(false, eq(3, 1));
        assert_eq!(false, eq(3, 2));
    }

    fn test_bytes_impl<T: ByteArrayType>() {
        let offsets = OffsetBuffer::from_lengths([3, 3, 1]);
        let a = GenericByteArray::<T>::new(offsets, b"abcdefa".into(), None);
        let eq = make_eq_comparator(&a, &a, false).unwrap();

        assert_eq!(false, eq(0, 1));
        assert_eq!(false, eq(0, 2));
        assert_eq!(true, eq(1, 1));
    }

    #[test]
    fn test_bytes() {
        test_bytes_impl::<Utf8Type>();
        test_bytes_impl::<LargeUtf8Type>();
        test_bytes_impl::<BinaryType>();
        test_bytes_impl::<LargeBinaryType>();
    }

    #[test]
    fn test_lists() {
        let mut a = ListBuilder::new(ListBuilder::new(Int32Builder::new()));
        a.extend([
            Some(vec![Some(vec![Some(1), Some(2), None]), Some(vec![None])]),
            Some(vec![
                Some(vec![Some(1), Some(2), Some(3)]),
                Some(vec![Some(1)]),
            ]),
            Some(vec![]),
            None,
            Some(vec![Some(vec![Some(1), Some(2)]), Some(vec![Some(1)])]),
        ]);
        let a = a.finish();
        let mut b = ListBuilder::new(ListBuilder::new(Int32Builder::new()));
        b.extend([
            Some(vec![Some(vec![Some(1), Some(2), None]), Some(vec![None])]),
            Some(vec![
                Some(vec![Some(1), Some(2), None]),
                Some(vec![Some(1)]),
            ]),
            Some(vec![
                Some(vec![Some(1), Some(2), Some(3), Some(4)]),
                Some(vec![Some(1)]),
            ]),
            None,
            Some(vec![Some(vec![Some(1), Some(2)]), Some(vec![Some(1)])]),
        ]);
        let b = b.finish();

        let eq = make_eq_comparator(&a, &b, false).unwrap();
        assert_eq!(eq(0, 0), false); // lists contains null never equal
        assert_eq!(eq(0, 1), false);
        assert_eq!(eq(0, 2), false);
        assert_eq!(eq(1, 2), false);
        assert_eq!(eq(1, 3), false);
        assert_eq!(eq(2, 0), false);
        assert_eq!(eq(4, 4), true);
    }

    #[test]
    fn test_struct() {
        let fields = Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new_list("b", Field::new("item", DataType::Int32, true), true),
        ]);

        let a = Int32Array::from(vec![Some(1), Some(2), None, None]);
        let mut b = ListBuilder::new(Int32Builder::new());
        b.extend([Some(vec![Some(1), Some(2)]), Some(vec![None]), None, None]);
        let b = b.finish();

        let nulls = Some(NullBuffer::from_iter([true, true, true, false]));
        let values = vec![Arc::new(a) as _, Arc::new(b) as _];
        let s1 = StructArray::new(fields.clone(), values, nulls);

        let a = Int32Array::from(vec![None, Some(2), None]);
        let mut b = ListBuilder::new(Int32Builder::new());
        b.extend([None, None, Some(vec![])]);
        let b = b.finish();

        let values = vec![Arc::new(a) as _, Arc::new(b) as _];
        let s2 = StructArray::new(fields.clone(), values, None);

        let eq = make_eq_comparator(&s1, &s2, false).unwrap();
        assert_eq!(eq(0, 1), false); // (1, [1, 2]) eq (2, None)
        assert_eq!(eq(0, 0), false); // (1, [1, 2]) eq (None, None)
        assert_eq!(eq(1, 1), false); // (2, [None]) eq (2, None)
        assert_eq!(eq(2, 2), false); // (None, None) eq (None, [])
        assert_eq!(eq(3, 0), false); // None eq (None, [])
        assert_eq!(eq(2, 0), false); // (None, None) eq (None, None)
        assert_eq!(eq(3, 0), false); // None eq (None, None)
    }
}
