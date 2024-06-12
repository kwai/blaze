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

use arrow::{
    array::*,
    datatypes::{DataType, IntervalUnit, TimeUnit},
};
use datafusion::common::Result;
use datafusion_ext_commons::{df_execution_err, downcast_any};

pub mod full_join;
pub mod semi_join;

#[derive(std::marker::ConstParamTy, Clone, Copy, PartialEq, Eq)]
pub enum ProbeSide {
    L,
    R,
}

fn filter_joined_indices(
    key_columns1: &[ArrayRef],
    key_columns2: &[ArrayRef],
    indices1: &mut Vec<u32>,
    indices2: &mut Vec<u32>,
) -> Result<()> {
    fn filter_one(
        key_column1: &ArrayRef,
        key_column2: &ArrayRef,
        indices1: &mut Vec<u32>,
        indices2: &mut Vec<u32>,
    ) -> Result<()> {
        macro_rules! filter_atomic {
            ($cast_type:ty) => {{
                let col1 = downcast_any!(key_column1, $cast_type)?;
                let col2 = downcast_any!(key_column2, $cast_type)?;
                let mut valid_count = 0;
                for i in 0..indices1.len() {
                    let idx1 = indices1[i] as usize;
                    let idx2 = indices2[i] as usize;
                    if col1.is_valid(idx1) && col2.is_valid(idx2) && {
                        let v1 = col1.value(idx1);
                        let v2 = col2.value(idx2);
                        v1 == v2
                    } {
                        indices1[valid_count] = indices1[i];
                        indices2[valid_count] = indices2[i];
                        valid_count += 1;
                    }
                }
                indices1.truncate(valid_count);
                indices2.truncate(valid_count);
            }};
        }

        let dt1 = key_column1.data_type();
        let dt2 = key_column2.data_type();
        if dt1 != dt2 {
            return df_execution_err!("join key data type not matched: {dt1:?} <-> {dt2:?}");
        }
        match dt1 {
            DataType::Null => {
                indices1.clear();
                indices2.clear();
            }
            DataType::Boolean => filter_atomic!(BooleanArray),
            DataType::Int8 => filter_atomic!(Int8Array),
            DataType::Int16 => filter_atomic!(Int16Array),
            DataType::Int32 => filter_atomic!(Int32Array),
            DataType::Int64 => filter_atomic!(Int64Array),
            DataType::UInt8 => filter_atomic!(UInt8Array),
            DataType::UInt16 => filter_atomic!(UInt16Array),
            DataType::UInt32 => filter_atomic!(UInt32Array),
            DataType::UInt64 => filter_atomic!(UInt64Array),
            DataType::Float16 => filter_atomic!(Float16Array),
            DataType::Float32 => filter_atomic!(Float32Array),
            DataType::Float64 => filter_atomic!(Float64Array),
            DataType::Timestamp(unit, _) => match unit {
                TimeUnit::Second => filter_atomic!(TimestampSecondArray),
                TimeUnit::Millisecond => filter_atomic!(TimestampMillisecondArray),
                TimeUnit::Microsecond => filter_atomic!(TimestampMicrosecondArray),
                TimeUnit::Nanosecond => filter_atomic!(TimestampNanosecondArray),
            },
            DataType::Date32 => filter_atomic!(Date32Array),
            DataType::Date64 => filter_atomic!(Date64Array),
            DataType::Time32(unit) => match unit {
                TimeUnit::Second => filter_atomic!(Time32SecondArray),
                TimeUnit::Millisecond => filter_atomic!(Time32MillisecondArray),
                TimeUnit::Microsecond => filter_atomic!(Time32MillisecondArray),
                TimeUnit::Nanosecond => filter_atomic!(Time32MillisecondArray),
            },
            DataType::Time64(unit) => match unit {
                TimeUnit::Microsecond => filter_atomic!(Time64MicrosecondArray),
                TimeUnit::Nanosecond => filter_atomic!(Time64NanosecondArray),
                _ => return df_execution_err!("unsupported time64 unit: {unit:?}"),
            },
            DataType::Duration(unit) => match unit {
                TimeUnit::Second => filter_atomic!(DurationSecondArray),
                TimeUnit::Millisecond => filter_atomic!(DurationMillisecondArray),
                TimeUnit::Microsecond => filter_atomic!(DurationMicrosecondArray),
                TimeUnit::Nanosecond => filter_atomic!(DurationNanosecondArray),
            },
            DataType::Interval(unit) => match unit {
                IntervalUnit::YearMonth => filter_atomic!(IntervalYearMonthArray),
                IntervalUnit::DayTime => filter_atomic!(IntervalDayTimeArray),
                IntervalUnit::MonthDayNano => filter_atomic!(IntervalMonthDayNanoArray),
            },
            DataType::Binary => filter_atomic!(BinaryArray),
            DataType::FixedSizeBinary(_) => filter_atomic!(FixedSizeBinaryArray),
            DataType::LargeBinary => filter_atomic!(LargeBinaryArray),
            DataType::Utf8 => filter_atomic!(StringArray),
            DataType::LargeUtf8 => filter_atomic!(LargeStringArray),
            DataType::List(_) => filter_atomic!(ListArray),
            DataType::FixedSizeList(..) => filter_atomic!(FixedSizeListArray),
            DataType::LargeList(_) => filter_atomic!(LargeListArray),
            DataType::Struct(_) => filter_joined_indices(
                key_column1.as_struct().columns(),
                key_column2.as_struct().columns(),
                indices1,
                indices2,
            )?,
            DataType::Decimal128(..) => filter_atomic!(Decimal128Array),
            DataType::Decimal256(..) => filter_atomic!(Decimal256Array),
            DataType::Map(..) => filter_atomic!(MapArray),
            dt => {
                return df_execution_err!("unsupported data type: {dt:?}");
            }
        }
        Ok(())
    }

    for (key_column1, key_column2) in key_columns1.iter().zip(key_columns2) {
        filter_one(key_column1, key_column2, indices1, indices2)?;
    }
    Ok(())
}
