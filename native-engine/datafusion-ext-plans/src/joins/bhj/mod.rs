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

use arrow::{array::*, datatypes::DataType};
use datafusion::common::Result;
use datafusion_ext_commons::{df_execution_err, downcast_any};

use crate::common::make_eq_comparator::{make_eq_comparator, DynEqComparator};

pub mod full_join;
pub mod semi_join;

#[derive(std::marker::ConstParamTy, Clone, Copy, PartialEq, Eq)]
pub enum ProbeSide {
    L,
    R,
}

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
