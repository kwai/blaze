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

use arrow::array::ArrayRef;
use datafusion::common::Result;
use datafusion_ext_commons::df_execution_err;

use crate::common::make_eq_comparator::{make_eq_comparator, DynEqComparator};

pub mod full_join;
pub mod semi_join;

#[derive(std::marker::ConstParamTy, Clone, Copy, PartialEq, Eq)]
pub enum ProbeSide {
    L,
    R,
}

pub fn make_eq_comparator_multiple_arrays(
    cols1: &[ArrayRef],
    cols2: &[ArrayRef],
    ignores_null: bool,
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
        .map(|(col1, col2)| Ok(make_eq_comparator(col1, col2, ignores_null)?))
        .collect::<Result<Vec<_>>>()?;
    Ok(Box::new(move |i, j| eqs.iter().all(|eq| eq(i, j))))
}
