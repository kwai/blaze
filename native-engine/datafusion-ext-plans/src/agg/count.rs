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

use crate::agg::agg_buf::{AccumInitialValue, AggBuf};
use crate::agg::Agg;
use arrow::array::*;
use arrow::datatypes::*;
use datafusion::common::{Result, ScalarValue};
use datafusion::physical_expr::PhysicalExpr;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub struct AggCount {
    child: Arc<dyn PhysicalExpr>,
    data_type: DataType,
}

impl AggCount {
    pub fn try_new(child: Arc<dyn PhysicalExpr>, data_type: DataType) -> Result<Self> {
        assert_eq!(data_type, DataType::Int64);
        Ok(Self {child, data_type})
    }
}

impl Debug for AggCount {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Count({:?})", self.child)
    }
}

impl Agg for AggCount {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn exprs(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.child.clone()]
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn nullable(&self) -> bool {
        false
    }

    fn accums_initial(&self) -> &[AccumInitialValue] {
        &[AccumInitialValue::Scalar(ScalarValue::Int64(Some(0)))]
    }

    fn partial_update(
        &self,
        agg_buf: &mut AggBuf,
        agg_buf_addrs: &[u64],
        values: &[ArrayRef],
        row_idx: usize,
    ) -> Result<()> {
        let addr = agg_buf_addrs[0];
        if values[0].is_valid(row_idx) {
            agg_buf.update_fixed_value::<i64>(addr, |v| v + 1);
        }
        Ok(())
    }

    fn partial_update_all(
        &self,
        agg_buf: &mut AggBuf,
        agg_buf_addrs: &[u64],
        values: &[ArrayRef],
    ) -> Result<()> {
        let addr = agg_buf_addrs[0];
        let num_valids = values[0].len() - values[0].null_count();
        agg_buf.update_fixed_value::<i64>(addr, |v| v + num_valids as i64);
        Ok(())
    }

    fn partial_merge(
        &self,
        agg_buf1: &mut AggBuf,
        agg_buf2: &mut AggBuf,
        agg_buf_addrs: &[u64],
    ) -> Result<()> {
        let addr = agg_buf_addrs[0];
        let num_valids2 = agg_buf2.fixed_value::<i64>(addr);
        agg_buf1.update_fixed_value::<i64>(addr, |v| v + num_valids2);
        Ok(())
    }

    fn final_merge(&self, agg_buf: &mut AggBuf, agg_buf_addrs: &[u64]) -> Result<ScalarValue> {
        let addr = agg_buf_addrs[0];
        Ok(ScalarValue::from(agg_buf.fixed_value::<i64>(addr)))
    }
}
