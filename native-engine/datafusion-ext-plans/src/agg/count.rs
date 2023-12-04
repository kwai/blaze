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

use std::{
    any::Any,
    fmt::{Debug, Formatter},
    sync::Arc,
};

use arrow::{array::*, datatypes::*};
use datafusion::{
    common::{Result, ScalarValue},
    physical_expr::PhysicalExpr,
};

use crate::agg::{
    agg_buf::{AccumInitialValue, AggBuf},
    Agg,
};

pub struct AggCount {
    child: Arc<dyn PhysicalExpr>,
    data_type: DataType,
}

impl AggCount {
    pub fn try_new(child: Arc<dyn PhysicalExpr>, data_type: DataType) -> Result<Self> {
        assert_eq!(data_type, DataType::Int64);
        Ok(Self { child, data_type })
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

    fn with_new_exprs(&self, exprs: Vec<Arc<dyn PhysicalExpr>>) -> Result<Arc<dyn Agg>> {
        Ok(Arc::new(Self::try_new(
            exprs[0].clone(),
            self.data_type.clone(),
        )?))
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

    fn partial_batch_update(
        &self,
        agg_bufs: &mut [AggBuf],
        agg_buf_addrs: &[u64],
        values: &[ArrayRef],
    ) -> Result<usize> {
        let addr = agg_buf_addrs[0];
        let value = &values[0];
        for (row_idx, agg_buf) in agg_bufs.iter_mut().enumerate() {
            if value.is_valid(row_idx) {
                agg_buf.update_fixed_value::<i64>(addr, |v| v + 1);
            }
        }
        Ok(0)
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

    fn partial_batch_merge(
        &self,
        agg_bufs: &mut [AggBuf],
        merging_agg_bufs: &mut [AggBuf],
        agg_buf_addrs: &[u64],
    ) -> Result<usize> {
        let addr = agg_buf_addrs[0];
        for (agg_buf, merging_agg_buf) in agg_bufs.iter_mut().zip(merging_agg_bufs) {
            let merging_num_valids = merging_agg_buf.fixed_value::<i64>(addr);
            agg_buf.update_fixed_value::<i64>(addr, |v| v + merging_num_valids);
        }
        Ok(0)
    }
    fn final_merge(&self, agg_buf: &mut AggBuf, agg_buf_addrs: &[u64]) -> Result<ScalarValue> {
        let addr = agg_buf_addrs[0];
        Ok(ScalarValue::from(agg_buf.fixed_value::<i64>(addr)))
    }

    fn final_batch_merge(
        &self,
        agg_bufs: &mut [AggBuf],
        agg_buf_addrs: &[u64],
    ) -> Result<ArrayRef> {
        let addr = agg_buf_addrs[0];
        Ok(Arc::new(
            agg_bufs
                .iter()
                .map(|agg_buf| agg_buf.fixed_value::<i64>(addr))
                .collect::<Int64Array>(),
        ))
    }
}
