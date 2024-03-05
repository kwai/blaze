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
    sync::{atomic::AtomicUsize, Arc},
};

use arrow::{array::*, datatypes::*};
use datafusion::{
    common::{Result, ScalarValue},
    physical_expr::PhysicalExpr,
};

use crate::agg::{
    acc::{AccumInitialValue, AccumStateRow, AccumStateValAddr, RefAccumStateRow},
    Agg, WithAggBufAddrs, WithMemTracking,
};

pub struct AggCount {
    child: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    accum_state_val_addr: AccumStateValAddr,
    mem_used_tracker: AtomicUsize,
}

impl WithAggBufAddrs for AggCount {
    fn set_accum_state_val_addrs(&mut self, accum_state_val_addrs: &[AccumStateValAddr]) {
        self.accum_state_val_addr = accum_state_val_addrs[0];
    }
}

impl WithMemTracking for AggCount {
    fn mem_used_tracker(&self) -> &AtomicUsize {
        &self.mem_used_tracker
    }
}

impl AggCount {
    pub fn try_new(child: Arc<dyn PhysicalExpr>, data_type: DataType) -> Result<Self> {
        assert_eq!(data_type, DataType::Int64);
        Ok(Self {
            child,
            data_type,
            accum_state_val_addr: AccumStateValAddr::default(),
            mem_used_tracker: AtomicUsize::new(0),
        })
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

    fn increase_acc_mem_used(&self, _acc: &mut RefAccumStateRow) {
        // do nothing
    }

    fn partial_update(
        &self,
        acc: &mut RefAccumStateRow,
        values: &[ArrayRef],
        row_idx: usize,
    ) -> Result<()> {
        let addr = self.accum_state_val_addr;
        if values[0].is_valid(row_idx) {
            acc.update_fixed_value::<i64>(addr, |v| v + 1);
        }
        Ok(())
    }

    fn partial_batch_update(
        &self,
        accs: &mut [RefAccumStateRow],
        values: &[ArrayRef],
    ) -> Result<()> {
        let addr = self.accum_state_val_addr;
        let value = &values[0];
        for (row_idx, acc) in accs.iter_mut().enumerate() {
            if value.is_valid(row_idx) {
                acc.update_fixed_value::<i64>(addr, |v| v + 1);
            }
        }
        Ok(())
    }

    fn partial_update_all(&self, acc: &mut RefAccumStateRow, values: &[ArrayRef]) -> Result<()> {
        let addr = self.accum_state_val_addr;
        let num_valids = values[0].len() - values[0].null_count();
        acc.update_fixed_value::<i64>(addr, |v| v + num_valids as i64);
        Ok(())
    }

    fn partial_merge(
        &self,
        acc1: &mut RefAccumStateRow,
        acc2: &mut RefAccumStateRow,
    ) -> Result<()> {
        let addr = self.accum_state_val_addr;
        let num_valids2 = acc2.fixed_value::<i64>(addr);
        acc1.update_fixed_value::<i64>(addr, |v| v + num_valids2);
        Ok(())
    }

    fn partial_batch_merge(
        &self,
        accs: &mut [RefAccumStateRow],
        merging_accs: &mut [RefAccumStateRow],
    ) -> Result<()> {
        let addr = self.accum_state_val_addr;
        for (acc, merging_acc) in accs.iter_mut().zip(merging_accs) {
            let merging_num_valids = merging_acc.fixed_value::<i64>(addr);
            acc.update_fixed_value::<i64>(addr, |v| v + merging_num_valids);
        }
        Ok(())
    }
    fn final_merge(&self, acc: &mut RefAccumStateRow) -> Result<ScalarValue> {
        let addr = self.accum_state_val_addr;
        Ok(ScalarValue::from(acc.fixed_value::<i64>(addr)))
    }

    fn final_batch_merge(&self, accs: &mut [RefAccumStateRow]) -> Result<ArrayRef> {
        let addr = self.accum_state_val_addr;
        Ok(Arc::new(
            accs.iter()
                .map(|acc| acc.fixed_value::<i64>(addr))
                .collect::<Int64Array>(),
        ))
    }
}
