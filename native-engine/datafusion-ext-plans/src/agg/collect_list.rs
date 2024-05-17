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
use datafusion_ext_commons::{df_execution_err, downcast_any};

use crate::agg::{
    acc::{
        AccumInitialValue, AccumStateRow, AccumStateValAddr, AggDynList, AggDynValue,
        RefAccumStateRow,
    },
    Agg, WithAggBufAddrs, WithMemTracking,
};

pub struct AggCollectList {
    child: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    arg_type: DataType,
    accum_initial: [AccumInitialValue; 1],
    accum_state_val_addr: AccumStateValAddr,
    mem_used_tracker: AtomicUsize,
}

impl WithAggBufAddrs for AggCollectList {
    fn set_accum_state_val_addrs(&mut self, accum_state_val_addrs: &[AccumStateValAddr]) {
        self.accum_state_val_addr = accum_state_val_addrs[0];
    }
}

impl WithMemTracking for AggCollectList {
    fn mem_used_tracker(&self) -> &AtomicUsize {
        &self.mem_used_tracker
    }
}

impl AggCollectList {
    pub fn try_new(
        child: Arc<dyn PhysicalExpr>,
        data_type: DataType,
        arg_type: DataType,
    ) -> Result<Self> {
        Ok(Self {
            child,
            data_type,
            accum_initial: [AccumInitialValue::DynList(arg_type.clone())],
            arg_type,
            accum_state_val_addr: AccumStateValAddr::default(),
            mem_used_tracker: AtomicUsize::new(0),
        })
    }

    pub fn arg_type(&self) -> &DataType {
        &self.arg_type
    }
}

impl Debug for AggCollectList {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CollectList({:?})", self.child)
    }
}

impl Agg for AggCollectList {
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
            self.arg_type.clone(),
        )?))
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn nullable(&self) -> bool {
        false
    }

    fn accums_initial(&self) -> &[AccumInitialValue] {
        &self.accum_initial
    }

    fn increase_acc_mem_used(&self, acc: &mut RefAccumStateRow) {
        if let Some(v) = acc.dyn_value(self.accum_state_val_addr) {
            self.add_mem_used(v.mem_size());
        }
    }

    fn partial_update(
        &self,
        acc: &mut RefAccumStateRow,
        values: &[ArrayRef],
        row_idx: usize,
    ) -> Result<()> {
        if values[0].is_valid(row_idx) {
            match acc.dyn_value_mut(self.accum_state_val_addr) {
                Some(dyn_list) => {
                    let list = downcast_any!(dyn_list, mut AggDynList)?;
                    self.sub_mem_used(list.mem_size());

                    list.append(&ScalarValue::try_from_array(&values[0], row_idx)?, false);
                    self.add_mem_used(list.mem_size());
                }
                w => {
                    let mut new_list = AggDynList::default();
                    new_list.append(&ScalarValue::try_from_array(&values[0], row_idx)?, false);
                    self.add_mem_used(new_list.mem_size());
                    *w = Some(Box::new(new_list));
                }
            }
        }
        Ok(())
    }

    fn partial_update_all(&self, acc: &mut RefAccumStateRow, values: &[ArrayRef]) -> Result<()> {
        let dyn_list = match acc.dyn_value_mut(self.accum_state_val_addr) {
            Some(dyn_list) => dyn_list,
            w => {
                let new_list = AggDynList::default();
                self.add_mem_used(new_list.mem_size());
                *w = Some(Box::new(new_list));
                w.as_mut().unwrap()
            }
        };
        let list = downcast_any!(dyn_list, mut AggDynList)?;
        self.sub_mem_used(list.mem_size());

        for i in 0..values[0].len() {
            if values[0].is_valid(i) {
                list.append(&ScalarValue::try_from_array(&values[0], i)?, false);
            }
        }
        self.add_mem_used(list.mem_size());
        Ok(())
    }

    fn partial_merge(
        &self,
        acc: &mut RefAccumStateRow,
        merging_acc: &mut RefAccumStateRow,
    ) -> Result<()> {
        match (
            acc.dyn_value_mut(self.accum_state_val_addr),
            merging_acc.dyn_value_mut(self.accum_state_val_addr),
        ) {
            (Some(w), Some(v)) => {
                let w = downcast_any!(w, mut AggDynList)?;
                let v = downcast_any!(v, mut AggDynList)?;
                self.sub_mem_used(w.mem_size());
                self.sub_mem_used(v.mem_size());

                w.merge(v);
                self.add_mem_used(w.mem_size());
            }
            (w_none, v @ Some(_)) => *w_none = std::mem::take(v),
            (None, _) => {}
            (_, None) => {}
        }
        Ok(())
    }

    fn final_merge(&self, acc: &mut RefAccumStateRow) -> Result<ScalarValue> {
        match std::mem::take(acc.dyn_value_mut(self.accum_state_val_addr)) {
            Some(w) => {
                let list = w
                    .as_any_boxed()
                    .downcast::<AggDynList>()
                    .or_else(|_| df_execution_err!("error downcasting to AggDynList"))?;
                self.sub_mem_used(list.mem_size());

                Ok(ScalarValue::List(ScalarValue::new_list(
                    &list
                        .into_values(self.arg_type.clone(), false)
                        .collect::<Vec<_>>(),
                    &self.arg_type,
                )))
            }
            None => ScalarValue::try_from(&self.data_type),
        }
    }

    fn final_batch_merge(&self, accs: &mut [RefAccumStateRow]) -> Result<ArrayRef> {
        let values: Vec<ScalarValue> = accs
            .iter_mut()
            .map(|acc| self.final_merge(acc))
            .collect::<Result<_>>()?;

        if values.is_empty() {
            return Ok(new_empty_array(self.data_type()));
        }
        Ok(ScalarValue::iter_to_array(values)?)
    }
}
