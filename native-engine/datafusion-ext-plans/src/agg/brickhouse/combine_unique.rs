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

use arrow::{
    array::{Array, ArrayRef, AsArray},
    datatypes::DataType,
};
use datafusion::{common::Result, physical_expr::PhysicalExpr, scalar::ScalarValue};

use crate::agg::{
    acc::{AccumInitialValue, AccumStateValAddr, RefAccumStateRow},
    collect_set::AggCollectSet,
    Agg, WithAggBufAddrs, WithMemTracking,
};

pub struct AggCombineUnique {
    innert_collect_set: AggCollectSet,
}

impl WithAggBufAddrs for AggCombineUnique {
    fn set_accum_state_val_addrs(&mut self, accum_state_val_addrs: &[AccumStateValAddr]) {
        self.innert_collect_set
            .set_accum_state_val_addrs(accum_state_val_addrs);
    }
}

impl WithMemTracking for AggCombineUnique {
    fn mem_used_tracker(&self) -> &AtomicUsize {
        self.innert_collect_set.mem_used_tracker()
    }
}

impl AggCombineUnique {
    pub fn try_new(child: Arc<dyn PhysicalExpr>, arg_list_inner_type: DataType) -> Result<Self> {
        let return_type = DataType::new_list(arg_list_inner_type.clone(), true);
        Ok(Self {
            innert_collect_set: AggCollectSet::try_new(child, return_type, arg_list_inner_type)?,
        })
    }
}

impl Debug for AggCombineUnique {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "brickhouse.CombineUnique({:?})",
            self.innert_collect_set.exprs()[0]
        )
    }
}

impl Agg for AggCombineUnique {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn exprs(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.innert_collect_set.exprs()
    }

    fn with_new_exprs(&self, exprs: Vec<Arc<dyn PhysicalExpr>>) -> Result<Arc<dyn Agg>> {
        Ok(Arc::new(Self::try_new(
            exprs[0].clone(),
            self.innert_collect_set.arg_type().clone(),
        )?))
    }

    fn data_type(&self) -> &DataType {
        self.innert_collect_set.data_type()
    }

    fn nullable(&self) -> bool {
        self.innert_collect_set.nullable()
    }

    fn accums_initial(&self) -> &[AccumInitialValue] {
        self.innert_collect_set.accums_initial()
    }

    fn increase_acc_mem_used(&self, acc: &mut RefAccumStateRow) {
        self.innert_collect_set.increase_acc_mem_used(acc);
    }

    fn partial_update(
        &self,
        acc: &mut RefAccumStateRow,
        values: &[ArrayRef],
        row_idx: usize,
    ) -> Result<()> {
        let list = values[0].as_list::<i32>();
        if list.is_valid(row_idx) {
            self.innert_collect_set
                .partial_update_all(acc, &[list.value(row_idx)])?;
        }
        Ok(())
    }

    fn partial_update_all(&self, acc: &mut RefAccumStateRow, values: &[ArrayRef]) -> Result<()> {
        self.innert_collect_set.partial_update_all(acc, values)
    }

    fn partial_merge(
        &self,
        acc: &mut RefAccumStateRow,
        merging_acc: &mut RefAccumStateRow,
    ) -> Result<()> {
        self.innert_collect_set.partial_merge(acc, merging_acc)
    }

    fn final_merge(&self, acc: &mut RefAccumStateRow) -> Result<ScalarValue> {
        self.innert_collect_set.final_merge(acc)
    }

    fn final_batch_merge(&self, accs: &mut [RefAccumStateRow]) -> Result<ArrayRef> {
        self.innert_collect_set.final_batch_merge(accs)
    }
}
