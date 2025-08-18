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

use std::{
    any::Any,
    fmt::{Debug, Formatter},
    sync::Arc,
};

use arrow::{
    array::{Array, ArrayRef, AsArray},
    datatypes::DataType,
};
use datafusion::{common::Result, physical_expr::PhysicalExprRef};

use crate::{
    agg::{Agg, acc::AccColumnRef, agg::IdxSelection, collect::AggCollectSet},
    idx_for_zipped,
};

pub struct AggCollect {
    innert_collect_list: AggCollectSet,
}

impl AggCollect {
    pub fn try_new(child: PhysicalExprRef, arg_list_inner_type: DataType) -> Result<Self> {
        let return_type = DataType::new_list(arg_list_inner_type.clone(), true);
        Ok(Self {
            innert_collect_list: AggCollectSet::try_new(child, return_type, arg_list_inner_type)?,
        })
    }
}

impl Debug for AggCollect {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "brickhouse.Collect({:?})",
            self.innert_collect_list.exprs()[0]
        )
    }
}

impl Agg for AggCollect {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn exprs(&self) -> Vec<PhysicalExprRef> {
        self.innert_collect_list.exprs()
    }

    fn with_new_exprs(&self, exprs: Vec<PhysicalExprRef>) -> Result<Arc<dyn Agg>> {
        Ok(Arc::new(Self::try_new(
            exprs[0].clone(),
            self.innert_collect_list.arg_type().clone(),
        )?))
    }

    fn data_type(&self) -> &DataType {
        self.innert_collect_list.data_type()
    }

    fn nullable(&self) -> bool {
        self.innert_collect_list.nullable()
    }

    fn create_acc_column(&self, num_rows: usize) -> AccColumnRef {
        self.innert_collect_list.create_acc_column(num_rows)
    }

    fn partial_update(
        &self,
        accs: &mut AccColumnRef,
        acc_idx: IdxSelection<'_>,
        partial_args: &[ArrayRef],
        partial_arg_idx: IdxSelection<'_>,
    ) -> Result<()> {
        accs.ensure_size(acc_idx);
        let list = partial_args[0].as_list::<i32>();

        idx_for_zipped! {
            ((acc_idx, partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                if list.is_valid(partial_arg_idx) {
                    let values = list.value(partial_arg_idx);
                    let values_len = values.len();
                    self.innert_collect_list.partial_update(
                        accs,
                        IdxSelection::Single(acc_idx),
                        &[values],
                        IdxSelection::Range(0, values_len),
                    )?;
                 }
            }
        }
        Ok(())
    }

    fn partial_merge(
        &self,
        accs: &mut AccColumnRef,
        acc_idx: IdxSelection<'_>,
        merging_accs: &mut AccColumnRef,
        merging_acc_idx: IdxSelection<'_>,
    ) -> Result<()> {
        self.innert_collect_list
            .partial_merge(accs, acc_idx, merging_accs, merging_acc_idx)
    }

    fn final_merge(&self, accs: &mut AccColumnRef, acc_idx: IdxSelection<'_>) -> Result<ArrayRef> {
        self.innert_collect_list.final_merge(accs, acc_idx)
    }
}
