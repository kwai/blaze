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
    ops::Add,
    sync::{atomic::AtomicUsize, Arc},
};

use arrow::{array::*, datatypes::*};
use datafusion::{
    common::{Result, ScalarValue},
    physical_expr::PhysicalExpr,
};
use datafusion_ext_commons::df_unimplemented_err;
use paste::paste;

use crate::agg::{
    acc::{AccumInitialValue, AccumStateRow, AccumStateValAddr, RefAccumStateRow},
    default_final_batch_merge_with_addr, default_final_merge_with_addr, Agg, WithAggBufAddrs,
    WithMemTracking,
};

pub struct AggSum {
    child: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    accums_initial: Vec<AccumInitialValue>,
    accum_state_val_addr: AccumStateValAddr,
    partial_updater: fn(&Self, &mut RefAccumStateRow, &ArrayRef, usize),
    partial_batch_updater: fn(&Self, &mut [RefAccumStateRow], &ArrayRef),
    partial_buf_merger: fn(&Self, &mut RefAccumStateRow, &mut RefAccumStateRow),
    mem_used_tracker: AtomicUsize,
}

impl WithAggBufAddrs for AggSum {
    fn set_accum_state_val_addrs(&mut self, accum_state_val_addrs: &[AccumStateValAddr]) {
        self.accum_state_val_addr = accum_state_val_addrs[0];
    }
}

impl WithMemTracking for AggSum {
    fn mem_used_tracker(&self) -> &AtomicUsize {
        &self.mem_used_tracker
    }
}

impl AggSum {
    pub fn try_new(child: Arc<dyn PhysicalExpr>, data_type: DataType) -> Result<Self> {
        let accums_initial = vec![AccumInitialValue::Scalar(ScalarValue::try_from(
            &data_type,
        )?)];
        let partial_updater = get_partial_updater(&data_type)?;
        let partial_batch_updater = get_partial_batch_updater(&data_type)?;
        let partial_buf_merger = get_partial_buf_merger(&data_type)?;
        Ok(Self {
            child,
            data_type,
            accums_initial,
            accum_state_val_addr: AccumStateValAddr::default(),
            partial_updater,
            partial_batch_updater,
            partial_buf_merger,
            mem_used_tracker: AtomicUsize::new(0),
        })
    }
}

impl Debug for AggSum {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Sum({:?})", self.child)
    }
}

impl Agg for AggSum {
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
        true
    }

    fn accums_initial(&self) -> &[AccumInitialValue] {
        &self.accums_initial
    }

    fn increase_acc_mem_used(&self, _acc: &mut RefAccumStateRow) {
        // do nothing
    }

    fn prepare_partial_args(&self, partial_inputs: &[ArrayRef]) -> Result<Vec<ArrayRef>> {
        // cast arg1 to target data type
        Ok(vec![datafusion_ext_commons::cast::cast(
            &partial_inputs[0],
            &self.data_type,
        )?])
    }

    fn partial_update(
        &self,
        acc: &mut RefAccumStateRow,
        values: &[ArrayRef],
        row_idx: usize,
    ) -> Result<()> {
        let partial_updater = self.partial_updater;
        partial_updater(self, acc, &values[0], row_idx);
        Ok(())
    }

    fn partial_batch_update(
        &self,
        accs: &mut [RefAccumStateRow],
        values: &[ArrayRef],
    ) -> Result<()> {
        let partial_batch_updater = self.partial_batch_updater;
        partial_batch_updater(self, accs, &values[0]);
        Ok(())
    }

    fn partial_update_all(&self, acc: &mut RefAccumStateRow, values: &[ArrayRef]) -> Result<()> {
        macro_rules! handle {
            ($ty:ident) => {{
                type TArray = paste! {[<$ty Array>]};
                let value = values[0].as_any().downcast_ref::<TArray>().unwrap();
                if let Some(sum) = arrow::compute::sum(value) {
                    partial_update_prim(acc, self.accum_state_val_addr, sum);
                }
            }};
        }
        match values[0].data_type() {
            DataType::Null => {}
            DataType::Float32 => handle!(Float32),
            DataType::Float64 => handle!(Float64),
            DataType::Int8 => handle!(Int8),
            DataType::Int16 => handle!(Int16),
            DataType::Int32 => handle!(Int32),
            DataType::Int64 => handle!(Int64),
            DataType::UInt8 => handle!(UInt8),
            DataType::UInt16 => handle!(UInt16),
            DataType::UInt32 => handle!(UInt32),
            DataType::UInt64 => handle!(UInt64),
            DataType::Decimal128(..) => handle!(Decimal128),
            other => df_unimplemented_err!("unsupported data type in sum(): {other}")?,
        }
        Ok(())
    }

    fn partial_merge(
        &self,
        acc1: &mut RefAccumStateRow,
        acc2: &mut RefAccumStateRow,
    ) -> Result<()> {
        let partial_buf_merger = self.partial_buf_merger;
        partial_buf_merger(self, acc1, acc2);
        Ok(())
    }

    fn partial_batch_merge(
        &self,
        accs: &mut [RefAccumStateRow],
        merging_accs: &mut [RefAccumStateRow],
    ) -> Result<()> {
        let partial_buf_merger = self.partial_buf_merger;
        for (acc, merging_acc) in accs.iter_mut().zip(merging_accs) {
            partial_buf_merger(self, acc, merging_acc);
        }
        Ok(())
    }

    fn final_merge(&self, acc: &mut RefAccumStateRow) -> Result<ScalarValue> {
        default_final_merge_with_addr(self, acc, self.accum_state_val_addr)
    }

    fn final_batch_merge(&self, accs: &mut [RefAccumStateRow]) -> Result<ArrayRef> {
        default_final_batch_merge_with_addr(self, accs, self.accum_state_val_addr)
    }
}

fn partial_update_prim<T: Copy + Add<Output = T>>(
    acc: &mut RefAccumStateRow,
    addr: AccumStateValAddr,
    v: T,
) {
    if acc.is_fixed_valid(addr) {
        acc.update_fixed_value::<T>(addr, |w| w + v);
    } else {
        acc.set_fixed_value::<T>(addr, v);
        acc.set_fixed_valid(addr, true);
    }
}

fn get_partial_updater(
    dt: &DataType,
) -> Result<fn(&AggSum, &mut RefAccumStateRow, &ArrayRef, usize)> {
    macro_rules! fn_fixed {
        ($ty:ident) => {{
            Ok(|this, acc, v, i| {
                type TArray = paste! {[<$ty Array>]};
                let value = v.as_any().downcast_ref::<TArray>().unwrap();
                if value.is_valid(i) {
                    partial_update_prim(acc, this.accum_state_val_addr, value.value(i));
                }
            })
        }};
    }
    match dt {
        DataType::Null => Ok(|_, _, _, _| ()),
        DataType::Float32 => fn_fixed!(Float32),
        DataType::Float64 => fn_fixed!(Float64),
        DataType::Int8 => fn_fixed!(Int8),
        DataType::Int16 => fn_fixed!(Int16),
        DataType::Int32 => fn_fixed!(Int32),
        DataType::Int64 => fn_fixed!(Int64),
        DataType::UInt8 => fn_fixed!(UInt8),
        DataType::UInt16 => fn_fixed!(UInt16),
        DataType::UInt32 => fn_fixed!(UInt32),
        DataType::UInt64 => fn_fixed!(UInt64),
        DataType::Decimal128(..) => fn_fixed!(Decimal128),
        other => df_unimplemented_err!("unsupported data type in sum(): {other}"),
    }
}

fn get_partial_batch_updater(
    dt: &DataType,
) -> Result<fn(&AggSum, &mut [RefAccumStateRow], &ArrayRef)> {
    macro_rules! fn_fixed {
        ($ty:ident) => {{
            Ok(|this, accs, v| {
                type TArray = paste! {[<$ty Array>]};
                let value = v.as_any().downcast_ref::<TArray>().unwrap();
                for (acc, value) in accs.iter_mut().zip(value.iter()) {
                    if let Some(value) = value {
                        partial_update_prim(acc, this.accum_state_val_addr, value);
                    }
                }
            })
        }};
    }
    match dt {
        DataType::Null => Ok(|_, _, _| ()),
        DataType::Float32 => fn_fixed!(Float32),
        DataType::Float64 => fn_fixed!(Float64),
        DataType::Int8 => fn_fixed!(Int8),
        DataType::Int16 => fn_fixed!(Int16),
        DataType::Int32 => fn_fixed!(Int32),
        DataType::Int64 => fn_fixed!(Int64),
        DataType::UInt8 => fn_fixed!(UInt8),
        DataType::UInt16 => fn_fixed!(UInt16),
        DataType::UInt32 => fn_fixed!(UInt32),
        DataType::UInt64 => fn_fixed!(UInt64),
        DataType::Decimal128(..) => fn_fixed!(Decimal128),
        other => df_unimplemented_err!("unsupported data type in sum(): {other}"),
    }
}

fn get_partial_buf_merger(
    dt: &DataType,
) -> Result<fn(&AggSum, &mut RefAccumStateRow, &mut RefAccumStateRow)> {
    macro_rules! fn_fixed {
        ($ty:ident) => {{
            Ok(|this, acc1, acc2| {
                type TType = paste! {[<$ty Type>]};
                type TNative = <TType as ArrowPrimitiveType>::Native;
                if acc2.is_fixed_valid(this.accum_state_val_addr) {
                    let v = acc2.fixed_value::<TNative>(this.accum_state_val_addr);
                    partial_update_prim(acc1, this.accum_state_val_addr, v);
                }
            })
        }};
    }
    match dt {
        DataType::Null => Ok(|_, _, _| ()),
        DataType::Float32 => fn_fixed!(Float32),
        DataType::Float64 => fn_fixed!(Float64),
        DataType::Int8 => fn_fixed!(Int8),
        DataType::Int16 => fn_fixed!(Int16),
        DataType::Int32 => fn_fixed!(Int32),
        DataType::Int64 => fn_fixed!(Int64),
        DataType::UInt8 => fn_fixed!(UInt8),
        DataType::UInt16 => fn_fixed!(UInt16),
        DataType::UInt32 => fn_fixed!(UInt32),
        DataType::UInt64 => fn_fixed!(UInt64),
        DataType::Decimal128(..) => fn_fixed!(Decimal128),
        other => df_unimplemented_err!("unsupported data type in sum(): {other}"),
    }
}
