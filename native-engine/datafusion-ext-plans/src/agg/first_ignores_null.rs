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
use datafusion_ext_commons::downcast_any;
use paste::paste;

use crate::agg::{
    acc::{
        AccumInitialValue, AccumStateRow, AccumStateValAddr, AggDynBinary, AggDynScalar, AggDynStr,
        AggDynValue, RefAccumStateRow,
    },
    default_final_batch_merge_with_addr, default_final_merge_with_addr, Agg, WithAggBufAddrs,
    WithMemTracking,
};

pub struct AggFirstIgnoresNull {
    child: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    accums_initial: Vec<AccumInitialValue>,
    accum_state_val_addr: AccumStateValAddr,
    partial_updater: fn(&AggFirstIgnoresNull, &mut RefAccumStateRow, &ArrayRef, usize),
    partial_buf_merger: fn(&AggFirstIgnoresNull, &mut RefAccumStateRow, &mut RefAccumStateRow),
    mem_used_tracker: AtomicUsize,
}

impl WithAggBufAddrs for AggFirstIgnoresNull {
    fn set_accum_state_val_addrs(&mut self, accum_state_val_addrs: &[AccumStateValAddr]) {
        self.accum_state_val_addr = accum_state_val_addrs[0];
    }
}

impl WithMemTracking for AggFirstIgnoresNull {
    fn mem_used_tracker(&self) -> &AtomicUsize {
        &self.mem_used_tracker
    }
}

impl AggFirstIgnoresNull {
    pub fn try_new(child: Arc<dyn PhysicalExpr>, data_type: DataType) -> Result<Self> {
        let accums_initial = vec![AccumInitialValue::Scalar(ScalarValue::try_from(
            &data_type,
        )?)];
        let partial_updater = get_partial_updater(&data_type)?;
        let partial_buf_merger = get_partial_buf_merger(&data_type)?;
        Ok(Self {
            child,
            data_type,
            accums_initial,
            accum_state_val_addr: AccumStateValAddr::default(),
            partial_updater,
            partial_buf_merger,
            mem_used_tracker: AtomicUsize::new(0),
        })
    }
}

impl Debug for AggFirstIgnoresNull {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FirstIgnoresNull({:?})", self.child)
    }
}

impl Agg for AggFirstIgnoresNull {
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

    fn increase_acc_mem_used(&self, acc: &mut RefAccumStateRow) {
        if self.data_type.is_primitive()
            || matches!(self.data_type, DataType::Null | DataType::Boolean)
        {
            return;
        }
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
        let partial_updater = self.partial_updater;
        let value = &values[0];
        partial_updater(self, acc, value, row_idx);
        Ok(())
    }

    fn partial_update_all(&self, acc: &mut RefAccumStateRow, values: &[ArrayRef]) -> Result<()> {
        let partial_updater = self.partial_updater;
        let value = &values[0];

        for i in 0..value.len() {
            if value.is_valid(i) {
                partial_updater(self, acc, value, i);
            }
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

    fn final_merge(&self, acc: &mut RefAccumStateRow) -> Result<ScalarValue> {
        default_final_merge_with_addr(self, acc, self.accum_state_val_addr)
    }

    fn final_batch_merge(&self, accs: &mut [RefAccumStateRow]) -> Result<ArrayRef> {
        default_final_batch_merge_with_addr(self, accs, self.accum_state_val_addr)
    }
}

fn get_partial_updater(
    dt: &DataType,
) -> Result<fn(&AggFirstIgnoresNull, &mut RefAccumStateRow, &ArrayRef, usize)> {
    macro_rules! fn_fixed {
        ($ty:ident) => {{
            Ok(|this, acc, v, i| {
                if !acc.is_fixed_valid(this.accum_state_val_addr) && v.is_valid(i) {
                    let value = v.as_any().downcast_ref::<paste! {[<$ty Array>]}>().unwrap();
                    acc.set_fixed_value(this.accum_state_val_addr, value.value(i));
                    acc.set_fixed_valid(this.accum_state_val_addr, true);
                }
            })
        }};
    }
    match dt {
        DataType::Null => Ok(|_, _, _, _| ()),
        DataType::Boolean => fn_fixed!(Boolean),
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
        DataType::Date32 => fn_fixed!(Date32),
        DataType::Date64 => fn_fixed!(Date64),
        DataType::Timestamp(TimeUnit::Second, _) => fn_fixed!(TimestampSecond),
        DataType::Timestamp(TimeUnit::Millisecond, _) => fn_fixed!(TimestampMillisecond),
        DataType::Timestamp(TimeUnit::Microsecond, _) => fn_fixed!(TimestampMicrosecond),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => fn_fixed!(TimestampNanosecond),
        DataType::Decimal128(..) => fn_fixed!(Decimal128),
        DataType::Utf8 => Ok(
            |this: &AggFirstIgnoresNull, acc: &mut RefAccumStateRow, v: &ArrayRef, i: usize| {
                if v.is_valid(i) {
                    let v = downcast_any!(v, StringArray).unwrap().value(i);
                    let w = acc.dyn_value_mut(this.accum_state_val_addr);
                    if w.is_none() {
                        let new = AggDynStr::from_str(v);
                        this.add_mem_used(new.mem_size());
                        *w = Some(Box::new(new));
                    }
                }
            },
        ),
        DataType::Binary => Ok(
            |this: &AggFirstIgnoresNull, acc: &mut RefAccumStateRow, v: &ArrayRef, i: usize| {
                if v.is_valid(i) {
                    let v = downcast_any!(v, BinaryArray).unwrap().value(i);
                    let w = acc.dyn_value_mut(this.accum_state_val_addr);
                    if w.is_none() {
                        let new = AggDynBinary::from_slice(v);
                        this.add_mem_used(new.mem_size());
                        *w = Some(Box::new(new));
                    }
                }
            },
        ),
        _other => Ok(
            |this: &AggFirstIgnoresNull, acc: &mut RefAccumStateRow, v: &ArrayRef, i: usize| {
                if v.is_valid(i) {
                    let w = acc.dyn_value_mut(this.accum_state_val_addr);
                    if w.is_none() {
                        let new =
                            AggDynScalar::new(ScalarValue::try_from_array(v, i).expect(
                                "FirstIgnoresNull::partial_update error creating ScalarValue",
                            ));
                        this.add_mem_used(new.mem_size());
                        *w = Some(Box::new(new));
                    }
                }
            },
        ),
    }
}

fn get_partial_buf_merger(
    dt: &DataType,
) -> Result<fn(&AggFirstIgnoresNull, &mut RefAccumStateRow, &mut RefAccumStateRow)> {
    macro_rules! fn_fixed {
        ($ty:ident) => {{
            Ok(|this, acc1, acc2| {
                type TType = paste! {[<$ty Type>]};
                type TNative = <TType as ArrowPrimitiveType>::Native;
                if !acc1.is_fixed_valid(this.accum_state_val_addr)
                    && acc2.is_fixed_valid(this.accum_state_val_addr)
                {
                    acc1.set_fixed_value(
                        this.accum_state_val_addr,
                        acc2.fixed_value::<TNative>(this.accum_state_val_addr),
                    );
                    acc1.set_fixed_valid(this.accum_state_val_addr, true);
                }
            })
        }};
    }
    match dt {
        DataType::Null => Ok(|_, _, _| ()),
        DataType::Boolean => Ok(|this, acc1, acc2| {
            if !acc1.is_fixed_valid(this.accum_state_val_addr)
                && acc2.is_fixed_valid(this.accum_state_val_addr)
            {
                acc1.set_fixed_value(
                    this.accum_state_val_addr,
                    acc2.fixed_value::<bool>(this.accum_state_val_addr),
                );
                acc1.set_fixed_valid(this.accum_state_val_addr, true);
            }
        }),
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
        DataType::Date32 => fn_fixed!(Date32),
        DataType::Date64 => fn_fixed!(Date64),
        DataType::Timestamp(TimeUnit::Second, _) => fn_fixed!(TimestampSecond),
        DataType::Timestamp(TimeUnit::Millisecond, _) => fn_fixed!(TimestampMillisecond),
        DataType::Timestamp(TimeUnit::Microsecond, _) => fn_fixed!(TimestampMicrosecond),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => fn_fixed!(TimestampNanosecond),
        DataType::Decimal128(..) => fn_fixed!(Decimal128),
        DataType::Utf8 | DataType::Binary | _ => Ok(|this, acc1, acc2| {
            let w = acc1.dyn_value_mut(this.accum_state_val_addr);
            let v = acc2.dyn_value_mut(this.accum_state_val_addr);
            if w.is_none() && v.is_some() {
                *w = std::mem::take(v);
            } else {
                if let Some(v) = v {
                    // v will be dropped
                    this.sub_mem_used(v.mem_size());
                }
            }
        }),
    }
}
