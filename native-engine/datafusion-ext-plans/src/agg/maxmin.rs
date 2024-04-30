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
    cmp::Ordering,
    fmt::{Debug, Formatter},
    marker::PhantomData,
    sync::{atomic::AtomicUsize, Arc},
};

use arrow::{array::*, datatypes::*};
use datafusion::{
    common::{Result, ScalarValue},
    physical_expr::PhysicalExpr,
};
use datafusion_ext_commons::{df_execution_err, downcast_any};
use paste::paste;

use crate::agg::{
    acc::{
        AccumInitialValue, AccumStateRow, AccumStateValAddr, AggDynBinary, AggDynScalar, AggDynStr,
        AggDynValue, RefAccumStateRow,
    },
    default_final_batch_merge_with_addr, default_final_merge_with_addr, Agg, WithAggBufAddrs,
    WithMemTracking,
};

pub type AggMax = AggMaxMin<AggMaxParams>;
pub type AggMin = AggMaxMin<AggMinParams>;

pub struct AggMaxMin<P: AggMaxMinParams> {
    child: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    accums_initial: Vec<AccumInitialValue>,
    accum_state_val_addr: AccumStateValAddr,
    partial_updater: fn(&AggMaxMin<P>, &mut RefAccumStateRow, &ArrayRef, usize),
    partial_batch_updater: fn(&AggMaxMin<P>, &mut [RefAccumStateRow], &ArrayRef),
    partial_buf_merger: fn(&AggMaxMin<P>, &mut RefAccumStateRow, &mut RefAccumStateRow),
    mem_used_tracker: AtomicUsize,
    _phantom: PhantomData<P>,
}

impl<P: AggMaxMinParams> WithAggBufAddrs for AggMaxMin<P> {
    fn set_accum_state_val_addrs(&mut self, accum_state_val_addrs: &[AccumStateValAddr]) {
        self.accum_state_val_addr = accum_state_val_addrs[0];
    }
}

impl<P: AggMaxMinParams> WithMemTracking for AggMaxMin<P> {
    fn mem_used_tracker(&self) -> &AtomicUsize {
        &self.mem_used_tracker
    }
}

impl<P: AggMaxMinParams> AggMaxMin<P> {
    pub fn try_new(child: Arc<dyn PhysicalExpr>, data_type: DataType) -> Result<Self> {
        let accums_initial = vec![AccumInitialValue::Scalar(ScalarValue::try_from(
            &data_type,
        )?)];
        let partial_updater = get_partial_updater::<P>(&data_type)?;
        let partial_batch_updater = get_partial_batch_updater::<P>(&data_type)?;
        let partial_buf_merger = get_partial_buf_merger::<P>(&data_type)?;
        Ok(Self {
            child,
            data_type,
            accums_initial,
            accum_state_val_addr: AccumStateValAddr::default(),
            partial_updater,
            partial_batch_updater,
            partial_buf_merger,
            mem_used_tracker: AtomicUsize::new(0),
            _phantom: Default::default(),
        })
    }
}

impl<P: AggMaxMinParams> Debug for AggMaxMin<P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({:?})", P::NAME, self.child)
    }
}

impl<P: AggMaxMinParams> Agg for AggMaxMin<P> {
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
        macro_rules! handle_fixed {
            ($ty:ident, $maxfun:expr) => {{
                type TArray = paste! {[<$ty Array>]};
                let value = values[0].as_any().downcast_ref::<TArray>().unwrap();
                if let Some(max) = $maxfun(value) {
                    partial_update_prim::<P, _>(acc, self.accum_state_val_addr, max);
                }
            }};
        }

        match values[0].data_type() {
            DataType::Null => {}
            DataType::Boolean => handle_fixed!(Boolean, P::maxmin_boolean),
            DataType::Float32 => handle_fixed!(Float32, P::maxmin),
            DataType::Float64 => handle_fixed!(Float64, P::maxmin),
            DataType::Int8 => handle_fixed!(Int8, P::maxmin),
            DataType::Int16 => handle_fixed!(Int16, P::maxmin),
            DataType::Int32 => handle_fixed!(Int32, P::maxmin),
            DataType::Int64 => handle_fixed!(Int64, P::maxmin),
            DataType::UInt8 => handle_fixed!(UInt8, P::maxmin),
            DataType::UInt16 => handle_fixed!(UInt16, P::maxmin),
            DataType::UInt32 => handle_fixed!(UInt32, P::maxmin),
            DataType::UInt64 => handle_fixed!(UInt64, P::maxmin),
            DataType::Date32 => handle_fixed!(Date32, P::maxmin),
            DataType::Date64 => handle_fixed!(Date64, P::maxmin),
            DataType::Timestamp(TimeUnit::Second, _) => {
                handle_fixed!(TimestampSecond, P::maxmin)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                handle_fixed!(TimestampMillisecond, P::maxmin)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                handle_fixed!(TimestampMicrosecond, P::maxmin)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                handle_fixed!(TimestampNanosecond, P::maxmin)
            }
            DataType::Decimal128(..) => handle_fixed!(Decimal128, P::maxmin),
            DataType::Utf8 => {
                let value = downcast_any!(values[0], StringArray)?;
                if let Some(max) = P::maxmin_string(value) {
                    match acc.dyn_value_mut(self.accum_state_val_addr) {
                        Some(w) => {
                            let w = downcast_any!(w.as_mut(), mut AggDynStr)?;
                            if max.partial_cmp(w.value()) == Some(P::ORD) {
                                *w = AggDynStr::from_str(max);
                            }
                        }
                        w @ None => {
                            *w = Some(Box::new(AggDynStr::from_str(max)));
                        }
                    }
                }
            }
            DataType::Binary => {
                let value = downcast_any!(values[0], BinaryArray)?;
                if let Some(max) = P::maxmin_binary(value) {
                    match acc.dyn_value_mut(self.accum_state_val_addr) {
                        Some(w) => {
                            let w = downcast_any!(w.as_mut(), mut AggDynBinary)?;
                            if max.partial_cmp(w.value()) == Some(P::ORD) {
                                *w = AggDynBinary::from_slice(max);
                            }
                        }
                        w @ None => {
                            *w = Some(Box::new(AggDynBinary::from_slice(max)));
                        }
                    }
                }
            }
            _ => {
                let scalars = (0..values[0].len())
                    .into_iter()
                    .map(|i| ScalarValue::try_from_array(&values[0], i))
                    .collect::<Result<Vec<_>>>()?;
                let max_scalar = if P::ORD == Ordering::Greater {
                    scalars
                        .into_iter()
                        .max_by(|v1, v2| v1.partial_cmp(v2).unwrap_or(Ordering::Equal))
                } else {
                    scalars
                        .into_iter()
                        .min_by(|v1, v2| v1.partial_cmp(v2).unwrap_or(Ordering::Equal))
                };

                if let Some(max_scalar) = max_scalar {
                    match acc.dyn_value_mut(self.accum_state_val_addr) {
                        Some(w) => {
                            let w = downcast_any!(w.as_mut(), mut AggDynScalar)?;
                            if max_scalar.partial_cmp(w.value()) == Some(P::ORD) {
                                *w = AggDynScalar::new(max_scalar);
                            }
                        }
                        w @ None => {
                            *w = Some(Box::new(AggDynScalar::new(max_scalar)));
                        }
                    }
                }
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

fn partial_update_prim<P: AggMaxMinParams, T: Copy + PartialEq + PartialOrd>(
    acc: &mut RefAccumStateRow,
    addr: AccumStateValAddr,
    v: T,
) {
    if acc.is_fixed_valid(addr) {
        acc.update_fixed_value::<T>(addr, |w| {
            if v.partial_cmp(&w) == Some(P::ORD) {
                v
            } else {
                w
            }
        });
    } else {
        acc.set_fixed_value::<T>(addr, v);
        acc.set_fixed_valid(addr, true);
    }
}

fn get_partial_updater<P: AggMaxMinParams>(
    dt: &DataType,
) -> Result<fn(&AggMaxMin<P>, &mut RefAccumStateRow, &ArrayRef, usize)> {
    macro_rules! fn_fixed {
        ($ty:ident) => {{
            Ok(|this, acc, v, i| {
                type TArray = paste! {[<$ty Array>]};
                let value = v.as_any().downcast_ref::<TArray>().unwrap();
                if value.is_valid(i) {
                    partial_update_prim::<P, _>(acc, this.accum_state_val_addr, value.value(i));
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
        DataType::Utf8 => Ok(|this, acc, v, i| {
            let value = downcast_any!(v, StringArray).unwrap();
            if value.is_valid(i) {
                let v = value.value(i);
                match acc.dyn_value_mut(this.accum_state_val_addr) {
                    Some(wv) => {
                        let wv = downcast_any!(wv, mut AggDynStr).unwrap();
                        if v.partial_cmp(wv.value()) == Some(P::ORD) {
                            this.add_mem_used(v.as_bytes().len());
                            *wv = AggDynStr::from_str(v);
                        }
                    }
                    w @ None => {
                        this.add_mem_used(v.as_bytes().len());
                        *w = Some(Box::new(AggDynStr::from_str(v)));
                    }
                }
            }
        }),
        DataType::Binary => Ok(|this, acc, v, i| {
            let value = downcast_any!(v, BinaryArray).unwrap();
            if value.is_valid(i) {
                let v = value.value(i);
                match acc.dyn_value_mut(this.accum_state_val_addr) {
                    Some(wv) => {
                        let wv = downcast_any!(wv, mut AggDynBinary).unwrap();
                        if v.partial_cmp(wv.value()) == Some(P::ORD) {
                            this.add_mem_used(v.len());
                            *wv = AggDynBinary::from_slice(v);
                        }
                    }
                    w @ None => {
                        this.add_mem_used(v.len());
                        *w = Some(Box::new(AggDynBinary::from_slice(v)));
                    }
                }
            }
        }),
        _ => Ok(|this, acc, v, i| {
            if v.is_valid(i) {
                let v = ScalarValue::try_from_array(v, i)
                    .expect(&format!("error cast to ScalarValue, dt={}", v.data_type()));
                match acc.dyn_value_mut(this.accum_state_val_addr) {
                    Some(wv) => {
                        let wv = downcast_any!(wv, mut AggDynScalar).unwrap();
                        if v.partial_cmp(wv.value()) == Some(P::ORD) {
                            this.add_mem_used(v.size());
                            *wv = AggDynScalar::new(v);
                        }
                    }
                    w @ None => {
                        this.add_mem_used(v.size());
                        *w = Some(Box::new(AggDynScalar::new(v)));
                    }
                }
            }
        }),
    }
}

fn get_partial_batch_updater<P: AggMaxMinParams>(
    dt: &DataType,
) -> Result<fn(&AggMaxMin<P>, &mut [RefAccumStateRow], &ArrayRef)> {
    macro_rules! fn_fixed {
        ($ty:ident) => {{
            Ok(|this, accs, v| {
                type TArray = paste! {[<$ty Array>]};
                let value = v.as_any().downcast_ref::<TArray>().unwrap();
                for (acc, value) in accs.iter_mut().zip(value.iter()) {
                    if let Some(value) = value {
                        partial_update_prim::<P, _>(acc, this.accum_state_val_addr, value);
                    }
                }
            })
        }};
    }
    match dt {
        DataType::Null => Ok(|_, _, _| ()),
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
        DataType::Utf8 => Ok(|this, accs, v| {
            let value = v.as_any().downcast_ref::<StringArray>().unwrap();
            for (acc, v) in accs.iter_mut().zip(value.iter()) {
                if let Some(v) = v {
                    match acc.dyn_value_mut(this.accum_state_val_addr) {
                        Some(wv) => {
                            let wv = downcast_any!(wv, mut AggDynStr).unwrap();
                            if v.partial_cmp(wv.value()) == Some(P::ORD) {
                                this.add_mem_used(v.as_bytes().len());
                                *wv = AggDynStr::from_str(v);
                            }
                        }
                        w @ None => {
                            *w = Some(Box::new(AggDynStr::from_str(v)));
                        }
                    }
                }
            }
        }),
        DataType::Binary => Ok(|this, accs, v| {
            let value = v.as_any().downcast_ref::<BinaryArray>().unwrap();
            for (acc, v) in accs.iter_mut().zip(value.iter()) {
                if let Some(v) = v {
                    match acc.dyn_value_mut(this.accum_state_val_addr) {
                        Some(wv) => {
                            let wv = downcast_any!(wv, mut AggDynBinary).unwrap();
                            if v.partial_cmp(wv.value()) == Some(P::ORD) {
                                this.add_mem_used(v.len());
                                *wv = AggDynBinary::from_slice(v);
                            }
                        }
                        w @ None => {
                            *w = Some(Box::new(AggDynBinary::from_slice(v)));
                        }
                    }
                }
            }
        }),
        _ => Ok(|this, accs, v| {
            for (row_idx, acc) in accs.iter_mut().enumerate() {
                let v = ScalarValue::try_from_array(v, row_idx)
                    .expect(&format!("error cast to ScalarValue, dt={}", v.data_type()));
                if !v.is_null() {
                    match acc.dyn_value_mut(this.accum_state_val_addr) {
                        Some(wv) => {
                            let wv = downcast_any!(wv, mut AggDynScalar).unwrap();
                            if v.partial_cmp(wv.value()) == Some(P::ORD) {
                                this.add_mem_used(v.size());
                                *wv = AggDynScalar::new(v);
                            }
                        }
                        w @ None => {
                            *w = Some(Box::new(AggDynScalar::new(v)));
                        }
                    }
                }
            }
        }),
    }
}

fn get_partial_buf_merger<P: AggMaxMinParams>(
    dt: &DataType,
) -> Result<fn(&AggMaxMin<P>, &mut RefAccumStateRow, &mut RefAccumStateRow)> {
    macro_rules! fn_fixed {
        ($ty:ident) => {{
            Ok(|this, acc1, acc2| {
                type TType = paste! {[<$ty Type>]};
                type TNative = <TType as ArrowPrimitiveType>::Native;
                if acc2.is_fixed_valid(this.accum_state_val_addr) {
                    let v = acc2.fixed_value::<TNative>(this.accum_state_val_addr);
                    partial_update_prim::<P, _>(acc1, this.accum_state_val_addr, v);
                }
            })
        }};
    }
    match dt {
        DataType::Null => Ok(|_, _, _| ()),
        DataType::Boolean => Ok(|this, acc1, acc2| {
            if acc2.is_fixed_valid(this.accum_state_val_addr) {
                let v = acc2.fixed_value::<bool>(this.accum_state_val_addr);
                partial_update_prim::<P, _>(acc1, this.accum_state_val_addr, v);
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
        DataType::Utf8 => Ok(|this, acc1, acc2| {
            let s1 = acc1.dyn_value_mut(this.accum_state_val_addr);
            let s2 = std::mem::take(acc2.dyn_value_mut(this.accum_state_val_addr));
            match (s1, s2) {
                (Some(w), Some(v)) => {
                    let w = downcast_any!(w, mut AggDynStr).unwrap();
                    let v = v
                        .as_any_boxed()
                        .downcast::<AggDynStr>()
                        .or_else(|_| df_execution_err!("error downcasting to AggSynStr"))
                        .unwrap();
                    if v.value().partial_cmp(w.value()) == Some(P::ORD) {
                        this.sub_mem_used(w.mem_size()); // w will be dropped
                        *w = AggDynStr::new(v.into_value());
                    } else {
                        this.sub_mem_used(v.mem_size()); // v will be dropped
                    }
                }
                (Some(_), None) => {}
                (s1 @ None, s2 @ _) => *s1 = s2,
            }
        }),
        DataType::Binary => Ok(|this, acc1, acc2| {
            let s1 = acc1.dyn_value_mut(this.accum_state_val_addr);
            let s2 = std::mem::take(acc2.dyn_value_mut(this.accum_state_val_addr));
            match (s1, s2) {
                (Some(w), Some(v)) => {
                    let w = downcast_any!(w, mut AggDynBinary).unwrap();
                    let v = v
                        .as_any_boxed()
                        .downcast::<AggDynBinary>()
                        .or_else(|_| df_execution_err!("error downcasting to AggSynBinary"))
                        .unwrap();
                    if v.value().partial_cmp(w.value()) == Some(P::ORD) {
                        this.sub_mem_used(w.mem_size()); // w will be dropped
                        *w = AggDynBinary::new(v.into_value());
                    } else {
                        this.sub_mem_used(v.mem_size()); // v will be dropped
                    }
                }
                (Some(_), None) => {}
                (s1 @ None, s2 @ _) => *s1 = s2,
            }
        }),
        _ => Ok(|this, acc1, acc2| {
            let s1 = acc1.dyn_value_mut(this.accum_state_val_addr);
            let s2 = std::mem::take(acc2.dyn_value_mut(this.accum_state_val_addr));
            match (s1, s2) {
                (Some(w), Some(v)) => {
                    let w = downcast_any!(w, mut AggDynScalar).unwrap();
                    let v = v
                        .as_any_boxed()
                        .downcast::<AggDynScalar>()
                        .or_else(|_| df_execution_err!("error downcasting to AggSynBinary"))
                        .unwrap();
                    if v.value().partial_cmp(w.value()) == Some(P::ORD) {
                        this.sub_mem_used(w.mem_size()); // w will be dropped
                        *w = AggDynScalar::new(v.into_value());
                    } else {
                        this.sub_mem_used(v.mem_size()); // v will be dropped
                    }
                }
                (Some(_), None) => {}
                (s1 @ None, s2 @ _) => *s1 = s2,
            }
        }),
    }
}

pub trait AggMaxMinParams: 'static + Send + Sync {
    const NAME: &'static str;
    const ORD: Ordering;

    fn maxmin_boolean(v: &BooleanArray) -> Option<bool>;
    fn maxmin<T>(array: &PrimitiveArray<T>) -> Option<<T as ArrowPrimitiveType>::Native>
    where
        T: ArrowNumericType,
        <T as ArrowPrimitiveType>::Native: ArrowNativeType;

    fn maxmin_string<T>(array: &GenericByteArray<GenericStringType<T>>) -> Option<&str>
    where
        T: OffsetSizeTrait;

    fn maxmin_binary<T>(array: &GenericByteArray<GenericBinaryType<T>>) -> Option<&[u8]>
    where
        T: OffsetSizeTrait;
}

pub struct AggMaxParams;
pub struct AggMinParams;

impl AggMaxMinParams for AggMaxParams {
    const NAME: &'static str = "max";
    const ORD: Ordering = Ordering::Greater;

    fn maxmin_boolean(v: &BooleanArray) -> Option<bool> {
        arrow::compute::max_boolean(v)
    }

    fn maxmin<T>(array: &PrimitiveArray<T>) -> Option<<T as ArrowPrimitiveType>::Native>
    where
        T: ArrowNumericType,
        <T as ArrowPrimitiveType>::Native: ArrowNativeType,
    {
        match array.data_type() {
            DataType::Float32 => array
                .iter()
                .flatten()
                .max_by(|a, b| a.partial_cmp(b).unwrap()),
            DataType::Float64 => array
                .iter()
                .flatten()
                .max_by(|a, b| a.partial_cmp(b).unwrap()),
            _ => arrow::compute::max(array),
        }
    }

    fn maxmin_string<T>(array: &GenericByteArray<GenericStringType<T>>) -> Option<&str>
    where
        T: OffsetSizeTrait,
    {
        arrow::compute::max_string(array)
    }

    fn maxmin_binary<T>(array: &GenericByteArray<GenericBinaryType<T>>) -> Option<&[u8]>
    where
        T: OffsetSizeTrait,
    {
        arrow::compute::max_binary(array)
    }
}

impl AggMaxMinParams for AggMinParams {
    const NAME: &'static str = "min";
    const ORD: Ordering = Ordering::Less;

    fn maxmin_boolean(v: &BooleanArray) -> Option<bool> {
        arrow::compute::min_boolean(v)
    }

    fn maxmin<T>(array: &PrimitiveArray<T>) -> Option<<T as ArrowPrimitiveType>::Native>
    where
        T: ArrowNumericType,
        <T as ArrowPrimitiveType>::Native: ArrowNativeType,
    {
        match array.data_type() {
            DataType::Float32 => array
                .iter()
                .flatten()
                .min_by(|a, b| a.partial_cmp(b).unwrap()),
            DataType::Float64 => array
                .iter()
                .flatten()
                .min_by(|a, b| a.partial_cmp(b).unwrap()),
            _ => arrow::compute::min(array),
        }
    }

    fn maxmin_string<T>(array: &GenericByteArray<GenericStringType<T>>) -> Option<&str>
    where
        T: OffsetSizeTrait,
    {
        arrow::compute::min_string(array)
    }

    fn maxmin_binary<T>(array: &GenericByteArray<GenericBinaryType<T>>) -> Option<&[u8]>
    where
        T: OffsetSizeTrait,
    {
        arrow::compute::min_binary(array)
    }
}
