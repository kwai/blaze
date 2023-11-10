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

use crate::agg::agg_buf::{AccumInitialValue, AggBuf, AggDynStr};
use crate::agg::Agg;
use arrow::array::*;
use arrow::datatypes::*;
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::physical_expr::PhysicalExpr;
use paste::paste;
use std::any::Any;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;

pub type AggMax = AggMaxMin<AggMaxParams>;
pub type AggMin = AggMaxMin<AggMinParams>;

pub struct AggMaxMin<P: AggMaxMinParams> {
    child: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    accums_initial: Vec<AccumInitialValue>,
    partial_updater: fn(&mut AggBuf, u64, &ArrayRef, usize),
    partial_batch_updater: fn(&mut [AggBuf], u64, &ArrayRef),
    partial_buf_merger: fn(&mut AggBuf, &mut AggBuf, u64),
    _phantom: PhantomData<P>,
}

impl<P: AggMaxMinParams> AggMaxMin<P> {
    pub fn try_new(child: Arc<dyn PhysicalExpr>, data_type: DataType) -> Result<Self> {
        let accums_initial = vec![AccumInitialValue::Scalar(ScalarValue::try_from(&data_type)?)];
        let partial_updater = get_partial_updater::<P>(&data_type)?;
        let partial_batch_updater = get_partial_batch_updater::<P>(&data_type)?;
        let partial_buf_merger = get_partial_buf_merger::<P>(&data_type)?;
        Ok(Self {
            child,
            data_type,
            accums_initial,
            partial_updater,
            partial_batch_updater,
            partial_buf_merger,
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

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn nullable(&self) -> bool {
        true
    }

    fn accums_initial(&self) -> &[AccumInitialValue] {
        &self.accums_initial
    }

    fn partial_update(
        &self,
        agg_buf: &mut AggBuf,
        agg_buf_addrs: &[u64],
        values: &[ArrayRef],
        row_idx: usize,
    ) -> Result<()> {
        let partial_updater = self.partial_updater;
        let addr = agg_buf_addrs[0];
        partial_updater(agg_buf, addr, &values[0], row_idx);
        Ok(())
    }

    fn partial_batch_update(
        &self,
        agg_bufs: &mut [AggBuf],
        agg_buf_addrs: &[u64],
        values: &[ArrayRef],
    ) -> Result<usize> {
        let partial_batch_updater = self.partial_batch_updater;
        let addr = agg_buf_addrs[0];

        if self.data_type.is_primitive()
            || matches!(self.data_type, DataType::Null | DataType::Boolean)
        {
            partial_batch_updater(agg_bufs, addr, &values[0]);
            Ok(0)
        } else {
            let mem_before = agg_bufs
                .iter()
                .map(|agg_buf| agg_buf.mem_size())
                .sum::<usize>();
            partial_batch_updater(agg_bufs, addr, &values[0]);
            let mem_after = agg_bufs
                .iter()
                .map(|agg_buf| agg_buf.mem_size())
                .sum::<usize>();
            Ok(mem_after - mem_before)
        }
    }

    fn partial_update_all(
        &self,
        agg_buf: &mut AggBuf,
        agg_buf_addrs: &[u64],
        values: &[ArrayRef],
    ) -> Result<()> {
        let addr = agg_buf_addrs[0];

        macro_rules! handle_fixed {
            ($ty:ident, $maxfun:expr) => {{
                type TArray = paste! {[<$ty Array>]};
                let value = values[0].as_any().downcast_ref::<TArray>().unwrap();
                if let Some(max) = $maxfun(value) {
                    partial_update_prim::<P, _>(agg_buf, addr, max);
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
            DataType::Decimal128(_, _) => handle_fixed!(Decimal128, P::maxmin),
            DataType::Utf8 => {
                let value = values[0].as_any().downcast_ref::<StringArray>().unwrap();
                if let Some(max) = P::maxmin_string(value) {
                    let w = AggDynStr::value_mut(agg_buf.dyn_value_mut(addr));
                    match w {
                        Some(w) => {
                            if w.as_ref() < max {
                                *w = max.to_owned().into();
                            }
                        }
                        w @ None => {
                            *w = Some(max.to_owned().into());
                        }
                    }
                }
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "unsupported data type in {}(): {other}",
                    P::NAME,
                )));
            }
        }
        Ok(())
    }

    fn partial_merge(
        &self,
        agg_buf1: &mut AggBuf,
        agg_buf2: &mut AggBuf,
        agg_buf_addrs: &[u64],
    ) -> Result<()> {
        let partial_buf_merger = self.partial_buf_merger;
        let addr = agg_buf_addrs[0];
        partial_buf_merger(agg_buf1, agg_buf2, addr);
        Ok(())
    }

    fn partial_batch_merge(
        &self,
        agg_bufs: &mut [AggBuf],
        merging_agg_bufs: &mut [AggBuf],
        agg_buf_addrs: &[u64],
    ) -> Result<usize> {
        let partial_buf_merger = self.partial_buf_merger;
        let addr = agg_buf_addrs[0];
        let mut mem_diff = 0;

        if self.data_type.is_primitive()
            || matches!(self.data_type, DataType::Null | DataType::Boolean)
        {
            for (agg_buf, merging_agg_buf) in agg_bufs.iter_mut().zip(merging_agg_bufs) {
                partial_buf_merger(agg_buf, merging_agg_buf, addr);
            }
        } else {
            for (agg_buf, merging_agg_buf) in agg_bufs.iter_mut().zip(merging_agg_bufs) {
                mem_diff -= agg_buf.mem_size();
                partial_buf_merger(agg_buf, merging_agg_buf, addr);
                mem_diff += agg_buf.mem_size();
            }
        }
        Ok(mem_diff)
    }
}

fn partial_update_prim<P: AggMaxMinParams, T: Copy + PartialEq + PartialOrd>(
    agg_buf: &mut AggBuf,
    addr: u64,
    v: T,
) {
    if agg_buf.is_fixed_valid(addr) {
        agg_buf.update_fixed_value::<T>(addr, |w| {
            if v.partial_cmp(&w) == Some(P::ORD) {
                v
            } else {
                w
            }
        });
    } else {
        agg_buf.set_fixed_value::<T>(addr, v);
        agg_buf.set_fixed_valid(addr, true);
    }
}

fn get_partial_updater<P: AggMaxMinParams>(
    dt: &DataType,
) -> Result<fn(&mut AggBuf, u64, &ArrayRef, usize)> {
    macro_rules! fn_fixed {
        ($ty:ident) => {{
            Ok(|agg_buf, addr, v, i| {
                type TArray = paste! {[<$ty Array>]};
                let value = v.as_any().downcast_ref::<TArray>().unwrap();
                if value.is_valid(i) {
                    partial_update_prim::<P, _>(agg_buf, addr, value.value(i));
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
        DataType::Decimal128(_, _) => fn_fixed!(Decimal128),
        DataType::Utf8 => Ok(|agg_buf: &mut AggBuf, addr: u64, v: &ArrayRef, i: usize| {
            let value = v.as_any().downcast_ref::<StringArray>().unwrap();
            if value.is_valid(i) {
                let w = AggDynStr::value_mut(agg_buf.dyn_value_mut(addr));
                let v = value.value(i);
                match w {
                    None => *w = Some(v.to_owned().into()),
                    Some(wv) => {
                        if v.partial_cmp(wv.as_ref()) == Some(P::ORD) {
                            *w = Some(v.to_owned().into());
                        }
                    }
                }
            }
        }),
        other => Err(DataFusionError::NotImplemented(format!(
            "unsupported data type in {}(): {other}",
            P::NAME,
        ))),
    }
}

fn get_partial_batch_updater<P: AggMaxMinParams>(
    dt: &DataType,
) -> Result<fn(&mut [AggBuf], u64, &ArrayRef)> {
    macro_rules! fn_fixed {
        ($ty:ident) => {{
            Ok(|agg_bufs, addr, v| {
                type TArray = paste! {[<$ty Array>]};
                let value = v.as_any().downcast_ref::<TArray>().unwrap();
                for (agg_buf, value) in agg_bufs.iter_mut().zip(value.iter()) {
                    if let Some(value) = value {
                        partial_update_prim::<P, _>(agg_buf, addr, value);
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
        DataType::Decimal128(_, _) => fn_fixed!(Decimal128),
        DataType::Utf8 => Ok(|agg_bufs: &mut [AggBuf], addr: u64, v: &ArrayRef| {
            let value = v.as_any().downcast_ref::<StringArray>().unwrap();
            for (agg_buf, v) in agg_bufs.iter_mut().zip(value.iter()) {
                if let Some(v) = v {
                    let w = AggDynStr::value_mut(agg_buf.dyn_value_mut(addr));
                    match w {
                        None => *w = Some(v.to_owned().into()),
                        Some(wv) => {
                            if v.partial_cmp(wv.as_ref()) == Some(P::ORD) {
                                *w = Some(v.to_owned().into());
                            }
                        }
                    }
                }
            }
        }),
        other => Err(DataFusionError::NotImplemented(format!(
            "unsupported data type in {}(): {other}",
            P::NAME,
        ))),
    }
}

fn get_partial_buf_merger<P: AggMaxMinParams>(
    dt: &DataType,
) -> Result<fn(&mut AggBuf, &mut AggBuf, u64)> {
    macro_rules! fn_fixed {
        ($ty:ident) => {{
            Ok(|agg_buf1, agg_buf2, addr| {
                type TType = paste! {[<$ty Type>]};
                type TNative = <TType as ArrowPrimitiveType>::Native;
                if agg_buf2.is_fixed_valid(addr) {
                    let v = agg_buf2.fixed_value::<TNative>(addr);
                    partial_update_prim::<P, _>(agg_buf1, addr, v);
                }
            })
        }};
    }
    match dt {
        DataType::Null => Ok(|_, _, _| ()),
        DataType::Boolean => Ok(|agg_buf1, agg_buf2, addr| {
            if agg_buf2.is_fixed_valid(addr) {
                let v = agg_buf2.fixed_value::<bool>(addr);
                partial_update_prim::<P, _>(agg_buf1, addr, v);
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
        DataType::Decimal128(_, _) => fn_fixed!(Decimal128),
        DataType::Utf8 => Ok(|agg_buf1, agg_buf2, addr| {
            let v = AggDynStr::value(agg_buf2.dyn_value_mut(addr));
            if v.is_some() {
                let w = AggDynStr::value_mut(agg_buf1.dyn_value_mut(addr));
                let v = v.as_ref().unwrap();
                if w.as_ref().filter(|w| w.as_ref() >= v.as_ref()).is_none() {
                    *w = Some(v.to_owned());
                }
            }
        }),
        other => Err(DataFusionError::NotImplemented(format!(
            "unsupported data type in {}(): {other}",
            P::NAME,
        ))),
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
        arrow::compute::max(array)
    }

    fn maxmin_string<T>(array: &GenericByteArray<GenericStringType<T>>) -> Option<&str>
    where
        T: OffsetSizeTrait,
    {
        arrow::compute::max_string(array)
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
        arrow::compute::min(array)
    }

    fn maxmin_string<T>(array: &GenericByteArray<GenericStringType<T>>) -> Option<&str>
    where
        T: OffsetSizeTrait,
    {
        arrow::compute::min_string(array)
    }
}
