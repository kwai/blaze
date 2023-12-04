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
use paste::paste;

use crate::agg::{
    agg_buf::{AccumInitialValue, AggBuf, AggDynBinary, AggDynScalar, AggDynStr},
    Agg,
};

pub struct AggFirstIgnoresNull {
    child: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    accums_initial: Vec<AccumInitialValue>,
    partial_updater: fn(&mut AggBuf, u64, &ArrayRef, usize),
    partial_buf_merger: fn(&mut AggBuf, &mut AggBuf, u64),
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
            partial_updater,
            partial_buf_merger,
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

    fn partial_update(
        &self,
        agg_buf: &mut AggBuf,
        agg_buf_addrs: &[u64],
        values: &[ArrayRef],
        row_idx: usize,
    ) -> Result<()> {
        let partial_updater = self.partial_updater;
        let addr = agg_buf_addrs[0];
        let value = &values[0];
        partial_updater(agg_buf, addr, value, row_idx);
        Ok(())
    }

    fn partial_update_all(
        &self,
        agg_buf: &mut AggBuf,
        agg_buf_addrs: &[u64],
        values: &[ArrayRef],
    ) -> Result<()> {
        let partial_updater = self.partial_updater;
        let addr = agg_buf_addrs[0];
        let value = &values[0];

        for i in 0..value.len() {
            if value.is_valid(i) {
                partial_updater(agg_buf, addr, value, i);
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
}

fn get_partial_updater(dt: &DataType) -> Result<fn(&mut AggBuf, u64, &ArrayRef, usize)> {
    macro_rules! fn_fixed {
        ($ty:ident) => {{
            Ok(|agg_buf, addr, v, i| {
                if !agg_buf.is_fixed_valid(addr) && v.is_valid(i) {
                    let value = v.as_any().downcast_ref::<paste! {[<$ty Array>]}>().unwrap();
                    agg_buf.set_fixed_value(addr, value.value(i));
                    agg_buf.set_fixed_valid(addr, true);
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
        DataType::Utf8 => Ok(|agg_buf: &mut AggBuf, addr: u64, v: &ArrayRef, i: usize| {
            let w = AggDynStr::value_mut(agg_buf.dyn_value_mut(addr));
            if w.is_none() && v.is_valid(i) {
                let value = v.as_any().downcast_ref::<StringArray>().unwrap();
                *w = Some(value.value(i).to_owned().into());
            }
        }),
        DataType::Binary => Ok(|agg_buf: &mut AggBuf, addr: u64, v: &ArrayRef, i: usize| {
            let w = AggDynBinary::value_mut(agg_buf.dyn_value_mut(addr));
            if w.is_none() && v.is_valid(i) {
                let value = v.as_any().downcast_ref::<BinaryArray>().unwrap();
                *w = Some(value.value(i).to_owned().into());
            }
        }),
        _other => Ok(|agg_buf: &mut AggBuf, addr: u64, v: &ArrayRef, i: usize| {
            let w = AggDynScalar::value_mut(agg_buf.dyn_value_mut(addr));
            if w.is_null() && v.is_valid(i) {
                *w = ScalarValue::try_from_array(v, i)
                    .expect("FirstIgnoresNull::partial_update error creating ScalarValue");
            }
        }),
    }
}

fn get_partial_buf_merger(dt: &DataType) -> Result<fn(&mut AggBuf, &mut AggBuf, u64)> {
    macro_rules! fn_fixed {
        ($ty:ident) => {{
            Ok(|agg_buf1, agg_buf2, addr| {
                type TType = paste! {[<$ty Type>]};
                type TNative = <TType as ArrowPrimitiveType>::Native;
                if !agg_buf1.is_fixed_valid(addr) && agg_buf2.is_fixed_valid(addr) {
                    agg_buf1.set_fixed_value(addr, agg_buf2.fixed_value::<TNative>(addr));
                    agg_buf1.set_fixed_valid(addr, true);
                }
            })
        }};
    }
    match dt {
        DataType::Null => Ok(|_, _, _| ()),
        DataType::Boolean => Ok(|agg_buf1, agg_buf2, addr| {
            if !agg_buf1.is_fixed_valid(addr) && agg_buf2.is_fixed_valid(addr) {
                agg_buf1.set_fixed_value(addr, agg_buf2.fixed_value::<bool>(addr));
                agg_buf1.set_fixed_valid(addr, true);
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
        DataType::Utf8 => Ok(|agg_buf1, agg_buf2, addr| {
            let w = AggDynStr::value_mut(agg_buf1.dyn_value_mut(addr));
            if w.is_none() {
                *w = std::mem::take(AggDynStr::value_mut(agg_buf2.dyn_value_mut(addr)));
            }
        }),
        DataType::Binary => Ok(|agg_buf1, agg_buf2, addr| {
            let w = AggDynBinary::value_mut(agg_buf1.dyn_value_mut(addr));
            if w.is_none() {
                *w = std::mem::take(AggDynBinary::value_mut(agg_buf2.dyn_value_mut(addr)));
            }
        }),
        _other => Ok(|agg_buf1, agg_buf2, addr| {
            let w = AggDynScalar::value_mut(agg_buf1.dyn_value_mut(addr));
            if w.is_null() {
                *w = std::mem::replace(
                    AggDynScalar::value_mut(agg_buf2.dyn_value_mut(addr)),
                    ScalarValue::Null,
                );
            }
        }),
    }
}
