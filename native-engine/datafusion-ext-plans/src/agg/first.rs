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

use crate::agg::agg_buf::{AccumInitialValue, AggBuf, AggDynBinary, AggDynScalar, AggDynStr};
use crate::agg::Agg;
use arrow::array::*;
use arrow::datatypes::*;
use datafusion::common::{Result, ScalarValue};
use datafusion::physical_expr::PhysicalExpr;
use paste::paste;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub struct AggFirst {
    child: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    accums_initial: Vec<AccumInitialValue>,
    partial_updater: fn(&mut AggBuf, &[u64], &ArrayRef, usize),
    partial_buf_merger: fn(&mut AggBuf, &mut AggBuf, &[u64]),
}

impl AggFirst {
    pub fn try_new(child: Arc<dyn PhysicalExpr>, data_type: DataType) -> Result<Self> {
        let accums_initial = vec![
            AccumInitialValue::Scalar(ScalarValue::try_from(&data_type)?),
            AccumInitialValue::Scalar(ScalarValue::Null), // touched
        ];
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

impl Debug for AggFirst {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "First({:?})", self.child)
    }
}

impl Agg for AggFirst {
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
        if !is_touched(agg_buf, agg_buf_addrs) {
            let partial_updater = self.partial_updater;
            partial_updater(agg_buf, agg_buf_addrs, &values[0], row_idx);
        }
        Ok(())
    }

    fn partial_update_all(
        &self,
        agg_buf: &mut AggBuf,
        agg_buf_addrs: &[u64],
        values: &[ArrayRef],
    ) -> Result<()> {
        if !is_touched(agg_buf, agg_buf_addrs) {
            let value = &values[0];
            if !value.is_empty() {
                let partial_updater = self.partial_updater;
                partial_updater(agg_buf, &agg_buf_addrs, value, 0);
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
        partial_buf_merger(agg_buf1, agg_buf2, agg_buf_addrs);
        Ok(())
    }
}

fn is_touched(agg_buf: &AggBuf, agg_buf_addrs: &[u64]) -> bool {
    agg_buf.is_fixed_valid(agg_buf_addrs[1])
}

fn set_touched(agg_buf: &mut AggBuf, agg_buf_addrs: &[u64]) {
    agg_buf.set_fixed_valid(agg_buf_addrs[1], true)
}

fn get_partial_updater(dt: &DataType) -> Result<fn(&mut AggBuf, &[u64], &ArrayRef, usize)> {
    // assert!(!is_touched(agg_buf, addrs))

    macro_rules! fn_fixed {
        ($ty:ident) => {{
            Ok(|agg_buf, addrs, v, i| {
                type TArray = paste! {[<$ty Array>]};
                if v.is_valid(i) {
                    let value = v.as_any().downcast_ref::<TArray>().unwrap();
                    agg_buf.set_fixed_value(addrs[0], value.value(i));
                    agg_buf.set_fixed_valid(addrs[0], true);
                }
                set_touched(agg_buf, addrs);
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
        DataType::Utf8 => Ok(
            |agg_buf: &mut AggBuf, addrs: &[u64], v: &ArrayRef, i: usize| {
                let w = AggDynStr::value_mut(agg_buf.dyn_value_mut(addrs[0]));
                if v.is_valid(i) {
                    let value = v.as_any().downcast_ref::<StringArray>().unwrap();
                    *w = Some(value.value(i).to_owned().into());
                }
                set_touched(agg_buf, addrs);
            },
        ),
        DataType::Binary => Ok(
            |agg_buf: &mut AggBuf, addrs: &[u64], v: &ArrayRef, i: usize| {
                let w = AggDynBinary::value_mut(agg_buf.dyn_value_mut(addrs[0]));
                if v.is_valid(i) {
                    let value = v.as_any().downcast_ref::<BinaryArray>().unwrap();
                    *w = Some(value.value(i).to_owned().into());
                }
                set_touched(agg_buf, addrs);
            },
        ),
        _other => Ok(
            |agg_buf: &mut AggBuf, addrs: &[u64], v: &ArrayRef, i: usize| {
                let w = AggDynScalar::value_mut(agg_buf.dyn_value_mut(addrs[0]));
                *w = ScalarValue::try_from_array(v, i)
                    .expect("First::partial_update error creating ScalarValue");
                set_touched(agg_buf, addrs);
            },
        ),
    }
}

fn get_partial_buf_merger(dt: &DataType) -> Result<fn(&mut AggBuf, &mut AggBuf, &[u64])> {
    // assert!(!is_touched(agg_buf, addrs))

    macro_rules! fn_fixed {
        ($ty:ident) => {{
            Ok(|agg_buf1, agg_buf2, addrs| {
                type TType = paste! {[<$ty Type>]};
                type TNative = <TType as ArrowPrimitiveType>::Native;
                if is_touched(agg_buf2, addrs) {
                    if agg_buf2.is_fixed_valid(addrs[0]) {
                        let value2 = agg_buf2.fixed_value::<TNative>(addrs[0]);
                        agg_buf1.set_fixed_value(addrs[0], value2);
                        agg_buf1.set_fixed_valid(addrs[0], true);
                    }
                    set_touched(agg_buf1, addrs);
                }
            })
        }};
    }
    match dt {
        DataType::Null => Ok(|_, _, _| ()),
        DataType::Boolean => Ok(|agg_buf1, agg_buf2, addrs| {
            if is_touched(agg_buf2, addrs) {
                if agg_buf2.is_fixed_valid(addrs[0]) {
                    agg_buf1.set_fixed_value(addrs[0], agg_buf2.fixed_value::<bool>(addrs[0]));
                    agg_buf1.set_fixed_valid(addrs[0], true);
                }
                set_touched(agg_buf1, addrs);
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
        DataType::Utf8 => Ok(|agg_buf1, agg_buf2, addrs| {
            if is_touched(agg_buf2, addrs) {
                let w = AggDynStr::value_mut(agg_buf1.dyn_value_mut(addrs[0]));
                *w = std::mem::take(AggDynStr::value_mut(agg_buf2.dyn_value_mut(addrs[0])));
                set_touched(agg_buf1, addrs);
            }
        }),
        DataType::Binary => Ok(|agg_buf1, agg_buf2, addrs| {
            if is_touched(agg_buf2, addrs) {
                let w = AggDynBinary::value_mut(agg_buf1.dyn_value_mut(addrs[0]));
                *w = std::mem::take(AggDynBinary::value_mut(agg_buf2.dyn_value_mut(addrs[0])));
                set_touched(agg_buf1, addrs);
            }
        }),
        _other => Ok(|agg_buf1, agg_buf2, addrs| {
            if is_touched(agg_buf2, addrs) {
                let w = AggDynScalar::value_mut(agg_buf1.dyn_value_mut(addrs[0]));
                *w = std::mem::replace(
                    AggDynScalar::value_mut(agg_buf2.dyn_value_mut(addrs[0])),
                    ScalarValue::Null,
                );
                set_touched(agg_buf1, addrs);
            }
        }),
    }
}
