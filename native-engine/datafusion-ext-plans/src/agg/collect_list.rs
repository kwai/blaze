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

use crate::agg::agg_buf::{AggBuf, AggDynList, AggDynStrList};
use crate::agg::Agg;
use arrow::array::*;
use arrow::datatypes::*;
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::physical_expr::PhysicalExpr;
use paste::paste;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub struct AggCollectList {
    child: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    arg_type: DataType,
    accums_initial: Vec<ScalarValue>,
    partial_updater: fn(&mut AggBuf, u64, &ArrayRef, usize),
    partial_buf_merger: fn(&mut AggBuf, &mut AggBuf, u64),
}

impl AggCollectList {
    pub fn try_new(child: Arc<dyn PhysicalExpr>, data_type: DataType, arg_type: DataType) -> Result<Self> {
        let field = Arc::new(Field::new("collect_list",arg_type.clone(), false));
        let accums_initial = vec![ScalarValue::List(None, field)];
        let partial_updater = get_partial_updater(&arg_type)?;
        let partial_buf_merger = get_partial_buf_merger(&arg_type)?;
        Ok(Self {
            child,
            data_type,
            arg_type,
            accums_initial,
            partial_updater,
            partial_buf_merger,
        })
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

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn nullable(&self) -> bool {
        true
    }

    fn accums_initial(&self) -> &[ScalarValue] {
        &self.accums_initial
    }

    fn partial_update(&self, agg_buf: &mut AggBuf, agg_buf_addrs: &[u64], values: &[ArrayRef], row_idx: usize) -> Result<()> {
        let partial_updater = self.partial_updater;
        let addr = agg_buf_addrs[0];
        partial_updater(agg_buf, addr, &values[0], row_idx);
        Ok(())
    }

    fn partial_update_all(&self, agg_buf: &mut AggBuf, agg_buf_addrs: &[u64], values: &[ArrayRef]) -> Result<()> {
        let addr = agg_buf_addrs[0];
        macro_rules! handle_fixed {
            ($ty:ident) => {{
                type TType = paste! {[<$ty Type>]};
                type TArray = paste! {[<$ty Array>]};
                type TNative = <TType as ArrowPrimitiveType>::Native;
                let value = values[0].as_any().downcast_ref::<TArray>().unwrap();
                let w = agg_buf
                    .dyn_value_mut(addr)
                    .as_any_mut()
                    .downcast_mut::<AggDynList<TNative>>()
                    .unwrap();
                for v in value.iter().flatten() {
                    w.append(v);
                }
            }};
        }

        match values[0].data_type() {
            DataType::Float32 => handle_fixed!(Float32),
            DataType::Float64 => handle_fixed!(Float64),
            DataType::Int8 => handle_fixed!(Int8),
            DataType::Int16 => handle_fixed!(Int16),
            DataType::Int32 => handle_fixed!(Int32),
            DataType::Int64 => handle_fixed!(Int64),
            DataType::UInt8 => handle_fixed!(UInt8),
            DataType::UInt16 => handle_fixed!(UInt16),
            DataType::UInt32 => handle_fixed!(UInt32),
            DataType::UInt64 => handle_fixed!(UInt64),
            DataType::Decimal128(_, _) => handle_fixed!(Decimal128),
            DataType::Utf8 => {
                let value = values[0].as_any().downcast_ref::<StringArray>().unwrap();
                let w = agg_buf
                    .dyn_value_mut(addr)
                    .as_any_mut()
                    .downcast_mut::<AggDynStrList>()
                    .unwrap();
                for v in value.iter().flatten() {
                    w.append(v);
                }
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "unsupported data type in collect_list(): {}",
                    other
                )));
            }
        }
        Ok(())
    }

    fn partial_merge(&self, agg_buf: &mut AggBuf, merging_agg_buf: &mut AggBuf, agg_buf_addrs: &[u64]) -> Result<()> {
        let partial_buf_merger = self.partial_buf_merger;
        let addr = agg_buf_addrs[0];
        partial_buf_merger(agg_buf, merging_agg_buf, addr);
        Ok(())

    }

    fn final_merge(&self, agg_buf: &mut AggBuf, agg_buf_addrs: &[u64]) -> Result<ScalarValue> {
        macro_rules! handle_fixed {
            ($ty:ident) => {{
                type TType = paste! {[<$ty Type>]};
                type TNative = <TType as ArrowPrimitiveType>::Native;
                let w = agg_buf
                    .dyn_value(agg_buf_addrs[0])
                    .as_any()
                    .downcast_ref::<AggDynList<TNative>>()
                    .unwrap()
                    .values
                    .as_ref()
                    .map(|s| s.into_iter()
                        .map(|v| ScalarValue::$ty(Some(v.clone())))
                        .collect::<Vec<_>>()
                    );
                ScalarValue::new_list(w, self.arg_type.clone())
            }}
        }
        Ok(match &self.arg_type {
            DataType::Int8 => handle_fixed!(Int8),
            DataType::Int16 => handle_fixed!(Int16),
            DataType::Int32 => handle_fixed!(Int32),
            DataType::Int64 => handle_fixed!(Int64),
            DataType::UInt8 => handle_fixed!(UInt8),
            DataType::UInt16 => handle_fixed!(UInt16),
            DataType::UInt32 => handle_fixed!(UInt32),
            DataType::UInt64 => handle_fixed!(UInt64),
            DataType::Float32 => handle_fixed!(Float32),
            DataType::Float64 => handle_fixed!(Float64),
            DataType::Decimal128(prec, scale) => {
                let w = agg_buf
                    .dyn_value(agg_buf_addrs[0])
                    .as_any()
                    .downcast_ref::<AggDynList<i128>>()
                    .unwrap()
                    .values
                    .as_ref()
                    .map(|s| s.into_iter()
                        .map(|v| ScalarValue::Decimal128(Some(v.clone()), *prec, *scale))
                        .collect::<Vec<_>>()
                    );
                ScalarValue::new_list(w, self.arg_type.clone())
            },
            DataType::Utf8 => {
                let w = agg_buf
                    .dyn_value(agg_buf_addrs[0])
                    .as_any()
                    .downcast_ref::<AggDynStrList>()
                    .unwrap();
                let mut strs = vec![];
                let mut woff = 0;
                let wbytes = w.strs
                    .as_ref()
                    .map(|str| str.as_bytes())
                    .unwrap_or(&[]);

                for &wlen in &w.lens {
                    let wlen = wlen as usize;
                    let wstr = String::from_utf8_lossy(&wbytes[woff..][..wlen]);
                    strs.push(ScalarValue::Utf8(Some(wstr.to_string())));
                    woff += wlen;
                }
                ScalarValue::new_list(Some(strs), self.arg_type.clone())
            },
            other => {
                return Err(DataFusionError::Execution(format!(
                    "unsupported agg data type: {:?}",
                    other
                )));
            }
        })
    }
}

fn get_partial_updater(dt: &DataType) -> Result<fn(&mut AggBuf, u64, &ArrayRef, usize)> {
    macro_rules! fn_fixed {
        ($ty:ident) => {{
            Ok(|agg_buf, addr, v, i| {
                type TType = paste! {[<$ty Type>]};
                type TArray = paste! {[<$ty Array>]};
                type TNative = <TType as ArrowPrimitiveType>::Native;
                let value = v.as_any().downcast_ref::<TArray>().unwrap();
                if value.is_valid(i) {
                    let w = agg_buf
                        .dyn_value_mut(addr)
                        .as_any_mut()
                        .downcast_mut::<AggDynList<TNative>>()
                        .unwrap();
                    let v = value.value(i);
                    w.append(v);
                }
            })
        }};
    }
    match dt {
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
        DataType::Timestamp(TimeUnit::Microsecond, _) => fn_fixed!(TimestampMicrosecond),
        DataType::Decimal128(_, _) => fn_fixed!(Decimal128),
        DataType::Utf8 => {
            Ok(|agg_buf: &mut AggBuf, addr: u64, v: &ArrayRef, i: usize| {
                let value = v.as_any().downcast_ref::<StringArray>().unwrap();
                if value.is_valid(i) {
                    let w = agg_buf
                        .dyn_value_mut(addr)
                        .as_any_mut()
                        .downcast_mut::<AggDynStrList>()
                        .unwrap();
                    let v = value.value(i);
                    w.append(v);
                }
            })
        }
        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "unsupported data type in collect_list(): {}",
                other
            )));
        }
    }
}

fn get_partial_buf_merger(dt: &DataType) -> Result<fn(&mut AggBuf, &mut AggBuf, u64)> {

    macro_rules! fn_fixed {
        ($ty:ident) => {{
            Ok(|agg_buf1, agg_buf2, addr| {
                let w = agg_buf1
                    .dyn_value_mut(addr)
                    .as_any_mut()
                    .downcast_mut::<AggDynList<$ty>>()
                    .unwrap();
                let v = agg_buf2
                    .dyn_value_mut(addr)
                    .as_any_mut()
                    .downcast_mut::<AggDynList<$ty>>()
                    .unwrap();
                w.merge(v);
            })
        }};
    }
    match dt {
        DataType::Float32 => fn_fixed!(f32),
        DataType::Float64 => fn_fixed!(f64),
        DataType::Int8 => fn_fixed!(i8),
        DataType::Int16 => fn_fixed!(i16),
        DataType::Int32 => fn_fixed!(i32),
        DataType::Int64 => fn_fixed!(i64),
        DataType::UInt8 => fn_fixed!(u8),
        DataType::UInt16 => fn_fixed!(u16),
        DataType::UInt32 => fn_fixed!(u32),
        DataType::UInt64 => fn_fixed!(u64),
        DataType::Date32 => fn_fixed!(i32),
        DataType::Date64 => fn_fixed!(i64),
        DataType::Timestamp(TimeUnit::Microsecond, _) => fn_fixed!(i64),
        DataType::Decimal128(_, _) => fn_fixed!(i128),
        DataType::Utf8 => Ok(|agg_buf1, agg_buf2, addr| {
            let w = agg_buf1
                .dyn_value_mut(addr)
                .as_any_mut()
                .downcast_mut::<AggDynStrList>()
                .unwrap();
            let v = agg_buf2
                .dyn_value_mut(addr)
                .as_any_mut()
                .downcast_mut::<AggDynStrList>()
                .unwrap();
            w.merge(v);
        }),
        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "unsupported data type in collect_list(): {}",
                other
            )));
        }
    }
}


