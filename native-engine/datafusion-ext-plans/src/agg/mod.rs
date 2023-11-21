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

pub mod agg_buf;
pub mod agg_context;
pub mod agg_tables;
pub mod avg;
pub mod collect_list;
pub mod collect_set;
pub mod count;
pub mod first;
pub mod first_ignores_null;
pub mod maxmin;
pub mod sum;

use std::{any::Any, fmt::Debug, sync::Arc};

use arrow::{array::*, datatypes::*};
use datafusion::{
    common::{DataFusionError, Result, ScalarValue},
    logical_expr::aggregate_function,
    physical_expr::PhysicalExpr,
};
use datafusion_ext_exprs::cast::TryCastExpr;

use crate::agg::agg_buf::{AccumInitialValue, AggBuf, AggDynBinary, AggDynScalar, AggDynStr};

pub const AGG_BUF_COLUMN_NAME: &str = "#9223372036854775807";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggExecMode {
    HashAgg,
    SortAgg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggMode {
    Partial,
    PartialMerge,
    Final,
}
impl AggMode {
    pub fn is_partial(&self) -> bool {
        matches!(self, AggMode::Partial)
    }

    pub fn is_partial_merge(&self) -> bool {
        matches!(self, AggMode::PartialMerge)
    }

    pub fn is_final(&self) -> bool {
        matches!(self, AggMode::Final)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunction {
    Count,
    Sum,
    Avg,
    Max,
    Min,
    First,
    FirstIgnoresNull,
    CollectList,
    CollectSet,
}

#[derive(Debug, Clone)]
pub struct GroupingExpr {
    pub field_name: String,
    pub expr: Arc<dyn PhysicalExpr>,
}

#[derive(Debug, Clone)]
pub struct AggExpr {
    pub field_name: String,
    pub mode: AggMode,
    pub agg: Arc<dyn Agg>,
}

pub trait Agg: Send + Sync + Debug {
    fn as_any(&self) -> &dyn Any;
    fn exprs(&self) -> Vec<Arc<dyn PhysicalExpr>>;
    fn data_type(&self) -> &DataType;
    fn nullable(&self) -> bool;
    fn accums_initial(&self) -> &[AccumInitialValue];
    fn with_new_exprs(&self, exprs: Vec<Arc<dyn PhysicalExpr>>) -> Result<Arc<dyn Agg>>;

    fn prepare_partial_args(&self, partial_inputs: &[ArrayRef]) -> Result<Vec<ArrayRef>> {
        // default implementation: directly return the inputs
        Ok(partial_inputs.iter().map(Clone::clone).collect())
    }

    fn partial_update(
        &self,
        agg_buf: &mut AggBuf,
        agg_buf_addrs: &[u64],
        values: &[ArrayRef],
        row_idx: usize,
    ) -> Result<()>;

    fn partial_batch_update(
        &self,
        agg_bufs: &mut [AggBuf],
        agg_buf_addrs: &[u64],
        values: &[ArrayRef],
    ) -> Result<usize> {
        let mut mem_diff = 0;
        for row_idx in 0..agg_bufs.len() {
            let agg_buf = &mut agg_bufs[row_idx];
            mem_diff -= agg_buf.mem_size();
            self.partial_update(agg_buf, agg_buf_addrs, values, row_idx)?;
            mem_diff += agg_buf.mem_size();
        }
        Ok(mem_diff)
    }

    fn partial_update_all(
        &self,
        agg_buf: &mut AggBuf,
        agg_buf_addrs: &[u64],
        values: &[ArrayRef],
    ) -> Result<()>;

    fn partial_merge(
        &self,
        agg_buf: &mut AggBuf,
        merging_agg_buf: &mut AggBuf,
        agg_buf_addrs: &[u64],
    ) -> Result<()>;

    fn partial_batch_merge(
        &self,
        agg_bufs: &mut [AggBuf],
        merging_agg_bufs: &mut [AggBuf],
        agg_buf_addrs: &[u64],
    ) -> Result<usize> {
        let mut mem_diff = 0;
        for row_idx in 0..agg_bufs.len() {
            let agg_buf = &mut agg_bufs[row_idx];
            let merging_agg_buf = &mut merging_agg_bufs[row_idx];
            mem_diff -= agg_buf.mem_size();
            self.partial_merge(agg_buf, merging_agg_buf, agg_buf_addrs)?;
            mem_diff += agg_buf.mem_size();
        }
        Ok(mem_diff)
    }

    fn final_merge(&self, agg_buf: &mut AggBuf, agg_buf_addrs: &[u64]) -> Result<ScalarValue> {
        // default implementation:
        // extract the only one values from agg_buf and convert to ScalarValue
        // this works for sum/min/max/first
        let addr = agg_buf_addrs[0];

        macro_rules! handle_fixed {
            ($ty:ident) => {{
                if agg_buf.is_fixed_valid(addr) {
                    ScalarValue::$ty(Some(agg_buf.fixed_value(addr)))
                } else {
                    ScalarValue::$ty(None)
                }
            }};
        }
        macro_rules! handle_timestamp {
            ($ty:ident, $tz:expr) => {{
                let v = if agg_buf.is_fixed_valid(addr) {
                    Some(agg_buf.fixed_value(addr))
                } else {
                    None
                };
                ScalarValue::$ty(v, $tz.clone())
            }};
        }
        Ok(match self.data_type() {
            DataType::Null => ScalarValue::Null,
            DataType::Boolean => handle_fixed!(Boolean),
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
            DataType::Decimal128(prec, scale) => {
                let v = if agg_buf.is_fixed_valid(addr) {
                    Some(agg_buf.fixed_value(addr))
                } else {
                    None
                };
                ScalarValue::Decimal128(v, *prec, *scale)
            }
            DataType::Date32 => handle_fixed!(Date32),
            DataType::Date64 => handle_fixed!(Date64),
            DataType::Timestamp(TimeUnit::Second, tz) => handle_timestamp!(TimestampSecond, tz),
            DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                handle_timestamp!(TimestampMillisecond, tz)
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                handle_timestamp!(TimestampMicrosecond, tz)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                handle_timestamp!(TimestampNanosecond, tz)
            }
            DataType::Utf8 => ScalarValue::Utf8(
                agg_buf
                    .dyn_value(addr)
                    .as_any()
                    .downcast_ref::<AggDynStr>()
                    .unwrap()
                    .value
                    .as_ref()
                    .map(|s| s.as_ref().to_owned()),
            ),
            DataType::Binary => ScalarValue::Binary(
                agg_buf
                    .dyn_value(addr)
                    .as_any()
                    .downcast_ref::<AggDynBinary>()
                    .unwrap()
                    .value
                    .as_ref()
                    .map(|s| s.as_ref().to_owned()),
            ),
            other => {
                if let Some(s) = agg_buf
                    .dyn_value(addr)
                    .as_any()
                    .downcast_ref::<AggDynScalar>()
                {
                    s.value.clone()
                } else {
                    return Err(DataFusionError::NotImplemented(format!(
                        "unsupported data type: {other}"
                    )));
                }
            }
        })
    }

    fn final_batch_merge(
        &self,
        agg_bufs: &mut [AggBuf],
        agg_buf_addrs: &[u64],
    ) -> Result<ArrayRef> {
        // default implementation:
        // extract the only one values from agg_buf and convert to ScalarValue
        // this works for sum/min/max/first
        let addr = agg_buf_addrs[0];

        macro_rules! handle_fixed {
            ($ty:ident) => {{
                type B = paste::paste! {[< $ty Builder >]};
                let mut builder = B::with_capacity(agg_bufs.len());
                for agg_buf in agg_bufs {
                    if agg_buf.is_fixed_valid(addr) {
                        builder.append_value(agg_buf.fixed_value(addr));
                    } else {
                        builder.append_null();
                    };
                }
                builder.finish()
            }};
        }
        macro_rules! mkarray {
            ($a:expr) => {{
                let array: Arc<dyn Array + 'static> = Arc::new($a);
                array
            }};
        }
        Ok(match self.data_type() {
            DataType::Null => mkarray!(NullArray::new(agg_bufs.len())),
            DataType::Boolean => mkarray!(handle_fixed!(Boolean)),
            DataType::Float32 => mkarray!(handle_fixed!(Float32)),
            DataType::Float64 => mkarray!(handle_fixed!(Float64)),
            DataType::Int8 => mkarray!(handle_fixed!(Int8)),
            DataType::Int16 => mkarray!(handle_fixed!(Int16)),
            DataType::Int32 => mkarray!(handle_fixed!(Int32)),
            DataType::Int64 => mkarray!(handle_fixed!(Int64)),
            DataType::UInt8 => mkarray!(handle_fixed!(UInt8)),
            DataType::UInt16 => mkarray!(handle_fixed!(UInt16)),
            DataType::UInt32 => mkarray!(handle_fixed!(UInt32)),
            DataType::UInt64 => mkarray!(handle_fixed!(UInt64)),
            DataType::Decimal128(prec, scale) => {
                mkarray!(handle_fixed!(Decimal128).with_precision_and_scale(*prec, *scale)?)
            }
            DataType::Date32 => mkarray!(handle_fixed!(Date32)),
            DataType::Date64 => mkarray!(handle_fixed!(Date64)),
            DataType::Timestamp(TimeUnit::Second, tz) => {
                mkarray!(handle_fixed!(TimestampSecond).with_timezone_opt(tz.clone()))
            }
            DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                mkarray!(handle_fixed!(TimestampMillisecond).with_timezone_opt(tz.clone()))
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                mkarray!(handle_fixed!(TimestampMicrosecond).with_timezone_opt(tz.clone()))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                mkarray!(handle_fixed!(TimestampNanosecond).with_timezone_opt(tz.clone()))
            }
            DataType::Utf8 => {
                mkarray!(agg_bufs
                    .iter_mut()
                    .map(|agg_buf| {
                        let value = std::mem::take(
                            &mut agg_buf
                                .dyn_value_mut(addr)
                                .as_any_mut()
                                .downcast_mut::<AggDynStr>()
                                .unwrap()
                                .value,
                        );
                        value.map(|v| v.into_string())
                    })
                    .collect::<StringArray>())
            }
            DataType::Binary => {
                mkarray!(agg_bufs
                    .iter_mut()
                    .map(|agg_buf| {
                        let value = std::mem::take(
                            &mut agg_buf
                                .dyn_value_mut(addr)
                                .as_any_mut()
                                .downcast_mut::<AggDynBinary>()
                                .unwrap()
                                .value,
                        );
                        value.map(|v| v.into_vec())
                    })
                    .collect::<BinaryArray>())
            }
            _other => {
                println!("{:?}", _other);
                let scalars = agg_bufs
                    .iter_mut()
                    .map(|agg_buf| {
                        let value = std::mem::replace(
                            &mut agg_buf
                                .dyn_value_mut(addr)
                                .as_any_mut()
                                .downcast_mut::<AggDynScalar>()
                                .unwrap()
                                .value,
                            ScalarValue::Null,
                        );
                        value
                    })
                    .collect::<Vec<_>>();
                ScalarValue::iter_to_array(scalars)?
            }
        })
    }
}

pub fn create_agg(
    agg_function: AggFunction,
    children: &[Arc<dyn PhysicalExpr>],
    input_schema: &SchemaRef,
) -> Result<Arc<dyn Agg>> {
    Ok(match agg_function {
        AggFunction::Count => {
            let return_type = DataType::Int64;
            Arc::new(count::AggCount::try_new(children[0].clone(), return_type)?)
        }
        AggFunction::Sum => {
            let arg_type = children[0].data_type(input_schema)?;
            let return_type = aggregate_function::AggregateFunction::return_type(
                &aggregate_function::AggregateFunction::Sum,
                &[arg_type],
            )?;
            Arc::new(sum::AggSum::try_new(
                Arc::new(TryCastExpr::new(children[0].clone(), return_type.clone())),
                return_type,
            )?)
        }
        AggFunction::Avg => {
            let arg_type = children[0].data_type(input_schema)?;
            let return_type = aggregate_function::AggregateFunction::return_type(
                &aggregate_function::AggregateFunction::Avg,
                &[arg_type],
            )?;
            Arc::new(avg::AggAvg::try_new(
                Arc::new(TryCastExpr::new(children[0].clone(), return_type.clone())),
                return_type,
            )?)
        }
        AggFunction::Max => {
            let dt = children[0].data_type(input_schema)?;
            Arc::new(maxmin::AggMax::try_new(children[0].clone(), dt)?)
        }
        AggFunction::Min => {
            let dt = children[0].data_type(input_schema)?;
            Arc::new(maxmin::AggMin::try_new(children[0].clone(), dt)?)
        }
        AggFunction::First => {
            let dt = children[0].data_type(input_schema)?;
            Arc::new(first::AggFirst::try_new(children[0].clone(), dt)?)
        }
        AggFunction::FirstIgnoresNull => {
            let dt = children[0].data_type(input_schema)?;
            Arc::new(first_ignores_null::AggFirstIgnoresNull::try_new(
                children[0].clone(),
                dt,
            )?)
        }
        AggFunction::CollectList => {
            let arg_type = children[0].data_type(input_schema)?;
            let return_type = DataType::List(Arc::new(Field::new("item", arg_type.clone(), true)));
            Arc::new(collect_list::AggCollectList::try_new(
                children[0].clone(),
                return_type,
                arg_type,
            )?)
        }
        AggFunction::CollectSet => {
            let arg_type = children[0].data_type(input_schema)?;
            let return_type = DataType::List(Arc::new(Field::new("item", arg_type.clone(), true)));
            Arc::new(collect_set::AggCollectSet::try_new(
                children[0].clone(),
                return_type,
                arg_type,
            )?)
        }
    })
}
