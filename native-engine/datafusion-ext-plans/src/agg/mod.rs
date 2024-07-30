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

pub mod acc;
pub mod agg_context;
pub mod agg_table;
pub mod avg;
pub mod bloom_filter;
pub mod brickhouse;
pub mod collect_list;
pub mod collect_set;
pub mod count;
pub mod first;
pub mod first_ignores_null;
pub mod maxmin;
pub mod sum;

use std::{
    any::Any,
    fmt::Debug,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

use arrow::{array::*, datatypes::*};
use datafusion::{
    common::{Result, ScalarValue},
    logical_expr::aggregate_function,
    physical_expr::PhysicalExpr,
};
use datafusion_ext_commons::df_execution_err;
use datafusion_ext_exprs::cast::TryCastExpr;
use slimmer_box::SlimmerBox;

use crate::agg::acc::{
    AccumInitialValue, AccumStateRow, AccumStateValAddr, AggDynBinary, AggDynScalar, AggDynStr,
    RefAccumStateRow,
};

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
    BloomFilter,
    BrickhouseCollect,
    BrickhouseCombineUnique,
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

pub trait WithAggBufAddrs {
    fn set_accum_state_val_addrs(&mut self, accum_state_val_addrs: &[AccumStateValAddr]);
}

pub trait WithMemTracking {
    fn mem_used_tracker(&self) -> &AtomicUsize;

    fn mem_used(&self) -> usize {
        self.mem_used_tracker().load(SeqCst)
    }

    fn add_mem_used(&self, mem_used: usize) {
        self.mem_used_tracker().fetch_add(mem_used, SeqCst);
    }

    fn sub_mem_used(&self, mem_used: usize) {
        let _ = self
            .mem_used_tracker()
            .fetch_update(SeqCst, SeqCst, |v| Some(v.saturating_sub(mem_used)));
    }

    fn reset_mem_used(&self) {
        self.mem_used_tracker().store(0, SeqCst);
    }
}

pub trait Agg: WithAggBufAddrs + WithMemTracking + Send + Sync + Debug {
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

    fn increase_acc_mem_used(&self, acc: &mut RefAccumStateRow);

    fn partial_update(
        &self,
        acc: &mut RefAccumStateRow,
        values: &[ArrayRef],
        row_idx: usize,
    ) -> Result<()>;

    fn partial_batch_update(
        &self,
        accs: &mut [RefAccumStateRow],
        values: &[ArrayRef],
    ) -> Result<()> {
        for row_idx in 0..accs.len() {
            let acc = &mut accs[row_idx];
            self.partial_update(acc, values, row_idx)?;
        }
        Ok(())
    }

    fn partial_update_all(&self, acc: &mut RefAccumStateRow, values: &[ArrayRef]) -> Result<()>;
    fn partial_merge(
        &self,
        acc: &mut RefAccumStateRow,
        merging_acc: &mut RefAccumStateRow,
    ) -> Result<()>;

    fn partial_batch_merge(
        &self,
        accs: &mut [RefAccumStateRow],
        merging_accs: &mut [RefAccumStateRow],
    ) -> Result<()> {
        for row_idx in 0..accs.len() {
            let acc = &mut accs[row_idx];
            let merging_acc = &mut merging_accs[row_idx];
            self.partial_merge(acc, merging_acc)?;
        }
        Ok(())
    }

    fn final_merge(&self, acc: &mut RefAccumStateRow) -> Result<ScalarValue>;
    fn final_batch_merge(&self, accs: &mut [RefAccumStateRow]) -> Result<ArrayRef>;
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
        AggFunction::BloomFilter => {
            let dt = children[0].data_type(input_schema)?;
            let empty_batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
            let estimated_num_items = children[1]
                .evaluate(&empty_batch)?
                .into_array(1)?
                .as_primitive::<Int64Type>()
                .value(0);
            let num_bits = children[2]
                .evaluate(&empty_batch)?
                .into_array(1)?
                .as_primitive::<Int64Type>()
                .value(0);
            Arc::new(bloom_filter::AggBloomFilter::new(
                children[0].clone(),
                dt,
                estimated_num_items as usize,
                num_bits as usize,
            ))
        }
        AggFunction::CollectList => {
            let arg_type = children[0].data_type(input_schema)?;
            let return_type = DataType::new_list(arg_type.clone(), true);
            Arc::new(collect_list::AggCollectList::try_new(
                children[0].clone(),
                return_type,
                arg_type,
            )?)
        }
        AggFunction::CollectSet => {
            let arg_type = children[0].data_type(input_schema)?;
            let return_type = DataType::new_list(arg_type.clone(), true);
            Arc::new(collect_set::AggCollectSet::try_new(
                children[0].clone(),
                return_type,
                arg_type,
            )?)
        }
        AggFunction::BrickhouseCollect => {
            let arg_type = children[0].data_type(input_schema)?;
            let arg_list_inner_type = match arg_type {
                DataType::List(field) => field.data_type().clone(),
                _ => return df_execution_err!("brickhouse.collect expect list type"),
            };
            Arc::new(brickhouse::collect::AggCollect::try_new(
                children[0].clone(),
                arg_list_inner_type,
            )?)
        }
        AggFunction::BrickhouseCombineUnique => {
            let arg_type = children[0].data_type(input_schema)?;
            let arg_list_inner_type = match arg_type {
                DataType::List(field) => field.data_type().clone(),
                _ => return df_execution_err!("brickhouse.combine_unique expect list type"),
            };
            Arc::new(brickhouse::collect::AggCollect::try_new(
                children[0].clone(),
                arg_list_inner_type,
            )?)
        }
    })
}

fn default_final_merge_with_addr(
    agg: &impl Agg,
    acc: &mut RefAccumStateRow,
    addr: AccumStateValAddr,
) -> Result<ScalarValue> {
    // default implementation:
    // extract the only one values from acc and convert to ScalarValue
    // this works for sum/min/max/first
    macro_rules! handle_fixed {
        ($ty:ident) => {{
            if acc.is_fixed_valid(addr) {
                ScalarValue::$ty(Some(acc.fixed_value(addr)))
            } else {
                ScalarValue::$ty(None)
            }
        }};
    }
    macro_rules! handle_timestamp {
        ($ty:ident, $tz:expr) => {{
            let v = if acc.is_fixed_valid(addr) {
                Some(acc.fixed_value(addr))
            } else {
                None
            };
            ScalarValue::$ty(v, $tz.clone())
        }};
    }
    Ok(match agg.data_type() {
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
            let v = if acc.is_fixed_valid(addr) {
                Some(acc.fixed_value(addr))
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
        DataType::Utf8 => ScalarValue::Utf8(match std::mem::take(acc.dyn_value_mut(addr)) {
            Some(v) => Some({
                agg.sub_mem_used(v.mem_size());
                let boxed = SlimmerBox::into_box(
                    v.as_any_boxed()
                        .downcast::<AggDynStr>()
                        .or_else(|_| df_execution_err!("error downcasting to AggDynStr"))?
                        .into_value(),
                );
                boxed.into_string()
            }),
            None => None,
        }),
        DataType::Binary => ScalarValue::Binary(match std::mem::take(acc.dyn_value_mut(addr)) {
            Some(v) => Some({
                agg.sub_mem_used(v.mem_size());
                v.as_any_boxed()
                    .downcast::<AggDynBinary>()
                    .or_else(|_| df_execution_err!("error downcasting to AggDynStr"))?
                    .into_value()
                    .into_vec()
            }),
            None => None,
        }),
        other => match std::mem::take(acc.dyn_value_mut(addr)) {
            Some(v) => {
                agg.sub_mem_used(v.mem_size());
                v.as_any_boxed()
                    .downcast::<AggDynScalar>()
                    .or_else(|_| df_execution_err!("error downcasting to AggDynScalar"))?
                    .into_value()
            }
            None => ScalarValue::try_from(other)?,
        },
    })
}

fn default_final_batch_merge_with_addr(
    agg: &impl Agg,
    accs: &mut [RefAccumStateRow],
    addr: AccumStateValAddr,
) -> Result<ArrayRef> {
    // default implementation:
    // extract the only one values from acc and convert to ScalarValue
    // this works for sum/min/max/first
    macro_rules! handle_fixed {
        ($ty:ident) => {{
            type B = paste::paste! {[< $ty Builder >]};
            let mut builder = B::with_capacity(accs.len());
            for acc in accs {
                if acc.is_fixed_valid(addr) {
                    builder.append_value(acc.fixed_value(addr));
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
    Ok(match agg.data_type() {
        DataType::Null => mkarray!(NullArray::new(accs.len())),
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
            mkarray!(accs
                .iter_mut()
                .map(|acc| {
                    let dyn_str = std::mem::take(acc.dyn_value_mut(addr));
                    match dyn_str {
                        Some(s) => {
                            agg.sub_mem_used(s.mem_size());
                            let boxed = SlimmerBox::into_box(
                                s.as_any_boxed()
                                    .downcast::<AggDynStr>()
                                    .unwrap()
                                    .into_value(),
                            );
                            Some(boxed.into_string())
                        }
                        None => None,
                    }
                })
                .collect::<StringArray>())
        }
        DataType::Binary => {
            mkarray!(accs
                .iter_mut()
                .map(|acc| {
                    let dyn_binary = std::mem::take(acc.dyn_value_mut(addr));
                    match dyn_binary {
                        Some(s) => Some({
                            agg.sub_mem_used(s.mem_size());
                            s.as_any_boxed()
                                .downcast::<AggDynBinary>()
                                .unwrap()
                                .into_value()
                                .into_vec()
                        }),
                        None => None,
                    }
                })
                .collect::<BinaryArray>())
        }
        other => {
            let scalars = accs
                .iter_mut()
                .map(|acc| {
                    let dyn_scalar = std::mem::take(acc.dyn_value_mut(addr));
                    match dyn_scalar {
                        Some(s) => {
                            agg.sub_mem_used(s.mem_size());
                            s.as_any_boxed()
                                .downcast::<AggDynScalar>()
                                .unwrap()
                                .into_value()
                        }
                        None => ScalarValue::try_from(other).unwrap(),
                    }
                })
                .collect::<Vec<_>>();
            ScalarValue::iter_to_array(scalars)?
        }
    })
}
