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
    common::{
        cast::{as_decimal128_array, as_int64_array},
        Result, ScalarValue,
    },
    physical_expr::PhysicalExpr,
};
use datafusion_ext_commons::df_unimplemented_err;

use crate::agg::{
    acc::{AccumInitialValue, AccumStateValAddr, RefAccumStateRow},
    count::AggCount,
    sum::AggSum,
    Agg, WithAggBufAddrs, WithMemTracking,
};

pub struct AggAvg {
    child: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    agg_sum: AggSum,
    agg_count: AggCount,
    accums_initial: Vec<AccumInitialValue>,
    final_merger: fn(ScalarValue, i64) -> ScalarValue,
    mem_used_tracker: AtomicUsize,
}

impl WithAggBufAddrs for AggAvg {
    fn set_accum_state_val_addrs(&mut self, accum_state_val_addrs: &[AccumStateValAddr]) {
        self.agg_sum
            .set_accum_state_val_addrs(accum_state_val_addrs);
        self.agg_count
            .set_accum_state_val_addrs(&accum_state_val_addrs[1..]);
    }
}

impl WithMemTracking for AggAvg {
    fn mem_used_tracker(&self) -> &AtomicUsize {
        &self.mem_used_tracker
    }
}

impl AggAvg {
    pub fn try_new(child: Arc<dyn PhysicalExpr>, data_type: DataType) -> Result<Self> {
        let agg_sum = AggSum::try_new(child.clone(), data_type.clone())?;
        let agg_count = AggCount::try_new(child.clone(), DataType::Int64)?;
        let accums_initial = [agg_sum.accums_initial(), agg_count.accums_initial()].concat();
        let final_merger = get_final_merger(&data_type)?;

        Ok(Self {
            child,
            data_type,
            agg_sum,
            agg_count,
            accums_initial,
            final_merger,
            mem_used_tracker: AtomicUsize::new(0),
        })
    }
}

impl Debug for AggAvg {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Avg({:?})", self.child)
    }
}

impl Agg for AggAvg {
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

    fn prepare_partial_args(&self, partial_inputs: &[ArrayRef]) -> Result<Vec<ArrayRef>> {
        // cast arg1 to target data type
        Ok(vec![datafusion_ext_commons::cast::cast(
            &partial_inputs[0],
            &self.data_type,
        )?])
    }

    fn increase_acc_mem_used(&self, _acc: &mut RefAccumStateRow) {
        // do nothing
    }

    fn partial_update(
        &self,
        acc: &mut RefAccumStateRow,
        values: &[ArrayRef],
        row_idx: usize,
    ) -> Result<()> {
        self.agg_sum.partial_update(acc, values, row_idx)?;
        self.agg_count.partial_update(acc, values, row_idx)?;
        Ok(())
    }

    fn partial_batch_update(
        &self,
        accs: &mut [RefAccumStateRow],
        values: &[ArrayRef],
    ) -> Result<()> {
        self.agg_sum.partial_batch_update(accs, values)?;
        self.agg_count.partial_batch_update(accs, values)?;
        Ok(())
    }

    fn partial_update_all(&self, acc: &mut RefAccumStateRow, values: &[ArrayRef]) -> Result<()> {
        self.agg_sum.partial_update_all(acc, values)?;
        self.agg_count.partial_update_all(acc, values)?;
        Ok(())
    }

    fn partial_merge(
        &self,
        acc1: &mut RefAccumStateRow,
        acc2: &mut RefAccumStateRow,
    ) -> Result<()> {
        self.agg_sum.partial_merge(acc1, acc2)?;
        self.agg_count.partial_merge(acc1, acc2)?;
        Ok(())
    }

    fn partial_batch_merge(
        &self,
        accs: &mut [RefAccumStateRow],
        merging_accs: &mut [RefAccumStateRow],
    ) -> Result<()> {
        self.agg_sum.partial_batch_merge(accs, merging_accs)?;
        self.agg_count.partial_batch_merge(accs, merging_accs)?;
        Ok(())
    }

    fn final_merge(&self, acc: &mut RefAccumStateRow) -> Result<ScalarValue> {
        let sum = self.agg_sum.final_merge(acc)?;
        let count = match self.agg_count.final_merge(acc)? {
            ScalarValue::Int64(Some(count)) => count,
            _ => unreachable!(),
        };
        let final_merger = self.final_merger;
        Ok(final_merger(sum, count))
    }

    fn final_batch_merge(&self, accs: &mut [RefAccumStateRow]) -> Result<ArrayRef> {
        let sums = self.agg_sum.final_batch_merge(accs)?;
        let counts = self.agg_count.final_batch_merge(accs)?;

        let counts_zero_free: Int64Array = as_int64_array(&counts)?.unary_opt(|count| {
            let not_zero = !count.is_zero();
            not_zero.then_some(count)
        });

        if let &DataType::Decimal128(prec, scale) = self.data_type() {
            let sums = as_decimal128_array(&sums)?;
            let counts = counts_zero_free;
            let avgs =
                arrow::compute::binary::<_, _, _, Decimal128Type>(&sums, &counts, |sum, count| {
                    sum.checked_div_euclid(count as i128).unwrap_or_default()
                })?;
            Ok(Arc::new(avgs.with_precision_and_scale(prec, scale)?))
        } else {
            let counts = counts_zero_free;
            Ok(arrow::compute::kernels::numeric::div(
                &arrow::compute::cast(&sums, &DataType::Float64)?,
                &arrow::compute::cast(&counts, &DataType::Float64)?,
            )?)
        }
    }
}

fn get_final_merger(dt: &DataType) -> Result<fn(ScalarValue, i64) -> ScalarValue> {
    macro_rules! get_fn {
        ($ty:ident,f64) => {{
            Ok(|sum: ScalarValue, count: i64| {
                let avg = match sum {
                    ScalarValue::$ty(sum, ..) => ScalarValue::Float64(if !count.is_zero() {
                        sum.map(|sum| sum as f64 / (count as f64))
                    } else {
                        None
                    }),
                    _ => unreachable!(),
                };
                avg
            })
        }};
        (Decimal128) => {{
            Ok(|sum: ScalarValue, count: i64| {
                let avg = match sum {
                    ScalarValue::Decimal128(sum, prec, scale) => ScalarValue::Decimal128(
                        if !count.is_zero() {
                            sum.map(|sum| sum / (count as i128))
                        } else {
                            None
                        },
                        prec,
                        scale,
                    ),
                    _ => unreachable!(),
                };
                avg
            })
        }};
    }
    match dt {
        DataType::Null => Ok(|_, _| ScalarValue::Null),
        DataType::Float32 => get_fn!(Float32, f64),
        DataType::Float64 => get_fn!(Float64, f64),
        DataType::Int8 => get_fn!(Int8, f64),
        DataType::Int16 => get_fn!(Int16, f64),
        DataType::Int32 => get_fn!(Int32, f64),
        DataType::Int64 => get_fn!(Int64, f64),
        DataType::UInt8 => get_fn!(UInt8, f64),
        DataType::UInt16 => get_fn!(UInt16, f64),
        DataType::UInt32 => get_fn!(UInt32, f64),
        DataType::UInt64 => get_fn!(UInt64, f64),
        DataType::Decimal128(..) => get_fn!(Decimal128),
        other => df_unimplemented_err!("unsupported data type in avg(): {other}"),
    }
}
