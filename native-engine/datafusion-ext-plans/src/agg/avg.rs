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

use crate::agg::agg_buf::AggBuf;
use crate::agg::count::AggCount;
use crate::agg::sum::AggSum;
use crate::agg::Agg;
use arrow::array::*;
use arrow::datatypes::*;
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use paste::paste;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub struct AggAvg {
    child: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    agg_sum: AggSum,
    agg_count: AggCount,
    accums_initial: Vec<ScalarValue>,
    final_merger: fn(ScalarValue, i64) -> ScalarValue,
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

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn nullable(&self) -> bool {
        true
    }

    fn accums_initial(&self) -> &[ScalarValue] {
        &self.accums_initial
    }

    fn prepare_partial_args(&self, partial_inputs: &[ArrayRef]) -> Result<Vec<ArrayRef>> {
        // cast arg1 to target data type
        Ok(vec![datafusion_ext_commons::cast::cast(
            ColumnarValue::Array(partial_inputs[0].clone()),
            &self.data_type,
        )?
        .into_array(0)])
    }

    fn partial_update(
        &self,
        agg_buf: &mut AggBuf,
        agg_buf_addrs: &[u64],
        values: &[ArrayRef],
        row_idx: usize,
    ) -> Result<()> {
        self.agg_sum
            .partial_update(agg_buf, agg_buf_addrs, values, row_idx)?;
        self.agg_count
            .partial_update(agg_buf, &agg_buf_addrs[1..], values, row_idx)?;
        Ok(())
    }

    fn partial_update_all(
        &self,
        agg_buf: &mut AggBuf,
        agg_buf_addrs: &[u64],
        values: &[ArrayRef],
    ) -> Result<()> {
        self.agg_sum
            .partial_update_all(agg_buf, agg_buf_addrs, values)?;
        self.agg_count
            .partial_update_all(agg_buf, &agg_buf_addrs[1..], values)?;
        Ok(())
    }

    fn partial_merge(
        &self,
        agg_buf1: &mut AggBuf,
        agg_buf2: &mut AggBuf,
        agg_buf_addrs: &[u64],
    ) -> Result<()> {
        self.agg_sum
            .partial_merge(agg_buf1, agg_buf2, agg_buf_addrs)?;
        self.agg_count
            .partial_merge(agg_buf1, agg_buf2, &agg_buf_addrs[1..])?;
        Ok(())
    }

    fn final_merge(&self, agg_buf: &mut AggBuf, agg_buf_addrs: &[u64]) -> Result<ScalarValue> {
        let sum = self.agg_sum.final_merge(agg_buf, agg_buf_addrs)?;
        let count = match self.agg_count.final_merge(agg_buf, &agg_buf_addrs[1..])? {
            ScalarValue::Int64(Some(count)) => count,
            _ => unreachable!(),
        };
        let final_merger = self.final_merger;
        Ok(final_merger(sum, count))
    }
}

fn get_final_merger(dt: &DataType) -> Result<fn(ScalarValue, i64) -> ScalarValue> {
    macro_rules! get_fn {
        ($ty:ident) => {{
            Ok(|mut sum: ScalarValue, count: i64| {
                type TArrowType = paste! {[<$ty Type>]};
                type TNative = <TArrowType as ArrowPrimitiveType>::Native;
                match &mut sum {
                    ScalarValue::$ty(None, ..) => {}
                    ScalarValue::$ty(Some(sum), ..) => {
                        *sum /= (count as TNative);
                    }
                    _ => unreachable!(),
                };
                sum
            })
        }};
    }
    match dt {
        DataType::Null => Ok(|_, _| ScalarValue::Null),
        DataType::Float32 => get_fn!(Float32),
        DataType::Float64 => get_fn!(Float64),
        DataType::Int8 => get_fn!(Int8),
        DataType::Int16 => get_fn!(Int16),
        DataType::Int32 => get_fn!(Int32),
        DataType::Int64 => get_fn!(Int64),
        DataType::UInt8 => get_fn!(UInt8),
        DataType::UInt16 => get_fn!(UInt16),
        DataType::UInt32 => get_fn!(UInt32),
        DataType::UInt64 => get_fn!(UInt64),
        DataType::Decimal128(..) => get_fn!(Decimal128),
        other => {
            Err(DataFusionError::NotImplemented(format!(
                "unsupported data type in avg(): {}",
                other
            )))
        }
    }
}
