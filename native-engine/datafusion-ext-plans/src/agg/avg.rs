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
use crate::agg::count::AggCount;
use crate::agg::sum::AggSum;

pub struct AggAvg {
    child: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    agg_sum: AggSum,
    agg_count: AggCount,
    accum_fields: Vec<Field>,
    accums_initial: Vec<ScalarValue>,
    final_merger: fn(ScalarValue, i64) -> ScalarValue,
}

impl AggAvg {
    pub fn try_new(child: Arc<dyn PhysicalExpr>, data_type: DataType) -> Result<Self> {
        let agg_sum = AggSum::try_new(child.clone(), data_type.clone())?;
        let agg_count = AggCount::try_new(child.clone(), DataType::Int64)?;
        let accum_fields = [
            agg_sum.accum_fields(),
            agg_count.accum_fields(),
        ].concat();
        let accums_initial = [
            agg_sum.accums_initial(),
            agg_count.accums_initial(),
        ].concat();
        let final_merger = get_final_merger(&data_type)?;

        Ok(Self {
            child,
            data_type,
            agg_sum,
            agg_count,
            accum_fields,
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

    fn accum_fields(&self) -> &[Field] {
        &self.accum_fields
    }

    fn accums_initial(&self) -> &[ScalarValue] {
        &self.accums_initial
    }

    fn prepare_partial_args(&self, partial_inputs: &[ArrayRef]) -> Result<Vec<ArrayRef>> {
        // cast arg1 to target data type
        Ok(vec![datafusion_ext_commons::cast::cast(
            ColumnarValue::Array(partial_inputs[0].clone()),
            self.accum_fields[0].data_type(),
        )?
        .into_array(0)])
    }

    fn partial_update(&self, accums: &mut [ScalarValue], values: &[ArrayRef], row_idx: usize) -> Result<()> {
        self.agg_sum.partial_update(accums, values, row_idx)?;
        self.agg_count.partial_update(&mut accums[1..], values, row_idx)?;
        Ok(())
    }

    fn partial_update_all(&self, accums: &mut [ScalarValue], values: &[ArrayRef]) -> Result<()> {
        self.agg_sum.partial_update_all(accums, values)?;
        self.agg_count.partial_update_all(&mut accums[1..], values)?;
        Ok(())
    }

    fn partial_merge(&self, accums: &mut [ScalarValue], values: &[ArrayRef], row_idx: usize) -> Result<()> {
        self.agg_sum.partial_merge(accums, values, row_idx)?;
        self.agg_count.partial_merge(&mut accums[1..], &values[1..], row_idx)?;
        Ok(())
    }

    fn partial_merge_scalar(&self, accums: &mut [ScalarValue], values: &mut [ScalarValue]) -> Result<()> {
        self.agg_sum.partial_merge_scalar(accums, values)?;
        self.agg_count.partial_merge_scalar(&mut accums[1..], &mut values[1..])?;
        Ok(())
    }

    fn partial_merge_all(&self, accums: &mut [ScalarValue], values: &[ArrayRef]) -> Result<()> {
        self.agg_sum.partial_merge_all(accums, values)?;
        self.agg_count.partial_merge_all(&mut accums[1..], &values[1..])?;
        Ok(())
    }

    fn final_merge(&self, accums: &mut [ScalarValue]) -> Result<ScalarValue> {
        let sum = self.agg_sum.final_merge(accums)?;
        let count = match self.agg_count.final_merge(&mut accums[1..])? {
            ScalarValue::Int64(Some(count)) => count,
            _ => unreachable!(),
        };
        let final_merger = self.final_merger;
        Ok(final_merger(sum, count))
    }
}

fn get_final_merger(
    dt: &DataType
) -> Result<fn(ScalarValue, i64) -> ScalarValue> {

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
            return Err(DataFusionError::NotImplemented(format!(
                "unsupported data type in sum(): {}",
                other
            )));
        }
    }
}