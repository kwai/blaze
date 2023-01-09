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

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use arrow::array::*;
use arrow::datatypes::*;
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use paste::paste;
use datafusion_ext_commons::array_builder::ConfiguredDecimal128Builder;
use crate::agg::{AggAccum, Agg, AggAccumRef};
use crate::agg::count::AggCountAccum;
use crate::agg::sum::AggSumAccum;

pub struct AggAvg {
    child: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    accum_fields: Vec<Field>,
}

impl AggAvg {
    pub fn try_new(child: Arc<dyn PhysicalExpr>, data_type: DataType) -> Result<Self> {
        let accum_fields = vec![
            Field::new("sum", data_type.clone(), true),
            Field::new("count", DataType::Int64, false),
        ];
        Ok(Self {
            child,
            data_type,
            accum_fields,
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

    fn create_accum(&self) -> Result<AggAccumRef> {
        Ok(Box::new(AggAvgAccum {
            sum: AggSumAccum {
                partial: self.data_type.clone().try_into()?,
            },
            count: AggCountAccum {
                partial: 0,
            },
        }))
    }

    fn prepare_partial_args(
        &self,
        partial_inputs: &[ArrayRef],
    ) -> Result<Vec<ArrayRef>> {
        // cast arg1 to target data type
        Ok(vec![
            datafusion_ext_commons::cast::cast(
                ColumnarValue::Array(partial_inputs[0].clone()),
                self.accum_fields[0].data_type(),
            )?.into_array(0),
        ])
    }
}

pub struct AggAvgAccum {
    pub sum: AggSumAccum,
    pub count: AggCountAccum,
}

impl AggAccum for AggAvgAccum {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        Box::new(*self)
    }

    fn mem_size(&self) -> usize {
        self.sum.mem_size() + self.count.mem_size()
    }

    fn load(&mut self, values: &[ArrayRef], row_idx: usize) -> Result<()> {
        self.sum.load(values, row_idx)?;
        self.count.load(&values[1..], row_idx)?;
        Ok(())
    }

    fn save(&self, builders: &mut [Box<dyn ArrayBuilder>]) -> Result<()> {
        self.sum.save(builders)?;
        self.count.save(&mut builders[1..])?;
        Ok(())
    }

    fn save_final(&self, builder: &mut Box<dyn ArrayBuilder>) -> Result<()> {
        macro_rules! handle {
            ($tyname:ident, $partial_value:expr) => {{
                type TType = paste! {[<$tyname Type>]};
                type TBuilder = paste! {[<$tyname Builder>]};
                let builder = builder.as_any_mut().downcast_mut::<TBuilder>().unwrap();
                let count = self.count.partial;
                if self.sum.partial.is_null() || count.is_zero() {
                    builder.append_null();
                    return Ok(());
                }
                let sum = $partial_value.unwrap() as <TType as ArrowPrimitiveType>::Native;
                let avg = sum / (count as <TType as ArrowPrimitiveType>::Native);
                builder.append_value(avg);
            }}
        }
        match &self.sum.partial {
            ScalarValue::Float32(v) => handle!(Float32, v),
            ScalarValue::Float64(v) => handle!(Float64, v),
            ScalarValue::Int8(v) => handle!(Int8, v),
            ScalarValue::Int16(v) => handle!(Int16, v),
            ScalarValue::Int32(v) => handle!(Int32, v),
            ScalarValue::Int64(v) => handle!(Int64, v),
            ScalarValue::UInt8(v) => handle!(UInt8, v),
            ScalarValue::UInt16(v) => handle!(UInt16, v),
            ScalarValue::UInt32(v) => handle!(UInt32, v),
            ScalarValue::UInt64(v) => handle!(UInt64, v),
            ScalarValue::Decimal128(v, _, _) => {
                type Decimal128Builder = ConfiguredDecimal128Builder;
                handle!(Decimal128, v)
            },
            other => {
                return Err(DataFusionError::NotImplemented(
                    format!("unsupported data type in avg(): {}", other)
                ));
            }
        }
        Ok(())
    }

    fn partial_update(&mut self, values: &[ArrayRef], row_idx: usize) -> Result<()> {
        self.sum.partial_update(values, row_idx)?;
        self.count.partial_update(values, row_idx)?;
        Ok(())
    }

    fn partial_update_all(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.sum.partial_update_all(values)?;
        self.count.partial_update_all(values)?;
        Ok(())
    }

    fn partial_merge(&mut self, another: AggAccumRef) -> Result<()> {
        let another_avg = another.into_any().downcast::<AggAvgAccum>().unwrap();
        self.sum.partial_merge_scalar(another_avg.sum.partial)?;
        self.count.partial += another_avg.count.partial;
        Ok(())
    }

    fn partial_merge_from_array(
        &mut self,
        partial_agg_values: &[ArrayRef],
        row_idx: usize,
    ) -> Result<()> {
        self.sum.partial_merge_from_array(partial_agg_values, row_idx)?;
        self.count.partial_merge_from_array(&partial_agg_values[1..], row_idx)?;
        Ok(())
    }
}