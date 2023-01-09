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
use datafusion::common::{downcast_value, Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use paste::paste;
use crate::agg::{AggAccum, Agg, load_scalar, save_scalar, AggAccumRef};

pub struct AggSum {
    child: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    accum_fields: Vec<Field>,
}

impl AggSum {
    pub fn try_new(child: Arc<dyn PhysicalExpr>, data_type: DataType) -> Result<Self> {
        let accum_fields = vec![
            Field::new("sum", data_type.clone(), true),
        ];
        Ok(Self {
            child,
            data_type,
            accum_fields,
        })
    }
}

impl Debug for AggSum {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Sum({:?})", self.child)
    }
}

impl Agg for AggSum {
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
        Ok(Box::new(AggSumAccum {
            partial: self.data_type.clone().try_into()?
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

pub struct AggSumAccum {
    pub partial: ScalarValue,
}

impl AggAccum for AggSumAccum {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        Box::new(*self)
    }

    fn mem_size(&self) -> usize {
        self.partial.size()
    }

    fn load(&mut self, values: &[ArrayRef], row_idx: usize) -> Result<()> {
        load_scalar(&mut self.partial, &values[0], row_idx)
    }

    fn save(&self, builders: &mut [Box<dyn ArrayBuilder>]) -> Result<()> {
        save_scalar(&self.partial, &mut builders[0])
    }

    fn save_final(&self, builder: &mut Box<dyn ArrayBuilder>) -> Result<()> {
        save_scalar(&self.partial, builder)
    }

    fn partial_update(&mut self, values: &[ArrayRef], row_idx: usize) -> Result<()> {
        macro_rules! handle {
            ($tyname:ident, $partial_value:expr) => {{
                type TArray = paste! {[<$tyname Array>]};
                let value = downcast_value!(values[0], TArray);
                if value.is_valid(row_idx) {
                    *$partial_value = Some(
                        $partial_value.unwrap_or_default() + value.value(row_idx)
                    );
                }
            }}
        }

        match &mut self.partial {
            ScalarValue::Null => {}
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
            ScalarValue::Decimal128(v, _, _) => handle!(Decimal128, v),
            other => {
                return Err(DataFusionError::NotImplemented(
                    format!("unsupported data type in sum(): {}", other)
                ));
            }
        }
        Ok(())
    }

    fn partial_update_all(&mut self, values: &[ArrayRef]) -> Result<()> {
        macro_rules! handle {
            ($tyname:ident, $partial_value:expr) => {{
                type TArray = paste! {[<$tyname Array>]};
                let value = downcast_value!(values[0], TArray);
                let sum = arrow::compute::sum(value);
                *$partial_value = Some(
                    $partial_value.unwrap_or_default() + sum.unwrap_or_default()
                );
            }}
        }

        match &mut self.partial {
            ScalarValue::Null => {}
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
            ScalarValue::Decimal128(v, _, _) => handle!(Decimal128, v),
            other => {
                return Err(DataFusionError::NotImplemented(
                    format!("unsupported data type in sum(): {}", other)
                ));
            }
        }
        Ok(())
    }

    fn partial_merge(&mut self, another: AggAccumRef) -> Result<()> {
        let another_sum = another.into_any().downcast::<AggSumAccum>().unwrap();
        self.partial_merge_scalar(another_sum.partial)
    }

    fn partial_merge_from_array(
        &mut self,
        partial_agg_values: &[ArrayRef],
        row_idx: usize,
    ) -> Result<()> {
        let mut scalar: ScalarValue = self.partial.get_datatype().try_into()?;
        load_scalar(&mut scalar, &partial_agg_values[0], row_idx)?;
        self.partial_merge_scalar(scalar)
    }
}

impl AggSumAccum {
    pub fn partial_merge_scalar(&mut self, another_value: ScalarValue) -> Result<()> {
        if another_value.is_null() {
            return Ok(());
        }
        if self.partial.is_null() {
            self.partial = another_value;
            return Ok(());
        }

        macro_rules! handle {
            ($a:expr, $b:expr) => {{
                *$a = Some($a.unwrap() + $b.unwrap());
            }}
        }
        match (&mut self.partial, another_value) {
            (ScalarValue::Float32(a), ScalarValue::Float32(b)) => handle!(a, b),
            (ScalarValue::Float64(a), ScalarValue::Float64(b)) => handle!(a, b),
            (ScalarValue::Int8(a), ScalarValue::Int8(b)) => handle!(a, b),
            (ScalarValue::Int16(a), ScalarValue::Int16(b)) => handle!(a, b),
            (ScalarValue::Int32(a), ScalarValue::Int32(b)) => handle!(a, b),
            (ScalarValue::Int64(a), ScalarValue::Int64(b)) => handle!(a, b),
            (ScalarValue::UInt8(a), ScalarValue::UInt8(b)) => handle!(a, b),
            (ScalarValue::UInt16(a), ScalarValue::UInt16(b)) => handle!(a, b),
            (ScalarValue::UInt32(a), ScalarValue::UInt32(b)) => handle!(a, b),
            (ScalarValue::UInt64(a), ScalarValue::UInt64(b)) => handle!(a, b),
            (ScalarValue::Decimal128(a, _, _), ScalarValue::Decimal128(b, _, _)) => handle!(a, b),
            (other, _) => {
                return Err(DataFusionError::NotImplemented(
                    format!("unsupported data type in sum(): {}", other)
                ));
            }
        }
        Ok(())
    }
}