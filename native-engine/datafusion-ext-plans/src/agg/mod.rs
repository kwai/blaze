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

pub mod agg_helper;
pub mod agg_tables;
pub mod avg;
pub mod count;
pub mod max;
pub mod min;
pub mod sum;

use arrow::array::*;
use arrow::datatypes::*;
use datafusion::common::{downcast_value, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::aggregate_function;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_ext_commons::array_builder::ConfiguredDecimal128Builder;
use datafusion_ext_exprs::cast::TryCastExpr;
use paste::paste;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

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
    COUNT,
    SUM,
    AVG,
    MAX,
    MIN,
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
    fn accum_fields(&self) -> &[Field];
    fn create_accum(&self) -> Result<AggAccumRef>;
    fn prepare_partial_args(&self, partial_inputs: &[ArrayRef]) -> Result<Vec<ArrayRef>> {
        Ok(partial_inputs.iter().map(Clone::clone).collect())
    }
}

pub trait AggAccum: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
    fn mem_size(&self) -> usize;
    fn load(&mut self, values: &[ArrayRef], row_idx: usize) -> Result<()>;
    fn save(&self, builders: &mut [Box<dyn ArrayBuilder>]) -> Result<()>;
    fn save_final(&self, builder: &mut Box<dyn ArrayBuilder>) -> Result<()>;

    fn partial_update(&mut self, values: &[ArrayRef], row_idx: usize) -> Result<()>;
    fn partial_update_all(&mut self, values: &[ArrayRef]) -> Result<()>;

    fn partial_merge(&mut self, another: AggAccumRef) -> Result<()>;
    fn partial_merge_from_array(
        &mut self,
        partial_agg_values: &[ArrayRef],
        row_idx: usize,
    ) -> Result<()>;
}
pub type AggAccumRef = Box<dyn AggAccum>;

pub fn create_agg(
    agg_function: AggFunction,
    children: &[Arc<dyn PhysicalExpr>],
    input_schema: &SchemaRef,
) -> Result<Arc<dyn Agg>> {
    Ok(match agg_function {
        AggFunction::COUNT => {
            let return_type = DataType::Int64;
            Arc::new(count::AggCount::try_new(children[0].clone(), return_type)?)
        }
        AggFunction::SUM => {
            let arg_type = children[0].data_type(input_schema)?;
            let return_type = aggregate_function::return_type(
                &aggregate_function::AggregateFunction::Sum,
                &[arg_type],
            )?;
            Arc::new(sum::AggSum::try_new(
                Arc::new(TryCastExpr::new(children[0].clone(), return_type.clone())),
                return_type,
            )?)
        }
        AggFunction::AVG => {
            let arg_type = children[0].data_type(input_schema)?;
            let return_type = aggregate_function::return_type(
                &aggregate_function::AggregateFunction::Avg,
                &[arg_type],
            )?;
            Arc::new(avg::AggAvg::try_new(
                Arc::new(TryCastExpr::new(children[0].clone(), return_type.clone())),
                return_type,
            )?)
        }
        AggFunction::MAX => {
            let arg_type = children[0].data_type(input_schema)?;
            let return_type = aggregate_function::return_type(
                &aggregate_function::AggregateFunction::Max,
                &[arg_type],
            )?;
            Arc::new(max::AggMax::try_new(
                Arc::new(TryCastExpr::new(children[0].clone(), return_type.clone())),
                return_type,
            )?)
        }
        AggFunction::MIN => {
            let arg_type = children[0].data_type(input_schema)?;
            let return_type = aggregate_function::return_type(
                &aggregate_function::AggregateFunction::Min,
                &[arg_type],
            )?;
            Arc::new(min::AggMin::try_new(
                Arc::new(TryCastExpr::new(children[0].clone(), return_type.clone())),
                return_type,
            )?)
        }
    })
}

fn save_scalar(scalar: &ScalarValue, builder: &mut Box<dyn ArrayBuilder>) -> Result<()> {
    macro_rules! handle {
        ($tyname:ident, $partial_value:expr) => {{
            let builder = builder
                .as_any_mut()
                .downcast_mut::<paste! {[<$tyname Builder>]}>()
                .unwrap();
            if let Some(partial_value) = $partial_value {
                builder.append_value(*partial_value);
            } else {
                builder.append_null();
            }
        }};
    }
    match scalar {
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
        ScalarValue::Decimal128(v, _, _) => {
            type Decimal128Builder = ConfiguredDecimal128Builder;
            handle!(Decimal128, v)
        }
        ScalarValue::Utf8(v) => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap();
            if let Some(partial_value) = v {
                builder.append_value(partial_value);
            } else {
                builder.append_null();
            }
        }
        ScalarValue::Date32(v) => handle!(Date32, v),
        ScalarValue::Date64(v) => handle!(Date64, v),
        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "unsupported data type: {}",
                other
            )));
        }
    }
    Ok(())
}

fn load_scalar(scalar: &mut ScalarValue, value: &ArrayRef, row_idx: usize) -> Result<()> {
    macro_rules! handle {
        ($tyname:ident, $partial_value:expr) => {{
            type TArray = paste! {[<$tyname Array>]};
            let value = downcast_value!(value, TArray);
            if value.is_valid(row_idx) {
                *$partial_value = Some(value.value(row_idx));
            } else {
                *$partial_value = None;
            }
        }};
    }
    match scalar {
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
        ScalarValue::Decimal128(v, _, _) => {
            let value = downcast_value!(value, Decimal128Array);
            if value.is_valid(row_idx) {
                *v = Some(value.value(row_idx));
            } else {
                *v = None;
            }
        }
        ScalarValue::Utf8(v) => {
            let value = downcast_value!(value, StringArray);
            if value.is_valid(row_idx) {
                *v = Some(value.value(row_idx).to_owned());
            } else {
                *v = None;
            }
        }
        ScalarValue::Date32(v) => handle!(Date32, v),
        ScalarValue::Date64(v) => handle!(Date64, v),
        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "unsupported data type: {}",
                other
            )));
        }
    }
    Ok(())
}
