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

mod agg_buf;
pub mod agg_context;
pub mod agg_tables;
pub mod avg;
pub mod count;
pub mod max;
pub mod min;
pub mod sum;

use crate::agg::agg_buf::{AggBuf, AggDynStr};
use arrow::array::*;
use arrow::datatypes::*;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::aggregate_function;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_ext_exprs::cast::TryCastExpr;
use derivative::Derivative;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

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
    fn accums_initial(&self) -> &[ScalarValue];

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

    fn final_merge(
        &self,
        agg_buf: &mut AggBuf,
        agg_buf_addrs: &[u64],
    ) -> Result<ScalarValue> {
        // default implementation:
        // extract the only one values from agg_buf and convert to ScalarValue
        // this works for sum/min/max
        let addr = agg_buf_addrs[0];

        macro_rules! handle_fixed {
            ($ty:ident) => {{
                if agg_buf.is_fixed_valid(addr) {
                    ScalarValue::$ty(Some(*agg_buf.fixed_value(addr)))
                } else {
                    ScalarValue::$ty(None)
                }
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
                    Some(*agg_buf.fixed_value(addr))
                } else {
                    None
                };
                ScalarValue::Decimal128(v, *prec, *scale)
            }
            DataType::Utf8 => ScalarValue::Utf8(
                agg_buf
                    .dyn_value(addr)
                    .as_any()
                    .downcast_ref::<AggDynStr>()
                    .unwrap()
                    .value
                    .as_ref()
                    .map(|s| s.as_ref().to_owned())
            ),
            DataType::Date32 => handle_fixed!(Date32),
            DataType::Date64 => handle_fixed!(Date64),
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "unsupported data type: {}",
                    other
                )));
            }
        })
    }
}

#[derive(Derivative)]
#[derivative(PartialOrd, PartialEq, Ord, Eq)]
pub struct AggRecord {
    pub grouping: Box<[u8]>,

    #[derivative(PartialOrd = "ignore")]
    #[derivative(PartialEq = "ignore")]
    #[derivative(Ord = "ignore")]
    pub agg_buf: AggBuf,
}

impl AggRecord {
    pub fn new(grouping: Box<[u8]>, agg_buf: AggBuf) -> Self {
        Self { grouping, agg_buf }
    }
}

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
