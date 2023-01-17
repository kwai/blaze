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
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::aggregate_function;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_ext_exprs::cast::TryCastExpr;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

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
        accums: &mut [ScalarValue],
        values: &[ArrayRef],
        row_idx: usize,
    ) -> Result<()>;

    fn partial_update_all(
        &self,
        accums: &mut [ScalarValue],
        values: &[ArrayRef],
    ) -> Result<()>;

    fn partial_merge(
        &self,
        accums: &mut [ScalarValue],
        values: &[ArrayRef],
        row_idx: usize,
    ) -> Result<()>;

    fn partial_merge_scalar(
        &self,
        accums: &mut [ScalarValue],
        values: &mut [ScalarValue],
    ) -> Result<()>;

    fn partial_merge_all(
        &self,
        accums: &mut [ScalarValue],
        values: &[ArrayRef],
    ) -> Result<()>;

    fn final_merge(&self, accums: &mut [ScalarValue]) -> Result<ScalarValue> {
        // default implementation: use accums[0] as final value
        // works for sum/max/min/count etc
        Ok(std::mem::replace(&mut accums[0], ScalarValue::Null))
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
