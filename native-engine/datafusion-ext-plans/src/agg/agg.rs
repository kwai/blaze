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

use std::{any::Any, fmt::Debug, sync::Arc};

use arrow::{
    array::{ArrayRef, AsArray, RecordBatch},
    datatypes::{DataType, Int64Type, Schema, SchemaRef},
};
use datafusion::{common::Result, physical_expr::PhysicalExpr};
use datafusion_ext_commons::df_execution_err;
use datafusion_ext_exprs::cast::TryCastExpr;

use crate::agg::{
    acc::AccColumnRef, avg, bloom_filter, brickhouse, collect, first, first_ignores_null, maxmin,
    sum, AggFunction,
};

pub trait Agg: Send + Sync + Debug {
    fn as_any(&self) -> &dyn Any;
    fn exprs(&self) -> Vec<Arc<dyn PhysicalExpr>>;
    fn data_type(&self) -> &DataType;
    fn nullable(&self) -> bool;
    fn create_acc_column(&self, num_rows: usize) -> AccColumnRef;
    fn with_new_exprs(&self, exprs: Vec<Arc<dyn PhysicalExpr>>) -> Result<Arc<dyn Agg>>;

    fn prepare_partial_args(&self, partial_inputs: &[ArrayRef]) -> Result<Vec<ArrayRef>> {
        // default implementation: directly return the inputs
        Ok(partial_inputs.iter().cloned().collect())
    }

    fn partial_update(
        &self,
        accs: &mut AccColumnRef,
        acc_idx: IdxSelection<'_>,
        partial_args: &[ArrayRef],
        partial_arg_idx: IdxSelection<'_>,
    ) -> Result<()>;

    fn partial_merge(
        &self,
        accs: &mut AccColumnRef,
        acc_idx: IdxSelection<'_>,
        merging_accs: &mut AccColumnRef,
        merging_acc_idx: IdxSelection<'_>,
    ) -> Result<()>;

    fn final_merge(&self, accs: &mut AccColumnRef, acc_idx: IdxSelection<'_>) -> Result<ArrayRef>;
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum IdxSelection<'a> {
    Single(usize),
    Indices(&'a [usize]),
    IndicesU32(&'a [u32]),
    Range(usize, usize),
}

impl IdxSelection<'_> {
    pub fn len(&self) -> usize {
        match *self {
            IdxSelection::Single(_) => 1,
            IdxSelection::Indices(indices) => indices.len(),
            IdxSelection::IndicesU32(indices) => indices.len(),
            IdxSelection::Range(begin, end) => end - begin,
        }
    }
}

#[macro_export]
macro_rules! idx_with_iter {
    (($iter_var:ident @ $iter:expr) => $($s:stmt);* ) => {
        #[allow(unused_mut)]
        match $iter {
            IdxSelection::Single(idx) => {
                let mut $iter_var = [idx].into_iter();
                $($s)*
            }
            IdxSelection::Indices(indices) => {
                let mut $iter_var = indices.iter().copied();
                $($s)*
            }
            IdxSelection::IndicesU32(indices) => {
                let mut $iter_var = indices.iter().map(|v| *v as usize);
                $($s)*
            }
            IdxSelection::Range(begin, end) => {
                let mut $iter_var = begin..end;
                $($s)*
            },
        }
    }
}

#[macro_export]
macro_rules! idx_for {
    (($var:ident in $iter:expr) => $($s:stmt);* ) => {{
        crate::idx_with_iter!((iter @ $iter) => {
            for $var in iter {
                $($s)*
            }
        })
    }}
}

#[macro_export]
macro_rules! idx_for_zipped {
    ((($var1:ident, $var2:ident) in ($iter1:expr, $iter2:expr)) => $($s:stmt);* ) => {{
        match ($iter1, $iter2) {
            (IdxSelection::Single(idx1), iter2) => {
                let $var1 = idx1;
                $crate::idx_for! {
                    ($var2 in iter2) => {
                        $($s)*
                    }
                }
            }
            (iter1, IdxSelection::Single(idx2)) => {
                let $var2 = idx2;
                $crate::idx_for! {
                    ($var1 in iter1) => {
                        $($s)*
                    }
                }
            },
            _ => {
                crate::idx_with_iter!((iter1 @ $iter1) => {
                    crate::idx_with_iter!((iter2 @ $iter2) => {
                        for ($var1, $var2) in iter1.zip(iter2) {
                            $($s)*
                        }
                    })
                })
            }
        }
    }}
}

pub fn create_agg(
    agg_function: AggFunction,
    children: &[Arc<dyn PhysicalExpr>],
    input_schema: &SchemaRef,
) -> Result<Arc<dyn Agg>> {
    use arrow::datatypes::DataType;
    use datafusion::logical_expr::type_coercion::aggregates::*;

    use crate::agg::count;

    Ok(match agg_function {
        AggFunction::Count => {
            let return_type = DataType::Int64;
            let children = children
                .iter()
                .filter(|expr| {
                    expr.nullable(input_schema)
                        .expect("error evaluating child.nullable()")
                })
                .cloned()
                .collect::<Vec<_>>();
            Arc::new(count::AggCount::try_new(children, return_type)?)
        }
        AggFunction::Sum => {
            let arg_type = children[0].data_type(input_schema)?;
            let return_type = match arg_type {
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                    DataType::Int64
                }
                other => sum_return_type(&other)?,
            };
            Arc::new(sum::AggSum::try_new(
                Arc::new(TryCastExpr::new(children[0].clone(), return_type.clone())),
                return_type,
            )?)
        }
        AggFunction::Avg => {
            let arg_type = children[0].data_type(input_schema)?;
            let return_type = avg_return_type("avg", &arg_type)?;
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
            Arc::new(collect::AggCollectList::try_new(
                children[0].clone(),
                return_type,
                arg_type,
            )?)
        }
        AggFunction::CollectSet => {
            let arg_type = children[0].data_type(input_schema)?;
            let return_type = DataType::new_list(arg_type.clone(), true);
            Arc::new(collect::AggCollectSet::try_new(
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