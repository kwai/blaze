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

pub struct AggSum {
    child: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    accum_fields: Vec<Field>,
    accums_initial: Vec<ScalarValue>,
    partial_updater: fn(&mut ScalarValue, &ArrayRef, usize),
}

impl AggSum {
    pub fn try_new(child: Arc<dyn PhysicalExpr>, data_type: DataType) -> Result<Self> {
        let accum_fields = vec![Field::new("sum", data_type.clone(), true)];
        let accums_initial = vec![ScalarValue::try_from(&data_type)?];
        let partial_updater = get_partial_updater(&data_type)?;
        Ok(Self {
            child,
            data_type,
            accum_fields,
            accums_initial,
            partial_updater,
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
        let partial_updater = self.partial_updater;
        partial_updater(&mut accums[0], &values[0], row_idx);
        Ok(())
    }

    fn partial_update_all(&self, accums: &mut [ScalarValue], values: &[ArrayRef]) -> Result<()> {
        macro_rules! handle {
            ($ty:ident) => {{
                type TArray = paste! {[<$ty Array>]};
                let value = values[0].as_any().downcast_ref::<TArray>().unwrap();
                if let Some(sum) = arrow::compute::sum(value) {
                    match &mut accums[0] {
                        ScalarValue::$ty(w @ None, ..) => *w = Some(sum),
                        ScalarValue::$ty(Some(w), ..) => *w += sum,
                        _ => unreachable!()
                    }
                }
            }}
        }
        match values[0].data_type() {
            DataType::Null => {},
            DataType::Float32 => handle!(Float32),
            DataType::Float64 => handle!(Float64),
            DataType::Int8 => handle!(Int8),
            DataType::Int16 => handle!(Int16),
            DataType::Int32 => handle!(Int32),
            DataType::Int64 => handle!(Int64),
            DataType::UInt8 => handle!(UInt8),
            DataType::UInt16 => handle!(UInt16),
            DataType::UInt32 => handle!(UInt32),
            DataType::UInt64 => handle!(UInt64),
            DataType::Decimal128(..) => handle!(Decimal128),
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "unsupported data type in max(): {}",
                    other
                )));
            }
        }
        Ok(())
    }

    fn partial_merge(&self, accums: &mut [ScalarValue], values: &[ArrayRef], row_idx: usize) -> Result<()> {
        self.partial_update(accums, values, row_idx)
    }

    fn partial_merge_scalar(&self, accums: &mut [ScalarValue], values: &mut [ScalarValue]) -> Result<()> {
        accums[0] = accums[0].add(std::mem::replace(&mut values[0], ScalarValue::Null))?;
        Ok(())
    }

    fn partial_merge_all(&self, accums: &mut [ScalarValue], values: &[ArrayRef]) -> Result<()> {
        self.partial_update_all(accums, values)
    }
}

fn get_partial_updater(
    dt: &DataType
) -> Result<fn(&mut ScalarValue, &ArrayRef, usize)> {

    macro_rules! get_fn {
        ($ty:ident) => {{
            Ok(|acc: &mut ScalarValue, v: &ArrayRef, i: usize| {
                type TArray = paste! {[<$ty Array>]};
                let value = v.as_any().downcast_ref::<TArray>().unwrap();
                if value.is_valid(i) {
                    let v = value.value(i);
                    match acc {
                        ScalarValue::$ty(w @ None, ..) => {
                            *w = Some(v);
                        }
                        ScalarValue::$ty(Some(w), ..) => {
                            *w += v;
                        }
                        _ => unreachable!(),
                    }
                }
            })
        }};
    }
    match dt {
        DataType::Null => Ok(|_, _, _| ()),
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