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
use datafusion::physical_expr::PhysicalExpr;
use paste::paste;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub struct AggMin {
    child: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    accum_fields: Vec<Field>,
    accums_initial: Vec<ScalarValue>,
    partial_updater: fn(&mut ScalarValue, &ArrayRef, usize),
}

impl AggMin {
    pub fn try_new(child: Arc<dyn PhysicalExpr>, data_type: DataType) -> Result<Self> {
        let accum_fields = vec![Field::new("min", data_type.clone(), true)];
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

impl Debug for AggMin {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Min({:?})", self.child)
    }
}

impl Agg for AggMin {
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

    fn partial_update(&self, accums: &mut [ScalarValue], values: &[ArrayRef], row_idx: usize) -> Result<()> {
        let partial_updater = self.partial_updater;
        partial_updater(&mut accums[0], &values[0], row_idx);
        Ok(())
    }

    fn partial_update_all(&self, accums: &mut [ScalarValue], values: &[ArrayRef]) -> Result<()> {
        macro_rules! handle {
            ($ty:ident, $minfun:ident) => {{
                type TArray = paste! {[<$ty Array>]};
                let value = values[0].as_any().downcast_ref::<TArray>().unwrap();
                if let Some(min) = arrow::compute::$minfun(value) {
                    match &mut accums[0] {
                        ScalarValue::$ty(w @ None, ..) => *w = Some(min),
                        ScalarValue::$ty(Some(w), ..) => {
                            if *w > min {
                                *w = min;
                            }
                        }
                        _ => unreachable!()
                    }
                }
            }}
        }
        match values[0].data_type() {
            DataType::Null => {},
            DataType::Boolean => handle!(Boolean, min_boolean),
            DataType::Float32 => handle!(Float32, min),
            DataType::Float64 => handle!(Float64, min),
            DataType::Int8 => handle!(Int8, min),
            DataType::Int16 => handle!(Int16, min),
            DataType::Int32 => handle!(Int32, min),
            DataType::Int64 => handle!(Int64, min),
            DataType::UInt8 => handle!(UInt8, min),
            DataType::UInt16 => handle!(UInt16, min),
            DataType::UInt32 => handle!(UInt32, min),
            DataType::UInt64 => handle!(UInt64, min),
            DataType::Decimal128(_, _) => handle!(Decimal128, min),
            DataType::Utf8 => {
                let value = values[0].as_any().downcast_ref::<StringArray>().unwrap();
                if let Some(min) = arrow::compute::min_string(value) {
                    match &mut accums[0] {
                        ScalarValue::Utf8(w @ None, ..) => *w = Some(min.to_owned()),
                        ScalarValue::Utf8(Some(w), ..) => {
                            if w.as_str() > min {
                                *w = min.to_owned();
                            }
                        }
                        _ => unreachable!()
                    }
                }
            }
            DataType::Date32 => handle!(Date32, min),
            DataType::Date64 => handle!(Date64, min),
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "unsupported data type in min(): {}",
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
        if accums[0].is_null() || accums[0] > values[0] {
            accums[0] = std::mem::replace(&mut values[0], ScalarValue::Null);
        }
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
                        ScalarValue::$ty(None) => {
                            *acc = ScalarValue::from(v);
                        }
                        ScalarValue::$ty(Some(w)) => {
                            if *w > v {
                                *w = v;
                            }
                        }
                        _ => unreachable!()
                    }
                }
            })
        }}
    }
    match dt {
        DataType::Null => Ok(|_, _, _| ()),
        DataType::Boolean => get_fn!(Boolean),
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
        DataType::Decimal128(_, _) => {
            Ok(|acc: &mut ScalarValue, v: &ArrayRef, i: usize| {
                let value = v.as_any().downcast_ref::<Decimal128Array>().unwrap();
                if value.is_valid(i) {
                    let v = value.value(i);
                    match acc {
                        ScalarValue::Decimal128(None, _, _) => {
                            *acc = ScalarValue::Decimal128(
                                Some(v),
                                value.precision(),
                                value.scale(),
                            );
                        }
                        ScalarValue::Decimal128(Some(w), _, _) => {
                            if *w > v {
                                *w = v;
                            }
                        }
                        _ => unreachable!()
                    }
                }
            })
        },
        DataType::Utf8 => {
            Ok(|acc: &mut ScalarValue, v: &ArrayRef, i: usize| {
                let value = v.as_any().downcast_ref::<StringArray>().unwrap();
                if value.is_valid(i) {
                    let v = value.value(i);
                    match acc {
                        ScalarValue::Null | ScalarValue::Utf8(None) => {
                            *acc = ScalarValue::from(v);
                        }
                        ScalarValue::Utf8(Some(w)) => {
                            if w.as_str() > v {
                                *w = v.to_owned();
                            }
                        }
                        _ => unreachable!()
                    }
                }
            })
        }
        DataType::Date32 => get_fn!(Date32),
        DataType::Date64 => get_fn!(Date64),
        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "unsupported data type in min(): {}",
                other
            )));
        }
    }
}
