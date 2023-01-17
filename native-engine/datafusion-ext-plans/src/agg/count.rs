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
use datafusion::common::{downcast_value, Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::physical_expr::PhysicalExpr;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub struct AggCount {
    child: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    accum_fields: Vec<Field>,
    accums_initial: Vec<ScalarValue>,
}

impl AggCount {
    pub fn try_new(child: Arc<dyn PhysicalExpr>, data_type: DataType) -> Result<Self> {
        assert_eq!(data_type, DataType::Int64);

        let accum_fields = vec![Field::new("count", data_type.clone(), true)];
        let accums_initial = vec![ScalarValue::Int64(Some(0))];
        Ok(Self {
            child,
            data_type,
            accum_fields,
            accums_initial,
        })
    }
}

impl Debug for AggCount {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Count({:?})", self.child)
    }
}

impl Agg for AggCount {
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
        false
    }

    fn accum_fields(&self) -> &[Field] {
        &self.accum_fields
    }

    fn accums_initial(&self) -> &[ScalarValue] {
        &self.accums_initial
    }

    fn partial_update(&self, accums: &mut [ScalarValue], values: &[ArrayRef], row_idx: usize) -> Result<()> {
        match &mut accums[0] {
            ScalarValue::Int64(Some(count)) => {
                *count += values[0].is_valid(row_idx) as i64
            },
            _ => unreachable!()
        }
        Ok(())
    }

    fn partial_update_all(&self, accums: &mut [ScalarValue], values: &[ArrayRef]) -> Result<()> {
        match &mut accums[0] {
            ScalarValue::Int64(Some(count)) => {
                *count += (values[0].len() - values[0].null_count()) as i64;
            },
            _ => unreachable!()
        }
        Ok(())
    }

    fn partial_merge(&self, accums: &mut [ScalarValue], values: &[ArrayRef], row_idx: usize) -> Result<()> {
        let value = downcast_value!(values[0], Int64Array);
        match &mut accums[0] {
            ScalarValue::Int64(Some(count)) => *count += value.value(row_idx),
            _ => unreachable!()
        }
        Ok(())
    }

    fn partial_merge_scalar(&self, accums: &mut [ScalarValue], values: &mut [ScalarValue]) -> Result<()> {
        match (&mut accums[0], &values[0]) {
            (ScalarValue::Int64(Some(w)), ScalarValue::Int64(Some(v))) => {
                *w += v;
            }
            _ => unreachable!()
        }
        Ok(())
    }

    fn partial_merge_all(&self, accums: &mut [ScalarValue], values: &[ArrayRef]) -> Result<()> {
        let value = values[0].as_any().downcast_ref::<Int64Array>().unwrap();
        let sum = arrow::compute::sum(value);
        match &mut accums[0] {
            ScalarValue::Int64(Some(count)) => *count += sum.unwrap_or(0),
            _ => unreachable!()
        }
        Ok(())
    }
}
