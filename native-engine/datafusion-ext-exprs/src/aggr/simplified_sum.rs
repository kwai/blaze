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

use arrow::array::ArrayRef;
use arrow::datatypes::Field;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{Accumulator, AggregateState};
use datafusion::physical_expr::{AggregateExpr, PhysicalExpr};
use datafusion::physical_plan::RowAccumulator;
use datafusion::row::accessor::RowAccessor;
use std::any::Any;
use std::sync::Arc;

/// SUM aggregate expression (simplified by removing `count` state field)
#[derive(Debug)]
pub struct SimplifiedSum {
    inner: Arc<dyn AggregateExpr>,
}

impl SimplifiedSum {
    /// Create a new SUM aggregate function
    pub fn new(inner_sum: Arc<dyn AggregateExpr>) -> Self {
        Self { inner: inner_sum }
    }
}

impl AggregateExpr for SimplifiedSum {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        self.inner.field()
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SimplifiedSumAccumulator {
            inner: self.inner.create_accumulator()?,
        }))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        let mut state_fields = self.inner.state_fields()?;
        state_fields.truncate(1); // retain `sum` state
        Ok(state_fields)
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.inner.expressions()
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn row_accumulator_supported(&self) -> bool {
        self.inner.row_accumulator_supported()
    }

    fn create_row_accumulator(
        &self,
        start_index: usize,
    ) -> Result<Box<dyn RowAccumulator>> {
        Ok(Box::new(SimplifiedSumRowAccumulator {
            inner: self.inner.create_row_accumulator(start_index)?,
        }))
    }
}

#[derive(Debug)]
struct SimplifiedSumAccumulator {
    inner: Box<dyn Accumulator>,
}

impl Accumulator for SimplifiedSumAccumulator {
    fn state(&self) -> Result<Vec<AggregateState>> {
        let mut state = self.inner.state()?;
        state.truncate(1); // retain `sum` state
        Ok(state)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.update_batch(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        self.inner.evaluate()
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}

#[derive(Debug)]
struct SimplifiedSumRowAccumulator {
    inner: Box<dyn RowAccumulator>,
}

impl RowAccumulator for SimplifiedSumRowAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        self.inner.update_batch(values, accessor)
    }

    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        self.inner.merge_batch(states, accessor)
    }

    fn evaluate(&self, accessor: &RowAccessor) -> Result<ScalarValue> {
        self.inner.evaluate(accessor)
    }

    fn state_index(&self) -> usize {
        self.inner.state_index()
    }
}
