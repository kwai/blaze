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
use datafusion::common::{downcast_value, Result};
use datafusion::error::DataFusionError;
use datafusion::physical_expr::PhysicalExpr;
use crate::agg::{AggAccum, Agg, AggAccumRef};

pub struct AggCount {
    child: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    accum_fields: Vec<Field>,
}

impl AggCount {
    pub fn try_new(child: Arc<dyn PhysicalExpr>, data_type: DataType) -> Result<Self> {
        assert_eq!(data_type, DataType::Int64);

        let accum_fields = vec![
            Field::new("count", data_type.clone(), true),
        ];
        Ok(Self {
            child,
            data_type: data_type,
            accum_fields,
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

    fn create_accum(&self) -> Result<AggAccumRef> {
        Ok(Box::new(AggCountAccum {
            partial: 0
        }))
    }
}

pub struct AggCountAccum {
    pub partial: i64,
}

impl AggAccum for AggCountAccum {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        Box::new(*self)
    }

    fn mem_size(&self) -> usize {
        std::mem::size_of_val(&self.partial)
    }

    fn load(&mut self, values: &[ArrayRef], row_idx: usize) -> Result<()> {
        let value = downcast_value!(values[0], Int64Array);
        self.partial = value.value(row_idx);
        Ok(())
    }

    fn save(&self, builders: &mut [Box<dyn ArrayBuilder>]) -> Result<()> {
        let builder = builders[0]
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .unwrap();
        builder.append_value(self.partial);
        Ok(())
    }

    fn save_final(&self, builder: &mut Box<dyn ArrayBuilder>) -> Result<()> {
        let builder = builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .unwrap();
        builder.append_value(self.partial);
        Ok(())
    }

    fn partial_update(&mut self, values: &[ArrayRef], row_idx: usize) -> Result<()> {
        if values[0].is_valid(row_idx) {
            self.partial += 1;
        }
        Ok(())
    }

    fn partial_update_all(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.partial += values[0].len() as i64 - values[0].null_count() as i64;
        Ok(())
    }

    fn partial_merge(&mut self, another: AggAccumRef) -> Result<()> {
        let another_cnt = another.into_any().downcast::<AggCountAccum>().unwrap();
        self.partial += another_cnt.partial;
        Ok(())
    }

    fn partial_merge_from_array(
        &mut self,
        partial_agg_values: &[ArrayRef],
        row_idx: usize,
    ) -> Result<()> {
        let another_cnts = partial_agg_values[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        self.partial += another_cnts.value(row_idx);
        Ok(())
    }
}
