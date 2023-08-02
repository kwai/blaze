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

use crate::agg::agg_buf::{AccumInitialValue, AggBuf, AggDynList};
use crate::agg::Agg;
use arrow::array::*;
use arrow::datatypes::*;
use datafusion::common::{Result, ScalarValue};
use datafusion::physical_expr::PhysicalExpr;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub struct AggCollectList {
    child: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    arg_type: DataType,
}

impl AggCollectList {
    pub fn try_new(
        child: Arc<dyn PhysicalExpr>,
        data_type: DataType,
        arg_type: DataType,
    ) -> Result<Self> {
        Ok(Self {
            child,
            data_type,
            arg_type,
        })
    }
}

impl Debug for AggCollectList {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CollectList({:?})", self.child)
    }
}

impl Agg for AggCollectList {
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

    fn accums_initial(&self) -> &[AccumInitialValue] {
        &[AccumInitialValue::DynList]
    }

    fn partial_update(
        &self,
        agg_buf: &mut AggBuf,
        agg_buf_addrs: &[u64],
        values: &[ArrayRef],
        row_idx: usize,
    ) -> Result<()> {
        let dyn_list = agg_buf
            .dyn_value_mut(agg_buf_addrs[0])
            .as_any_mut()
            .downcast_mut::<AggDynList>()
            .unwrap();
        let values = &values[0];

        dyn_list.append(ScalarValue::try_from_array(&values, row_idx)?);
        Ok(())
    }

    fn partial_update_all(
        &self,
        agg_buf: &mut AggBuf,
        agg_buf_addrs: &[u64],
        values: &[ArrayRef],
    ) -> Result<()> {
        let dyn_list = agg_buf
            .dyn_value_mut(agg_buf_addrs[0])
            .as_any_mut()
            .downcast_mut::<AggDynList>()
            .unwrap();
        let values = &values[0];

        for i in 0..values.len() {
            dyn_list.append(ScalarValue::try_from_array(&values, i)?);
        }
        Ok(())
    }

    fn partial_merge(
        &self,
        agg_buf: &mut AggBuf,
        merging_agg_buf: &mut AggBuf,
        agg_buf_addrs: &[u64],
    ) -> Result<()> {
        let dyn_list1 = agg_buf
            .dyn_value_mut(agg_buf_addrs[0])
            .as_any_mut()
            .downcast_mut::<AggDynList>()
            .unwrap();
        let dyn_list2 = merging_agg_buf
            .dyn_value_mut(agg_buf_addrs[0])
            .as_any_mut()
            .downcast_mut::<AggDynList>()
            .unwrap();

        dyn_list1.merge(dyn_list2);
        Ok(())
    }

    fn final_merge(&self, agg_buf: &mut AggBuf, agg_buf_addrs: &[u64]) -> Result<ScalarValue> {
        let dyn_list = agg_buf
            .dyn_value_mut(agg_buf_addrs[0])
            .as_any_mut()
            .downcast_mut::<AggDynList>()
            .unwrap();
        Ok(ScalarValue::new_list(
            Some(std::mem::take(&mut dyn_list.values)),
            self.arg_type.clone(),
        ))
    }
}
