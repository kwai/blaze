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

use crate::agg::agg_buf::{AccumInitialValue, AggBuf, AggDynSet};
use crate::agg::Agg;
use arrow::array::*;
use arrow::datatypes::*;
use datafusion::common::{Result, ScalarValue};
use datafusion::physical_expr::PhysicalExpr;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub struct AggCollectSet {
    child: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    arg_type: DataType,
}

impl AggCollectSet {
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

impl Debug for AggCollectSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CollectSet({:?})", self.child)
    }
}

impl Agg for AggCollectSet {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn exprs(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.child.clone()]
    }

    fn with_new_exprs(&self, exprs: Vec<Arc<dyn PhysicalExpr>>) -> Result<Arc<dyn Agg>> {
        Ok(Arc::new(Self::try_new(
            exprs[0].clone(),
            self.data_type.clone(),
            self.arg_type.clone(),
        )?))
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn nullable(&self) -> bool {
        false
    }

    fn accums_initial(&self) -> &[AccumInitialValue] {
        &[AccumInitialValue::DynSet]
    }

    fn partial_update(
        &self,
        agg_buf: &mut AggBuf,
        agg_buf_addrs: &[u64],
        values: &[ArrayRef],
        row_idx: usize,
    ) -> Result<()> {
        let dyn_set = agg_buf
            .dyn_value_mut(agg_buf_addrs[0])
            .as_any_mut()
            .downcast_mut::<AggDynSet>()
            .unwrap();
        let values = &values[0];

        if values.is_valid(row_idx) {
            dyn_set.append(ScalarValue::try_from_array(&values, row_idx)?);
        }
        Ok(())
    }

    fn partial_update_all(
        &self,
        agg_buf: &mut AggBuf,
        agg_buf_addrs: &[u64],
        values: &[ArrayRef],
    ) -> Result<()> {
        let dyn_set = agg_buf
            .dyn_value_mut(agg_buf_addrs[0])
            .as_any_mut()
            .downcast_mut::<AggDynSet>()
            .unwrap();
        let values = &values[0];

        for i in 0..values.len() {
            if values.is_valid(i) {
                dyn_set.append(ScalarValue::try_from_array(&values, i)?);
            }
        }
        Ok(())
    }

    fn partial_merge(
        &self,
        agg_buf: &mut AggBuf,
        merging_agg_buf: &mut AggBuf,
        agg_buf_addrs: &[u64],
    ) -> Result<()> {
        let dyn_set1 = agg_buf
            .dyn_value_mut(agg_buf_addrs[0])
            .as_any_mut()
            .downcast_mut::<AggDynSet>()
            .unwrap();
        let dyn_set2 = merging_agg_buf
            .dyn_value_mut(agg_buf_addrs[0])
            .as_any_mut()
            .downcast_mut::<AggDynSet>()
            .unwrap();

        dyn_set1.merge(dyn_set2);
        Ok(())
    }

    fn final_merge(&self, agg_buf: &mut AggBuf, agg_buf_addrs: &[u64]) -> Result<ScalarValue> {
        let dyn_set = agg_buf
            .dyn_value_mut(agg_buf_addrs[0])
            .as_any_mut()
            .downcast_mut::<AggDynSet>()
            .unwrap();
        Ok(ScalarValue::new_list(
            Some(std::mem::take(&mut dyn_set.values).into_iter().collect()),
            self.arg_type.clone(),
        ))
    }

    fn final_batch_merge(
        &self,
        agg_bufs: &mut [AggBuf],
        agg_buf_addrs: &[u64],
    ) -> Result<ArrayRef> {
        let values: Vec<ScalarValue> = agg_bufs
            .iter_mut()
            .map(|agg_buf| self.final_merge(agg_buf, agg_buf_addrs))
            .collect::<Result<_>>()?;
        Ok(ScalarValue::iter_to_array(values)?)
    }
}
