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

use std::sync::Arc;

use arrow::{array::ArrayRef, record_batch::RecordBatch};
use datafusion::common::{Result, ScalarValue};
use datafusion_ext_commons::slim_bytes::SlimBytes;

use crate::{
    agg::{
        acc::{create_acc_from_initial_value, OwnedAccumStateRow},
        Agg,
    },
    window::{window_context::WindowContext, WindowFunctionProcessor},
};

pub struct AggProcessor {
    cur_partition: SlimBytes,
    agg: Arc<dyn Agg>,
    acc_init: OwnedAccumStateRow,
    acc: OwnedAccumStateRow,
}

impl AggProcessor {
    pub fn try_new(agg: Arc<dyn Agg>) -> Result<Self> {
        let (acc, accum_state_val_addrs) = create_acc_from_initial_value(agg.accums_initial())?;

        let mut agg = agg;
        unsafe {
            // safety - accum_state_val_addrs is guaranteed not to be used at this time
            Arc::get_mut_unchecked(&mut agg).set_accum_state_val_addrs(&accum_state_val_addrs);
        }

        Ok(Self {
            cur_partition: Default::default(),
            agg,
            acc_init: acc.clone(),
            acc,
        })
    }
}

impl WindowFunctionProcessor for AggProcessor {
    fn process_batch(&mut self, context: &WindowContext, batch: &RecordBatch) -> Result<ArrayRef> {
        let partition_rows = context.get_partition_rows(batch)?;
        let mut output = vec![];

        let children_cols: Vec<ArrayRef> = self
            .agg
            .exprs()
            .iter()
            .map(|expr| {
                expr.evaluate(batch)
                    .and_then(|v| v.into_array(batch.num_rows()))
            })
            .collect::<Result<_>>()?;

        for row_idx in 0..batch.num_rows() {
            let same_partition = !context.has_partition() || {
                let partition_row = partition_rows.row(row_idx);
                if partition_row.as_ref() != self.cur_partition.as_ref() {
                    self.cur_partition = partition_row.as_ref().into();
                    false
                } else {
                    true
                }
            };

            if !same_partition {
                self.acc = self.acc_init.clone();
            }
            self.agg
                .partial_update(&mut self.acc.as_mut(), &children_cols, row_idx)
                .map_err(|err| err.context("window: agg_processor partial_update() error"))?;
            output.push(self.agg.final_merge(&mut self.acc.clone().as_mut())?);
        }
        Ok(Arc::new(ScalarValue::iter_to_array(output.into_iter())?))
    }

    fn process_batch_without_partitions(
        &mut self,
        _: &WindowContext,
        batch: &RecordBatch,
    ) -> Result<ArrayRef> {
        let mut output = vec![];

        let children_cols: Vec<ArrayRef> = self
            .agg
            .exprs()
            .iter()
            .map(|expr| {
                expr.evaluate(batch)
                    .and_then(|v| v.into_array(batch.num_rows()))
            })
            .collect::<Result<_>>()?;

        for row_idx in 0..batch.num_rows() {
            self.agg
                .partial_update(&mut self.acc.as_mut(), &children_cols, row_idx)
                .map_err(|err| err.context("window: agg_processor partial_update() error"))?;
            output.push(self.agg.final_merge(&mut self.acc.clone().as_mut())?);
        }
        Ok(Arc::new(ScalarValue::iter_to_array(output.into_iter())?))
    }
}
