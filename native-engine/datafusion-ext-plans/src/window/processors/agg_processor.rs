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
use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::{Result, ScalarValue};
use crate::agg::Agg;
use crate::agg::agg_buf::{AggBuf, create_agg_buf_from_scalar};
use crate::window::window_context::WindowContext;
use crate::window::WindowFunctionProcessor;

pub struct AggProcessor {
    cur_partition: Box<[u8]>,
    agg: Arc<dyn Agg>,
    agg_buf_init: AggBuf,
    agg_buf: AggBuf,
    agg_buf_addrs: Box<[u64]>,
}

impl AggProcessor {
    pub fn try_new(
        agg: Arc<dyn Agg>,
    ) -> Result<Self> {
        let (agg_buf, agg_buf_addrs) = create_agg_buf_from_scalar(agg.accums_initial())?;
        Ok(Self {
            cur_partition: Box::default(),
            agg,
            agg_buf_init: agg_buf.clone(),
            agg_buf,
            agg_buf_addrs,
        })
    }
}

impl WindowFunctionProcessor for AggProcessor {
    fn process_batch(
        &mut self,
        context: &WindowContext,
        batch: &RecordBatch,
    ) -> Result<ArrayRef> {
        let partition_rows = context.get_partition_rows(batch)?;
        let mut output = vec![];

        let children_cols: Vec<ArrayRef> = self.agg.exprs()
            .iter()
            .map(|expr| expr.evaluate(batch).map(|v| v.into_array(batch.num_rows())))
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
                self.agg_buf = self.agg_buf_init.clone();
            }
            self.agg.partial_update(
                &mut self.agg_buf,
                &self.agg_buf_addrs,
                &children_cols,
                row_idx,
            ).map_err(|err| err.context("window: agg_processor partial_update() error"))?;

            output.push(
                self.agg.final_merge(
                    &mut self.agg_buf.clone(),
                    &self.agg_buf_addrs,
                )?);
        }
        Ok(Arc::new(ScalarValue::iter_to_array(output.into_iter())?))
    }
}