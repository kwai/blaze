// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use arrow::{array::ArrayRef, record_batch::RecordBatch};
use datafusion::common::Result;
use datafusion_ext_commons::arrow::coalesce::coalesce_arrays_unchecked;

use crate::{
    agg::{
        acc::AccColumnRef,
        agg::{Agg, IdxSelection},
    },
    window::{WindowFunctionProcessor, window_context::WindowContext},
};

pub struct AggProcessor {
    cur_partition: Vec<u8>,
    agg: Arc<dyn Agg>,
    acc_col: AccColumnRef,
}

impl AggProcessor {
    pub fn try_new(agg: Arc<dyn Agg>) -> Result<Self> {
        let acc_col = agg.create_acc_column(1);
        Ok(Self {
            cur_partition: Default::default(),
            agg,
            acc_col,
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
                if partition_row.as_ref() != &self.cur_partition {
                    self.cur_partition = partition_row.as_ref().into();
                    false
                } else {
                    true
                }
            };

            if !same_partition {
                self.acc_col = self.agg.create_acc_column(1);
            }

            self.agg.partial_update(
                &mut self.acc_col,
                IdxSelection::Single(0),
                &children_cols,
                IdxSelection::Single(row_idx),
            )?;
            output.push(
                self.agg
                    .final_merge(&mut self.acc_col, IdxSelection::Single(0))?,
            );
        }
        Ok(Arc::new(coalesce_arrays_unchecked(
            self.agg.data_type(),
            &output,
        )))
    }
}
