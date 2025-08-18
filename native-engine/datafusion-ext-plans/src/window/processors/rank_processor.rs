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

use arrow::{
    array::{ArrayRef, Int32Builder},
    record_batch::RecordBatch,
};
use datafusion::common::Result;

use crate::window::{WindowFunctionProcessor, window_context::WindowContext};

pub struct RankProcessor {
    cur_partition: Vec<u8>,
    cur_order: Vec<u8>,
    cur_rank: i32,
    cur_equals: i32,
    is_dense: bool,
}

impl RankProcessor {
    pub fn new(is_dense: bool) -> Self {
        Self {
            cur_partition: Default::default(),
            cur_order: Default::default(),
            cur_rank: 0,
            cur_equals: 1,
            is_dense,
        }
    }
}

impl WindowFunctionProcessor for RankProcessor {
    fn process_batch(&mut self, context: &WindowContext, batch: &RecordBatch) -> Result<ArrayRef> {
        let partition_rows = context.get_partition_rows(batch)?;
        let order_rows = context.get_order_rows(batch)?;
        let mut builder = Int32Builder::with_capacity(batch.num_rows());

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
            let order_row = order_rows.row(row_idx);

            if same_partition {
                if order_row.as_ref() == &self.cur_order {
                    self.cur_equals += 1;
                } else {
                    self.cur_rank += if !self.is_dense { self.cur_equals } else { 1 };
                    self.cur_equals = 1;
                    self.cur_order = order_row.as_ref().into();
                }
            } else {
                self.cur_rank = 1;
                self.cur_equals = 1;
                self.cur_order = order_row.as_ref().into();
            }
            builder.append_value(self.cur_rank);
        }
        Ok(Arc::new(builder.finish()))
    }
}
