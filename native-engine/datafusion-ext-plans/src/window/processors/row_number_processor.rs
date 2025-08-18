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

pub struct RowNumberProcessor {
    cur_partition: Box<[u8]>,
    cur_row_number: i32,
}

impl Default for RowNumberProcessor {
    fn default() -> Self {
        Self::new()
    }
}

impl RowNumberProcessor {
    pub fn new() -> Self {
        Self {
            cur_partition: Box::default(),
            cur_row_number: 0,
        }
    }
}

impl WindowFunctionProcessor for RowNumberProcessor {
    fn process_batch(&mut self, context: &WindowContext, batch: &RecordBatch) -> Result<ArrayRef> {
        let partition_rows = context.get_partition_rows(batch)?;
        let mut builder = Int32Builder::with_capacity(batch.num_rows());

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
                self.cur_row_number = 0;
            }

            self.cur_row_number += 1;
            builder.append_value(self.cur_row_number);
        }
        Ok(Arc::new(builder.finish()))
    }
}
