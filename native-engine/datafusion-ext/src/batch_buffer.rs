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

use datafusion::arrow::array::make_builder;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;

pub struct MutableRecordBatch {
    pub(crate) arrays: Vec<Box<dyn ArrayBuilder>>,
    target_batch_size: usize,
    current_size: usize,
    schema: Arc<Schema>,
}

impl MutableRecordBatch {
    pub fn new(target_batch_size: usize, schema: Arc<Schema>) -> Self {
        let arrays = new_arrays(&schema, target_batch_size);
        Self {
            arrays,
            target_batch_size,
            current_size: 0,
            schema,
        }
    }

    pub fn output_and_reset(&mut self) -> ArrowResult<RecordBatch> {
        let result = self.output();
        let mut new = new_arrays(&self.schema, self.target_batch_size);
        self.arrays.append(&mut new);
        result
    }

    pub fn output(&mut self) -> ArrowResult<RecordBatch> {
        let result = make_batch(self.schema.clone(), self.arrays.drain(..).collect());
        self.current_size = 0;
        result
    }

    pub fn append(&mut self, size: usize) {
        self.current_size += size;
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.current_size > self.target_batch_size
    }
}

pub fn new_arrays(schema: &Arc<Schema>, batch_size: usize) -> Vec<Box<dyn ArrayBuilder>> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let dt = field.data_type();
            make_builder(dt, batch_size)
        })
        .collect::<Vec<_>>()
}

pub fn make_batch(
    schema: Arc<Schema>,
    mut arrays: Vec<Box<dyn ArrayBuilder>>,
) -> ArrowResult<RecordBatch> {
    let columns = arrays.iter_mut().map(|array| array.finish()).collect();
    RecordBatch::try_new(schema, columns)
}
