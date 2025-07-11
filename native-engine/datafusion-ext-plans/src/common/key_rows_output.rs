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

use std::{pin::Pin, sync::Arc};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch, row::Rows};
use datafusion::{common::Result, physical_expr::PhysicalSortExpr};
use futures::Stream;

/// A batch with associated key rows for optimization
#[derive(Debug)]
pub struct RecordBatchWithKeyRows {
    pub batch: RecordBatch,
    pub key_rows: Arc<Rows>,
}

impl RecordBatchWithKeyRows {
    pub fn new(batch: RecordBatch, key_rows: Arc<Rows>) -> Self {
        Self { batch, key_rows }
    }
}

/// A stream that yields RecordBatch with associated key rows
pub trait RecordBatchWithKeyRowsStream: Stream<Item = Result<RecordBatchWithKeyRows>> {
    fn schema(&self) -> SchemaRef;
    fn keys(&self) -> &[PhysicalSortExpr];
}

pub type SendableRecordBatchWithKeyRowsStream = Pin<Box<dyn RecordBatchWithKeyRowsStream + Send>>;
