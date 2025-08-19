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

use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch, row::Rows};
use datafusion::{common::Result, physical_expr::PhysicalSortExpr};
use futures::Stream;
use futures_util::StreamExt;

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

pub struct RecordBatchWithKeyRowsStreamAdapter {
    stream: Pin<Box<dyn Stream<Item = Result<RecordBatchWithKeyRows>> + Send>>,
    schema: SchemaRef,
    keys: Vec<PhysicalSortExpr>,
}

impl RecordBatchWithKeyRowsStreamAdapter {
    pub fn new(
        stream: impl Stream<Item = Result<RecordBatchWithKeyRows>> + Send + 'static,
        schema: SchemaRef,
        keys: Vec<PhysicalSortExpr>,
    ) -> Self {
        Self {
            stream: Box::pin(stream),
            schema,
            keys,
        }
    }
}

impl Stream for RecordBatchWithKeyRowsStreamAdapter {
    type Item = Result<RecordBatchWithKeyRows>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

impl RecordBatchWithKeyRowsStream for RecordBatchWithKeyRowsStreamAdapter {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn keys(&self) -> &[PhysicalSortExpr] {
        &self.keys
    }
}
