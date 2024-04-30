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

use std::{
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};

use arrow::{
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use datafusion::{
    common::Result,
    execution::TaskContext,
    physical_plan::{
        metrics::{BaselineMetrics, Time},
        RecordBatchStream, SendableRecordBatchStream,
    },
};
use futures::{Stream, StreamExt};

use crate::{array_size::ArraySize, batch_size, suggested_output_batch_mem_size};

pub trait CoalesceInput {
    fn coalesce_input(
        &self,
        input: SendableRecordBatchStream,
        batch_size: usize,
        metrics: &BaselineMetrics,
    ) -> Result<SendableRecordBatchStream>;

    fn coalesce_with_default_batch_size(
        &self,
        input: SendableRecordBatchStream,
        metrics: &BaselineMetrics,
    ) -> Result<SendableRecordBatchStream>;
}

impl CoalesceInput for Arc<TaskContext> {
    fn coalesce_input(
        &self,
        input: SendableRecordBatchStream,
        batch_size: usize,
        metrics: &BaselineMetrics,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(CoalesceStream::new(
            input,
            batch_size,
            metrics.elapsed_compute().clone(),
        )))
    }

    fn coalesce_with_default_batch_size(
        &self,
        input: SendableRecordBatchStream,
        metrics: &BaselineMetrics,
    ) -> Result<SendableRecordBatchStream> {
        self.coalesce_input(input, batch_size(), metrics)
    }
}

pub struct CoalesceStream {
    input: SendableRecordBatchStream,
    staging_batches: Vec<RecordBatch>,
    staging_rows: usize,
    staging_batches_mem_size: usize,
    batch_size: usize,
    elapsed_compute: Time,
}

impl CoalesceStream {
    pub fn new(input: SendableRecordBatchStream, batch_size: usize, elapsed_compute: Time) -> Self {
        Self {
            input,
            staging_batches: vec![],
            staging_rows: 0,
            staging_batches_mem_size: 0,
            batch_size,
            elapsed_compute,
        }
    }

    fn coalesce(&mut self) -> Result<RecordBatch> {
        // better concat_batches() implementation that releases old batch columns asap.
        let schema = self.input.schema();

        // collect all columns
        let mut all_cols = schema.fields().iter().map(|_| vec![]).collect::<Vec<_>>();
        for batch in std::mem::take(&mut self.staging_batches) {
            for i in 0..all_cols.len() {
                all_cols[i].push(batch.column(i).clone());
            }
        }

        // coalesce each column
        let mut coalesced_cols = vec![];
        for cols in all_cols {
            let ref_cols = cols.iter().map(|col| col.as_ref()).collect::<Vec<_>>();
            coalesced_cols.push(arrow::compute::concat(&ref_cols)?);
        }
        let coalesced_batch = RecordBatch::try_new_with_options(
            schema,
            coalesced_cols,
            &RecordBatchOptions::new().with_row_count(Some(self.staging_rows)),
        )?;
        self.staging_rows = 0;
        self.staging_batches_mem_size = 0;
        Ok(coalesced_batch)
    }

    fn should_flush(&self) -> bool {
        let size_limit = suggested_output_batch_mem_size();
        let (batch_size_limit, mem_size_limit) = if self.staging_batches.len() > 1 {
            (self.batch_size, size_limit)
        } else {
            (self.batch_size / 2, size_limit / 2)
        };
        self.staging_rows >= batch_size_limit || self.staging_batches_mem_size > mem_size_limit
    }
}

impl RecordBatchStream for CoalesceStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

impl Stream for CoalesceStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let elapsed_time = self.elapsed_compute.clone();
        loop {
            match ready!(self.input.poll_next_unpin(cx)).transpose()? {
                Some(batch) => {
                    let _timer = elapsed_time.timer();
                    let num_rows = batch.num_rows();
                    if num_rows > 0 {
                        self.staging_rows += batch.num_rows();
                        self.staging_batches_mem_size += batch.get_array_mem_size();
                        self.staging_batches.push(batch);
                        if self.should_flush() {
                            let coalesced = self.coalesce()?;
                            return Poll::Ready(Some(Ok(coalesced)));
                        }
                        continue;
                    }
                }
                None if !self.staging_batches.is_empty() => {
                    let _timer = elapsed_time.timer();
                    let coalesced = self.coalesce()?;
                    return Poll::Ready(Some(Ok(coalesced)));
                }
                None => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}
