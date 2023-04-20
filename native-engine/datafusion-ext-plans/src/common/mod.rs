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

use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use blaze_commons::is_task_running;
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{FutureExt, TryFutureExt};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::Sender;

pub mod memory_manager;
pub mod onheap_spill;

pub struct BatchesInterleaver {
    schema: SchemaRef,
    batches_arrays: Vec<Vec<ArrayRef>>,
}

impl BatchesInterleaver {
    pub fn new(schema: SchemaRef, batches: &[RecordBatch]) -> Self {
        let mut batches_arrays: Vec<Vec<ArrayRef>> = schema
            .fields()
            .iter()
            .map(|_| Vec::with_capacity(batches.len()))
            .collect();
        for batch in batches {
            for (col_idx, column) in batch.columns().iter().enumerate() {
                batches_arrays[col_idx].push(column.clone());
            }
        }

        Self {
            schema,
            batches_arrays,
        }
    }

    pub fn interleave(&self, indices: &[(usize, usize)]) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new_with_options(
            self.schema.clone(),
            self.batches_arrays
                .iter()
                .map(|arrays| {
                    arrow::compute::interleave(
                        &arrays
                            .iter()
                            .map(|array| array.as_ref())
                            .collect::<Vec<_>>(),
                        indices,
                    )
                })
                .collect::<ArrowResult<Vec<_>>>()?,
            &RecordBatchOptions::new().with_row_count(Some(indices.len())),
        )?)
    }
}

pub struct WrappedRecordBatchSender(Sender<Result<RecordBatch>>);
impl WrappedRecordBatchSender {
    pub async fn send(
        &self,
        r: Result<RecordBatch>,
    ) -> std::result::Result<(), SendError<Result<RecordBatch>>> {
        // panic if we meet an error
        let send_result =
            self.0.send(Ok(r.unwrap_or_else(|err| {
                panic!("output_with_sender: received an error: {}", err)
            })));
        send_result.await
    }
}

pub fn output_with_sender<Fut: Future<Output = Result<()>> + Send>(
    desc: &'static str,
    output_schema: SchemaRef,
    output: impl FnOnce(WrappedRecordBatchSender) -> Fut + Send + 'static,
) -> Result<SendableRecordBatchStream> {
    let (sender, receiver) = tokio::sync::mpsc::channel(2);
    let err_sender = sender.clone();

    let join_handle = tokio::task::spawn(async move {
        let wrapped = WrappedRecordBatchSender(sender);
        let result = AssertUnwindSafe(async move {
            output(wrapped)
                .unwrap_or_else(|err| {
                    panic!(
                        "output_with_sender[{}]: output() returns error: {}",
                        desc, err,
                    );
                })
                .await
        })
        .catch_unwind()
        .await
        .map(|_| Ok(()))
        .unwrap_or_else(|err| {
            let panic_message = panic_message::get_panic_message(&err).unwrap_or("unknown error");
            Err(DataFusionError::Execution(panic_message.to_owned()))
        });

        if let Err(err) = result {
            let err_message = err.to_string();
            let _ = err_sender.send(Err(err)).await;

            // panic current spawn
            panic!(
                "output_with_sender[{}] error (task_running={}: {}",
                desc,
                is_task_running(),
                err_message,
            );
        }
    });

    Ok(RecordBatchReceiverStream::create(
        &output_schema,
        receiver,
        join_handle,
    ))
}
