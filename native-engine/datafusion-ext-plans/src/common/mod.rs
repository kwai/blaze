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

use std::future::Future;
use std::panic::AssertUnwindSafe;
use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use futures::FutureExt;
use tokio::sync::mpsc::Sender;
use blaze_commons::is_task_running;

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
                .map(|arrays| arrow::compute::interleave(
                    &arrays.iter().map(|array| array.as_ref()).collect::<Vec<_>>(),
                    indices))
                .collect::<ArrowResult<Vec<_>>>()?,
            &RecordBatchOptions::new().with_row_count(Some(indices.len())),
        )?)
    }
}

pub fn output_with_sender<Fut: Future<Output = Result<()>> + Send>(
    output_schema: SchemaRef,
    output: impl FnOnce(Sender<Result<RecordBatch>>) -> Fut + Send + 'static,
) -> Result<SendableRecordBatchStream> {

    let (sender, receiver) = tokio::sync::mpsc::channel(2);
    let err_sender = sender.clone();

    let join_handle = tokio::task::spawn(async move {
        let result = AssertUnwindSafe(async move {
            let err_sender = sender.clone();
            if let Err(err) = output(sender).await {
                err_sender.send(Err(err))
                    .await
                    .map_err(|err| DataFusionError::Execution(
                        format!("output_with_sender channel error: {}", err)
                    ))?;
            }
            Ok(())
        })
        .catch_unwind()
        .await
        .map_err(|err| DataFusionError::Execution(format!("{:?}", err)));

        if let Err(e) | Ok(Err(e)) = result {
            err_sender.send(Err(e)).await.unwrap_or_else(|err| {
                let task_running = is_task_running();
                log::warn!(
                    "output_with_sender broken (task_running={}): {}",
                    task_running,
                    err,
                );
                if task_running {
                    panic!("output_with_sender channel error: {}", err);
                }
            });
        }
    });

    Ok(RecordBatchReceiverStream::create(
        &output_schema,
        receiver,
        join_handle,
    ))
}

