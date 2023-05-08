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

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use blaze_commons::is_task_running;
use datafusion::common::DataFusionError;
use datafusion::common;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::metrics::ScopedTimerGuard;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use futures::{FutureExt, TryFutureExt};
use tokio::sync::mpsc::Sender;

pub struct WrappedRecordBatchSender(Sender<common::Result<RecordBatch>>);

impl WrappedRecordBatchSender {
    pub async fn send(
        &self,
        batch_result: common::Result<RecordBatch>,
        mut stop_timer: Option<&mut ScopedTimerGuard<'_>>,
    ) {
        // panic if we meet an error
        let batch = batch_result.unwrap_or_else(|err| {
            panic!("output_with_sender: received an error: {}", err)
        });

        stop_timer.iter_mut().for_each(|timer| timer.stop());
        self.0.send(Ok(batch)).await.unwrap_or_else(|err| {
            panic!("output_with_sender: send error: {}", err)
        });
        stop_timer.iter_mut().for_each(|timer| timer.restart());
    }
}

pub fn output_with_sender<Fut: Future<Output = common::Result<()>> + Send>(
    desc: &'static str,
    output_schema: SchemaRef,
    output: impl FnOnce(WrappedRecordBatchSender) -> Fut + Send + 'static,
) -> common::Result<SendableRecordBatchStream> {
    let (sender, receiver) = tokio::sync::mpsc::channel(2);
    let err_sender = sender.clone();

    let join_handle = tokio::task::spawn(async move {
        let wrapped = WrappedRecordBatchSender(sender);
        let result = AssertUnwindSafe(async move {
            output(wrapped)
                .unwrap_or_else(|err| {
                    panic!("output_with_sender[{}]: output() returns error: {}", desc, err);
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
            let task_running = is_task_running();
            if task_running {
                panic!(
                    "output_with_sender[{}] error (task_running={}: {}",
                    desc,
                    task_running,
                    err_message,
                );
            }
        }
    });

    Ok(RecordBatchReceiverStream::create(
        &output_schema,
        receiver,
        join_handle,
    ))
}
