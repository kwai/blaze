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
use std::sync::{Arc, Weak};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use blaze_jni_bridge::is_task_running;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::metrics::ScopedTimerGuard;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use futures::{FutureExt, TryFutureExt};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use tokio::sync::mpsc::Sender;

fn working_senders() -> &'static Mutex<Vec<Weak<WrappedRecordBatchSender>>> {
    static WORKING_SENDERS: OnceCell<Mutex<Vec<Weak<WrappedRecordBatchSender>>>> = OnceCell::new();
    WORKING_SENDERS.get_or_init(|| Mutex::default())
}

pub struct WrappedRecordBatchSender {
    task_context: Arc<TaskContext>,
    sender: Sender<Result<RecordBatch>>,
}

impl WrappedRecordBatchSender {
    pub fn new(task_context: Arc<TaskContext>, sender: Sender<Result<RecordBatch>>) -> Arc<Self> {
        let wrapped = Arc::new(Self {
            task_context,
            sender
        });
        let mut working_senders = working_senders().lock();
        working_senders.push(Arc::downgrade(&wrapped));
        wrapped
    }

    pub fn cancel_task(task_context: &Arc<TaskContext>) {
        let mut working_senders = working_senders().lock();
        *working_senders = std::mem::take(&mut *working_senders)
            .into_iter()
            .filter(|wrapped| match wrapped.upgrade() {
                Some(wrapped) if Arc::ptr_eq(&wrapped.task_context, task_context) => {
                    let _ = wrapped.sender.send(Err(DataFusionError::Execution(
                        format!("task completed/cancelled")
                    )));
                    false
                }
                Some(_) => true, // do not modify senders from other tasks
                None => false, // already released
            })
            .collect();
    }

    pub async fn send(
        &self,
        batch_result: Result<RecordBatch>,
        mut stop_timer: Option<&mut ScopedTimerGuard<'_>>,
    ) {
        // panic if we meet an error
        let batch = batch_result.unwrap_or_else(|err| {
            panic!("output_with_sender: received an error: {}", err)
        });

        stop_timer.iter_mut().for_each(|timer| timer.stop());
        self.sender.send(Ok(batch)).await.unwrap_or_else(|err| {
            panic!("output_with_sender: send error: {}", err)
        });
        stop_timer.iter_mut().for_each(|timer| timer.restart());
    }
}

pub fn output_with_sender<Fut: Future<Output = Result<()>> + Send>(
    desc: &'static str,
    task_context: Arc<TaskContext>,
    output_schema: SchemaRef,
    output: impl FnOnce(Arc<WrappedRecordBatchSender>) -> Fut + Send + 'static,
) -> Result<SendableRecordBatchStream> {
    let (sender, receiver) = tokio::sync::mpsc::channel(2);
    let err_sender = sender.clone();

    let join_handle = tokio::task::spawn(async move {
        let wrapped = WrappedRecordBatchSender::new(task_context, sender);
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
