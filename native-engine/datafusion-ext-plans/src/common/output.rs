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
    future::Future,
    io::{Cursor, Write},
    panic::AssertUnwindSafe,
    sync::{Arc, Weak},
};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use blaze_jni_bridge::is_task_running;
use datafusion::{
    common::Result,
    execution::context::TaskContext,
    physical_plan::{
        metrics::ScopedTimerGuard, stream::RecordBatchReceiverStream, SendableRecordBatchStream,
    },
};
use datafusion_ext_commons::{
    df_execution_err,
    io::{read_one_batch, write_one_batch},
};
use futures::{FutureExt, StreamExt};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use tokio::sync::mpsc::Sender;

use crate::memmgr::{onheap_spill::try_new_spill, MemConsumer, MemManager};

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
            sender,
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
                    wrapped
                        .sender
                        .try_send(df_execution_err!("task completed/cancelled"))
                        .unwrap_or_default();
                    false
                }
                Some(_) => true, // do not modify senders from other tasks
                None => false,   // already released
            })
            .collect();
    }

    pub async fn send(
        &self,
        batch_result: Result<RecordBatch>,
        mut stop_timer: Option<&mut ScopedTimerGuard<'_>>,
    ) {
        // panic if we meet an error
        let batch = batch_result
            .unwrap_or_else(|err| panic!("output_with_sender: received an error: {err}"));

        stop_timer.iter_mut().for_each(|timer| timer.stop());
        self.sender
            .send(Ok(batch))
            .await
            .unwrap_or_else(|err| panic!("output_with_sender: send error: {err}"));
        stop_timer.iter_mut().for_each(|timer| timer.restart());
    }
}

pub trait TaskOutputter {
    fn output_with_sender<Fut: Future<Output = Result<()>> + Send>(
        &self,
        desc: &'static str,
        output_schema: SchemaRef,
        output: impl FnOnce(Arc<WrappedRecordBatchSender>) -> Fut + Send + 'static,
    ) -> Result<SendableRecordBatchStream>;

    fn output_bufferable_with_spill(
        &self,
        mem_consumer: Arc<dyn MemConsumer>,
        stream: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream>;

    fn cancel_task(&self);
}

impl TaskOutputter for Arc<TaskContext> {
    fn output_with_sender<Fut: Future<Output = Result<()>> + Send>(
        &self,
        desc: &'static str,
        output_schema: SchemaRef,
        output: impl FnOnce(Arc<WrappedRecordBatchSender>) -> Fut + Send + 'static,
    ) -> Result<SendableRecordBatchStream> {
        let mut stream_builder = RecordBatchReceiverStream::builder(output_schema, 1);
        let err_sender = stream_builder.tx().clone();
        let wrapped_sender =
            WrappedRecordBatchSender::new(self.clone(), stream_builder.tx().clone());

        stream_builder.spawn(async move {
            let result = AssertUnwindSafe(async move {
                if let Err(err) = output(wrapped_sender).await {
                    panic!("output_with_sender[{desc}]: output() returns error: {err}");
                }
            })
            .catch_unwind()
            .await
            .map(|_| Ok(()))
            .unwrap_or_else(|err| {
                let panic_message =
                    panic_message::get_panic_message(&err).unwrap_or("unknown error");
                df_execution_err!("{panic_message}")
            });

            if let Err(err) = result {
                let err_message = err.to_string();
                err_sender
                    .send(df_execution_err!("{err}"))
                    .await
                    .unwrap_or_default();

                // panic current spawn
                let task_running = is_task_running();
                if !task_running {
                    panic!("output_with_sender[{desc}] canceled due to task finished/killed");
                } else {
                    panic!("output_with_sender[{desc}] error: {err_message}");
                }
            }
        });
        Ok(stream_builder.build())
    }

    fn output_bufferable_with_spill(
        &self,
        mem_consumer: Arc<dyn MemConsumer>,
        mut stream: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream> {
        let schema = stream.schema();
        let desc = "OutputBufferableWithSpill";
        self.output_with_sender(desc, schema.clone(), move |sender| async move {
            while let Some(batch) = {
                // if consumer is holding too much memory, we will create a spill
                // to receive all of its outputs and release all memory.
                // outputs can be read from spill later.
                if MemManager::get().num_consumers() > 1 && mem_consumer.mem_used_percent() > 0.8 {
                    let spill = try_new_spill()?;
                    let mut spill_writer = spill.get_buf_writer();

                    // write all batches to spill
                    while let Some(batch) = stream.next().await.transpose()? {
                        let mut buf = vec![];
                        write_one_batch(&batch, &mut Cursor::new(&mut buf), true, None)?;
                        spill_writer.write_all(&buf)?;
                    }
                    spill_writer.flush()?;
                    spill.complete()?;

                    // read all batches from spill and output
                    let mut spill_reader = spill.get_buf_reader();
                    while let Some(batch) =
                        read_one_batch(&mut spill_reader, Some(schema.clone()), true)?
                    {
                        sender.send(Ok(batch), None).await;
                    }
                    return Ok(());
                }
                stream.next().await.transpose()
            }? {
                sender.send(Ok(batch), None).await;
            }
            Ok(())
        })
    }

    fn cancel_task(&self) {
        WrappedRecordBatchSender::cancel_task(self);
    }
}
