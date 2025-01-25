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
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{Arc, Weak},
    task::{ready, Context, Poll},
    time::Instant,
};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use blaze_jni_bridge::{conf, conf::BooleanConf, is_task_running};
use datafusion::{
    common::Result,
    execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext},
    physical_plan::{
        metrics::{BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, Time},
        stream::{RecordBatchReceiverStream, RecordBatchStreamAdapter},
        ExecutionPlan,
    },
};
use datafusion_ext_commons::{
    arrow::{array_size::ArraySize, coalesce::coalesce_batches_unchecked},
    batch_size, df_execution_err, suggested_output_batch_mem_size,
};
use futures::{executor::block_on_stream, Stream, StreamExt};
use futures_util::FutureExt;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use tokio::sync::mpsc::Sender;

use crate::{
    common::{column_pruning::ExecuteWithColumnPruning, timer_helper::TimerHelper},
    memmgr::metrics::SpillMetrics,
};

pub struct ExecutionContext {
    task_ctx: Arc<TaskContext>,
    partition_id: usize,
    output_schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    baseline_metrics: BaselineMetrics,
    spill_metrics: Arc<OnceCell<SpillMetrics>>,
    input_stat_metrics: Arc<OnceCell<Option<InputBatchStatistics>>>,
}

impl ExecutionContext {
    pub fn new(
        task_ctx: Arc<TaskContext>,
        partition_id: usize,
        output_schema: SchemaRef,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Arc<Self> {
        Arc::new(Self {
            task_ctx,
            partition_id,
            output_schema,
            baseline_metrics: BaselineMetrics::new(&metrics, partition_id),
            metrics: metrics.clone(),
            spill_metrics: Arc::default(),
            input_stat_metrics: Arc::default(),
        })
    }

    pub fn with_new_output_schema(&self, output_schema: SchemaRef) -> Arc<Self> {
        Arc::new(Self {
            task_ctx: self.task_ctx.clone(),
            partition_id: self.partition_id,
            output_schema,
            metrics: self.metrics.clone(),
            baseline_metrics: self.baseline_metrics.clone(),
            spill_metrics: self.spill_metrics.clone(),
            input_stat_metrics: self.input_stat_metrics.clone(),
        })
    }

    pub fn task_ctx(&self) -> Arc<TaskContext> {
        self.task_ctx.clone()
    }

    pub fn partition_id(&self) -> usize {
        self.partition_id
    }

    pub fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    pub fn execution_plan_metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    pub fn baseline_metrics(&self) -> &BaselineMetrics {
        &self.baseline_metrics
    }

    pub fn spill_metrics(&self) -> &SpillMetrics {
        self.spill_metrics
            .get_or_init(|| SpillMetrics::new(&self.metrics, self.partition_id))
    }

    pub fn register_timer_metric(&self, name: &str) -> Time {
        MetricBuilder::new(self.execution_plan_metrics())
            .subset_time(name.to_owned(), self.partition_id)
    }

    pub fn register_counter_metric(&self, name: &str) -> Count {
        MetricBuilder::new(self.execution_plan_metrics())
            .counter(name.to_owned(), self.partition_id)
    }

    pub fn spawn_worker_thread_on_stream(
        self: &Arc<Self>,
        input: SendableRecordBatchStream,
    ) -> SendableRecordBatchStream {
        let (batch_sender, mut batch_receiver) = tokio::sync::mpsc::channel(1);

        tokio::task::spawn_blocking(move || {
            let mut blocking_stream = block_on_stream(input);
            while is_task_running()
                && let Some(batch_result) = blocking_stream.next()
            {
                if batch_sender.blocking_send(batch_result).is_err() {
                    break;
                }
            }
        });

        self.output_with_sender("WorkerThreadOnStream", move |sender| async move {
            while is_task_running()
                && let Some(batch_result) = batch_receiver.recv().await
            {
                sender.send(batch_result?).await;
            }
            Ok(())
        })
    }

    pub fn coalesce_with_default_batch_size(
        self: &Arc<Self>,
        input: SendableRecordBatchStream,
    ) -> SendableRecordBatchStream {
        pub struct CoalesceStream {
            input: SendableRecordBatchStream,
            staging_batches: Vec<RecordBatch>,
            staging_rows: usize,
            staging_batches_mem_size: usize,
            batch_size: usize,
            elapsed_compute: Time,
        }

        impl CoalesceStream {
            pub fn new(
                input: SendableRecordBatchStream,
                batch_size: usize,
                elapsed_compute: Time,
            ) -> Self {
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
                let coalesced_batch = coalesce_batches_unchecked(schema, &self.staging_batches);
                self.staging_batches.clear();
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
                self.staging_rows >= batch_size_limit
                    || self.staging_batches_mem_size > mem_size_limit
            }
        }

        impl RecordBatchStream for CoalesceStream {
            fn schema(&self) -> SchemaRef {
                self.input.schema()
            }
        }

        impl Stream for CoalesceStream {
            type Item = Result<RecordBatch>;

            fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
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

        Box::pin(CoalesceStream {
            input,
            staging_batches: vec![],
            staging_rows: 0,
            staging_batches_mem_size: 0,
            batch_size: batch_size(),
            elapsed_compute: self.baseline_metrics().elapsed_compute().clone(),
        })
    }

    pub fn execute_with_input_stats(
        self: &Arc<Self>,
        input: &Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        let executed = self.execute(input)?;
        Ok(self.stat_input(executed))
    }

    pub fn execute_projected_with_input_stats(
        self: &Arc<Self>,
        input: &Arc<dyn ExecutionPlan>,
        projection: &[usize],
    ) -> Result<SendableRecordBatchStream> {
        let executed = self.execute_projected(input, projection)?;
        Ok(self.stat_input(executed))
    }

    pub fn execute(
        self: &Arc<Self>,
        input: &Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        input.execute(self.partition_id, self.task_ctx.clone())
    }

    pub fn execute_projected(
        self: &Arc<Self>,
        input: &Arc<dyn ExecutionPlan>,
        projection: &[usize],
    ) -> Result<SendableRecordBatchStream> {
        input.execute_projected(self.partition_id, self.task_ctx.clone(), projection)
    }

    pub fn stat_input(
        self: &Arc<Self>,
        input: SendableRecordBatchStream,
    ) -> SendableRecordBatchStream {
        let input_batch_statistics = self.input_stat_metrics.get_or_init(|| {
            InputBatchStatistics::from_metrics_set_and_blaze_conf(
                self.execution_plan_metrics(),
                self.partition_id,
            )
            .expect("error creating input batch statistics")
        });

        if let Some(input_batch_statistics) = input_batch_statistics.clone() {
            let stat_input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
                input.schema(),
                input.inspect(move |batch_result| {
                    if let Ok(batch) = &batch_result {
                        input_batch_statistics.record_input_batch(batch);
                    }
                }),
            ));
            return stat_input;
        }
        input
    }

    pub fn output_with_sender<Fut: Future<Output = Result<()>> + Send>(
        self: &Arc<Self>,
        desc: &'static str,
        output: impl FnOnce(Arc<WrappedRecordBatchSender>) -> Fut + Send + 'static,
    ) -> SendableRecordBatchStream {
        let mut stream_builder = RecordBatchReceiverStream::builder(self.output_schema(), 1);
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
                err_sender
                    .send(df_execution_err!("{err}"))
                    .await
                    .unwrap_or_default();

                // panic current spawn
                let task_running = is_task_running();
                if !task_running {
                    panic!("output_with_sender[{desc}] canceled due to task finished/killed");
                } else {
                    panic!("output_with_sender[{desc}] error: {}", err.to_string());
                }
            }
            Ok(())
        });
        stream_builder.build()
    }
}

#[derive(Clone)]
pub struct InputBatchStatistics {
    input_batch_count: Count,
    input_batch_mem_size: Count,
    input_row_count: Count,
}

impl InputBatchStatistics {
    pub fn from_metrics_set_and_blaze_conf(
        metrics_set: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Result<Option<Self>> {
        let enabled = conf::INPUT_BATCH_STATISTICS_ENABLE.value().unwrap_or(false);
        Ok(enabled.then_some(Self::from_metrics_set(metrics_set, partition)))
    }

    pub fn from_metrics_set(metrics_set: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            input_batch_count: MetricBuilder::new(metrics_set)
                .counter("input_batch_count", partition),
            input_batch_mem_size: MetricBuilder::new(metrics_set)
                .counter("input_batch_mem_size", partition),
            input_row_count: MetricBuilder::new(metrics_set).counter("input_row_count", partition),
        }
    }

    pub fn record_input_batch(&self, input_batch: &RecordBatch) {
        let mem_size = input_batch.get_array_mem_size();
        let num_rows = input_batch.num_rows();
        self.input_batch_count.add(1);
        self.input_batch_mem_size.add(mem_size);
        self.input_row_count.add(num_rows);
    }
}

fn working_senders() -> &'static Mutex<Vec<Weak<WrappedRecordBatchSender>>> {
    static WORKING_SENDERS: OnceCell<Mutex<Vec<Weak<WrappedRecordBatchSender>>>> = OnceCell::new();
    WORKING_SENDERS.get_or_init(|| Mutex::default())
}

pub struct WrappedRecordBatchSender {
    exec_ctx: Arc<ExecutionContext>,
    sender: Sender<Result<RecordBatch>>,
    exclude_time: OnceCell<Time>,
}

impl WrappedRecordBatchSender {
    pub fn new(exec_ctx: Arc<ExecutionContext>, sender: Sender<Result<RecordBatch>>) -> Arc<Self> {
        let wrapped = Arc::new(Self {
            exec_ctx,
            sender,
            exclude_time: OnceCell::new(),
        });
        let mut working_senders = working_senders().lock();
        working_senders.push(Arc::downgrade(&wrapped));
        wrapped
    }

    pub fn exclude_time(&self, exclude_time: &Time) {
        assert!(
            self.exclude_time.get().is_none(),
            "already used a exclude_time"
        );
        self.exclude_time.get_or_init(|| exclude_time.clone());
    }

    pub async fn send(&self, batch: RecordBatch) {
        if batch.num_rows() == 0 {
            return;
        }
        let exclude_time = self.exclude_time.get().cloned();
        let send_time = exclude_time.as_ref().map(|_| Instant::now());
        self.sender
            .send(Ok(batch))
            .await
            .unwrap_or_else(|err| panic!("output_with_sender: send error: {err}"));

        send_time.inspect(|send_time| {
            exclude_time
                .as_ref()
                .unwrap()
                .sub_duration(send_time.elapsed());
        });
    }
}

pub fn cancel_all_tasks(task_ctx: &Arc<TaskContext>) {
    let mut working_senders = working_senders().lock();
    *working_senders = std::mem::take(&mut *working_senders)
        .into_iter()
        .filter(|wrapped| match wrapped.upgrade() {
            Some(wrapped) if Arc::ptr_eq(&wrapped.exec_ctx.task_ctx, task_ctx) => {
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
