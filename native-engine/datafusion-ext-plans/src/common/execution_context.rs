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
    sync::{Arc, Weak},
    time::Instant,
};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use blaze_jni_bridge::{conf, conf::BooleanConf, is_jni_bridge_inited, is_task_running};
use datafusion::{
    common::Result,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{
        metrics::{BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, Time},
        stream::{RecordBatchReceiverStream, RecordBatchStreamAdapter},
        ExecutionPlan,
    },
};
use datafusion_ext_commons::{
    array_size::ArraySize, batch_size, coalesce::coalesce_batches_unchecked, df_execution_err,
    suggested_output_batch_mem_size,
};
use futures::StreamExt;
use futures_util::{FutureExt, TryStreamExt};
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
    spill_metrics: OnceCell<SpillMetrics>,
    input_stat_metrics: OnceCell<Option<InputBatchStatistics>>,
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
            spill_metrics: OnceCell::new(),
            input_stat_metrics: OnceCell::new(),
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

    pub fn coalesce_with_default_batch_size(
        self: &Arc<Self>,
        mut input: SendableRecordBatchStream,
    ) -> SendableRecordBatchStream {
        let baseline_metrics = self.baseline_metrics().clone();
        let schema = input.schema();
        let mut staging_batches = vec![];
        let mut staging_rows = 0;
        let mut staging_batches_mem_size = 0;
        let mem_size_limit = suggested_output_batch_mem_size();
        let batch_size_limit = batch_size();

        self.output_with_sender("CoalesceStream", move |sender| async move {
            while let Some(batch) = input.next().await.transpose()? {
                if batch.num_rows() == 0 {
                    continue;
                }
                let elapsed_compute = baseline_metrics.elapsed_compute().clone();
                let _timer = elapsed_compute.timer();

                staging_rows += batch.num_rows();
                staging_batches_mem_size += batch.get_array_mem_size();
                staging_batches.push(batch);

                let (batch_size_limit, mem_size_limit) = if staging_batches.len() > 1 {
                    (batch_size_limit, mem_size_limit)
                } else {
                    (batch_size_limit / 2, mem_size_limit / 2)
                };

                let should_flush =
                    staging_rows >= batch_size_limit || staging_batches_mem_size >= mem_size_limit;
                if should_flush {
                    let coalesced = coalesce_batches_unchecked(
                        schema.clone(),
                        &std::mem::take(&mut staging_batches),
                    );
                    staging_rows = 0;
                    staging_batches_mem_size = 0;
                    sender.send(coalesced).await;
                }
            }
            if staging_rows > 0 {
                let coalesced = coalesce_batches_unchecked(
                    schema.clone(),
                    &std::mem::take(&mut staging_batches),
                );
                sender.send(coalesced).await;
            }
            Ok(())
        })
    }

    pub fn build_output_stream(
        self: &Arc<Self>,
        fut: impl Future<Output = Result<SendableRecordBatchStream>> + Send + 'static,
    ) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema(),
            futures_util::stream::once(fut).try_flatten(),
        ))
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
            Ok(())
        });
        stream_builder.build()
    }

    pub fn cancel_task(self: &Arc<Self>) {
        WrappedRecordBatchSender::cancel_task(self);
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
        let enabled = is_jni_bridge_inited() && conf::INPUT_BATCH_STATISTICS_ENABLE.value()?;
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

    pub fn cancel_task(exec_ctx: &Arc<ExecutionContext>) {
        let mut working_senders = working_senders().lock();
        *working_senders = std::mem::take(&mut *working_senders)
            .into_iter()
            .filter(|wrapped| match wrapped.upgrade() {
                Some(wrapped) if Arc::ptr_eq(&wrapped.exec_ctx, exec_ctx) => {
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

    pub async fn send(&self, batch: RecordBatch) {
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
