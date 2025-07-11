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
    any::Any,
    future::Future,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{Arc, Weak},
    task::{Context, Poll, ready},
    time::Instant,
};

use arrow::{
    array::RecordBatch,
    datatypes::SchemaRef,
    row::{RowConverter, SortField},
};
use blaze_jni_bridge::{conf, conf::BooleanConf, is_task_running};
use datafusion::{
    common::{DataFusionError, Result},
    execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext},
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        ExecutionPlan,
        metrics::{BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, Time},
        stream::RecordBatchStreamAdapter,
    },
};
use datafusion_ext_commons::{
    arrow::{array_size::BatchSize, coalesce::coalesce_batches_unchecked},
    batch_size, df_execution_err, downcast_any, suggested_batch_mem_size,
};
use futures::{Stream, StreamExt};
use futures_util::FutureExt;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use tokio::sync::mpsc::Sender;

use crate::{
    common::{
        column_pruning::{ExecuteWithColumnPruning, extend_projection_by_expr},
        key_rows_output::{
            RecordBatchWithKeyRows, RecordBatchWithKeyRowsStream,
            SendableRecordBatchWithKeyRowsStream,
        },
        timer_helper::TimerHelper,
    },
    memmgr::metrics::SpillMetrics,
    sort_exec::SortExec,
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
                let size_limit = suggested_batch_mem_size();
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
                                self.staging_batches_mem_size += batch.get_batch_mem_size();
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

    pub fn execute_with_key_rows_output(
        self: &Arc<Self>,
        input: &Arc<dyn ExecutionPlan>,
        keys: &[PhysicalSortExpr],
    ) -> Result<SendableRecordBatchWithKeyRowsStream> {
        if let Ok(sort) = downcast_any!(input, SortExec)
            && keys == sort.sort_exprs()
        {
            return sort.execute_with_key_rows(self.partition_id, self.task_ctx.clone());
        }

        let output_schema = self.output_schema();
        let key_converter = RowConverter::new(
            keys.iter()
                .map(|k| {
                    Ok(SortField::new_with_options(
                        k.expr.data_type(&output_schema)?,
                        k.options,
                    ))
                })
                .collect::<Result<_>>()?,
        )?;
        let key_exprs = keys.iter().map(|k| k.expr.clone()).collect::<Vec<_>>();

        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let mut input = self.execute(input)?;
        Ok(self.output_with_sender_with_key_rows(
            "ExecuteWithKeyRowsOutput",
            keys,
            move |sender| async move {
                sender.exclude_time(&elapsed_compute);
                while let Some(batch) = input.next().await.transpose()? {
                    let _timer = elapsed_compute.timer();
                    let keys = key_exprs
                        .iter()
                        .map(|k| {
                            k.evaluate(&batch)
                                .and_then(|r| r.into_array(batch.num_rows()))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let key_rows = key_converter.convert_columns(&keys)?;
                    sender
                        .send(RecordBatchWithKeyRows::new(batch, Arc::new(key_rows)))
                        .await;
                }
                Ok(())
            },
        ))
    }

    pub fn execute_projected_with_key_rows_output(
        self: &Arc<Self>,
        input: &Arc<dyn ExecutionPlan>,
        keys: &[PhysicalSortExpr],
        projection: &[usize],
    ) -> Result<SendableRecordBatchWithKeyRowsStream> {
        if let Ok(sort) = downcast_any!(input, SortExec)
            && keys == sort.sort_exprs()
        {
            return sort.execute_projected_with_key_rows(
                self.partition_id,
                self.task_ctx.clone(),
                projection,
            );
        }

        let output_schema = self.output_schema();
        let key_converter = RowConverter::new(
            keys.iter()
                .map(|k| {
                    Ok(SortField::new_with_options(
                        k.expr.data_type(&output_schema)?,
                        k.options,
                    ))
                })
                .collect::<Result<_>>()?,
        )?;

        let projection = projection.to_vec();
        let mut projection_with_keys = projection.clone();
        let key_exprs = keys
            .iter()
            .map(|k| extend_projection_by_expr(&mut projection_with_keys, &k.expr))
            .collect::<Vec<_>>();

        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let mut input = self.execute_projected(input, &projection_with_keys)?;

        Ok(self
            .with_new_output_schema(Arc::new(output_schema.project(&projection)?))
            .output_with_sender_with_key_rows(
                "ExecuteWithKeyRowsOutput",
                keys,
                move |sender| async move {
                    sender.exclude_time(&elapsed_compute);
                    while let Some(mut batch) = input.next().await.transpose()? {
                        let _timer = elapsed_compute.timer();
                        let keys = key_exprs
                            .iter()
                            .map(|k| {
                                k.evaluate(&batch)
                                    .and_then(|r| r.into_array(batch.num_rows()))
                            })
                            .collect::<Result<Vec<_>>>()?;
                        let key_rows = key_converter.convert_columns(&keys)?;
                        if projection.len() < projection_with_keys.len() {
                            batch = batch.project(&(0..projection.len()).collect::<Vec<_>>())?;
                        }
                        sender
                            .send(RecordBatchWithKeyRows::new(batch, Arc::new(key_rows)))
                            .await;
                    }
                    Ok(())
                },
            ))
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

    pub fn stream_on_completion(
        self: &Arc<Self>,
        input: SendableRecordBatchStream,
        on_completion: Box<dyn FnOnce() -> Result<()> + Send + 'static>,
    ) -> SendableRecordBatchStream {
        struct CompletionStream {
            input: SendableRecordBatchStream,
            on_completion: Option<Box<dyn FnOnce() -> Result<()> + Send + 'static>>,
        }

        impl RecordBatchStream for CompletionStream {
            fn schema(&self) -> SchemaRef {
                self.input.schema()
            }
        }

        impl Stream for CompletionStream {
            type Item = Result<RecordBatch>;

            fn poll_next(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                match ready!(self.as_mut().input.poll_next_unpin(cx)) {
                    Some(r) => Poll::Ready(Some(r)),
                    None => {
                        if let Some(on_completion) = self.as_mut().on_completion.take() {
                            if let Err(e) = on_completion() {
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                        Poll::Ready(None)
                    }
                }
            }
        }

        Box::pin(CompletionStream {
            input,
            on_completion: Some(on_completion),
        })
    }

    pub fn output_with_sender<Fut: Future<Output = Result<()>> + Send>(
        self: &Arc<Self>,
        desc: &'static str,
        output: impl FnOnce(Arc<WrappedRecordBatchSender>) -> Fut + Send + 'static,
    ) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            self.output_with_sender_impl::<RecordBatch, _>(desc, output),
        ))
    }

    pub fn output_with_sender_with_key_rows<Fut: Future<Output = Result<()>> + Send>(
        self: &Arc<Self>,
        desc: &'static str,
        keys: &[PhysicalSortExpr],
        output: impl FnOnce(Arc<WrappedRecordBatchWithKeyRowsSender>) -> Fut + Send + 'static,
    ) -> SendableRecordBatchWithKeyRowsStream {
        struct RecordBatchWithKeyRowsStreamAdapter {
            stream: Pin<Box<dyn Stream<Item = Result<RecordBatchWithKeyRows>> + Send>>,
            schema: SchemaRef,
            keys: Vec<PhysicalSortExpr>,
        }

        impl Stream for RecordBatchWithKeyRowsStreamAdapter {
            type Item = Result<RecordBatchWithKeyRows>;

            fn poll_next(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
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

        Box::pin(RecordBatchWithKeyRowsStreamAdapter {
            stream: Box::pin({
                self.output_with_sender_impl::<RecordBatchWithKeyRows, _>(desc, output)
            }),
            schema: self.output_schema.clone(),
            keys: keys.to_vec(),
        })
    }

    fn output_with_sender_impl<
        T: RecordBatchWithPayload,
        Fut: Future<Output = Result<()>> + Send,
    >(
        self: &Arc<Self>,
        desc: &'static str,
        output: impl FnOnce(Arc<WrappedSender<T>>) -> Fut + Send + 'static,
    ) -> impl Stream<Item = Result<T>> + Send + 'static {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let err_sender = tx.clone();
        let wrapped_sender = WrappedSender::<T>::new(self.clone(), tx.clone());

        tokio::spawn(async move {
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
            Ok::<_, DataFusionError>(())
        });
        Box::pin(
            futures::stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|item| (item, rx))
            })
            .fuse(),
        )
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
        let mem_size = input_batch.get_batch_mem_size();
        let num_rows = input_batch.num_rows();
        self.input_batch_count.add(1);
        self.input_batch_mem_size.add(mem_size);
        self.input_row_count.add(num_rows);
    }
}

fn working_senders() -> &'static Mutex<Vec<Weak<dyn WrappedSenderTrait>>> {
    static WORKING_SENDERS: OnceCell<Mutex<Vec<Weak<dyn WrappedSenderTrait>>>> = OnceCell::new();
    WORKING_SENDERS.get_or_init(|| Mutex::default())
}

pub trait RecordBatchWithPayload: Send + 'static {
    fn is_empty(&self) -> bool;
}

impl RecordBatchWithPayload for RecordBatch {
    fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }
}

impl RecordBatchWithPayload for RecordBatchWithKeyRows {
    fn is_empty(&self) -> bool {
        self.batch.num_rows() == 0
    }
}

pub type WrappedRecordBatchSender = WrappedSender<RecordBatch>;
pub type WrappedRecordBatchWithKeyRowsSender = WrappedSender<RecordBatchWithKeyRows>;

pub trait WrappedSenderTrait: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn exec_ctx(&self) -> &Arc<ExecutionContext>;
}

impl<T: RecordBatchWithPayload> WrappedSenderTrait for WrappedSender<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn exec_ctx(&self) -> &Arc<ExecutionContext> {
        &self.exec_ctx
    }
}

pub struct WrappedSender<T: RecordBatchWithPayload> {
    exec_ctx: Arc<ExecutionContext>,
    sender: Sender<Result<T>>,
    exclude_time: OnceCell<Time>,
}

impl<T: RecordBatchWithPayload> WrappedSender<T> {
    pub fn new(exec_ctx: Arc<ExecutionContext>, sender: Sender<Result<T>>) -> Arc<Self> {
        let wrapped = Arc::new(Self {
            exec_ctx,
            sender,
            exclude_time: OnceCell::new(),
        });
        let mut working_senders = working_senders().lock();
        working_senders.push(Arc::downgrade(&wrapped) as Weak<dyn WrappedSenderTrait>);
        wrapped
    }

    pub fn exclude_time(&self, exclude_time: &Time) {
        assert!(
            self.exclude_time.get().is_none(),
            "already used a exclude_time"
        );
        self.exclude_time.get_or_init(|| exclude_time.clone());
    }

    pub async fn send(&self, batch: T) {
        if batch.is_empty() {
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
            Some(wrapped) if Arc::ptr_eq(&wrapped.exec_ctx().task_ctx, task_ctx) => {
                if let Ok(wrapped) = downcast_any!(wrapped, WrappedRecordBatchSender) {
                    wrapped
                        .sender
                        .try_send(df_execution_err!("task completed/cancelled"))
                        .unwrap_or_default();
                    return false;
                }
                if let Ok(wrapped) = downcast_any!(wrapped, WrappedRecordBatchWithKeyRowsSender) {
                    wrapped
                        .sender
                        .try_send(df_execution_err!("task completed/cancelled"))
                        .unwrap_or_default();
                    return false;
                }
                true
            }
            Some(_) => true, // do not modify senders from other tasks
            None => false,   // already released
        })
        .collect();
}
