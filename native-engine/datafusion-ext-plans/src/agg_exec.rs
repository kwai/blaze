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
use arrow::datatypes::{Field, SchemaRef};
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use arrow::row::{RowConverter, SortField};
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use datafusion_ext_commons::streams::coalesce_stream::CoalesceStream;
use datafusion_ext_commons::streams::receiver_stream::ReceiverStream;
use futures::stream::once;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

use crate::agg::agg_helper::{AggContext, AggRecord};
use crate::agg::agg_tables::{AggTables, InMemTable};
use crate::agg::{AggExecMode, AggExpr, GroupingExpr};

#[derive(Debug)]
pub struct AggExec {
    input: Arc<dyn ExecutionPlan>,
    agg_ctx: Arc<AggContext>,
    metrics: ExecutionPlanMetricsSet,
}

impl AggExec {
    pub fn try_new(
        exec_mode: AggExecMode,
        groupings: Vec<GroupingExpr>,
        aggs: Vec<AggExpr>,
        initial_input_buffer_offset: usize,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let agg_ctx = Arc::new(AggContext::try_new(
            exec_mode,
            input.schema(),
            groupings,
            aggs,
            initial_input_buffer_offset,
        )?);

        Ok(Self {
            input,
            agg_ctx,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl ExecutionPlan for AggExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.agg_ctx.output_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: children[0].clone(),
            agg_ctx: self.agg_ctx.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = execute_agg(
            self.input.clone(),
            context,
            self.agg_ctx.clone(),
            partition,
            self.metrics.clone(),
        )
        .map_err(|e| ArrowError::ExternalError(Box::new(e)));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            once(stream).try_flatten(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Agg {:?}", self.agg_ctx)
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}

async fn execute_agg(
    input: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    agg_ctx: Arc<AggContext>,
    partition_id: usize,
    metrics: ExecutionPlanMetricsSet,
) -> Result<SendableRecordBatchStream> {

    match agg_ctx.exec_mode {
        AggExecMode::HashAgg => {
            if !agg_ctx.groupings.is_empty() {
                execute_agg_with_grouping_hash(
                    input,
                    context,
                    agg_ctx,
                    partition_id,
                    metrics,
                ).await
            } else {
                execute_agg_no_grouping(
                    input,
                    context,
                    agg_ctx,
                    partition_id,
                    metrics,
                ).await
            }
        }
        AggExecMode::SortAgg => {
            execute_agg_sorted(
                input,
                context,
                agg_ctx,
                partition_id,
                metrics,
            ).await
        }
    }
}

async fn execute_agg_with_grouping_hash(
    input: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    agg_ctx: Arc<AggContext>,
    partition_id: usize,
    metrics: ExecutionPlanMetricsSet,
) -> Result<SendableRecordBatchStream> {
    let baseline_metrics = BaselineMetrics::new(&metrics, partition_id);
    let timer = baseline_metrics.elapsed_compute().timer();

    // create grouping row converter and parser
    let mut grouping_row_converter = RowConverter::new(
        agg_ctx
            .grouping_schema
            .fields()
            .iter()
            .map(|field: &Field| SortField::new(field.data_type().clone()))
            .collect(),
    )?;

    // create tables
    let tables = Arc::new(AggTables::new(
        agg_ctx.clone(),
        partition_id,
        BaselineMetrics::new(&metrics, partition_id),
        context.clone(),
    ));
    drop(timer);

    // start processing input batches
    let input = input.execute(partition_id, context.clone())?;
    let mut coalesced = Box::pin(CoalesceStream::new(
        input,
        context.session_config().batch_size(),
        BaselineMetrics::new(&metrics, partition_id)
            .elapsed_compute()
            .clone(),
    ));
    while let Some(input_batch) = coalesced.next().await.transpose()? {
        let _timer = baseline_metrics.elapsed_compute().timer();

        // compute grouping rows
        let grouping_arrays: Vec<ArrayRef> = agg_ctx
            .groupings
            .iter()
            .map(|grouping: &GroupingExpr| grouping.expr.evaluate(&input_batch))
            .map(|r| r.map(|columnar| columnar.into_array(input_batch.num_rows())))
            .collect::<Result<_>>()?;
        let grouping_rows: Vec<Box<[u8]>> = grouping_row_converter
            .convert_columns(&grouping_arrays)?
            .into_iter()
            .map(|row| row.as_ref().into())
            .collect();

        // update to in-mem table
        let agg_input_arrays = agg_ctx.create_input_arrays(&input_batch)?;
        tables
            .update_in_mem(|in_mem: &mut InMemTable| {
                for (row_idx, grouping_row) in grouping_rows.into_iter().enumerate() {
                    in_mem.update(
                        &agg_ctx,
                        grouping_row,
                        &agg_input_arrays,
                        row_idx,
                    )?;
                }
                Ok(())
            })
            .await?;
    }

    // merge all tables and output
    let elapsed_compute = baseline_metrics.elapsed_compute().clone();
    start_output(agg_ctx.clone(), baseline_metrics, |sender| async move {
        tables.output(grouping_row_converter, elapsed_compute, sender).await?;
        Ok(())
    })
}

async fn execute_agg_no_grouping(
    input: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    agg_ctx: Arc<AggContext>,
    partition_id: usize,
    metrics: ExecutionPlanMetricsSet,
) -> Result<SendableRecordBatchStream> {
    let baseline_metrics = BaselineMetrics::new(&metrics, partition_id);
    let elapsed_compute = baseline_metrics.elapsed_compute().clone();
    let mut accums = agg_ctx.create_initial_accums();

    // start processing input batches
    let input = input.execute(partition_id, context.clone())?;
    let mut coalesced = Box::pin(CoalesceStream::new(
        input,
        context.session_config().batch_size(),
        baseline_metrics.elapsed_compute().clone(),
    ));
    while let Some(input_batch) = coalesced.next().await.transpose()? {
        let _timer = elapsed_compute.timer();

        // update to accums
        agg_ctx.partial_update_or_merge_all(
            &mut accums,
            &agg_ctx.create_input_arrays(&input_batch)?,
        )?;
    }

    // output
    // in no-grouping mode, we always output only one record, so it is not
    // necessary to record elapsed computed time.
    start_output(agg_ctx.clone(), baseline_metrics, |sender| async move {
        let record: AggRecord = AggRecord::new(Box::default(), accums);
        let batch_result = agg_ctx
            .build_agg_columns(&mut [record])
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))
            .and_then(|agg_columns| {
                RecordBatch::try_new_with_options(
                    agg_ctx.output_schema.clone(),
                    agg_columns,
                    &RecordBatchOptions::new().with_row_count(Some(1)),
                )
            });
        sender.send(batch_result).map_err(|err| {
            DataFusionError::Execution(format!("{:?}", err))
        }).await?;
        log::info!("aggregate exec (no grouping) outputing one record");
        Ok(())
    })
}

async fn execute_agg_sorted(
    input: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    agg_ctx: Arc<AggContext>,
    partition_id: usize,
    metrics: ExecutionPlanMetricsSet,
) -> Result<SendableRecordBatchStream> {
    let baseline_metrics = BaselineMetrics::new(&metrics, partition_id);
    let elapsed_compute = baseline_metrics.elapsed_compute().clone();

    // create grouping row converter and parser
    let mut grouping_row_converter = RowConverter::new(
        agg_ctx
            .grouping_schema
            .fields()
            .iter()
            .map(|field: &Field| SortField::new(field.data_type().clone()))
            .collect(),
    )?;

    // start processing input batches
    let input = input.execute(partition_id, context.clone())?;
    let mut coalesced = Box::pin(CoalesceStream::new(
        input,
        context.session_config().batch_size(),
        baseline_metrics.elapsed_compute().clone(),
    ));
    start_output(agg_ctx.clone(), baseline_metrics, |sender| async move {
        let batch_size = context.session_config().batch_size();
        let mut staging_records = vec![];
        let mut current_record: Option<AggRecord> = None;

        while let Some(input_batch) = coalesced.next().await.transpose()? {
            let mut timer = elapsed_compute.timer();

            // compute grouping rows
            let grouping_arrays: Vec<ArrayRef> = agg_ctx
                .groupings
                .iter()
                .map(|grouping: &GroupingExpr| grouping.expr.evaluate(&input_batch))
                .map(|r| r.map(|columnar| columnar.into_array(input_batch.num_rows())))
                .collect::<Result<_>>()?;
            let grouping_rows: Vec<Box<[u8]>> = grouping_row_converter
                .convert_columns(&grouping_arrays)?
                .into_iter()
                .map(|row| row.as_ref().into())
                .collect();

            let agg_input_arrays =
                agg_ctx.create_input_arrays(&input_batch)?;

            // update to current record
            for (row_idx, grouping_row) in grouping_rows.into_iter().enumerate() {

                // if group key differs, renew one and move the old record to staging
                if Some(&grouping_row) != current_record.as_ref().map(|r| &r.grouping) {
                    let finished_record = current_record.replace(AggRecord::new(
                        grouping_row,
                        agg_ctx.create_initial_accums(),
                    ));
                    if let Some(record) = finished_record {
                        staging_records.push(record);
                        if staging_records.len() >= batch_size {
                            let batch = agg_ctx.convert_records_to_batch(
                                &mut grouping_row_converter,
                                &mut std::mem::take(&mut staging_records),
                            )?;
                            timer.stop();

                            log::info!(
                                "aggregate exec (sorted) outputing one batch: num_rows={}",
                                batch.num_rows(),
                            );
                            sender.send(Ok(batch)).map_err(|err| {
                                DataFusionError::Execution(format!("{:?}", err))
                            }).await?;
                            timer.restart();
                        }
                    }
                }

                // update to accums
                agg_ctx.partial_update_or_merge_one_row(
                    &mut current_record.as_mut().unwrap().accums,
                    &agg_input_arrays,
                    row_idx,
                )?;
            }
        }

        let mut timer = elapsed_compute.timer();
        if let Some(record) = current_record {
            staging_records.push(record);
        }
        if !staging_records.is_empty() {
            let batch = agg_ctx.convert_records_to_batch(
                &mut grouping_row_converter,
                &mut staging_records,
            )?;
            timer.stop();

            log::info!(
                "aggregate exec (sorted) outputing one batch: num_rows={}",
                batch.num_rows(),
            );
            sender.send(Ok(batch)).map_err(|err| {
                DataFusionError::Execution(format!("{:?}", err))
            }).await?;
        }
        Ok(())
    })
}

fn start_output<Fut: Future<Output = Result<()>> + Send>(
    agg_ctx: Arc<AggContext>,
    baseline_metrics: BaselineMetrics,
    output: impl FnOnce(Sender<ArrowResult<RecordBatch>>) -> Fut + Send + 'static
) -> Result<SendableRecordBatchStream> {

    let (sender, receiver) = tokio::sync::mpsc::channel(2);
    let output_schema = agg_ctx.output_schema.clone();
    let join_handle = tokio::task::spawn(async move {
        let err_sender = sender.clone();
        let result = AssertUnwindSafe(async move {
            output(sender).await.unwrap();
        })
        .catch_unwind()
        .await;

        if let Err(e) = result {
            let err_message = panic_message::panic_message(&e).to_owned();
            err_sender
                .send(Err(ArrowError::ExternalError(Box::new(
                    DataFusionError::Execution(err_message),
                ))))
                .await
                .unwrap();
        }
    });

    Ok(Box::pin(ReceiverStream::new(
        output_schema,
        receiver,
        baseline_metrics,
        vec![join_handle],
    )))
}