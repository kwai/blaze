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

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use arrow::row::{RowConverter, SortField};
use datafusion::common::{DataFusionError, Result, ScalarValue, Statistics};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream::once;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use datafusion_ext_commons::streams::coalesce_stream::CoalesceStream;
use datafusion_ext_commons::streams::receiver_stream::ReceiverStream;

use crate::agg::{AggAccumRef, AggExpr, GroupingExpr};
use crate::agg::agg_helper::AggContext;
use crate::agg::agg_tables::{AggTables, InMemHashTable};

#[derive(Debug)]
pub struct AggExec {
    input: Arc<dyn ExecutionPlan>,
    agg_ctx: Arc<AggContext>,
    metrics: ExecutionPlanMetricsSet,
}

impl AggExec {
    pub fn try_new(
        groupings: Vec<GroupingExpr>,
        aggs: Vec<AggExpr>,
        initial_input_buffer_offset: usize,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let agg_ctx = Arc::new(
            AggContext::try_new(
                input.schema(),
                groupings,
                aggs,
                initial_input_buffer_offset,
            )?
        );

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

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: children[0].clone(),
            agg_ctx: self.agg_ctx.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let stream = execute_agg(
            self.input.clone(),
            context.clone(),
            self.agg_ctx.clone(),
            partition,
            self.metrics.clone(),
        ).map_err(|e| {
            ArrowError::ExternalError(Box::new(e))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            once(stream).try_flatten()
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
    task_context: Arc<TaskContext>,
    agg_ctx: Arc<AggContext>,
    partition_id: usize,
    metrics: ExecutionPlanMetricsSet,
) -> Result<SendableRecordBatchStream> {

    let baseline_metrics = BaselineMetrics::new(&metrics, partition_id);
    let timer = baseline_metrics.elapsed_compute().timer();

    // aggregate with no groupings
    if agg_ctx.groupings.is_empty() {
        return execute_agg_no_grouping(
            input,
            task_context,
            agg_ctx,
            partition_id,
            metrics,
        ).await;
    }

    // create grouping row converter and parser
    let mut grouping_row_converter = RowConverter::new(
        agg_ctx.grouping_schema
            .fields()
            .into_iter()
            .map(|field: &Field| SortField::new(field.data_type().clone()))
            .collect())?;

    // create tables
    let tables = Arc::new(AggTables::new(
        agg_ctx.clone(),
        partition_id,
        BaselineMetrics::new(&metrics, partition_id),
        task_context.clone(),
    ));

    // start processing input batches
    let input = input.execute(partition_id, task_context.clone())?;
    let mut coalesced = Box::pin(CoalesceStream::new(
        input,
        task_context.session_config().batch_size(),
        BaselineMetrics::new(&metrics, partition_id).elapsed_compute().clone(),
    ));
    drop(timer);

    while let Some(input_batch) = coalesced.next().await.transpose()? {
        let _timer = baseline_metrics.elapsed_compute().timer();

        // compute grouping rows
        let grouping_arrays: Vec<ArrayRef> = agg_ctx.groupings
            .iter()
            .map(|grouping: &GroupingExpr| grouping.expr.evaluate(&input_batch))
            .map(|r| r.map(|columnar| columnar.into_array(input_batch.num_rows())))
            .collect::<Result<_>>()?;
        let grouping_rows: Vec<Box<[u8]>> = grouping_row_converter
            .convert_columns(&grouping_arrays)?
            .into_iter()
            .map(|row| row.as_ref().into())
            .collect();

        let agg_children_input_arrays =
            agg_ctx.create_children_input_arrays(&input_batch)?;

        // update to in-mem table
        tables.update_in_mem(|in_mem: &mut InMemHashTable| {
            for (row_idx, grouping_row) in grouping_rows.into_iter().enumerate() {
                in_mem.update(
                    &agg_ctx,
                    grouping_row,
                    &agg_children_input_arrays,
                    row_idx,
                )?;
            }
            Ok(())
        }).await?;
    }

    // merge all tables and output
    let (sender, receiver) = tokio::sync::mpsc::channel(2);
    let elapsed_time_clonned = baseline_metrics.elapsed_compute().clone();

    let join_handle = tokio::task::spawn(async move {
        let err_sender = sender.clone();
        let result = AssertUnwindSafe(async move {
            tables.output(
                grouping_row_converter,
                elapsed_time_clonned,
                sender,
            ).await
        }).catch_unwind().await;

        if let Err(e) = result {
            let err_message = panic_message::panic_message(&e).to_owned();
            err_sender.send(Err(ArrowError::ExternalError(
                Box::new(DataFusionError::Execution(err_message))
            ))).await.unwrap();
        }
    });

    Ok(Box::pin(ReceiverStream::new(
        agg_ctx.output_schema.clone(),
        receiver,
        baseline_metrics,
        vec![join_handle],
    )))
}

async fn execute_agg_no_grouping(
    input: Arc<dyn ExecutionPlan>,
    task_context: Arc<TaskContext>,
    agg_ctx: Arc<AggContext>,
    partition_id: usize,
    metrics: ExecutionPlanMetricsSet,
) -> Result<SendableRecordBatchStream> {
    let baseline_metrics = BaselineMetrics::new(&metrics, partition_id);
    let elapsed_compute = baseline_metrics.elapsed_compute().clone();
    let timer = elapsed_compute.timer();

    let mut accums: Box<[AggAccumRef]> = agg_ctx.aggs
        .iter()
        .map(|agg: &AggExpr| agg.agg.create_accum())
        .collect::<Result<_>>()?;

    // start processing input batches
    let input = input.execute(partition_id, task_context.clone())?;
    let mut coalesced = Box::pin(CoalesceStream::new(
        input,
        task_context.session_config().batch_size(),
        baseline_metrics.elapsed_compute().clone(),
    ));
    drop(timer);

    while let Some(input_batch) = coalesced.next().await.transpose()? {
        // compute agg children projected arrays
        let agg_children_projected_arrays =
            agg_ctx.create_children_input_arrays(&input_batch)?;

        // update to accums
        for (idx, accum) in accums.iter_mut().enumerate() {
            if agg_ctx.aggs[idx].mode.is_partial() {
                accum.partial_update_all(&agg_children_projected_arrays[idx])?;
            } else {
                for row_idx in 0..input_batch.num_rows() {
                    accum.partial_merge_from_array(
                        &agg_children_projected_arrays[idx],
                        row_idx,
                    )?;
                }
            }
        }
    }

    // output
    let (sender, receiver) = tokio::sync::mpsc::channel(2);
    let agg_ctx_clonned = agg_ctx.clone();
    let elapsed_time_clonned = baseline_metrics.elapsed_compute().clone();

    let mut stub_row_converter =
        RowConverter::new(vec![SortField::new(DataType::Int8)])?;
    let stub_row: Box<[u8]> = stub_row_converter
        .convert_columns(&[ScalarValue::Int8(None).to_array()])?
        .row(0)
        .as_ref()
        .into();
    let record = (stub_row, accums);

    let join_handle = tokio::task::spawn(async move {
        let err_sender = sender.clone();
        if let Err(e) = AssertUnwindSafe(async move {
            let agg_ctx = agg_ctx_clonned;
            let mut timer = elapsed_time_clonned.timer();

            let batch_result = agg_ctx.build_agg_columns(&[record])
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))
                .and_then(|agg_columns| RecordBatch::try_new_with_options(
                    agg_ctx.output_schema.clone(),
                    agg_columns,
                    &RecordBatchOptions::new().with_row_count(Some(1)),
                ));

            log::info!("aggregate exec (no grouping) outputing one record");
            timer.stop();
            sender.send(batch_result).await.unwrap();

        }).catch_unwind().await {
            let err_message = panic_message::panic_message(&e).to_owned();
            err_sender.send(Err(ArrowError::ExternalError(
                Box::new(DataFusionError::Execution(err_message))
            ))).await.unwrap();
        }
    });

    Ok(Box::pin(ReceiverStream::new(
        agg_ctx.output_schema.clone(),
        receiver,
        baseline_metrics,
        vec![join_handle],
    )))
}