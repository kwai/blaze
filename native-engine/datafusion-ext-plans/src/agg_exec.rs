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
use arrow::datatypes::{FieldRef, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use arrow::row::{RowConverter, SortField};
use datafusion::common::{Result, Statistics};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use datafusion_ext_commons::streams::coalesce_stream::CoalesceStream;
use futures::stream::once;
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use crate::agg::agg_buf::AggBuf;
use crate::agg::agg_context::AggContext;
use crate::agg::agg_tables::AggTables;
use crate::agg::{AggExecMode, AggExpr, GroupingExpr};
use crate::common::batch_statisitcs::{stat_input, InputBatchStatistics};
use crate::common::memory_manager::MemManager;
use crate::common::output::{output_bufferable_with_spill, output_with_sender};

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

    fn statistics(&self) -> Statistics {
        todo!()
    }
}

impl DisplayAs for AggExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Agg {:?}", self.agg_ctx)
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
        _ if agg_ctx.groupings.is_empty() => {
            execute_agg_no_grouping(input, context, agg_ctx, partition_id, metrics)
                .await
                .map_err(|err| err.context("agg: execute_agg_no_grouping() error"))
        }
        AggExecMode::HashAgg => {
            execute_agg_with_grouping_hash(input, context, agg_ctx, partition_id, metrics)
                .await
                .map_err(|err| err.context("agg: execute_agg_with_grouping_hash() error"))
        }
        AggExecMode::SortAgg => execute_agg_sorted(input, context, agg_ctx, partition_id, metrics)
            .await
            .map_err(|err| err.context("agg: execute_agg_sorted() error")),
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
            .map(|field: &FieldRef| SortField::new(field.data_type().clone()))
            .collect(),
    )?;

    // create tables
    let tables = Arc::new(AggTables::new(
        partition_id,
        agg_ctx.clone(),
        BaselineMetrics::new(&metrics, partition_id),
        context.clone(),
    ));
    MemManager::register_consumer(tables.clone(), true);
    drop(timer);

    // start processing input batches
    let input = stat_input(
        InputBatchStatistics::from_metrics_set_and_blaze_conf(&metrics, partition_id)?,
        input.execute(partition_id, context.clone())?,
    )?;
    let mut coalesced = Box::pin(CoalesceStream::new(
        input,
        context.session_config().batch_size(),
        BaselineMetrics::new(&metrics, partition_id)
            .elapsed_compute()
            .clone(),
    ));
    while let Some(input_batch) = coalesced
        .next()
        .await
        .transpose()
        .map_err(|err| err.context("agg: polling batches from input error"))?
    {
        let _timer = baseline_metrics.elapsed_compute().timer();

        // compute grouping rows
        let grouping_arrays: Vec<ArrayRef> = agg_ctx
            .groupings
            .iter()
            .map(|grouping: &GroupingExpr| grouping.expr.evaluate(&input_batch))
            .map(|r| r.map(|columnar| columnar.into_array(input_batch.num_rows())))
            .collect::<Result<_>>()
            .map_err(|err| err.context("agg: evaluating grouping arrays error"))?;
        let grouping_rows = grouping_row_converter.convert_columns(&grouping_arrays)?;

        // compute input arrays
        let input_arrays = agg_ctx
            .create_input_arrays(&input_batch)
            .map_err(|err| err.context("agg: evaluating input arrays error"))?;
        let agg_buf_array = agg_ctx
            .get_input_agg_buf_array(&input_batch)
            .map_err(|err| err.context("agg: evaluating input agg-buf arrays error"))?;

        // insert or update rows into in-mem table
        tables
            .update_entries(grouping_rows, |agg_bufs| {
                let mut mem_diff = 0;
                mem_diff += agg_ctx.partial_batch_update_input(agg_bufs, &input_arrays)?;
                mem_diff += agg_ctx.partial_batch_merge_input(agg_bufs, agg_buf_array)?;
                Ok(mem_diff)
            })
            .await?;
    }
    let has_spill = tables.has_spill().await;
    let tables_cloned = tables.clone();

    // merge all tables and output
    let output = output_with_sender(
        "Agg",
        context.clone(),
        agg_ctx.output_schema.clone(),
        |sender| async move {
            tables
                .output(grouping_row_converter, baseline_metrics, sender)
                .await
                .map_err(|err| err.context("agg: executing output error"))?;
            Ok(())
        },
    )?;

    // if running in-memory, buffer output when memory usage is high
    if !has_spill {
        return output_bufferable_with_spill(tables_cloned, context, output);
    }
    Ok(output)
}

async fn execute_agg_no_grouping(
    input: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    agg_ctx: Arc<AggContext>,
    partition_id: usize,
    metrics: ExecutionPlanMetricsSet,
) -> Result<SendableRecordBatchStream> {
    let baseline_metrics = BaselineMetrics::new(&metrics, partition_id);
    let mut agg_buf = agg_ctx.initial_agg_buf.clone();

    // start processing input batches
    let input = stat_input(
        InputBatchStatistics::from_metrics_set_and_blaze_conf(&metrics, partition_id)?,
        input.execute(partition_id, context.clone())?,
    )?;
    let mut coalesced = Box::pin(CoalesceStream::new(
        input,
        context.session_config().batch_size(),
        baseline_metrics.elapsed_compute().clone(),
    ));

    while let Some(input_batch) = coalesced.next().await.transpose()? {
        let elapsed_compute = baseline_metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer();

        let input_arrays = agg_ctx
            .create_input_arrays(&input_batch)
            .map_err(|err| err.context("agg: evaluating input arrays error"))?;
        agg_ctx
            .partial_update_input_all(&mut agg_buf, &input_arrays)
            .map_err(|err| err.context("agg: executing partial_update_input_all() error"))?;

        let agg_buf_array = agg_ctx
            .get_input_agg_buf_array(&input_batch)
            .map_err(|err| err.context("agg: evaluating input agg-buf arrays error"))?;
        agg_ctx
            .partial_merge_input_all(&mut agg_buf, agg_buf_array)
            .map_err(|err| err.context("agg: executing partial_merge_input_all() error"))?;
    }

    // output
    // in no-grouping mode, we always output only one record, so it is not
    // necessary to record elapsed computed time.
    output_with_sender(
        "Agg",
        context,
        agg_ctx.output_schema.clone(),
        move |sender| async move {
            let elapsed_compute = baseline_metrics.elapsed_compute().clone();
            let mut timer = elapsed_compute.timer();

            let batch_result = agg_ctx
                .build_agg_columns(&mut [(&[], agg_buf)])
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))
                .and_then(|agg_columns| {
                    RecordBatch::try_new_with_options(
                        agg_ctx.output_schema.clone(),
                        agg_columns,
                        &RecordBatchOptions::new().with_row_count(Some(1)),
                    )
                    .map(|batch| {
                        baseline_metrics.record_output(1);
                        batch
                    })
                });
            sender.send(Ok(batch_result?), Some(&mut timer)).await;
            log::info!("aggregate exec (no grouping) outputting one record");
            Ok(())
        },
    )
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
            .map(|field: &FieldRef| SortField::new(field.data_type().clone()))
            .collect(),
    )?;

    // start processing input batches
    let input = stat_input(
        InputBatchStatistics::from_metrics_set_and_blaze_conf(&metrics, partition_id)?,
        input.execute(partition_id, context.clone())?,
    )?;
    let mut coalesced = Box::pin(CoalesceStream::new(
        input,
        context.session_config().batch_size(),
        baseline_metrics.elapsed_compute().clone(),
    ));
    output_with_sender(
        "Agg",
        context.clone(),
        agg_ctx.output_schema.clone(),
        |sender| async move {
            let batch_size = context.session_config().batch_size();
            let mut staging_records = vec![];
            let mut current_record: Option<(Box<[u8]>, AggBuf)> = None;
            let mut timer = elapsed_compute.timer();
            timer.stop();

            macro_rules! flush_staging {
                () => {{
                    let batch = agg_ctx.convert_records_to_batch(
                        &mut grouping_row_converter,
                        &mut staging_records,
                    )?;
                    staging_records.clear();
                    log::info!(
                        "aggregate exec (sorted) outputting one batch: num_rows={}",
                        batch.num_rows(),
                    );
                    baseline_metrics.record_output(batch.num_rows());
                    sender.send(Ok(batch), Some(&mut timer)).await;
                }};
            }
            while let Some(input_batch) = coalesced.next().await.transpose()? {
                timer.restart();

                // compute grouping rows
                let grouping_arrays: Vec<ArrayRef> = agg_ctx
                    .groupings
                    .iter()
                    .map(|grouping: &GroupingExpr| grouping.expr.evaluate(&input_batch))
                    .map(|r| r.map(|columnar| columnar.into_array(input_batch.num_rows())))
                    .collect::<Result<_>>()
                    .map_err(|err| err.context("agg: evaluating grouping arrays error"))?;
                let grouping_rows: Vec<Box<[u8]>> = grouping_row_converter
                    .convert_columns(&grouping_arrays)?
                    .into_iter()
                    .map(|row| row.as_ref().into())
                    .collect();

                // compute input arrays
                let input_arrays = agg_ctx
                    .create_input_arrays(&input_batch)
                    .map_err(|err| err.context("agg: evaluating input arrays error"))?;
                let agg_buf_array = agg_ctx
                    .get_input_agg_buf_array(&input_batch)
                    .map_err(|err| err.context("agg: evaluating input agg-buf arrays error"))?;

                // update to current record
                for (row_idx, grouping_row) in grouping_rows.into_iter().enumerate() {
                    // if group key differs, renew one and move the old record to staging
                    if Some(&grouping_row) != current_record.as_ref().map(|r| &r.0) {
                        let finished_record =
                            current_record.replace((grouping_row, agg_ctx.initial_agg_buf.clone()));
                        if let Some(record) = finished_record {
                            staging_records.push(record);
                            if staging_records.len() >= batch_size {
                                flush_staging!();
                            }
                        }
                    }
                    let agg_buf = &mut current_record.as_mut().unwrap().1;
                    agg_ctx
                        .partial_update_input(agg_buf, &input_arrays, row_idx)
                        .map_err(|err| {
                            err.context("agg: executing partial_update_input() error")
                        })?;
                    agg_ctx
                        .partial_merge_input(agg_buf, agg_buf_array, row_idx)
                        .map_err(|err| err.context("agg: executing partial_merge_input() error"))?;
                }
                timer.stop();
            }

            if let Some(record) = current_record {
                staging_records.push(record);
            }
            if !staging_records.is_empty() {
                flush_staging!();
            }
            Ok(())
        },
    )
}
#[cfg(test)]
mod test {
    use crate::agg::AggExecMode::HashAgg;
    use crate::agg::AggMode::{Final, Partial};
    use crate::agg::{create_agg, AggExpr, AggFunction, GroupingExpr};
    use crate::agg_exec::AggExec;
    use crate::common::memory_manager::MemManager;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::common::Result;
    use datafusion::physical_expr::expressions as phys_expr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::{common, ExecutionPlan};
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    fn build_table_i32(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
        d: (&str, &Vec<i32>),
        e: (&str, &Vec<i32>),
        f: (&str, &Vec<i32>),
        g: (&str, &Vec<i32>),
        h: (&str, &Vec<i32>),
    ) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Int32, false),
            Field::new(b.0, DataType::Int32, false),
            Field::new(c.0, DataType::Int32, false),
            Field::new(d.0, DataType::Int32, false),
            Field::new(e.0, DataType::Int32, false),
            Field::new(f.0, DataType::Int32, false),
            Field::new(g.0, DataType::Int32, false),
            Field::new(h.0, DataType::Int32, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(a.1.clone())),
                Arc::new(Int32Array::from(b.1.clone())),
                Arc::new(Int32Array::from(c.1.clone())),
                Arc::new(Int32Array::from(d.1.clone())),
                Arc::new(Int32Array::from(e.1.clone())),
                Arc::new(Int32Array::from(f.1.clone())),
                Arc::new(Int32Array::from(g.1.clone())),
                Arc::new(Int32Array::from(h.1.clone())),
            ],
        )
        .unwrap()
    }

    fn build_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
        d: (&str, &Vec<i32>),
        e: (&str, &Vec<i32>),
        f: (&str, &Vec<i32>),
        g: (&str, &Vec<i32>),
        h: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a, b, c, d, e, f, g, h);
        let schema = batch.schema();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    #[tokio::test]
    async fn test_agg() -> Result<()> {
        MemManager::init(10000);
        let input = build_table(
            ("a", &vec![2, 9, 3, 1, 0, 4, 6]),
            ("b", &vec![1, 0, 0, 3, 5, 6, 3]),
            ("c", &vec![7, 8, 7, 8, 9, 2, 5]),
            ("d", &vec![-7, 86, 71, 83, 90, -2, 5]),
            ("e", &vec![-7, 86, 71, 83, 90, -2, 5]),
            ("f", &vec![0, 1, 2, 3, 4, 5, 6]),
            ("g", &vec![6, 3, 6, 3, 1, 5, 4]),
            ("h", &vec![6, 3, 6, 3, 1, 5, 4]),
        );
        //create AggExec
        let groupings_agg_expr = vec![GroupingExpr {
            field_name: "grouping_column".to_string(),
            expr: Arc::new(Column::new("c", 2)),
        }];

        let agg_agg_expr_sum = create_agg(
            AggFunction::Sum,
            &[phys_expr::col("a", &input.schema()).unwrap()],
            &input.schema(),
        )
        .unwrap();

        let agg_agg_expr_avg = create_agg(
            AggFunction::Avg,
            &[phys_expr::col("b", &input.schema()).unwrap()],
            &input.schema(),
        )
        .unwrap();

        let agg_agg_expr_max = create_agg(
            AggFunction::Max,
            &[phys_expr::col("d", &input.schema()).unwrap()],
            &input.schema(),
        )
        .unwrap();

        let agg_agg_expr_min = create_agg(
            AggFunction::Min,
            &[phys_expr::col("e", &input.schema()).unwrap()],
            &input.schema(),
        )
        .unwrap();

        let agg_agg_expr_count = create_agg(
            AggFunction::Count,
            &[phys_expr::col("f", &input.schema()).unwrap()],
            &input.schema(),
        )
        .unwrap();

        let agg_agg_expr_collectlist = create_agg(
            AggFunction::CollectList,
            &[phys_expr::col("g", &input.schema()).unwrap()],
            &input.schema(),
        )
        .unwrap();

        let agg_agg_expr_collectset = create_agg(
            AggFunction::CollectSet,
            &[phys_expr::col("h", &input.schema()).unwrap()],
            &input.schema(),
        )
        .unwrap();

        let agg_agg_expr_firstign = create_agg(
            AggFunction::FirstIgnoresNull,
            &[phys_expr::col("h", &input.schema()).unwrap()],
            &input.schema(),
        )
        .unwrap();

        let aggs_agg_expr = vec![
            AggExpr {
                field_name: "agg_agg_expr".to_string(),
                mode: Partial,
                agg: agg_agg_expr_sum,
            },
            AggExpr {
                field_name: "agg_agg_expr".to_string(),
                mode: Partial,
                agg: agg_agg_expr_avg,
            },
            AggExpr {
                field_name: "agg_agg_expr".to_string(),
                mode: Partial,
                agg: agg_agg_expr_max,
            },
            AggExpr {
                field_name: "agg_agg_expr".to_string(),
                mode: Partial,
                agg: agg_agg_expr_min,
            },
            AggExpr {
                field_name: "agg_agg_expr".to_string(),
                mode: Partial,
                agg: agg_agg_expr_count,
            },
            AggExpr {
                field_name: "agg_agg_expr".to_string(),
                mode: Partial,
                agg: agg_agg_expr_collectlist,
            },
            AggExpr {
                field_name: "agg_agg_expr".to_string(),
                mode: Partial,
                agg: agg_agg_expr_collectset,
            },
            AggExpr {
                field_name: "agg_agg_firstign".to_string(),
                mode: Partial,
                agg: agg_agg_expr_firstign,
            },
        ];

        let agg_exec_partial =
            AggExec::try_new(HashAgg, groupings_agg_expr, aggs_agg_expr, 0, input).unwrap();

        let input_1 = build_table(
            ("a", &vec![2, 9, 3, 1, 0, 4, 6]),
            ("b", &vec![1, 0, 0, 3, 5, 6, 3]),
            ("c", &vec![7, 8, 7, 8, 9, 2, 5]),
            ("d", &vec![-7, 86, 71, 83, 90, -2, 5]),
            ("e", &vec![-7, 86, 71, 83, 90, -2, 5]),
            ("f", &vec![0, 1, 2, 3, 4, 5, 6]),
            ("g", &vec![6, 3, 0, 3, 1, 5, 4]),
            ("h", &vec![6, 3, 0, 3, 1, 5, 4]),
        );

        let groupings_agg_expr_1 = vec![GroupingExpr {
            field_name: "grouping_column(c)".to_string(),
            expr: Arc::new(Column::new("c", 0)),
        }];

        let agg_agg_expr_1 = create_agg(
            AggFunction::Sum,
            &[phys_expr::col("a", &input_1.schema()).unwrap()],
            &input_1.schema(),
        )
        .unwrap();

        let agg_agg_expr_2 = create_agg(
            AggFunction::Avg,
            &[phys_expr::col("b", &input_1.schema()).unwrap()],
            &input_1.schema(),
        )
        .unwrap();

        let agg_agg_expr_3 = create_agg(
            AggFunction::Max,
            &[phys_expr::col("d", &input_1.schema()).unwrap()],
            &input_1.schema(),
        )
        .unwrap();

        let agg_agg_expr_4 = create_agg(
            AggFunction::Min,
            &[phys_expr::col("e", &input_1.schema()).unwrap()],
            &input_1.schema(),
        )
        .unwrap();

        let agg_agg_expr_5 = create_agg(
            AggFunction::Count,
            &[phys_expr::col("f", &input_1.schema()).unwrap()],
            &input_1.schema(),
        )
        .unwrap();

        let agg_agg_expr_6 = create_agg(
            AggFunction::CollectList,
            &[phys_expr::col("g", &input_1.schema()).unwrap()],
            &input_1.schema(),
        )
        .unwrap();

        let agg_agg_expr_7 = create_agg(
            AggFunction::CollectSet,
            &[phys_expr::col("h", &input_1.schema()).unwrap()],
            &input_1.schema(),
        )
        .unwrap();

        let agg_agg_expr_8 = create_agg(
            AggFunction::FirstIgnoresNull,
            &[phys_expr::col("h", &input_1.schema()).unwrap()],
            &input_1.schema(),
        )
        .unwrap();

        let aggs_agg_expr_1 = vec![
            AggExpr {
                field_name: "Sum(a)".to_string(),
                mode: Final,
                agg: agg_agg_expr_1,
            },
            AggExpr {
                field_name: "Avg(b)".to_string(),
                mode: Final,
                agg: agg_agg_expr_2,
            },
            AggExpr {
                field_name: "Max(d)".to_string(),
                mode: Final,
                agg: agg_agg_expr_3,
            },
            AggExpr {
                field_name: "Min(e)".to_string(),
                mode: Final,
                agg: agg_agg_expr_4,
            },
            AggExpr {
                field_name: "Count(f)".to_string(),
                mode: Final,
                agg: agg_agg_expr_5,
            },
            AggExpr {
                field_name: "CollectList(g)".to_string(),
                mode: Final,
                agg: agg_agg_expr_6,
            },
            AggExpr {
                field_name: "CollectSet(h)".to_string(),
                mode: Final,
                agg: agg_agg_expr_7,
            },
            AggExpr {
                field_name: "FirstIgn(h)".to_string(),
                mode: Final,
                agg: agg_agg_expr_8,
            },
        ];

        let agg_exec_final = AggExec::try_new(
            HashAgg,
            groupings_agg_expr_1,
            aggs_agg_expr_1,
            0,
            Arc::new(agg_exec_partial),
        )
        .unwrap();

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let output_final = agg_exec_final.execute(0, task_ctx)?;
        let batches = common::collect(output_final).await?;
        let expected = vec![
            "+--------------------+--------+--------+--------+--------+----------+----------------+---------------+-------------+",
            "| grouping_column(c) | Sum(a) | Avg(b) | Max(d) | Min(e) | Count(f) | CollectList(g) | CollectSet(h) | FirstIgn(h) |",
            "+--------------------+--------+--------+--------+--------+----------+----------------+---------------+-------------+",
            "| 2                  | 4      | 6.0    | -2     | -2     | 1        | [5]            | [5]           | 5           |",
            "| 5                  | 6      | 3.0    | 5      | 5      | 1        | [4]            | [4]           | 4           |",
            "| 7                  | 5      | 0.5    | 71     | -7     | 2        | [6, 6]         | [6]           | 6           |",
            "| 8                  | 10     | 1.5    | 86     | 83     | 2        | [3, 3]         | [3]           | 3           |",
            "| 9                  | 0      | 5.0    | 90     | 90     | 1        | [1]            | [1]           | 1           |",
            "+--------------------+--------+--------+--------+--------+----------+----------------+---------------+-------------+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }
}
