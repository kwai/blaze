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
    fmt::{Debug, Formatter},
    sync::Arc,
};

use arrow::{
    datatypes::SchemaRef,
    error::ArrowError,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use datafusion::{
    common::{Result, Statistics},
    execution::context::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    },
};
use datafusion_ext_commons::{
    batch_size, slim_bytes::SlimBytes, streams::coalesce_stream::CoalesceInput,
};
use futures::{stream::once, StreamExt, TryFutureExt, TryStreamExt};

use crate::{
    agg::{
        acc::OwnedAccumStateRow,
        agg_context::AggContext,
        agg_table::{AggTable, InMemMode},
        AggExecMode, AggExpr, GroupingExpr,
    },
    common::{
        batch_statisitcs::{stat_input, InputBatchStatistics},
        output::TaskOutputter,
    },
    memmgr::MemManager,
};

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
        supports_partial_skipping: bool,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let agg_ctx = Arc::new(AggContext::try_new(
            exec_mode,
            input.schema(),
            groupings,
            aggs,
            initial_input_buffer_offset,
            supports_partial_skipping,
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
            context.clone(),
            self.agg_ctx.clone(),
            partition,
            self.metrics.clone(),
        )
        .map_err(|e| ArrowError::ExternalError(Box::new(e)));

        let output = Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            once(stream).try_flatten(),
        ));

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        context.coalesce_with_default_batch_size(output, &baseline_metrics)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
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
    // create tables
    let tables = Arc::new(AggTable::new(
        partition_id,
        agg_ctx.clone(),
        context.clone(),
        &metrics,
    ));
    MemManager::register_consumer(tables.clone(), true);

    // start processing input batches
    let input = stat_input(
        InputBatchStatistics::from_metrics_set_and_blaze_conf(&metrics, partition_id)?,
        input.execute(partition_id, context.clone())?,
    )?;
    let mut coalesced = context
        .coalesce_with_default_batch_size(input, &BaselineMetrics::new(&metrics, partition_id))?;

    while let Some(input_batch) = coalesced
        .next()
        .await
        .transpose()
        .map_err(|err| err.context("agg: polling batches from input error"))?
    {
        // insert or update rows into in-mem table
        tables.process_input_batch(input_batch).await?;

        // stop aggregating if triggered partial skipping
        if tables.mode().await == InMemMode::PartialSkipped {
            break;
        }
    }
    let has_spill = tables.has_spill().await;
    let tables_cloned = tables.clone();

    // merge all tables and output
    let output_schema = agg_ctx.output_schema.clone();
    let output = context.output_with_sender("Agg", output_schema, |sender| async move {
        // output all aggregated records in table
        tables.output(sender.clone()).await?;

        // in partial skipping mode, there might be unconsumed records in input stream
        while let Some(input_batch) = coalesced
            .next()
            .await
            .transpose()
            .map_err(|err| err.context("agg: polling batches from input error"))?
        {
            tables
                .process_partial_skipped(input_batch, sender.clone())
                .await?;
        }
        Ok(())
    })?;

    // if running in-memory, buffer output when memory usage is high
    if !has_spill {
        return context.output_bufferable_with_spill(partition_id, tables_cloned, output, metrics);
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
    let mut acc = agg_ctx.initial_acc.clone();

    // start processing input batches
    let input = stat_input(
        InputBatchStatistics::from_metrics_set_and_blaze_conf(&metrics, partition_id)?,
        input.execute(partition_id, context.clone())?,
    )?;
    let mut coalesced = context.coalesce_with_default_batch_size(input, &baseline_metrics)?;

    while let Some(input_batch) = coalesced.next().await.transpose()? {
        let _timer = baseline_metrics.elapsed_compute().timer();
        let input_arrays = agg_ctx.create_input_arrays(&input_batch)?;
        let acc_array = agg_ctx.get_input_acc_array(&input_batch)?;
        agg_ctx.partial_update_input_all(&mut acc.as_mut(), &input_arrays)?;
        agg_ctx.partial_merge_input_all(&mut acc.as_mut(), acc_array)?;
    }

    // output
    // in no-grouping mode, we always output only one record, so it is not
    // necessary to record elapsed computed time.
    let output_schema = agg_ctx.output_schema.clone();
    context.output_with_sender("Agg", output_schema, move |sender| async move {
        let mut timer = baseline_metrics.elapsed_compute().timer();
        let agg_columns = agg_ctx.build_agg_columns(vec![(&[], acc.as_mut())])?;
        let batch = RecordBatch::try_new_with_options(
            agg_ctx.output_schema.clone(),
            agg_columns,
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )?;
        baseline_metrics.record_output(1);
        sender.send(Ok(batch), Some(&mut timer)).await;
        log::info!("[partition={partition_id}] aggregate exec (no grouping) outputting one record");
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
    let batch_size = batch_size();

    // start processing input batches
    let input = stat_input(
        InputBatchStatistics::from_metrics_set_and_blaze_conf(&metrics, partition_id)?,
        input.execute(partition_id, context.clone())?,
    )?;
    let mut coalesced = context.coalesce_with_default_batch_size(input, &baseline_metrics)?;

    let output_schema = agg_ctx.output_schema.clone();
    context.output_with_sender("Agg", output_schema, move |sender| async move {
        let mut staging_records = vec![];
        let mut current_record: Option<(SlimBytes, OwnedAccumStateRow)> = None;
        let mut timer = baseline_metrics.elapsed_compute().timer();
        timer.stop();

        macro_rules! flush_staging {
            () => {{
                let mut staging_records = std::mem::take(&mut staging_records);
                let batch = agg_ctx.convert_records_to_batch(
                    staging_records
                        .iter_mut()
                        .map(|(key, acc)| (key, acc.as_mut()))
                        .collect(),
                )?;
                let num_rows = batch.num_rows();
                baseline_metrics.record_output(num_rows);
                sender.send(Ok(batch), Some(&mut timer)).await;
            }};
        }
        while let Some(input_batch) = coalesced.next().await.transpose()? {
            timer.restart();

            // compute grouping rows
            let grouping_rows = agg_ctx.create_grouping_rows(&input_batch)?;

            // compute input arrays
            let input_arrays = agg_ctx.create_input_arrays(&input_batch)?;
            let acc_array = agg_ctx.get_input_acc_array(&input_batch)?;

            // update to current record
            for (row_idx, grouping_row) in grouping_rows.into_iter().enumerate() {
                // if group key differs, renew one and move the old record to staging
                if Some(grouping_row.as_ref()) != current_record.as_ref().map(|r| r.0.as_ref()) {
                    let finished_record = current_record
                        .replace((grouping_row.as_ref().into(), agg_ctx.initial_acc.clone()));
                    if let Some(record) = finished_record {
                        staging_records.push(record);
                        if staging_records.len() >= batch_size {
                            flush_staging!();
                        }
                    }
                }
                let acc = &mut current_record.as_mut().unwrap().1.as_mut();
                agg_ctx.partial_update_input(acc, &input_arrays, row_idx)?;
                agg_ctx.partial_merge_input(acc, acc_array, row_idx)?;
            }
            timer.stop();
        }

        timer.restart();
        if let Some(record) = current_record {
            staging_records.push(record);
        }
        if !staging_records.is_empty() {
            flush_staging!();
        }
        Ok(())
    })
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::{
        assert_batches_sorted_eq,
        common::{Result, ScalarValue},
        physical_expr::{expressions as phys_expr, expressions::Column},
        physical_plan::{common, memory::MemoryExec, ExecutionPlan},
        prelude::SessionContext,
    };

    use crate::{
        agg::{
            create_agg,
            AggExecMode::HashAgg,
            AggExpr, AggFunction,
            AggMode::{Final, Partial},
            GroupingExpr,
        },
        agg_exec::AggExec,
        memmgr::MemManager,
    };

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

        let agg_expr_sum = create_agg(
            AggFunction::Sum,
            &[phys_expr::col("a", &input.schema())?],
            &input.schema(),
        )?;

        let agg_expr_avg = create_agg(
            AggFunction::Avg,
            &[phys_expr::col("b", &input.schema())?],
            &input.schema(),
        )?;

        let agg_expr_max = create_agg(
            AggFunction::Max,
            &[phys_expr::col("d", &input.schema())?],
            &input.schema(),
        )?;

        let agg_expr_min = create_agg(
            AggFunction::Min,
            &[phys_expr::col("e", &input.schema())?],
            &input.schema(),
        )?;

        let agg_expr_count = create_agg(
            AggFunction::Count,
            &[phys_expr::col("f", &input.schema())?],
            &input.schema(),
        )?;

        let agg_expr_collectlist = create_agg(
            AggFunction::CollectList,
            &[phys_expr::col("g", &input.schema())?],
            &input.schema(),
        )?;

        let agg_expr_collectset = create_agg(
            AggFunction::CollectSet,
            &[phys_expr::col("h", &input.schema())?],
            &input.schema(),
        )?;

        let agg_expr_collectlist_nil = create_agg(
            AggFunction::CollectList,
            &[Arc::new(phys_expr::Literal::new(ScalarValue::Utf8(None)))],
            &input.schema(),
        )?;

        let agg_expr_collectset_nil = create_agg(
            AggFunction::CollectSet,
            &[Arc::new(phys_expr::Literal::new(ScalarValue::Utf8(None)))],
            &input.schema(),
        )?;

        let agg_expr_firstign = create_agg(
            AggFunction::FirstIgnoresNull,
            &[phys_expr::col("h", &input.schema())?],
            &input.schema(),
        )?;

        let aggs_agg_expr = vec![
            AggExpr {
                field_name: "agg_expr_sum".to_string(),
                mode: Partial,
                agg: agg_expr_sum,
            },
            AggExpr {
                field_name: "agg_expr_avg".to_string(),
                mode: Partial,
                agg: agg_expr_avg,
            },
            AggExpr {
                field_name: "agg_expr_max".to_string(),
                mode: Partial,
                agg: agg_expr_max,
            },
            AggExpr {
                field_name: "agg_expr_min".to_string(),
                mode: Partial,
                agg: agg_expr_min,
            },
            AggExpr {
                field_name: "agg_expr_count".to_string(),
                mode: Partial,
                agg: agg_expr_count,
            },
            AggExpr {
                field_name: "agg_expr_collectlist".to_string(),
                mode: Partial,
                agg: agg_expr_collectlist,
            },
            AggExpr {
                field_name: "agg_expr_collectset".to_string(),
                mode: Partial,
                agg: agg_expr_collectset,
            },
            AggExpr {
                field_name: "agg_expr_collectlist_nil".to_string(),
                mode: Partial,
                agg: agg_expr_collectlist_nil,
            },
            AggExpr {
                field_name: "agg_expr_collectset_nil".to_string(),
                mode: Partial,
                agg: agg_expr_collectset_nil,
            },
            AggExpr {
                field_name: "agg_agg_firstign".to_string(),
                mode: Partial,
                agg: agg_expr_firstign,
            },
        ];

        let agg_exec_partial = AggExec::try_new(
            HashAgg,
            vec![GroupingExpr {
                field_name: "c".to_string(),
                expr: Arc::new(Column::new("c", 2)),
            }],
            aggs_agg_expr.clone(),
            0,
            false,
            input,
        )?;

        let agg_exec_final = AggExec::try_new(
            HashAgg,
            vec![GroupingExpr {
                field_name: "c".to_string(),
                expr: Arc::new(Column::new("c", 0)),
            }],
            aggs_agg_expr
                .into_iter()
                .map(|mut agg| {
                    agg.agg = agg
                        .agg
                        .with_new_exprs(vec![Arc::new(phys_expr::Literal::new(
                            ScalarValue::Null,
                        ))])?;
                    agg.mode = Final;
                    Ok(agg)
                })
                .collect::<Result<_>>()?,
            0,
            false,
            Arc::new(agg_exec_partial),
        )?;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let output_final = agg_exec_final.execute(0, task_ctx)?;
        let batches = common::collect(output_final).await?;
        let expected = vec![
            "+---+--------------+--------------+--------------+--------------+----------------+----------------------+---------------------+--------------------------+-------------------------+------------------+",
            "| c | agg_expr_sum | agg_expr_avg | agg_expr_max | agg_expr_min | agg_expr_count | agg_expr_collectlist | agg_expr_collectset | agg_expr_collectlist_nil | agg_expr_collectset_nil | agg_agg_firstign |",
            "+---+--------------+--------------+--------------+--------------+----------------+----------------------+---------------------+--------------------------+-------------------------+------------------+",
            "| 2 | 4            | 6.0          | -2           | -2           | 1              | [5]                  | [5]                 | []                       | []                      | 5                |",
            "| 5 | 6            | 3.0          | 5            | 5            | 1              | [4]                  | [4]                 | []                       | []                      | 4                |",
            "| 7 | 5            | 0.5          | 71           | -7           | 2              | [6, 6]               | [6]                 | []                       | []                      | 6                |",
            "| 8 | 10           | 1.5          | 86           | 83           | 2              | [3, 3]               | [3]                 | []                       | []                      | 3                |",
            "| 9 | 0            | 5.0          | 90           | 90           | 1              | [1]                  | [1]                 | []                       | []                      | 1                |",
            "+---+--------------+--------------+--------------+--------------+----------------+----------------------+---------------------+--------------------------+-------------------------+------------------+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }
}
