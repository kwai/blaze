// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
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
    array::{RecordBatch, RecordBatchOptions},
    datatypes::SchemaRef,
};
use auron_jni_bridge::conf::{IntConf, UDAF_FALLBACK_NUM_UDAFS_TRIGGER_SORT_AGG};
use datafusion::{
    common::{Result, Statistics},
    error::DataFusionError,
    execution::context::TaskContext,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
    },
};
use datafusion_ext_commons::{batch_size, downcast_any};
use futures::StreamExt;
use once_cell::sync::OnceCell;

use crate::{
    agg::{
        AggExecMode, AggExpr, GroupingExpr,
        agg::IdxSelection,
        agg_ctx::AggContext,
        agg_table::{AggTable, OwnedKey},
        spark_udaf_wrapper::SparkUDAFWrapper,
    },
    common::{execution_context::ExecutionContext, timer_helper::TimerHelper},
    expand_exec::ExpandExec,
    memmgr::MemManager,
    project_exec::ProjectExec,
    sort_exec::create_default_ascending_sort_exec,
};

#[derive(Debug)]
pub struct AggExec {
    input: Arc<dyn ExecutionPlan>,
    agg_ctx: Arc<AggContext>,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}

impl AggExec {
    pub fn try_new(
        exec_mode: AggExecMode,
        groupings: Vec<GroupingExpr>,
        aggs: Vec<AggExpr>,
        supports_partial_skipping: bool,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        // do not trigger partial skipping if input is ExpandExec
        let is_expand_agg = match &input {
            e if downcast_any!(e, ExpandExec).is_ok() => true,
            e if downcast_any!(e, ProjectExec).is_ok() => {
                downcast_any!(&e.children()[0], ExpandExec).is_ok()
            }
            _ => false,
        };

        let agg_ctx = Arc::new(AggContext::try_new(
            exec_mode,
            input.schema(),
            groupings,
            aggs,
            supports_partial_skipping,
            is_expand_agg,
        )?);

        Ok(Self {
            input,
            agg_ctx,
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
        })
    }
}

impl ExecutionPlan for AggExec {
    fn name(&self) -> &str {
        "AggExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.agg_ctx.output_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        self.props.get_or_init(|| {
            PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                self.input.output_partitioning().clone(),
                EmissionType::Both,
                Boundedness::Bounded,
            )
        })
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: children[0].clone(),
            agg_ctx: self.agg_ctx.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);
        let output = execute_agg(self.input.clone(), exec_ctx.clone(), self.agg_ctx.clone())?;
        Ok(exec_ctx.coalesce_with_default_batch_size(output))
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

fn execute_agg(
    input: Arc<dyn ExecutionPlan>,
    exec_ctx: Arc<ExecutionContext>,
    agg_ctx: Arc<AggContext>,
) -> Result<SendableRecordBatchStream> {
    if agg_ctx.groupings.is_empty() {
        let input = exec_ctx.execute_with_input_stats(&input)?;
        return execute_agg_no_grouping(input, exec_ctx, agg_ctx);
    }

    Ok(match agg_ctx.exec_mode {
        AggExecMode::HashAgg => {
            let num_udafs_trigger_sort_agg = UDAF_FALLBACK_NUM_UDAFS_TRIGGER_SORT_AGG
                .value()
                .unwrap_or(1) as usize;
            let num_udafs = agg_ctx
                .aggs
                .iter()
                .filter(|agg| downcast_any!(agg.agg, SparkUDAFWrapper).is_ok())
                .count();
            if num_udafs >= num_udafs_trigger_sort_agg {
                let input_sort_exec = create_default_ascending_sort_exec(
                    input,
                    &agg_ctx
                        .groupings
                        .iter()
                        .map(|g| g.expr.clone())
                        .collect::<Vec<_>>(),
                    Some(exec_ctx.execution_plan_metrics().clone()),
                    false, // do not record output metric
                );
                let input_sorted = exec_ctx.clone().execute(&input_sort_exec)?;
                execute_agg_sorted(input_sorted, exec_ctx.clone(), agg_ctx)?
            } else {
                let input = exec_ctx.execute_with_input_stats(&input)?;
                execute_agg_with_grouping_hash(input, exec_ctx, agg_ctx)?
            }
        }
        AggExecMode::SortAgg => {
            let input = exec_ctx.execute_with_input_stats(&input)?;
            execute_agg_sorted(input, exec_ctx, agg_ctx)?
        }
    })
}

fn execute_agg_with_grouping_hash(
    input_stream: SendableRecordBatchStream,
    exec_ctx: Arc<ExecutionContext>,
    agg_ctx: Arc<AggContext>,
) -> Result<SendableRecordBatchStream> {
    // create tables
    let tables = Arc::new(AggTable::try_new(agg_ctx.clone(), exec_ctx.clone())?);
    MemManager::register_consumer(tables.clone(), true);

    // start processing input batches
    let mut coalesced = exec_ctx.coalesce_with_default_batch_size(input_stream);

    Ok(exec_ctx
        .clone()
        .output_with_sender("Agg", |sender| async move {
            let elapsed_compute = exec_ctx.baseline_metrics().elapsed_compute().clone();
            sender.exclude_time(&elapsed_compute);
            let _timer = elapsed_compute.timer();

            log::info!(
                "start hash aggregating, supports_partial_skipping={}, num_groupings={}, num_partial={}, num_partial_merge={}, num_final={}",
                agg_ctx.supports_partial_skipping,
                agg_ctx.groupings.len(),
                agg_ctx.aggs.iter().filter(|agg| agg.mode.is_partial()).count(),
                agg_ctx.aggs.iter().filter(|agg| agg.mode.is_partial_merge()).count(),
                agg_ctx.aggs.iter().filter(|agg| agg.mode.is_final()).count(),
            );
            let mut partial_skipping_triggered = false;

            while let Some(batch) = elapsed_compute
                .exclude_timer_async(coalesced.next())
                .await
                .transpose()?
            {
                // output records without aggregation if partial skipping is triggered
                if partial_skipping_triggered {
                    let exec_ctx = exec_ctx.clone();
                    let sender = sender.clone();
                    agg_ctx
                        .process_partial_skipped(batch, exec_ctx, sender)
                        .await?;
                    continue;
                }

                // insert or update rows into in-mem table
                match tables.process_input_batch(batch).await {
                    Ok(()) => {}
                    Err(DataFusionError::Execution(s)) if s == "AGG_TRIGGER_PARTIAL_SKIPPING" => {
                        // trigger partial skipping: flush in-mem table and directly
                        // output rest records without aggregation
                        // note: current batch has been updated to table
                        tables.output(sender.clone()).await?;
                        partial_skipping_triggered = true;
                        continue;
                    }
                    Err(DataFusionError::Execution(s)) if s == "AGG_SPILL_PARTIAL_SKIPPING" => {
                        // never spill if partial skipping is enabled
                        // note: current batch has been updated to table
                        tables.output(sender.clone()).await?;
                        continue;
                    }
                    Err(err) => return Err(err),
                }
            }
            tables.output(sender.clone()).await?;
            Ok(())
        }))
}

fn execute_agg_no_grouping(
    input_stream: SendableRecordBatchStream,
    exec_ctx: Arc<ExecutionContext>,
    agg_ctx: Arc<AggContext>,
) -> Result<SendableRecordBatchStream> {
    let mut acc_table = agg_ctx.create_acc_table(1);

    // start processing input batches
    let mut coalesced = exec_ctx.coalesce_with_default_batch_size(input_stream);

    // output
    // in no-grouping mode, we always output only one record, so it is not
    // necessary to record elapsed computed time.
    Ok(exec_ctx
        .clone()
        .output_with_sender("Agg", move |sender| async move {
            let elapsed_compute = exec_ctx.baseline_metrics().elapsed_compute().clone();
            sender.exclude_time(&elapsed_compute);
            let _timer = elapsed_compute.timer();

            while let Some(batch) = elapsed_compute
                .exclude_timer_async(coalesced.next())
                .await
                .transpose()?
            {
                agg_ctx.update_batch_to_acc_table(
                    &batch,
                    &mut acc_table,
                    IdxSelection::Single(0),
                )?;
            }

            let agg_columns = agg_ctx.build_agg_columns(&mut acc_table, IdxSelection::Single(0))?;
            let batch = RecordBatch::try_new_with_options(
                agg_ctx.output_schema.clone(),
                agg_columns,
                &RecordBatchOptions::new().with_row_count(Some(1)),
            )?;
            exec_ctx.baseline_metrics().record_output(1);
            sender.send(batch).await;
            log::info!("aggregate exec (no grouping) outputting one record");
            Ok(())
        }))
}

fn execute_agg_sorted(
    input: SendableRecordBatchStream,
    exec_ctx: Arc<ExecutionContext>,
    agg_ctx: Arc<AggContext>,
) -> Result<SendableRecordBatchStream> {
    let batch_size = batch_size();

    // start processing input batches
    let mut coalesced = exec_ctx.coalesce_with_default_batch_size(input);

    Ok(exec_ctx
        .clone()
        .output_with_sender("Agg", move |sender| async move {
            let elapsed_compute = exec_ctx.baseline_metrics().elapsed_compute().clone();
            sender.exclude_time(&elapsed_compute);
            let _timer = elapsed_compute.timer();

            let mut staging_keys: Vec<OwnedKey> = vec![];
            let mut staging_acc_table = agg_ctx.create_acc_table(0);
            let mut acc_indices = vec![];

            macro_rules! flush_staging {
                () => {{
                    let batch = agg_ctx.convert_records_to_batch(
                        &staging_keys,
                        &mut staging_acc_table,
                        IdxSelection::Range(0, staging_keys.len()),
                    )?;
                    let num_rows = batch.num_rows();
                    staging_keys.clear();
                    staging_acc_table.resize(0);
                    exec_ctx.baseline_metrics().record_output(num_rows);
                    sender.send((batch)).await;
                }};
            }

            while let Some(batch) = elapsed_compute
                .exclude_timer_async(coalesced.next())
                .await
                .transpose()?
            {
                // compute grouping rows
                let grouping_rows = agg_ctx.create_grouping_rows(&batch)?;

                // update to current record
                let mut batch_range_start = 0;
                let mut batch_range_end = 0;
                while batch_range_end < batch.num_rows() {
                    let grouping_row = &grouping_rows.row(batch_range_end);
                    let same_key =
                        matches!(staging_keys.last(), Some(k) if k == grouping_row.as_ref());
                    if !same_key {
                        if staging_keys.len() >= batch_size {
                            agg_ctx.update_batch_slice_to_acc_table(
                                &batch,
                                batch_range_start,
                                batch_range_end,
                                &mut staging_acc_table,
                                IdxSelection::Indices(&acc_indices),
                            )?;
                            acc_indices.clear();
                            batch_range_start = batch_range_end;
                            flush_staging!();
                        }
                        staging_keys.push(OwnedKey::from(grouping_row.as_ref()));
                    }
                    acc_indices.push(staging_keys.len() - 1);
                    batch_range_end += 1;
                }

                agg_ctx.update_batch_slice_to_acc_table(
                    &batch,
                    batch_range_start,
                    batch_range_end,
                    &mut staging_acc_table,
                    IdxSelection::Indices(&acc_indices),
                )?;
                acc_indices.clear();
            }

            if !staging_keys.is_empty() {
                flush_staging!();
            }
            Ok(())
        }))
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
        physical_plan::{ExecutionPlan, test::TestMemoryExec},
        prelude::SessionContext,
    };

    use crate::{
        agg::{
            AggExecMode::HashAgg,
            AggExpr, AggFunction,
            AggMode::{Final, Partial},
            GroupingExpr,
            agg::create_agg,
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
        Arc::new(TestMemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
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
            DataType::Int64,
        )?;

        let agg_expr_avg = create_agg(
            AggFunction::Avg,
            &[phys_expr::col("b", &input.schema())?],
            &input.schema(),
            DataType::Float64,
        )?;

        let agg_expr_max = create_agg(
            AggFunction::Max,
            &[phys_expr::col("d", &input.schema())?],
            &input.schema(),
            DataType::Int32,
        )?;

        let agg_expr_min = create_agg(
            AggFunction::Min,
            &[phys_expr::col("e", &input.schema())?],
            &input.schema(),
            DataType::Int32,
        )?;

        let agg_expr_count = create_agg(
            AggFunction::Count,
            &[phys_expr::col("f", &input.schema())?],
            &input.schema(),
            DataType::Int64,
        )?;

        let agg_expr_collectlist = create_agg(
            AggFunction::CollectList,
            &[phys_expr::col("g", &input.schema())?],
            &input.schema(),
            DataType::new_list(DataType::Int32, false),
        )?;

        let agg_expr_collectset = create_agg(
            AggFunction::CollectSet,
            &[phys_expr::col("h", &input.schema())?],
            &input.schema(),
            DataType::new_list(DataType::Int32, false),
        )?;

        let agg_expr_collectlist_nil = create_agg(
            AggFunction::CollectList,
            &[Arc::new(phys_expr::Literal::new(ScalarValue::Utf8(None)))],
            &input.schema(),
            DataType::new_list(DataType::Utf8, false),
        )?;

        let agg_expr_collectset_nil = create_agg(
            AggFunction::CollectSet,
            &[Arc::new(phys_expr::Literal::new(ScalarValue::Utf8(None)))],
            &input.schema(),
            DataType::new_list(DataType::Utf8, false),
        )?;

        let agg_expr_firstign = create_agg(
            AggFunction::FirstIgnoresNull,
            &[phys_expr::col("h", &input.schema())?],
            &input.schema(),
            DataType::Int32,
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
            false,
            Arc::new(agg_exec_partial),
        )?;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let output_final = agg_exec_final.execute(0, task_ctx)?;
        let batches = datafusion::physical_plan::common::collect(output_final).await?;
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

#[cfg(test)]
mod fuzztest {
    use std::{collections::HashMap, sync::Arc};

    use arrow::{
        array::{Array, ArrayRef, AsArray, Float64Builder, Int64Builder},
        compute::concat_batches,
        datatypes::{DataType, Float64Type, Int64Type},
        record_batch::RecordBatch,
    };
    use datafusion::{
        common::Result,
        physical_expr::expressions as phys_expr,
        physical_plan::test::TestMemoryExec,
        prelude::{SessionConfig, SessionContext},
    };

    use crate::{
        agg::{
            AggExecMode::HashAgg,
            AggExpr,
            AggMode::{Final, Partial},
            GroupingExpr,
            count::AggCount,
            sum::AggSum,
        },
        agg_exec::AggExec,
        memmgr::MemManager,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn fuzztest() -> Result<()> {
        MemManager::init(1000); // small memory config to trigger spill
        let session_ctx =
            SessionContext::new_with_config(SessionConfig::new().with_batch_size(10000));
        let task_ctx = session_ctx.task_ctx();

        let mut verify_sum_map: HashMap<i64, f64> = HashMap::new();
        let mut verify_cnt_map: HashMap<i64, i64> = HashMap::new();
        let mut batches = vec![];
        for _batch_id in 0..100 {
            let mut key_builder = Int64Builder::new();
            let mut val_builder = Float64Builder::new();

            for _ in 0..10000 {
                // will trigger spill
                let key = (rand::random::<u32>() % 1_000_000) as i64;
                let val = (rand::random::<u32>() % 1_000_000) as f64;
                let test_null = rand::random::<u32>() % 1000 == 0;

                key_builder.append_value(key);
                if !test_null {
                    val_builder.append_null();
                    continue;
                }
                val_builder.append_value(val);
                verify_sum_map
                    .entry(key)
                    .and_modify(|v| *v += val)
                    .or_insert(val);
                verify_cnt_map
                    .entry(key)
                    .and_modify(|v| *v += 1)
                    .or_insert(1);
            }
            let key_col: ArrayRef = Arc::new(key_builder.finish());
            let val_col: ArrayRef = Arc::new(val_builder.finish());
            let batch = RecordBatch::try_from_iter_with_nullable(vec![
                ("key", key_col, false),
                ("val", val_col, true),
            ])?;
            batches.push(batch);
        }

        let schema = batches[0].schema();
        let input = Arc::new(TestMemoryExec::try_new(
            &[batches.clone()],
            schema.clone(),
            None,
        )?);
        let partial_agg = Arc::new(AggExec::try_new(
            HashAgg,
            vec![GroupingExpr {
                field_name: format!("key"),
                expr: phys_expr::col("key", &schema)?,
            }],
            vec![
                AggExpr {
                    field_name: "sum".to_string(),
                    mode: Partial,
                    agg: Arc::new(AggSum::try_new(
                        phys_expr::col("val", &schema)?,
                        DataType::Float64,
                    )?),
                },
                AggExpr {
                    field_name: "cnt".to_string(),
                    mode: Partial,
                    agg: Arc::new(AggCount::try_new(
                        vec![phys_expr::col("val", &schema)?],
                        DataType::Int64,
                    )?),
                },
            ],
            true,
            input,
        )?);
        let final_agg = Arc::new(AggExec::try_new(
            HashAgg,
            vec![GroupingExpr {
                field_name: format!("key"),
                expr: phys_expr::col("key", &schema)?,
            }],
            vec![
                AggExpr {
                    field_name: "sum".to_string(),
                    mode: Final,
                    agg: Arc::new(AggSum::try_new(
                        phys_expr::col("val", &schema)?,
                        DataType::Float64,
                    )?),
                },
                AggExpr {
                    field_name: "cnt".to_string(),
                    mode: Final,
                    agg: Arc::new(AggCount::try_new(
                        vec![phys_expr::col("val", &schema)?],
                        DataType::Int64,
                    )?),
                },
            ],
            false,
            partial_agg,
        )?);

        let output = datafusion::physical_plan::collect(final_agg, task_ctx.clone()).await?;
        let a = concat_batches(&output[0].schema(), &output)?;

        let key_col = a.column(0).as_primitive::<Int64Type>();
        let sum_col = a.column(1).as_primitive::<Float64Type>();
        let cnt_col = a.column(2).as_primitive::<Int64Type>();
        for i in 0..key_col.len() {
            assert!(key_col.is_valid(i));
            assert!(cnt_col.is_valid(i));
            if sum_col.is_valid(i) {
                let key = key_col.value(i);
                let val = sum_col.value(i);
                let cnt = cnt_col.value(i);
                assert_eq!(
                    verify_sum_map[&key] as i64, val as i64,
                    "key={key}, sum not matched"
                );
                assert_eq!(verify_cnt_map[&key], cnt, "key={key}, cnt not matched");
            } else {
                let cnt = cnt_col.value(i);
                assert_eq!(cnt, 0);
            }
        }
        Ok(())
    }
}
