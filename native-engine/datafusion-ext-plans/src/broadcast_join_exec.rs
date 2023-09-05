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

use crate::sort_exec::SortExec;
use crate::sort_merge_join_exec::SortMergeJoinExec;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use blaze_jni_bridge::jni_call_static;
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::joins::utils::{
    build_join_schema, check_join_is_valid, JoinFilter, JoinOn,
};
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use futures::stream::once;
use futures::{StreamExt, TryStreamExt};
use jni::sys::{jboolean, JNI_TRUE};
use parking_lot::Mutex;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

#[derive(Debug)]
pub struct BroadcastJoinExec {
    /// Left sorted joining execution plan
    left: Arc<dyn ExecutionPlan>,
    /// Right sorting joining execution plan
    right: Arc<dyn ExecutionPlan>,
    /// Set of common columns used to join on
    on: JoinOn,
    /// How the join is performed
    join_type: JoinType,
    /// Optional filter before outputting
    join_filter: Option<JoinFilter>,
    /// The schema once the join is applied
    schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl BroadcastJoinExec {
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
        join_filter: Option<JoinFilter>,
    ) -> Result<Self> {
        if matches!(
            join_type,
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::RightSemi | JoinType::RightAnti,
        ) {
            if join_filter.is_some() {
                return Err(DataFusionError::Plan(format!(
                    "Semi/Anti join with filter is not supported yet"
                )));
            }
        }

        let left_schema = left.schema();
        let right_schema = right.schema();

        check_join_is_valid(&left_schema, &right_schema, &on)?;
        let schema = Arc::new(build_join_schema(&left_schema, &right_schema, &join_type).0);

        Ok(Self {
            left,
            right,
            on,
            join_type,
            join_filter,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl ExecutionPlan for BroadcastJoinExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.right.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::try_new(
            children[0].clone(),
            children[1].clone(),
            self.on.iter().cloned().collect(),
            self.join_type,
            self.join_filter.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = execute_broadcast_join(
            self.left.clone(),
            self.right.clone(),
            partition,
            context,
            self.on.clone(),
            self.join_type,
            self.join_filter.clone(),
            BaselineMetrics::new(&self.metrics, partition),
        );

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            once(stream).try_flatten(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        unimplemented!()
    }
}

impl DisplayAs for BroadcastJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "BroadcastJoin")
    }
}

async fn execute_broadcast_join(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    partition: usize,
    context: Arc<TaskContext>,
    on: JoinOn,
    join_type: JoinType,
    join_filter: Option<JoinFilter>,
    metrics: BaselineMetrics,
) -> Result<SendableRecordBatchStream> {
    let enabled_fallback_to_smj: bool =
        jni_call_static!(BlazeConf.enableBhjFallbacksToSmj() -> jboolean)? == JNI_TRUE;
    let bhj_num_rows_limit: usize =
        jni_call_static!(BlazeConf.bhjFallbacksToSmjRowsThreshold() -> i32)? as usize;
    let bhj_mem_size_limit: usize =
        jni_call_static!(BlazeConf.bhjFallbacksToSmjMemThreshold() -> i32)? as usize;

    // if broadcasted size is small enough, use hash join
    // otherwise use sort-merge join
    #[derive(Debug)]
    enum JoinMode {
        Hash,
        SortMerge,
    }
    let mut join_mode = JoinMode::Hash;

    let left_schema = left.schema();
    let mut left = left;

    if enabled_fallback_to_smj {
        let mut left_stream = left.execute(0, context.clone())?.fuse();
        let mut left_cached: Vec<RecordBatch> = vec![];
        let mut left_num_rows = 0;
        let mut left_mem_size = 0;

        // read and cache batches from broadcasted side until reached limits
        while let Some(batch) = left_stream.next().await.transpose()? {
            left_num_rows += batch.num_rows();
            left_mem_size += batch.get_array_memory_size();
            left_cached.push(batch);
            if left_num_rows > bhj_num_rows_limit || left_mem_size > bhj_mem_size_limit {
                join_mode = JoinMode::SortMerge;
                break;
            }
        }

        // convert left cached and rest batches into execution plan
        let left_cached_stream: SendableRecordBatchStream = Box::pin(MemoryStream::try_new(
            left_cached,
            left_schema.clone(),
            None,
        )?);
        let left_rest_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            left_schema.clone(),
            left_stream,
        ));
        let left_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            left_schema.clone(),
            left_cached_stream.chain(left_rest_stream),
        ));
        left = Arc::new(RecordBatchStreamsWrapperExec {
            schema: left_schema.clone(),
            stream: Mutex::new(Some(left_stream)),
            output_partitioning: right.output_partitioning(),
        });
    }

    match join_mode {
        JoinMode::Hash => {
            let join = Arc::new(HashJoinExec::try_new(
                left.clone(),
                right.clone(),
                on,
                join_filter,
                &join_type,
                PartitionMode::CollectLeft,
                false,
            )?);
            log::info!("BroadcastJoin is using hash join mode: {:?}", &join);

            let join_schema = join.schema();
            let completed = join
                .execute(partition, context)?
                .chain(futures::stream::poll_fn(move |_| {
                    // update metrics
                    let join_metrics = join.metrics().unwrap();
                    metrics.record_output(join_metrics.output_rows().unwrap_or(0));
                    metrics.elapsed_compute().add_duration(Duration::from_nanos(
                        [
                            join_metrics
                                .sum_by_name("build_time")
                                .map(|v| v.as_usize() as u64),
                            join_metrics
                                .sum_by_name("join_time")
                                .map(|v| v.as_usize() as u64),
                        ]
                        .into_iter()
                        .flatten()
                        .sum(),
                    ));
                    Poll::Ready(None)
                }));
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                join_schema,
                completed,
            )))
        }
        JoinMode::SortMerge => {
            let sort_exprs: Vec<PhysicalSortExpr> = on
                .iter()
                .map(|(_col_left, col_right)| PhysicalSortExpr {
                    expr: Arc::new(Column::new("", col_right.index())),
                    options: Default::default(),
                })
                .collect();

            let right_sorted = Arc::new(SortExec::new(right, sort_exprs.clone(), None));
            let join = Arc::new(SortMergeJoinExec::try_new(
                left.clone(),
                right_sorted.clone(),
                on,
                join_type,
                join_filter,
                sort_exprs.into_iter().map(|se| se.options).collect(),
            )?);
            log::info!("BroadcastJoin is using sort-merge join mode: {:?}", &join);

            let join_schema = join.schema();
            let completed = join
                .execute(partition, context)?
                .chain(futures::stream::poll_fn(move |_| {
                    // update metrics
                    let right_sorted_metrics = right_sorted.metrics().unwrap();
                    let join_metrics = join.metrics().unwrap();
                    metrics.record_output(join_metrics.output_rows().unwrap_or(0));
                    metrics.elapsed_compute().add_duration(Duration::from_nanos(
                        [right_sorted_metrics.elapsed_compute(), join_metrics.elapsed_compute()]
                            .into_iter()
                            .flatten()
                            .sum::<usize>() as u64,
                    ));
                    Poll::Ready(None)
                }));
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                join_schema,
                completed,
            )))
        }
    }
}

struct RecordBatchStreamsWrapperExec {
    schema: SchemaRef,
    stream: Mutex<Option<SendableRecordBatchStream>>,
    output_partitioning: Partitioning,
}

impl Debug for RecordBatchStreamsWrapperExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordBatchStreamsWrapper")
    }
}

impl DisplayAs for RecordBatchStreamsWrapperExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "RecordBatchStreamsWrapper")
    }
}

impl ExecutionPlan for RecordBatchStreamsWrapperExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.output_partitioning.clone()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = std::mem::take(&mut *self.stream.lock());
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            Box::pin(futures::stream::iter(stream).flatten()),
        )))
    }

    fn statistics(&self) -> Statistics {
        unimplemented!()
    }
}
