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

use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    common::{JoinType, Result, Statistics},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::{Partitioning, PhysicalSortExpr},
    physical_plan::{
        joins::{
            utils::{build_join_schema, check_join_is_valid, JoinFilter},
            NestedLoopJoinExec,
        },
        memory::MemoryExec,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan,
    },
};
use futures::{stream::once, StreamExt, TryStreamExt};
use parking_lot::Mutex;

use crate::broadcast_join_exec::RecordBatchStreamsWrapperExec;

#[derive(Debug)]
pub struct BroadcastNestedLoopJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_type: JoinType,
    filter: Option<JoinFilter>,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
}

impl BroadcastNestedLoopJoinExec {
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_type: JoinType,
        filter: Option<JoinFilter>,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        check_join_is_valid(&left_schema, &right_schema, &[])?;
        let (schema, _column_indices) = build_join_schema(&left_schema, &right_schema, &join_type);

        Ok(Self {
            left,
            right,
            filter,
            join_type,
            schema: Arc::new(schema),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for BroadcastNestedLoopJoinExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "BroadcastNestedLoopJoin")
    }
}

impl ExecutionPlan for BroadcastNestedLoopJoinExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        if left_is_build_side(self.join_type) {
            self.right.output_partitioning()
        } else {
            self.left.output_partitioning()
        }
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
            self.join_type,
            self.filter.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let joined = Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            once(execute_join(
                partition,
                context,
                self.left.clone(),
                self.right.clone(),
                self.join_type,
                self.filter.clone(),
                self.metrics.clone(),
            ))
            .try_flatten(),
        ));
        Ok(joined)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}

async fn execute_join(
    partition: usize,
    context: Arc<TaskContext>,
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_type: JoinType,
    filter: Option<JoinFilter>,
    metrics: ExecutionPlanMetricsSet,
) -> Result<SendableRecordBatchStream> {
    // inner side
    let mut inner_stream = if left_is_build_side(join_type) {
        left.execute(partition, context.clone())?
    } else {
        right.execute(partition, context.clone())?
    };
    let inner_schema = inner_stream.schema();
    let mut inner_batches = vec![];
    while let Some(batch) = inner_stream.next().await.transpose()? {
        inner_batches.push(batch);
    }

    let inner_batch_max_num_rows = inner_batches
        .iter()
        .map(|batch| batch.num_rows())
        .max()
        .unwrap_or(0);
    let inner_batch_max_mem_size = inner_batches
        .iter()
        .map(|batch| batch.get_array_memory_size())
        .max()
        .unwrap_or(0);

    let target_output_num_rows = context.session_config().batch_size();
    let target_output_mem_size = 1 << 26; // 64MB
    let inner_exec: Arc<dyn ExecutionPlan> =
        Arc::new(MemoryExec::try_new(&[inner_batches], inner_schema, None)?);

    // outer side
    let (outer_schema, outer_partitioning, outer_stream) = if left_is_build_side(join_type) {
        (
            right.schema(),
            right.output_partitioning(),
            right.execute(partition, context.clone())?,
        )
    } else {
        (
            left.schema(),
            left.output_partitioning(),
            left.execute(partition, context.clone())?,
        )
    };
    let chunked_outer_stream = Box::pin(RecordBatchStreamAdapter::new(
        outer_schema.clone(),
        outer_stream.flat_map(move |batch_result| match batch_result {
            Ok(batch) => {
                let batch_num_rows = batch.num_rows();
                let batch_mem_size = batch.get_array_memory_size();
                let output_num_rows = batch_num_rows * inner_batch_max_num_rows;
                let output_mem_size = batch_num_rows * inner_batch_max_mem_size
                    + batch_mem_size * inner_batch_max_num_rows;
                let chunk_count = std::cmp::min(
                    (output_num_rows / target_output_num_rows).max(1),
                    (output_mem_size / target_output_mem_size).max(1),
                );
                let chunk_len = (batch_num_rows / chunk_count).max(1);

                let mut chunks = vec![];
                for beg in (0..batch.num_rows()).step_by(chunk_len) {
                    chunks.push(Ok(batch.slice(beg, chunk_len.min(batch.num_rows() - beg))));
                }
                futures::stream::iter(chunks)
            }
            Err(err) => futures::stream::iter(vec![Err(err)]),
        }),
    ));
    let outer_exec: Arc<dyn ExecutionPlan> = Arc::new(RecordBatchStreamsWrapperExec {
        schema: outer_schema,
        stream: Mutex::new(Some(chunked_outer_stream)),
        output_partitioning: outer_partitioning,
    });

    // join with datafusion's builtin NestedLoopJoinExec
    let nlj = if left_is_build_side(join_type) {
        NestedLoopJoinExec::try_new(inner_exec, outer_exec, filter, &join_type)?
    } else {
        NestedLoopJoinExec::try_new(outer_exec, inner_exec, filter, &join_type)?
    };
    let joined = nlj.execute(partition, context)?;

    let baseline_metrics = BaselineMetrics::new(&metrics, partition);
    let output_stream = Box::pin(RecordBatchStreamAdapter::new(
        joined.schema(),
        joined.map(move |batch_result| {
            if let Ok(batch) = &batch_result {
                baseline_metrics.record_output(batch.num_rows());
            }
            batch_result
        }),
    ));
    Ok(output_stream)
}

fn left_is_build_side(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Right | JoinType::RightSemi | JoinType::RightAnti | JoinType::Full
    )
}
