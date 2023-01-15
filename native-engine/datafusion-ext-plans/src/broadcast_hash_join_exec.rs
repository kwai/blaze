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
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use datafusion::common::{Result, Statistics};
use datafusion::execution::context::TaskContext;
use datafusion::execution::MemoryConsumerId;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream};
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::joins::utils::{JoinFilter, JoinOn};
use datafusion::physical_plan::metrics::{MetricsSet, Time};
use futures::{ready, Stream, StreamExt};
use datafusion_ext_commons::streams::coalesce_stream::CoalesceStream;

// TODO:
//  in spark broadcast hash join, the hash table is built on driver side.
//  we should consider reimplement a hash join which takes a built hash map
//  instead of record batches as input.
//  at this moment, this implementtation is just a wrapper of datafusion's
//  HashJoinExec with a memory tracker.

#[derive(Debug)]
pub struct BroadcastHashJoinExec {
    inner: Arc<HashJoinExec>,
}

impl BroadcastHashJoinExec {
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: Option<JoinFilter>,
        join_type: &JoinType,
        partition_mode: PartitionMode,
        null_equals_null: &bool,
    ) -> Result<Self> {

        let inner = Arc::new(HashJoinExec::try_new(
            left,
            right,
            on,
            filter,
            join_type,
            partition_mode,
            null_equals_null,
        )?);
        Ok(Self {inner})
    }
}

impl ExecutionPlan for BroadcastHashJoinExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.inner.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.inner.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            inner: Arc::new(HashJoinExec::try_new(
                children[0].clone(),
                children[1].clone(),
                self.inner.on().iter().map(Clone::clone).collect::<Vec<_>>(),
                self.inner.filter().map(Clone::clone),
                self.inner.join_type(),
                self.inner.partition_mode().clone(),
                self.inner.null_equals_null()
            )?)
        }))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        // left is always the built side, wrap it with a BroadcastSideWrapperExec
        let wrapped_left = Arc::new(BroadcastSideWrapperExec {
            inner: self.inner.children()[0].clone(),
        });

        // execute with the wrapped children
        self.inner.clone().with_new_children(vec![
            wrapped_left,
            self.inner.children()[1].clone(),
        ])?
        .execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        self.inner.fmt_as(t, f)
    }

    fn statistics(&self) -> Statistics {
        self.inner.statistics()
    }
}

/// Wrap the broadcast side with coalescing and memory tracking
#[derive(Debug)]
struct BroadcastSideWrapperExec {
    inner: Arc<dyn ExecutionPlan>,
}
impl ExecutionPlan for BroadcastSideWrapperExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.inner.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.inner.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.clone().with_new_children(children)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let elapsed_compute = Time::new(); // TODO: count this time
        let input = self.inner.execute(partition, context.clone())?;
        let coalesced = Box::pin(CoalesceStream::new(
            input,
            context.clone().session_config().batch_size(),
            elapsed_compute,
        ));
        let mem_tracked = Box::pin(BroadcastMemTrackerStream::new(
            context.runtime_env(), coalesced,
            partition,
        ));
        Ok(mem_tracked)
    }

    fn statistics(&self) -> Statistics {
        self.inner.statistics()
    }
}

struct BroadcastMemTrackerStream {
    id: MemoryConsumerId,
    input: SendableRecordBatchStream,
    runtime: Arc<RuntimeEnv>,
    mem_used: usize,
}

impl BroadcastMemTrackerStream {
    fn new(
        runtime: Arc<RuntimeEnv>,
        input: SendableRecordBatchStream,
        partition_id: usize,
    ) -> Self {
        Self {
            id: MemoryConsumerId::new(partition_id),
            input,
            runtime,
            mem_used: 0,
        }
    }
}

impl Drop for BroadcastMemTrackerStream {
    fn drop(&mut self) {
        self.runtime.drop_consumer(&self.id, self.mem_used);
    }
}

impl RecordBatchStream for BroadcastMemTrackerStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

impl Stream for BroadcastMemTrackerStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match ready!(Pin::new(&mut self.input).poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                // assume the hash map uses 1.5x memory
                let mem_increased = batch.get_array_memory_size() * 3 / 2;
                self.runtime.grow_tracker_usage(mem_increased);
                self.mem_used += mem_increased;
                log::info!(
                    "broadcast hash join received built batch, num_rows={}, total_mem_tracking={}",
                    batch.num_rows(),
                    self.mem_used,
                );
                Poll::Ready(Some(Ok(batch)))
            }
            Some(Err(err)) => {
                Poll::Ready(Some(Err(err)))
            }
            None => {
                Poll::Ready(None)
            }
        }
    }
}