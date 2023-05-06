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

use crate::common::memory_manager::{MemConsumer, MemConsumerInfo, MemManager};
use crate::common::output::output_with_sender;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::common::{Result, Statistics};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::joins::utils::{JoinFilter, JoinOn};
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::metrics::{MetricsSet, Time};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use datafusion_ext_commons::streams::coalesce_stream::CoalesceStream;
use futures::stream::once;
use futures::{StreamExt, TryStreamExt};
use std::any::Any;
use std::fmt::Formatter;
use std::sync::{Arc, Weak};

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
            *null_equals_null,
        )?);
        Ok(Self { inner })
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
                *self.inner.partition_mode(),
                self.inner.null_equals_null(),
            )?),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mem_tracker = Arc::new(MemTracker {
            name: format!("BroadcastHashSide[partition={}]", partition),
            mem_consumer_info: None,
        });
        MemManager::register_consumer(mem_tracker.clone(), false);

        // left is always the built side, wrap it with a BroadcastSideWrapperExec
        let wrapped_left = Arc::new(BroadcastSideWrapperExec {
            inner: self.inner.children()[0].clone(),
            mem_tracker: mem_tracker.clone(),
        });

        // execute with the wrapped children
        let output = self
            .inner
            .clone()
            .with_new_children(vec![wrapped_left, self.inner.children()[1].clone()])?
            .execute(partition, context)?;

        // hold mem_tracker until join finished
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            once(stream_holding_mem_tracker(output, mem_tracker)).try_flatten(),
        ));
        Ok(stream)
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
    mem_tracker: Arc<MemTracker>,
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
            context.session_config().batch_size(),
            elapsed_compute,
        ));

        let stream = Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            once(stream_with_mem_tracker(coalesced, self.mem_tracker.clone())).try_flatten(),
        ));
        Ok(stream)
    }

    fn statistics(&self) -> Statistics {
        self.inner.statistics()
    }
}

#[derive(Debug)]
struct MemTracker {
    name: String,
    mem_consumer_info: Option<Weak<MemConsumerInfo>>,
}

#[async_trait]
impl MemConsumer for MemTracker {
    fn name(&self) -> &str {
        &self.name
    }

    fn set_consumer_info(&mut self, consumer_info: Weak<MemConsumerInfo>) {
        self.mem_consumer_info = Some(consumer_info);
    }

    fn get_consumer_info(&self) -> &Weak<MemConsumerInfo> {
        self.mem_consumer_info
            .as_ref()
            .expect("consumer info not set")
    }
}

impl Drop for MemTracker {
    fn drop(&mut self) {
        MemManager::deregister_consumer(self);
    }
}

async fn stream_with_mem_tracker(
    mut input: SendableRecordBatchStream,
    mem_tracker: Arc<MemTracker>,
) -> Result<SendableRecordBatchStream> {
    output_with_sender("BroadcastHashJoin", input.schema(), |sender| async move {
        let mut mem_used = 0;
        while let Some(batch) = input.next().await.transpose()? {
            mem_used += batch.get_array_memory_size() * 3 / 2;
            mem_tracker.update_mem_used(mem_used).await?;
            sender.send(Ok(batch), None).await;
        }
        Ok(())
    })
}

async fn stream_holding_mem_tracker(
    mut input: SendableRecordBatchStream,
    mem_tracker: Arc<MemTracker>,
) -> Result<SendableRecordBatchStream> {
    output_with_sender("BroadcastHashJoin", input.schema(), |sender| async move {
        while let Some(batch) = input.next().await.transpose()? {
            sender.send(Ok(batch), None).await;
        }
        mem_tracker.update_mem_used(0).await?;
        Ok(())
    })
}
