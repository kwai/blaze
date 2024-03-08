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
    fmt::Formatter,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use async_trait::async_trait;
use datafusion::{
    error::Result,
    execution::context::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        metrics::MetricsSet, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
        Partitioning::UnknownPartitioning, RecordBatchStream, SendableRecordBatchStream,
        Statistics,
    },
};
use futures::Stream;

#[derive(Debug, Clone)]
pub struct EmptyPartitionsExec {
    schema: SchemaRef,
    num_partitions: usize,
}

impl EmptyPartitionsExec {
    pub fn new(schema: SchemaRef, num_partitions: usize) -> Self {
        Self {
            schema,
            num_partitions,
        }
    }
}

impl DisplayAs for EmptyPartitionsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "EmptyPartitionsExec: partitions={}, schema={:?}",
            &self.num_partitions, &self.schema,
        )
    }
}

#[async_trait]
impl ExecutionPlan for EmptyPartitionsExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        UnknownPartitioning(self.num_partitions)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(EmptyStream(self.schema.clone())))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn statistics(&self) -> Result<Statistics> {
        todo!()
    }
}

struct EmptyStream(SchemaRef);

impl RecordBatchStream for EmptyStream {
    fn schema(&self) -> SchemaRef {
        self.0.clone()
    }
}

impl Stream for EmptyStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}
