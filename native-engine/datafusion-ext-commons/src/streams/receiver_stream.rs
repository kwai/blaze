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

use std::task::Poll;
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::common::AbortOnDropMany;
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::RecordBatchStream;
use futures::Stream;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

pub struct ReceiverStream {
    schema: SchemaRef,
    input: Receiver<ArrowResult<RecordBatch>>,
    baseline_metrics: BaselineMetrics,
    _drop_helper: AbortOnDropMany<()>,
}

impl ReceiverStream {
    pub fn new(
        schema: SchemaRef,
        input: Receiver<ArrowResult<RecordBatch>>,
        baseline_metrics: BaselineMetrics,
        join_handles: Vec<JoinHandle<()>>,
    ) -> Self {
        Self {
            schema,
            input,
            baseline_metrics,
            _drop_helper: AbortOnDropMany(join_handles),
        }
    }
}

impl Stream for ReceiverStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.input.poll_recv(cx);
        self.baseline_metrics.record_poll(poll)
    }
}

impl RecordBatchStream for ReceiverStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
