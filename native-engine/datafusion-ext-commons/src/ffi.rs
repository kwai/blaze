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

use std::sync::mpsc::Receiver;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use datafusion::common::Result;
use blaze_commons::is_task_running;

/// RecordBatchReader for FFI_ArrowArrayStraem
pub struct MpscBatchReader {
    pub schema: SchemaRef,
    pub receiver: Receiver<Option<Result<RecordBatch>>>,
}

impl RecordBatchReader for MpscBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Iterator for MpscBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.receiver
            .recv()
            .unwrap_or_else(|err| {
                // sender is unexpectedly died, terminate this stream
                // errors should have been handled in sender side
                let task_running = is_task_running();
                log::warn!(
                    "MpscBatchReader broken (task_running={}): {}",
                    task_running,
                    err,
                );
                None
            })
            .map(|result| {
                result.map_err(|err| err.into())
            })
    }
}
