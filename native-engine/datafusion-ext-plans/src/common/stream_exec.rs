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
    fmt::{Debug, Formatter},
    sync::Arc,
};

use arrow_schema::SchemaRef;
use datafusion::{
    common::Result,
    execution::TaskContext,
    physical_plan::{
        EmptyRecordBatchStream, ExecutionPlan, SendableRecordBatchStream,
        streaming::{PartitionStream, StreamingTableExec},
    },
};
use parking_lot::Mutex;

// wrap a record batch stream to datafusion execution plan
pub fn create_record_batch_stream_exec(
    stream: SendableRecordBatchStream,
    partition_id: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = stream.schema();
    let empty_partition_stream: Arc<dyn PartitionStream> = Arc::new(SinglePartitionStream::new(
        Box::pin(EmptyRecordBatchStream::new(schema.clone())),
    ));
    let mut streams: Vec<Arc<dyn PartitionStream>> = (0..=partition_id)
        .map(|_| empty_partition_stream.clone())
        .collect();
    streams[partition_id] = Arc::new(SinglePartitionStream::new(stream));

    Ok(Arc::new(StreamingTableExec::try_new(
        schema,
        streams,
        None,
        vec![],
        false,
        None,
    )?))
}

struct SinglePartitionStream(SchemaRef, Arc<Mutex<SendableRecordBatchStream>>);

impl SinglePartitionStream {
    fn new(stream: SendableRecordBatchStream) -> Self {
        Self(stream.schema(), Arc::new(Mutex::new(stream)))
    }
}

impl Debug for SinglePartitionStream {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SinglePartitionStream")
    }
}

impl PartitionStream for SinglePartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.0
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut stream = self.1.lock();
        std::mem::replace(
            &mut *stream,
            Box::pin(EmptyRecordBatchStream::new(self.0.clone())),
        )
    }
}
