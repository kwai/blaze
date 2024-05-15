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
    pin::Pin,
    task::{Context, Poll},
};

use arrow::{
    array::{Array, StructArray},
    datatypes::SchemaRef,
    ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use blaze_jni_bridge::{jni_call, jni_new_object};
use datafusion::{
    error::Result,
    physical_plan::{
        metrics::{BaselineMetrics, Count},
        RecordBatchStream,
    },
};
use futures::Stream;
use jni::objects::{GlobalRef, JObject};

use crate::array_size::ArraySize;

pub struct FFIReaderStream {
    schema: SchemaRef,
    export_iter: GlobalRef,
    baseline_metrics: BaselineMetrics,
    size_counter: Count,
}

impl FFIReaderStream {
    pub fn new(
        schema: SchemaRef,
        export_iter: GlobalRef,
        baseline_metrics: BaselineMetrics,
        size_counter: Count,
    ) -> Self {
        Self {
            schema,
            export_iter,
            baseline_metrics,
            size_counter,
        }
    }
}

impl RecordBatchStream for FFIReaderStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for FFIReaderStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(batch) = self.next_batch()? {
            return self
                .baseline_metrics
                .record_poll(Poll::Ready(Some(Ok(batch))));
        }
        Poll::Ready(None)
    }
}

impl FFIReaderStream {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if !jni_call!(ScalaIterator(self.export_iter.as_obj()).hasNext() -> bool)? {
            return Ok(None);
        }
        let consumer = jni_call!(
            ScalaIterator(self.export_iter.as_obj()).next() -> JObject
        )?;

        // load batch from ffi
        let mut ffi_arrow_schema = FFI_ArrowSchema::empty();
        let mut ffi_arrow_array = FFI_ArrowArray::empty();

        let ffi_arrow_schema_ptr = jni_new_object!(JavaLong(
            &mut ffi_arrow_schema as *mut FFI_ArrowSchema as i64
        ))?;
        let ffi_arrow_array_ptr =
            jni_new_object!(JavaLong(&mut ffi_arrow_array as *mut FFI_ArrowArray as i64))?;
        jni_call!(ScalaFunction2(consumer.as_obj()).apply(
            ffi_arrow_schema_ptr.as_obj(),
            ffi_arrow_array_ptr.as_obj(),
        ) -> JObject)?;

        let imported = unsafe { from_ffi(ffi_arrow_array, &ffi_arrow_schema)? };
        let struct_array = StructArray::from(imported);
        let batch = RecordBatch::try_new_with_options(
            self.schema(), // reuse the shared schema
            struct_array.columns().to_vec(),
            &RecordBatchOptions::new().with_row_count(Some(struct_array.len())),
        )?;
        self.size_counter.add(batch.get_array_mem_size());
        Ok(Some(batch))
    }
}
