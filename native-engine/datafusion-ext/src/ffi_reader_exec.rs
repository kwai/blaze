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
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use datafusion::arrow::array::{make_array_from_raw, StructArray};
use datafusion::arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream, Statistics};
use datafusion::physical_plan::common::batch_byte_size;
use datafusion::physical_plan::metrics::{BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::Partitioning::UnknownPartitioning;
use futures::Stream;
use jni::objects::{GlobalRef, JObject};
use jni::sys::{jboolean, JNI_TRUE};
use crate::{jni_call, jni_call_static, jni_delete_local_ref, jni_new_global_ref, jni_new_object, jni_new_string};

pub struct FFIReaderExec {
    num_partitions: usize,
    schema: SchemaRef,
    export_iter_provider_resource_id: String,
    metrics: ExecutionPlanMetricsSet,
}

impl FFIReaderExec {
    pub fn new(
        num_partitions: usize,
        export_iter_provider_resource_id: String,
        schema: SchemaRef,
    ) -> FFIReaderExec {
        FFIReaderExec {
            num_partitions,
            export_iter_provider_resource_id,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl Debug for FFIReaderExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FFIReader")
    }
}

impl ExecutionPlan for FFIReaderExec {
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

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan(
                "Blaze FFIReaderExec expects 0 children".to_owned(),
            ));
        }
        Ok(self)
    }

    fn execute(&self, partition: usize, _context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let export_iter_provider = jni_call_static!(
            JniBridge.getResource(
                jni_new_string!(&self.export_iter_provider_resource_id)?
            ) -> JObject
        )?;

        let export_iter = jni_new_global_ref!(
            jni_call!(ScalaFunction0(export_iter_provider).apply() -> JObject)?
        )?;

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let size_counter = MetricBuilder::new(&self.metrics).counter("size", partition);

        Ok(Box::pin(FFIReaderStream {
            schema: self.schema.clone(),
            export_iter,
            baseline_metrics,
            size_counter,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "FFIReader")
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}

struct FFIReaderStream {
    schema: SchemaRef,
    export_iter: GlobalRef,
    baseline_metrics: BaselineMetrics,
    size_counter: Count,
}

unsafe impl Sync for FFIReaderStream {} // safety: export_iter is safe to be shared
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for FFIReaderStream {}

impl RecordBatchStream for FFIReaderStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for FFIReaderStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(batch) = self.next_batch()? {
            return self.baseline_metrics.record_poll(Poll::Ready(Some(Ok(batch))));
        }
        Poll::Ready(None)
    }
}

impl FFIReaderStream {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let has_next = jni_call!(
            ScalaIterator(self.export_iter.as_obj()).hasNext() -> jboolean
        )?;
        if has_next != JNI_TRUE {
            return Ok(None);
        }
        let consumer = jni_call!(
            ScalaIterator(self.export_iter.as_obj()).next() -> JObject
        )?;

        // load batch from ffi
        let mut ffi_arrow_schema = Box::new(FFI_ArrowSchema::empty());
        let mut ffi_arrow_array = Box::new(FFI_ArrowArray::empty());

        let ffi_arrow_schema_ptr = jni_new_object!(
            JavaLong, ffi_arrow_schema.as_mut() as *mut FFI_ArrowSchema as i64
        )?;
        let ffi_arrow_array_ptr = jni_new_object!(
            JavaLong, ffi_arrow_array.as_mut() as *mut FFI_ArrowArray as i64
        )?;
        let _unit = jni_call!(ScalaFunction2(consumer).apply(
            ffi_arrow_schema_ptr,
            ffi_arrow_array_ptr,
        ) -> JObject)?;

        // consumer is no longer needed
        jni_delete_local_ref!(consumer)?;

        let imported = unsafe {
            make_array_from_raw(ffi_arrow_array.as_ref(), ffi_arrow_schema.as_ref())
                .map_err(|err| DataFusionError::Execution(format!("{}", err)))?
        };
        let struct_array = imported
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let batch = RecordBatch::from(struct_array);

        self.size_counter.add(batch_byte_size(&batch));
        Ok(Some(batch))
    }
}
