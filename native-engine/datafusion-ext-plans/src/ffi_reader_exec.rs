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
    fmt::{Debug, Formatter},
    sync::Arc,
};

use arrow::{
    array::{Array, RecordBatch, RecordBatchOptions, StructArray},
    datatypes::{DataType, SchemaRef},
    ffi::{from_ffi_and_data_type, FFI_ArrowArray},
};
use blaze_jni_bridge::{jni_call, jni_call_static, jni_new_global_ref, jni_new_string};
use datafusion::{
    error::Result,
    execution::context::TaskContext,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
        DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan,
        Partitioning::UnknownPartitioning,
        PlanProperties, SendableRecordBatchStream, Statistics,
    },
};
use datafusion_ext_commons::arrow::array_size::BatchSize;
use jni::objects::GlobalRef;
use once_cell::sync::OnceCell;

use crate::common::execution_context::ExecutionContext;

pub struct FFIReaderExec {
    num_partitions: usize,
    schema: SchemaRef,
    export_iter_provider_resource_id: String,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
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
            props: OnceCell::new(),
        }
    }
}

impl Debug for FFIReaderExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FFIReader")
    }
}

impl DisplayAs for FFIReaderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "FFIReader")
    }
}

impl ExecutionPlan for FFIReaderExec {
    fn name(&self) -> &str {
        "FFIReaderExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        self.props.get_or_init(|| {
            PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                UnknownPartitioning(self.num_partitions),
                ExecutionMode::Bounded,
            )
        })
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            self.num_partitions,
            self.export_iter_provider_resource_id.clone(),
            self.schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);
        let resource_id = jni_new_string!(&self.export_iter_provider_resource_id)?;
        let exporter = jni_new_global_ref!(
            jni_call_static!(JniBridge.getResource(resource_id.as_obj()) -> JObject)?.as_obj()
        )?;

        read_ffi(self.schema(), exporter, exec_ctx.clone())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        todo!()
    }
}

fn read_ffi(
    schema: SchemaRef,
    exporter: GlobalRef,
    exec_ctx: Arc<ExecutionContext>,
) -> Result<SendableRecordBatchStream> {
    let size_counter = exec_ctx.register_counter_metric("size");
    let exec_ctx_cloned = exec_ctx.clone();
    Ok(exec_ctx
        .clone()
        .output_with_sender("FFIReader", move |sender| async move {
            struct AutoCloseableExporter(GlobalRef);
            impl Drop for AutoCloseableExporter {
                fn drop(&mut self) {
                    let _ = jni_call!(JavaAutoCloseable(self.0.as_obj()).close() -> ());
                }
            }
            let exporter = AutoCloseableExporter(exporter);

            loop {
                let batch = {
                    // load batch from ffi
                    let mut ffi_arrow_array = FFI_ArrowArray::empty();
                    let ffi_arrow_array_ptr = &mut ffi_arrow_array as *mut FFI_ArrowArray as i64;
                    let exporter_obj = exporter.0.clone();
                    let has_next = tokio::task::spawn_blocking(move || {
                        jni_call!(
                            BlazeArrowFFIExporter(exporter_obj.as_obj())
                                .exportNextBatch(ffi_arrow_array_ptr) -> bool
                        )
                    })
                    .await
                    .expect("tokio spawn_blocking error")?;

                    if !has_next {
                        break;
                    }
                    let import_data_type = DataType::Struct(schema.fields().clone());
                    let imported =
                        unsafe { from_ffi_and_data_type(ffi_arrow_array, import_data_type)? };
                    let struct_array = StructArray::from(imported);
                    let batch = RecordBatch::try_new_with_options(
                        schema.clone(),
                        struct_array.columns().to_vec(),
                        &RecordBatchOptions::new().with_row_count(Some(struct_array.len())),
                    )?;
                    size_counter.add(batch.get_batch_mem_size());
                    exec_ctx_cloned
                        .baseline_metrics()
                        .record_output(batch.num_rows());
                    batch
                };
                sender.send(batch).await;
            }
            Ok(())
        }))
}
