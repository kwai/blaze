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

use arrow::datatypes::SchemaRef;
use blaze_jni_bridge::{jni_call, jni_call_static, jni_new_global_ref, jni_new_string};
use datafusion::{
    error::Result,
    execution::context::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
        Partitioning::UnknownPartitioning,
        SendableRecordBatchStream, Statistics,
    },
};
use datafusion_ext_commons::streams::ffi_stream::FFIReaderStream;
use jni::objects::JObject;

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

impl DisplayAs for FFIReaderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
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
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let resource_id = jni_new_string!(&self.export_iter_provider_resource_id)?;
        let export_iter_provider =
            jni_call_static!(JniBridge.getResource(resource_id.as_obj()) -> JObject)?;
        let export_iter_local =
            jni_call!(ScalaFunction0(export_iter_provider.as_obj()).apply() -> JObject)?;
        let export_iter = jni_new_global_ref!(export_iter_local.as_obj())?;

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let size_counter = MetricBuilder::new(&self.metrics).counter("size", partition);

        Ok(Box::pin(FFIReaderStream::new(
            self.schema.clone(),
            export_iter,
            baseline_metrics,
            size_counter,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        todo!()
    }
}
