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

use async_trait::async_trait;
use blaze_commons::{jni_call, jni_call_static, jni_new_global_ref, jni_new_string};
use arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::metrics::{BaselineMetrics, MetricBuilder};
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::Partitioning::UnknownPartitioning;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::Statistics;
use datafusion_ext_commons::streams::ipc_stream::{IpcReadMode, IpcReaderStream};
use jni::objects::JObject;
use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use datafusion_ext_commons::streams::coalesce_stream::CoalesceStream;

#[derive(Debug, Clone)]
pub struct IpcReaderExec {
    pub num_partitions: usize,
    pub ipc_provider_resource_id: String,
    pub schema: SchemaRef,
    pub mode: IpcReadMode,
    pub metrics: ExecutionPlanMetricsSet,
}
impl IpcReaderExec {
    pub fn new(
        num_partitions: usize,
        ipc_provider_resource_id: String,
        schema: SchemaRef,
        mode: IpcReadMode,
    ) -> IpcReaderExec {
        IpcReaderExec {
            num_partitions,
            ipc_provider_resource_id,
            schema,
            mode,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

#[async_trait]
impl ExecutionPlan for IpcReaderExec {
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
        Err(DataFusionError::Plan(
            "Blaze ShuffleReaderExec does not support with_new_children()".to_owned(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let size_counter = MetricBuilder::new(&self.metrics).counter("size", partition);

        let elapsed_compute = baseline_metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer();

        let segments_provider = jni_call_static!(
            JniBridge.getResource(
                jni_new_string!(&self.ipc_provider_resource_id)?
            ) -> JObject
        )?;
        let segments = jni_new_global_ref!(
            jni_call!(ScalaFunction0(segments_provider).apply() -> JObject)?
        )?;

        let schema = self.schema.clone();
        let mode = self.mode;
        let ipc_stream = Box::pin(IpcReaderStream::new(
            schema,
            segments,
            mode,
            baseline_metrics,
            size_counter,
        ));
        Ok(Box::pin(CoalesceStream::new(
            ipc_stream,
            context.session_config().batch_size(),
            BaselineMetrics::new(&self.metrics, partition)
                .elapsed_compute()
                .clone(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "IpcReader: [{:?}]", &self.schema)
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}
