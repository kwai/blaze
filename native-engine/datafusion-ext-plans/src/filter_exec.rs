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

use arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::common::Statistics;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use datafusion_ext_commons::streams::coalesce_stream::CoalesceStream;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct FilterExec {
    inner: Arc<datafusion::physical_plan::filter::FilterExec>,
}

impl FilterExec {
    pub fn try_new(
        predicate: Arc<dyn PhysicalExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let inner = Arc::new(datafusion::physical_plan::filter::FilterExec::try_new(
            predicate, input,
        )?);
        Ok(Self { inner })
    }
}

impl ExecutionPlan for FilterExec {
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
        Ok(Arc::new(Self::try_new(
            self.inner.predicate().clone(),
            children[0].clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let batch_size = context.session_config().batch_size();
        let filtered = self.inner.execute(partition, context)?;
        let elapsed_compute = self
            .inner
            .metrics()
            .unwrap()
            .iter()
            .filter_map(|m| match m.value() {
                MetricValue::ElapsedCompute(timer) => Some(timer.clone()),
                _ => None,
            })
            .next()
            .unwrap();

        let coalesced =
            Box::pin(CoalesceStream::new(filtered, batch_size, elapsed_compute));
        Ok(coalesced)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        self.inner.fmt_as(t, f)
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}
