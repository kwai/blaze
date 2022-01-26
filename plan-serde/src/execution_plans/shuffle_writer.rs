// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! ShuffleWriterExec represents a section of a query plan that has consistent partitioning and
//! can be executed as one unit with each partition being executed in parallel. The output of each
//! partition is re-partitioned and streamed to disk in Arrow IPC format. Future stages of the query
//! will use the ShuffleReaderExec to read these results.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::Result;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};

/// ShuffleWriterExec represents a section of a query plan that has consistent partitioning and
/// can be executed as one unit with each partition being executed in parallel. The output of each
/// partition is re-partitioned and streamed to disk in Arrow IPC format. Future stages of the query
/// will use the ShuffleReaderExec to read these results.
#[derive(Debug, Clone)]
pub struct ShuffleWriterExec {
    /// Unique ID for the job (query) that this stage is a part of
    job_id: String,
    /// Unique query stage ID within the job
    stage_id: usize,
    /// Physical execution plan for this query stage
    plan: Arc<dyn ExecutionPlan>,
    /// Path to write output streams to
    _work_dir: String,
    /// Optional shuffle output partitioning
    shuffle_output_partitioning: Option<Partitioning>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl ShuffleWriterExec {
    /// Create a new shuffle writer
    pub fn try_new(
        job_id: String,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        _work_dir: String,
        shuffle_output_partitioning: Option<Partitioning>,
    ) -> Result<Self> {
        Ok(Self {
            job_id,
            stage_id,
            plan,
            _work_dir,
            shuffle_output_partitioning,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Get the Job ID for this query stage
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    /// Get the Stage ID for this query stage
    pub fn stage_id(&self) -> usize {
        self.stage_id
    }

    /// Get the true output partitioning
    pub fn shuffle_output_partitioning(&self) -> Option<&Partitioning> {
        self.shuffle_output_partitioning.as_ref()
    }
}

#[async_trait]
impl ExecutionPlan for ShuffleWriterExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        // This operator needs to be executed once for each *input* partition and there
        // isn't really a mechanism yet in DataFusion to support this use case so we report
        // the input partitioning as the output partitioning here. The executor reports
        // output partition meta data back to the scheduler.
        self.plan.output_partitioning()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.plan.clone()]
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    async fn execute(
        &self,
        _partition: usize,
        _runtime: Arc<RuntimeEnv>,
    ) -> Result<SendableRecordBatchStream> {
        todo!()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "ShuffleWriterExec: {:?}",
                    self.shuffle_output_partitioning
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.plan.statistics()
    }
}
