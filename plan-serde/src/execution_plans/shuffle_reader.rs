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

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};

/// ShuffleReaderExec reads partitions that have already been materialized by a ShuffleWriterExec
/// being executed by an executor
#[derive(Debug, Clone)]
pub struct ShuffleReaderExec {
    pub(crate) schema: SchemaRef,
    /// Execution metrics
    _metrics: ExecutionPlanMetricsSet,
}

impl ShuffleReaderExec {
    /// Create a new ShuffleReaderExec
    pub fn try_new(schema: SchemaRef) -> Result<Self> {
        Ok(Self {
            schema,
            _metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

#[async_trait]
impl ExecutionPlan for ShuffleReaderExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        todo!()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "Ballista ShuffleReaderExec does not support with_new_children()".to_owned(),
        ))
    }

    async fn execute(
        &self,
        _partition: usize,
        _runtime: Arc<RuntimeEnv>,
    ) -> Result<SendableRecordBatchStream> {
        todo!()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        todo!()
    }

    fn fmt_as(&self, _t: DisplayFormatType, _f: &mut std::fmt::Formatter) -> std::fmt::Result {
        todo!()
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}
