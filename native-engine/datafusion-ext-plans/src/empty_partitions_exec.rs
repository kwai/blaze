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

use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    error::Result,
    execution::context::TaskContext,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan,
        Partitioning::UnknownPartitioning,
        PlanProperties, SendableRecordBatchStream, Statistics,
        execution_plan::{Boundedness, EmissionType},
        memory::MemoryStream,
        metrics::MetricsSet,
    },
};
use once_cell::sync::OnceCell;

#[derive(Debug, Clone)]
pub struct EmptyPartitionsExec {
    schema: SchemaRef,
    num_partitions: usize,
    props: OnceCell<PlanProperties>,
}

impl EmptyPartitionsExec {
    pub fn new(schema: SchemaRef, num_partitions: usize) -> Self {
        Self {
            schema,
            num_partitions,
            props: OnceCell::new(),
        }
    }
}

impl DisplayAs for EmptyPartitionsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "EmptyPartitionsExec: partitions={}, schema={:?}",
            &self.num_partitions, &self.schema,
        )
    }
}

#[async_trait]
impl ExecutionPlan for EmptyPartitionsExec {
    fn name(&self) -> &str {
        "EmptyPartitionsExec"
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
                EmissionType::Both,
                Boundedness::Bounded,
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
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(MemoryStream::try_new(
            vec![],
            self.schema(),
            None,
        )?))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn statistics(&self) -> Result<Statistics> {
        todo!()
    }
}
