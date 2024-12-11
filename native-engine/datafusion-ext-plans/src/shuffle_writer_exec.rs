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

//! Defines the External shuffle repartition plan

use std::{any::Any, fmt::Debug, sync::Arc};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    error::Result,
    execution::context::TaskContext,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
        DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream, Statistics,
    },
};
use datafusion_ext_commons::df_execution_err;
use once_cell::sync::OnceCell;

use crate::{
    common::execution_context::ExecutionContext,
    memmgr::MemManager,
    shuffle::{
        single_repartitioner::SingleShuffleRepartitioner,
        sort_repartitioner::SortShuffleRepartitioner, ShuffleRepartitioner,
    },
};

/// The shuffle writer operator maps each input partition to M output partitions
/// based on a partitioning scheme. No guarantees are made about the order of
/// the resulting partitions.
#[derive(Debug)]
pub struct ShuffleWriterExec {
    input: Arc<dyn ExecutionPlan>,
    partitioning: Partitioning,
    output_data_file: String,
    output_index_file: String,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}

impl DisplayAs for ShuffleWriterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self.partitioning {
            Partitioning::Hash(..) => {
                write!(f, "ShuffleWriterExec: partitioning={:?} partition type=Hash", self.partitioning, )
            }
            Partitioning::RoundRobinBatch(..) => {
                write!(f, "ShuffleWriterExec: partitioning={:?} partition type=RoundRobin", self.partitioning, )
            }
            _ =>  write!(f, "ShuffleWriterExec: partitioning={:?} partition type=unknow", self.partitioning, )
        }
    }
}

#[async_trait]
impl ExecutionPlan for ShuffleWriterExec {
    fn name(&self) -> &str {
        "ShuffleWriterExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        self.props.get_or_init(|| {
            PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                self.partitioning.clone(),
                ExecutionMode::Bounded,
            )
        })
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(ShuffleWriterExec::try_new(
                children[0].clone(),
                self.partitioning.clone(),
                self.output_data_file.clone(),
                self.output_index_file.clone(),
            )?)),
            _ => df_execution_err!("ShuffleWriterExec wrong number of children"),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // record uncompressed data size
        let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);
        let output_time = exec_ctx.register_timer_metric("output_io_time");

        let repartitioner: Arc<dyn ShuffleRepartitioner> = match &self.partitioning {
            p if p.partition_count() == 1 => Arc::new(SingleShuffleRepartitioner::new(
                self.output_data_file.clone(),
                self.output_index_file.clone(),
                output_time,
            )),
            Partitioning::Hash(..) | Partitioning::RoundRobinBatch(..) => {
                let partitioner = Arc::new(SortShuffleRepartitioner::new(
                    exec_ctx.clone(),
                    self.output_data_file.clone(),
                    self.output_index_file.clone(),
                    self.partitioning.clone(),
                    output_time,
                ));
                MemManager::register_consumer(partitioner.clone(), true);
                partitioner
            }
            p => unreachable!("unsupported partitioning: {:?}", p),
        };

        let input = exec_ctx.execute_with_input_stats(&self.input)?;
        repartitioner.execute(exec_ctx, input)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.input.statistics()
    }
}

impl ShuffleWriterExec {
    /// Create a new ShuffleWriterExec
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
        output_data_file: String,
        output_index_file: String,
    ) -> Result<Self> {
        Ok(ShuffleWriterExec {
            input,
            partitioning,
            metrics: ExecutionPlanMetricsSet::new(),
            output_data_file,
            output_index_file,
            props: OnceCell::new(),
        })
    }
}
