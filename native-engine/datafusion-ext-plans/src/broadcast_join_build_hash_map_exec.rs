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
    datatypes::SchemaRef,
    row::{RowConverter, SortField},
};
use datafusion::{
    common::Result,
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::{Partitioning, PhysicalExpr, PhysicalSortExpr},
    physical_plan::{
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan,
    },
};
use futures::{stream::once, TryStreamExt};

use crate::{
    common::output::{NextBatchWithTimer, TaskOutputter},
    joins::join_hash_map::{build_join_hash_map, join_hash_map_schema},
};

pub struct BroadcastJoinBuildHashMapExec {
    input: Arc<dyn ExecutionPlan>,
    keys: Vec<Arc<dyn PhysicalExpr>>,
    metrics: ExecutionPlanMetricsSet,
}

impl BroadcastJoinBuildHashMapExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, keys: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        Self {
            input,
            keys,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl Debug for BroadcastJoinBuildHashMapExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BroadcastJoinBuildHashMap [{:?}]", self.keys)
    }
}

impl DisplayAs for BroadcastJoinBuildHashMapExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "BroadcastJoinBuildHashMapExec [{:?}]", self.keys)
    }
}

impl ExecutionPlan for BroadcastJoinBuildHashMapExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        join_hash_map_schema(&self.input.schema())
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.input.output_partitioning().partition_count())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children[0].clone(), self.keys.clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let input = self.input.execute(partition, context.clone())?;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            once(execute_build_hash_map(
                context,
                input,
                self.keys.clone(),
                baseline_metrics,
            ))
            .try_flatten(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

async fn execute_build_hash_map(
    context: Arc<TaskContext>,
    mut input: SendableRecordBatchStream,
    keys: Vec<Arc<dyn PhysicalExpr>>,
    metrics: BaselineMetrics,
) -> Result<SendableRecordBatchStream> {
    let elapsed_compute = metrics.elapsed_compute().clone();
    let mut timer = elapsed_compute.timer();

    let mut data_batches = vec![];
    let data_schema = input.schema();

    // collect all input batches
    while let Some(batch) = input.next_batch(Some(&mut timer)).await? {
        data_batches.push(batch);
    }

    // evaluate keys
    let key_row_converter = RowConverter::new(
        keys.iter()
            .map(|key| Ok(SortField::new(key.data_type(&data_schema)?)))
            .collect::<Result<_>>()?,
    )?;
    let keys = data_batches
        .iter()
        .map(|batch| {
            let key_columns = keys
                .iter()
                .map(|key| {
                    Ok::<_, DataFusionError>(key.evaluate(batch)?.into_array(batch.num_rows()))?
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(key_row_converter.convert_columns(&key_columns)?)
        })
        .collect::<Result<Vec<_>>>()?;

    // build hash map
    let hash_map_schema = join_hash_map_schema(&data_schema);
    let hash_map = build_join_hash_map(&data_batches, data_schema, &keys)?;
    drop(timer);

    // output hash map batches as stream
    context.output_with_sender("BuildHashMap", hash_map_schema, move |sender| async move {
        let mut timer = elapsed_compute.timer();
        for batch in hash_map.into_hash_map_batches() {
            sender.send(Ok(batch), Some(&mut timer)).await;
        }
        Ok(())
    })
}
