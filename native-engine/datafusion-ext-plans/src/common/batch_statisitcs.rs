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

use arrow::record_batch::RecordBatch;
use blaze_jni_bridge::{conf, conf::BooleanConf, is_jni_bridge_inited};
use datafusion::{
    common::Result,
    execution::SendableRecordBatchStream,
    physical_plan::{
        metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder},
        stream::RecordBatchStreamAdapter,
    },
};
use datafusion_ext_commons::array_size::ArraySize;
use futures::StreamExt;

#[derive(Clone)]
pub struct InputBatchStatistics {
    input_batch_count: Count,
    input_batch_mem_size: Count,
    input_row_count: Count,
}

impl InputBatchStatistics {
    pub fn from_metrics_set_and_blaze_conf(
        metrics_set: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Result<Option<Self>> {
        let enabled = is_jni_bridge_inited() && conf::INPUT_BATCH_STATISTICS_ENABLE.value()?;
        Ok(enabled.then_some(Self::from_metrics_set(metrics_set, partition)))
    }

    pub fn from_metrics_set(metrics_set: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            input_batch_count: MetricBuilder::new(metrics_set)
                .counter("input_batch_count", partition),
            input_batch_mem_size: MetricBuilder::new(metrics_set)
                .counter("input_batch_mem_size", partition),
            input_row_count: MetricBuilder::new(metrics_set).counter("input_row_count", partition),
        }
    }

    pub fn record_input_batch(&self, input_batch: &RecordBatch) {
        let mem_size = input_batch.get_array_mem_size();
        let num_rows = input_batch.num_rows();
        self.input_batch_count.add(1);
        self.input_batch_mem_size.add(mem_size);
        self.input_row_count.add(num_rows);
    }
}

pub fn stat_input(
    input_batch_statistics: Option<InputBatchStatistics>,
    input: SendableRecordBatchStream,
) -> Result<SendableRecordBatchStream> {
    if let Some(input_batch_statistics) = input_batch_statistics {
        let input_batch_statistics_cloned = input_batch_statistics.clone();
        let stat_input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            input.schema(),
            input.inspect(move |batch_result| {
                if let Ok(batch) = &batch_result {
                    input_batch_statistics_cloned.record_input_batch(batch);
                }
            }),
        ));
        return Ok(stat_input);
    }
    Ok(input)
}
