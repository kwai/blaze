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
use futures::StreamExt;

#[derive(Clone)]
pub struct InputBatchStatistics {
    input_batch_count: Count,
    input_batch_mem_size_total: Count,
    input_batch_mem_size_avg: Count,
    input_batch_num_rows_avg: Count,
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
            input_batch_mem_size_total: MetricBuilder::new(metrics_set)
                .counter("input_batch_mem_size_total", partition),
            input_batch_mem_size_avg: MetricBuilder::new(metrics_set)
                .counter("input_batch_mem_size_avg", partition),
            input_batch_num_rows_avg: MetricBuilder::new(metrics_set)
                .counter("input_batch_num_rows_avg", partition),
            input_row_count: MetricBuilder::new(metrics_set).counter("input_row_count", partition),
        }
    }

    pub fn record_input_batch(&self, input_batch: &RecordBatch) {
        let mem_size = input_batch.get_array_memory_size();
        let num_rows = input_batch.num_rows();

        self.input_batch_count.add(1);
        self.input_batch_mem_size_total.add(mem_size);
        self.input_row_count.add(num_rows);

        let input_batch_count = self.input_batch_count.value();
        let num_rows_avg = self.input_row_count.value() / input_batch_count;
        let mem_size_avg = self.input_batch_mem_size_total.value() / input_batch_count;
        self.input_batch_num_rows_avg
            .add(num_rows_avg - self.input_batch_num_rows_avg.value());
        self.input_batch_mem_size_avg
            .add(mem_size_avg - self.input_batch_mem_size_avg.value());
    }
}

pub fn stat_input(
    input_batch_statistics: Option<InputBatchStatistics>,
    input: SendableRecordBatchStream,
) -> Result<SendableRecordBatchStream> {
    if let Some(input_batch_statistics) = input_batch_statistics {
        return Ok(Box::pin(RecordBatchStreamAdapter::new(
            input.schema(),
            input.inspect(move |batch_result| {
                if let Ok(batch) = &batch_result {
                    input_batch_statistics.record_input_batch(batch);
                }
            }),
        )));
    }
    Ok(input)
}
