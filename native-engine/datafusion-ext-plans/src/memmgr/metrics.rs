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

use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, Time,
};

#[derive(Clone)]
pub struct SpillMetrics {
    pub mem_spill_count: Count,
    pub mem_spill_size: Gauge,
    pub mem_spill_iotime: Time,
    pub disk_spill_size: Gauge,
    pub disk_spill_iotime: Time,
}

impl SpillMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            mem_spill_count: MetricBuilder::new(metrics).counter("mem_spill_count", partition),
            mem_spill_size: MetricBuilder::new(metrics).gauge("mem_spill_size", partition),
            mem_spill_iotime: MetricBuilder::new(metrics)
                .subset_time("mem_spill_iotime", partition),
            disk_spill_size: MetricBuilder::new(metrics).gauge("disk_spill_size", partition),
            disk_spill_iotime: MetricBuilder::new(metrics)
                .subset_time("disk_spill_iotime", partition),
        }
    }
}
