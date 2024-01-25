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

use std::sync::{Arc, Weak};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::{
    common::Result,
    execution::context::TaskContext,
    physical_plan::{metrics::Count, Partitioning},
};
use futures::lock::Mutex;
use jni::objects::GlobalRef;

use crate::{
    memmgr::{MemConsumer, MemConsumerInfo, MemManager},
    shuffle::{
        buffered_data::BufferedData,
        rss::{rss_flush, rss_write_batch},
        sort_batch_by_partition_id, ShuffleRepartitioner,
    },
};

pub struct RssSortShuffleRepartitioner {
    name: String,
    mem_consumer_info: Option<Weak<MemConsumerInfo>>,
    data: Mutex<BufferedData>,
    partitioning: Partitioning,
    rss_partition_writer: GlobalRef,
    batch_size: usize,
    data_size_metric: Count,
}

impl RssSortShuffleRepartitioner {
    pub fn new(
        partition_id: usize,
        rss_partition_writer: GlobalRef,
        partitioning: Partitioning,
        data_size_metric: Count,
        context: Arc<TaskContext>,
    ) -> Self {
        let batch_size = context.session_config().batch_size();
        Self {
            name: format!("RssSortShufflePartitioner[partition={}]", partition_id),
            mem_consumer_info: None,
            data: Mutex::default(),
            partitioning,
            rss_partition_writer,
            batch_size,
            data_size_metric,
        }
    }
}

#[async_trait]
impl MemConsumer for RssSortShuffleRepartitioner {
    fn name(&self) -> &str {
        &self.name
    }

    fn set_consumer_info(&mut self, consumer_info: Weak<MemConsumerInfo>) {
        self.mem_consumer_info = Some(consumer_info);
    }

    fn get_consumer_info(&self) -> &Weak<MemConsumerInfo> {
        self.mem_consumer_info
            .as_ref()
            .expect("consumer info not set")
    }

    async fn spill(&self) -> Result<()> {
        let data = std::mem::take(&mut *self.data.lock().await);

        for (partition_id, batch) in data.into_sorted_batches(self.batch_size) {
            let mut uncompressed_size = 0;
            rss_write_batch(
                &self.rss_partition_writer,
                partition_id as usize,
                batch,
                &mut uncompressed_size,
            )?;
            self.data_size_metric.add(uncompressed_size);
        }
        rss_flush(&self.rss_partition_writer)?;
        self.update_mem_used(0).await?;
        Ok(())
    }
}

impl Drop for RssSortShuffleRepartitioner {
    fn drop(&mut self) {
        MemManager::deregister_consumer(self);
    }
}

#[async_trait]
impl ShuffleRepartitioner for RssSortShuffleRepartitioner {
    async fn insert_batch(&self, input: RecordBatch) -> Result<()> {
        let (partition_indices, sorted_batch) =
            sort_batch_by_partition_id(input, &self.partitioning)?;

        let mut data = self.data.lock().await;
        data.num_rows += sorted_batch.num_rows();
        data.batch_mem_size += sorted_batch.get_array_memory_size();
        data.sorted_partition_indices.push(partition_indices);
        data.sorted_batches.push(sorted_batch);
        let mem_used = data.mem_used();
        drop(data);

        self.update_mem_used(mem_used).await?;
        Ok(())
    }

    async fn shuffle_write(&self) -> Result<()> {
        self.spill().await
    }
}
