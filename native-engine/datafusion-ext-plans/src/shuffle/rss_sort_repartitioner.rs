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

use std::sync::Weak;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::{common::Result, physical_plan::Partitioning};
use datafusion_ext_commons::df_execution_err;
use futures::lock::Mutex;
use jni::objects::GlobalRef;

use crate::{
    memmgr::{MemConsumer, MemConsumerInfo, MemManager},
    shuffle::{buffered_data::BufferedData, ShuffleRepartitioner},
};

pub struct RssSortShuffleRepartitioner {
    name: String,
    mem_consumer_info: Option<Weak<MemConsumerInfo>>,
    data: Mutex<BufferedData>,
    partitioning: Partitioning,
    rss: GlobalRef,
}

impl RssSortShuffleRepartitioner {
    pub fn new(
        partition_id: usize,
        rss_partition_writer: GlobalRef,
        partitioning: Partitioning,
    ) -> Self {
        Self {
            name: format!("RssSortShufflePartitioner[partition={}]", partition_id),
            mem_consumer_info: None,
            data: Mutex::new(BufferedData::new(partition_id)),
            partitioning,
            rss: rss_partition_writer,
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
        let data = self.data.lock().await.drain();
        let rss = self.rss.clone();
        let partitioning = self.partitioning.clone();

        tokio::task::spawn_blocking(move || data.write_rss(rss, &partitioning))
            .await
            .or_else(|err| df_execution_err!("{err}"))??;
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
        // update memory usage before adding to buffered data
        let mem_used = self.data.lock().await.mem_used() + input.get_array_memory_size() * 2;
        self.update_mem_used(mem_used).await?;

        // add batch to buffered data
        let mem_used = {
            let mut data = self.data.lock().await;
            data.add_batch(input, &self.partitioning)?;
            data.mem_used()
        };
        self.update_mem_used(mem_used).await?;

        // we are likely to spill more frequently because the cost of spilling a shuffle
        // repartition is lower than other consumers.
        // rss shuffle spill has even lower cost than normal shuffle
        if self.mem_used_percent() > 0.4 {
            self.spill().await?;
        }
        Ok(())
    }

    async fn shuffle_write(&self) -> Result<()> {
        self.set_spillable(false);
        let has_data = self.data.lock().await.mem_used() > 0;
        if has_data {
            self.spill().await?;
        }
        Ok(())
    }
}
