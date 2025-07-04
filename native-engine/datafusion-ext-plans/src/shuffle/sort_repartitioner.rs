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
    fs::OpenOptions,
    io::{Read, Write},
    sync::{Arc, Weak},
};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytesize::ByteSize;
use datafusion::{
    common::{DataFusionError, Result},
    physical_plan::metrics::Time,
};
use datafusion_ext_commons::{arrow::array_size::BatchSize, df_execution_err};
use futures::lock::Mutex;

use crate::{
    common::{
        execution_context::ExecutionContext,
        offsetted::{Offsetted, OffsettedMergeIterator},
        timer_helper::TimerHelper,
    },
    memmgr::{
        MemConsumer, MemConsumerInfo, MemManager,
        spill::{OwnedSpillBufReader, Spill, try_new_spill},
    },
    shuffle::{Partitioning, ShuffleRepartitioner, buffered_data::BufferedData},
};

pub struct SortShuffleRepartitioner {
    exec_ctx: Arc<ExecutionContext>,
    mem_consumer_info: Option<Weak<MemConsumerInfo>>,
    output_data_file: String,
    output_index_file: String,
    data: Mutex<BufferedData>,
    spills: Mutex<Vec<Offsetted<u64, Box<dyn Spill>>>>,
    num_output_partitions: usize,
    output_io_time: Time,
}

impl SortShuffleRepartitioner {
    pub fn new(
        exec_ctx: Arc<ExecutionContext>,
        output_data_file: String,
        output_index_file: String,
        partitioning: Partitioning,
        output_io_time: Time,
    ) -> Self {
        let partition_id = exec_ctx.partition_id();
        let num_output_partitions = partitioning.partition_count();
        Self {
            exec_ctx,
            mem_consumer_info: None,
            output_data_file,
            output_index_file,
            data: Mutex::new(BufferedData::new(
                partitioning,
                partition_id,
                output_io_time.clone(),
            )),
            spills: Mutex::default(),
            num_output_partitions,
            output_io_time,
        }
    }
}

#[async_trait]
impl MemConsumer for SortShuffleRepartitioner {
    fn name(&self) -> &str {
        "SortShuffleRepartitioner"
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
        let spill_metrics = self.exec_ctx.spill_metrics().clone();
        let spill = tokio::task::spawn_blocking(move || {
            let mut spill = try_new_spill(&spill_metrics)?;
            let offsets = data.write(spill.get_buf_writer())?;
            Ok::<_, DataFusionError>(Offsetted::new(offsets, spill))
        })
        .await
        .expect("tokio spawn_blocking error")?;

        self.spills.lock().await.push(spill);
        self.update_mem_used(0).await?;
        Ok(())
    }
}

impl Drop for SortShuffleRepartitioner {
    fn drop(&mut self) {
        MemManager::deregister_consumer(self);
    }
}

#[async_trait]
impl ShuffleRepartitioner for SortShuffleRepartitioner {
    async fn insert_batch(&self, input: RecordBatch) -> Result<()> {
        // update memory usage before adding to buffered data
        let mem_used = self.data.lock().await.mem_used() + input.get_batch_mem_size() * 2;
        self.update_mem_used(mem_used).await?;

        // add batch to buffered data
        let mem_used = {
            let mut data = self.data.lock().await;
            data.add_batch(input)?;
            data.mem_used()
        };
        self.update_mem_used(mem_used).await?;

        // we are likely to spill more frequently because the cost of spilling a shuffle
        // repartition is lower than other consumers.
        let mem_used_percent = self.mem_used_percent();
        if mem_used_percent > 0.8 {
            log::info!(
                "{} memory usage: {}, percent: {:.3}, spilling...",
                self.name(),
                ByteSize(mem_used as u64),
                mem_used_percent,
            );
            self.force_spill().await?;
        }
        Ok(())
    }

    async fn shuffle_write(&self) -> Result<()> {
        self.set_spillable(false);
        let mut spills = std::mem::take(&mut *self.spills.lock().await);
        let data = self.data.lock().await.drain();

        log::info!(
            "{} starts outputting ({} spills + in_mem: {})",
            self.name(),
            spills.len(),
            ByteSize(data.mem_used() as u64)
        );

        let data_file = self.output_data_file.clone();
        let index_file = self.output_index_file.clone();

        // no spills - directly write current batches into final file
        if spills.is_empty() {
            let output_io_time = self.output_io_time.clone();
            tokio::task::spawn_blocking(move || {
                let output_io_time_cloned = output_io_time.clone();
                let _output_io_timer = output_io_time_cloned.timer();

                let mut output_data = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&data_file)?;
                let mut output_index = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&index_file)?;

                // write data file
                // exclude io timer because it is already included buffered_data.write()
                let offsets = output_io_time.exclude_timer(|| data.write(&mut output_data))?;

                // write index file
                let mut offsets_data = vec![];
                for offset in offsets {
                    offsets_data.extend_from_slice(&(offset as i64).to_le_bytes()[..]);
                }
                output_index.write_all(&offsets_data)?;

                Ok::<(), DataFusionError>(())
            })
            .await
            .or_else(|e| df_execution_err!("shuffle write error: {e:?}"))??;
            self.update_mem_used(0).await?;
            return Ok(());
        }

        // write rest data into a spill
        if !data.is_empty() {
            if self.mem_used_percent() < 0.5 {
                let mut spill = Box::new(vec![]);
                let writer = spill.get_buf_writer();
                let offsets = data.write(writer)?;
                self.update_mem_used(spill.len()).await?;
                spills.push(Offsetted::new(offsets, spill));
            } else {
                let spill_metrics = self.exec_ctx.spill_metrics().clone();
                let spill = tokio::task::spawn_blocking(move || {
                    let mut spill = try_new_spill(&spill_metrics)?;
                    let offsets = data.write(spill.get_buf_writer())?;
                    Ok::<_, DataFusionError>(Offsetted::new(offsets, spill))
                })
                .await
                .expect("tokio spawn_blocking error")?;
                self.update_mem_used(0).await?;
                spills.push(spill);
            }
        }

        // append partition in each spills
        let num_output_partitions = self.num_output_partitions;
        let output_io_time = self.output_io_time.clone();
        tokio::task::spawn_blocking(move || {
            let _output_io_timer = output_io_time.timer();
            let mut output_data = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&data_file)?;
            let mut output_index = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&index_file)?;

            let mut merge_iter = OffsettedMergeIterator::new(
                num_output_partitions,
                spills
                    .into_iter()
                    .map(|spill| spill.map_data(|s| OwnedSpillBufReader::from(s)))
                    .collect(),
            );

            while let Some((_partition_id, reader, range)) = merge_iter.next() {
                let mut reader = reader.buf_reader().take(range.end - range.start);
                std::io::copy(&mut reader, &mut output_data)?;
            }
            let offsets = merge_iter.merged_offsets();

            // write index file
            let mut offsets_data = vec![];
            for &offset in offsets {
                offsets_data.extend_from_slice(&(offset as i64).to_le_bytes()[..]);
            }
            output_index.write_all(&offsets_data)?;

            Ok::<(), DataFusionError>(())
        })
        .await
        .or_else(|e| df_execution_err!("shuffle write error: {e:?}"))??;

        self.update_mem_used(0).await?;
        Ok(())
    }
}
