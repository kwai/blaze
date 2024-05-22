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
    fs::{File, OpenOptions},
    io::{BufReader, Read, Seek, Write},
    sync::Weak,
};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::{
    common::{DataFusionError, Result},
    physical_plan::{metrics::ExecutionPlanMetricsSet, Partitioning},
};
use datafusion_ext_commons::{
    df_execution_err,
    ds::rdx_tournament_tree::{KeyForRadixTournamentTree, RadixTournamentTree},
};
use futures::lock::Mutex;

use crate::{
    memmgr::{
        metrics::SpillMetrics,
        spill::{try_new_spill, Spill},
        MemConsumer, MemConsumerInfo, MemManager,
    },
    shuffle::{buffered_data::BufferedData, ShuffleRepartitioner, ShuffleSpill},
};

pub struct SortShuffleRepartitioner {
    name: String,
    mem_consumer_info: Option<Weak<MemConsumerInfo>>,
    output_data_file: String,
    output_index_file: String,
    data: Mutex<BufferedData>,
    spills: Mutex<Vec<ShuffleSpill>>,
    partitioning: Partitioning,
    num_output_partitions: usize,
    spill_metrics: SpillMetrics,
}

impl SortShuffleRepartitioner {
    pub fn new(
        partition_id: usize,
        output_data_file: String,
        output_index_file: String,
        partitioning: Partitioning,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        let num_output_partitions = partitioning.partition_count();
        Self {
            name: format!("SortShufflePartitioner[partition={}]", partition_id),
            mem_consumer_info: None,
            output_data_file,
            output_index_file,
            data: Mutex::new(BufferedData::new(partition_id)),
            spills: Mutex::default(),
            partitioning,
            num_output_partitions,
            spill_metrics: SpillMetrics::new(metrics, partition_id),
        }
    }
}

#[async_trait]
impl MemConsumer for SortShuffleRepartitioner {
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
        let mut spill = try_new_spill(&self.spill_metrics)?;

        let offsets = data.write(spill.get_buf_writer(), &self.partitioning)?;
        self.spills
            .lock()
            .await
            .push(ShuffleSpill { spill, offsets });
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
        if self.mem_used_percent() > 0.8 {
            self.spill().await?;
        }
        Ok(())
    }

    async fn shuffle_write(&self) -> Result<()> {
        self.set_spillable(false);
        let mut spills = std::mem::take(&mut *self.spills.lock().await);
        let data = self.data.lock().await.drain();

        log::info!(
            "{} starts outputting ({} spills)",
            self.name(),
            spills.len()
        );

        let data_file = self.output_data_file.clone();
        let index_file = self.output_index_file.clone();

        // no spills - directly write current batches into final file
        if spills.is_empty() {
            let partitioning = self.partitioning.clone();
            tokio::task::spawn_blocking(move || {
                let mut output_data = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&data_file)?;

                let offsets = data.write(&mut output_data, &partitioning)?;
                output_data.sync_data()?;
                output_data.flush()?;

                let mut output_index = File::create(&index_file)?;
                for offset in offsets {
                    output_index.write_all(&(offset as i64).to_le_bytes()[..])?;
                }
                output_index.sync_data()?;
                output_index.flush()?;
                Ok::<(), DataFusionError>(())
            })
            .await
            .or_else(|e| df_execution_err!("shuffle write error: {e:?}"))??;
            self.update_mem_used(0).await?;
            return Ok(());
        }

        struct SpillCursor<'a> {
            cur: usize,
            reader: BufReader<Box<dyn Read + Send + 'a>>,
            offsets: Vec<u64>,
        }

        impl<'a> KeyForRadixTournamentTree for SpillCursor<'a> {
            fn rdx(&self) -> usize {
                self.cur
            }
        }

        impl<'a> SpillCursor<'a> {
            fn skip_empty_partitions(&mut self) {
                let offsets = &self.offsets;
                while self.cur + 1 < offsets.len() && offsets[self.cur + 1] == offsets[self.cur] {
                    self.cur += 1;
                }
            }
        }

        // write rest data into an in-memory buffer
        if data.mem_used() > 0 {
            let mut spill = Box::new(vec![]);
            let writer = spill.get_buf_writer();
            let offsets = data.write(writer, &self.partitioning)?;
            self.update_mem_used(spill.len()).await?;
            spills.push(ShuffleSpill { spill, offsets });
        }

        let num_output_partitions = self.num_output_partitions;
        let mut offsets = vec![0];

        // append partition in each spills
        tokio::task::spawn_blocking(move || {
            let mut output_data = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(data_file)?;
            let mut cur_partition_id = 0;

            if !spills.is_empty() {
                // select partitions from spills
                let mut cursors = RadixTournamentTree::new(
                    spills
                        .iter_mut()
                        .map(|spill| SpillCursor {
                            cur: 0,
                            reader: spill.spill.get_buf_reader(),
                            offsets: std::mem::take(&mut spill.offsets),
                        })
                        .map(|mut spill| {
                            spill.skip_empty_partitions();
                            spill
                        })
                        .filter(|spill| spill.cur < spill.offsets.len())
                        .collect(),
                    num_output_partitions,
                );

                loop {
                    let mut min_spill = cursors.peek_mut();
                    if min_spill.cur + 1 >= min_spill.offsets.len() {
                        break;
                    }

                    while cur_partition_id < min_spill.cur {
                        offsets.push(output_data.stream_position()?);
                        cur_partition_id += 1;
                    }
                    let (spill_offset_start, spill_offset_end) = (
                        min_spill.offsets[cur_partition_id],
                        min_spill.offsets[cur_partition_id + 1],
                    );

                    let spill_range = spill_offset_start as usize..spill_offset_end as usize;
                    let reader = &mut min_spill.reader;
                    std::io::copy(&mut reader.take(spill_range.len() as u64), &mut output_data)?;

                    // forward partition id in min_spill
                    min_spill.cur += 1;
                    min_spill.skip_empty_partitions();
                }
            }
            output_data.sync_data()?;
            output_data.flush()?;

            // add one extra offset at last to ease partition length computation
            offsets.resize(num_output_partitions + 1, output_data.stream_position()?);

            let mut output_index = File::create(index_file)?;
            for offset in offsets {
                output_index.write_all(&(offset as i64).to_le_bytes()[..])?;
            }
            output_index.sync_data()?;
            output_index.flush()?;
            Ok::<(), DataFusionError>(())
        })
        .await
        .or_else(|e| df_execution_err!("shuffle write error: {e:?}"))??;

        self.update_mem_used(0).await?;
        Ok(())
    }
}
