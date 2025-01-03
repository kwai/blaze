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
    io::{BufReader, Read, Seek, Write},
    sync::{Arc, Weak},
};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytesize::ByteSize;
use datafusion::{
    common::{DataFusionError, Result},
    physical_plan::metrics::Time,
};
use datafusion_ext_commons::{
    algorithm::rdx_tournament_tree::{KeyForRadixTournamentTree, RadixTournamentTree},
    arrow::array_size::ArraySize,
    df_execution_err,
};
use futures::lock::Mutex;

use crate::{
    common::{execution_context::ExecutionContext, timer_helper::TimerHelper},
    memmgr::{
        spill::{try_new_spill, Spill},
        MemConsumer, MemConsumerInfo, MemManager,
    },
    shuffle::{buffered_data::BufferedData, RePartitioning, ShuffleRepartitioner, ShuffleSpill},
};

pub struct SortShuffleRepartitioner {
    exec_ctx: Arc<ExecutionContext>,
    name: String,
    mem_consumer_info: Option<Weak<MemConsumerInfo>>,
    output_data_file: String,
    output_index_file: String,
    data: Mutex<BufferedData>,
    spills: Mutex<Vec<ShuffleSpill>>,
    num_output_partitions: usize,
    output_io_time: Time,
}

impl SortShuffleRepartitioner {
    pub fn new(
        exec_ctx: Arc<ExecutionContext>,
        output_data_file: String,
        output_index_file: String,
        partitioning: RePartitioning,
        output_io_time: Time,
    ) -> Self {
        let partition_id = exec_ctx.partition_id();
        let sort_time = exec_ctx.register_timer_metric("sort_time");
        let num_output_partitions = partitioning.partition_count();
        Self {
            exec_ctx,
            name: format!("SortShufflePartitioner[partition={partition_id}]"),
            mem_consumer_info: None,
            output_data_file,
            output_index_file,
            data: Mutex::new(BufferedData::new(partitioning, partition_id, sort_time)),
            spills: Mutex::default(),
            num_output_partitions,
            output_io_time,
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
        let mut spill = try_new_spill(self.exec_ctx.spill_metrics())?;

        let offsets = data.write(spill.get_buf_writer())?;
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
        let mem_used = self.data.lock().await.mem_used() + input.get_array_mem_size() * 2;
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
            self.spill().await?;
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
                let mut output_data = output_io_time.wrap_writer(
                    OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(&data_file)?,
                );
                let mut output_index = output_io_time.wrap_writer(
                    OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(&index_file)?,
                );

                // write data file
                let offsets = data.write(&mut output_data)?;

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
        if !data.is_empty() {
            let mut spill = Box::new(vec![]);
            let writer = spill.get_buf_writer();
            let offsets = data.write(writer)?;
            self.update_mem_used(spill.len()).await?;
            spills.push(ShuffleSpill { spill, offsets });
        }

        let num_output_partitions = self.num_output_partitions;
        let mut offsets = vec![0];

        // append partition in each spills
        let output_io_time = self.output_io_time.clone();
        tokio::task::spawn_blocking(move || {
            let mut output_data = output_io_time.wrap_writer(
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&data_file)?,
            );
            let mut output_index = output_io_time.wrap_writer(
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&index_file)?,
            );

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

                let mut cur_partition_id = 0;
                loop {
                    let mut min_spill = cursors.peek_mut();
                    if min_spill.cur + 1 >= min_spill.offsets.len() {
                        break;
                    }

                    while cur_partition_id < min_spill.cur {
                        offsets.push(output_data.0.stream_position()?);
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

            // add one extra offset at last to ease partition length computation
            offsets.resize(num_output_partitions + 1, output_data.0.stream_position()?);

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
        Ok(())
    }
}
