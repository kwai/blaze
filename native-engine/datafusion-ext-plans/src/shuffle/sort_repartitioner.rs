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
    sync::{Arc, Weak},
};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::{
    common::{DataFusionError, Result},
    execution::context::TaskContext,
    physical_plan::{
        metrics::{Count, ExecutionPlanMetricsSet},
        Partitioning,
    },
};
use datafusion_ext_commons::{
    df_execution_err,
    loser_tree::{ComparableForLoserTree, LoserTree},
};
use futures::lock::Mutex;

use crate::{
    memmgr::{
        metrics::SpillMetrics,
        onheap_spill::{try_new_spill, Spill},
        MemConsumer, MemConsumerInfo, MemManager,
    },
    shuffle,
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
    batch_size: usize,
    data_size_metric: Count,
    spill_metrics: SpillMetrics,
}

impl SortShuffleRepartitioner {
    pub fn new(
        partition_id: usize,
        output_data_file: String,
        output_index_file: String,
        partitioning: Partitioning,
        metrics: &ExecutionPlanMetricsSet,
        data_size_metric: Count,
        context: Arc<TaskContext>,
    ) -> Self {
        let num_output_partitions = partitioning.partition_count();
        let batch_size = context.session_config().batch_size();

        Self {
            name: format!("SortShufflePartitioner[partition={}]", partition_id),
            mem_consumer_info: None,
            output_data_file,
            output_index_file,
            data: Mutex::default(),
            spills: Mutex::default(),
            partitioning,
            num_output_partitions,
            batch_size,
            data_size_metric,
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
        let data = std::mem::take(&mut *self.data.lock().await);
        let spill = try_new_spill(&self.spill_metrics)?;
        let mut uncompressed_size = 0;

        let offsets = data.write(
            spill.get_buf_writer(),
            self.batch_size,
            self.partitioning.partition_count(),
            &mut uncompressed_size,
        )?;
        self.data_size_metric.add(uncompressed_size);
        spill.complete()?;

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
        let (partition_indices, sorted_batch) =
            shuffle::sort_batch_by_partition_id(input, &self.partitioning)?;

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
        self.set_spillable(false);
        let mut spills = std::mem::take(&mut *self.spills.lock().await);
        let data = std::mem::take(&mut *self.data.lock().await);

        log::info!(
            "sort repartitioner starts outputting with {} ({} spills)",
            self.name(),
            spills.len(),
        );

        let partitioning = self.partitioning.clone();
        let batch_size = self.batch_size;
        let data_size_metric = self.data_size_metric.clone();
        let data_file = self.output_data_file.clone();
        let index_file = self.output_index_file.clone();

        // no spills - directly write current batches into final file
        if spills.is_empty() {
            tokio::task::spawn_blocking(move || {
                let mut output_data = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&data_file)?;

                let mut uncompressed_size = 0;
                let offsets = data.write(
                    &mut output_data,
                    batch_size,
                    partitioning.partition_count(),
                    &mut uncompressed_size,
                )?;
                data_size_metric.add(uncompressed_size);
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

        struct SpillCursor {
            cur: usize,
            reader: BufReader<Box<dyn Read + Send>>,
            offsets: Vec<u64>,
        }

        impl ComparableForLoserTree for SpillCursor {
            #[inline(always)]
            fn lt(&self, other: &Self) -> bool {
                match (self, other) {
                    (c1, _) if c1.finished() => false,
                    (_, c2) if c2.finished() => true,
                    (c1, c2) => c1.cur < c2.cur,
                }
            }
        }

        impl SpillCursor {
            fn skip_empty_partitions(&mut self) {
                let offsets = &self.offsets;
                while !self.finished() && offsets[self.cur + 1] == offsets[self.cur] {
                    self.cur += 1;
                }
            }

            fn finished(&self) -> bool {
                self.cur + 1 >= self.offsets.len()
            }
        }

        // add rest data to spills
        if data.num_rows > 0 {
            let spill = try_new_spill(&self.spill_metrics)?;
            let mut uncompressed_size = 0;
            let offsets = data.write(
                spill.get_buf_writer(),
                batch_size,
                partitioning.partition_count(),
                &mut uncompressed_size,
            )?;
            self.data_size_metric.add(uncompressed_size);
            spill.complete()?;
            spills.push(ShuffleSpill { spill, offsets });
        }

        // use loser tree to select partitions from spills
        let mut cursors: LoserTree<SpillCursor> = LoserTree::new(
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
                .filter(|spill| !spill.finished())
                .collect(),
        );
        let _raw_spills: Vec<Box<dyn Spill>> =
            spills.into_iter().map(|spill| spill.spill).collect();
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

            if cursors.len() > 0 {
                loop {
                    let mut min_spill = cursors.peek_mut();
                    if min_spill.finished() {
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
