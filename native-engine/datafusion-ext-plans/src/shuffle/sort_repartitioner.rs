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
    io::{BufReader, Cursor, Read, Seek, Write},
    sync::{Arc, Weak},
};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use async_trait::async_trait;
use datafusion::{
    common::{DataFusionError, Result},
    execution::context::TaskContext,
    physical_plan::{
        metrics::{BaselineMetrics, Count},
        Partitioning,
    },
};
use datafusion_ext_commons::{io::write_one_batch, loser_tree::LoserTree};
use derivative::Derivative;
use futures::lock::Mutex;

use crate::{
    common::BatchesInterleaver,
    memmgr::{
        onheap_spill::{try_new_spill, Spill},
        MemConsumer, MemConsumerInfo, MemManager,
    },
    shuffle::{evaluate_hashes, evaluate_partition_ids, ShuffleRepartitioner, ShuffleSpill},
};

pub struct SortShuffleRepartitioner {
    name: String,
    mem_consumer_info: Option<Weak<MemConsumerInfo>>,
    output_data_file: String,
    output_index_file: String,
    schema: SchemaRef,
    buffered_batches: Mutex<Vec<RecordBatch>>,
    spills: Mutex<Vec<ShuffleSpill>>,
    partitioning: Partitioning,
    num_output_partitions: usize,
    batch_size: usize,
    metrics: BaselineMetrics,
    data_size_metric: Count,
}

impl SortShuffleRepartitioner {
    pub fn new(
        partition_id: usize,
        output_data_file: String,
        output_index_file: String,
        schema: SchemaRef,
        partitioning: Partitioning,
        metrics: BaselineMetrics,
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
            schema,
            buffered_batches: Mutex::default(),
            spills: Mutex::default(),
            partitioning,
            num_output_partitions,
            batch_size,
            metrics,
            data_size_metric,
        }
    }

    fn build_sorted_pi_vec(&self, buffered_batches: &[RecordBatch]) -> Result<Vec<PI>> {
        // combine all buffered batches
        let num_buffered_rows = buffered_batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>();

        let mut pi_vec = Vec::with_capacity(num_buffered_rows);
        for (batch_idx, batch) in buffered_batches.iter().enumerate() {
            let hashes = evaluate_hashes(&self.partitioning, batch)?;
            let partition_ids = evaluate_partition_ids(&hashes, self.num_output_partitions);

            // compute partition ids and sorted indices
            pi_vec.extend(
                hashes
                    .into_iter()
                    .zip(partition_ids.into_iter())
                    .enumerate()
                    .map(|(i, (hash, partition_id))| PI {
                        partition_id,
                        hash,
                        batch_idx: batch_idx as u32,
                        row_idx: i as u32,
                    }),
            );
        }
        pi_vec.shrink_to_fit();
        pi_vec.sort_unstable();
        Ok(pi_vec)
    }

    fn write_buffered_batches(
        &self,
        buffered_batches: &[RecordBatch],
        pi_vec: Vec<PI>,
        mut w: impl Write,
    ) -> Result<Vec<u64>> {
        let interleaver = BatchesInterleaver::new(self.schema.clone(), buffered_batches);
        let mut cur_partition_id = 0;
        let mut cur_slice_start = 0;
        let mut offsets = vec![0];
        let mut offset = 0;

        macro_rules! write_sub_batch {
            ($range:expr) => {{
                let sub_pi_vec = &pi_vec[$range];
                let sub_indices = sub_pi_vec
                    .iter()
                    .map(|pi| (pi.batch_idx as usize, pi.row_idx as usize))
                    .collect::<Vec<_>>();
                let sub_batch = interleaver.interleave(&sub_indices)?;

                let mut buf = vec![];
                let mut num_bytes_written_uncompressed = 0;
                write_one_batch(
                    &sub_batch,
                    &mut Cursor::new(&mut buf),
                    true,
                    Some(&mut num_bytes_written_uncompressed),
                )?;
                self.data_size_metric.add(num_bytes_written_uncompressed);
                offset += buf.len() as u64;
                w.write_all(&buf)?;
            }};
        }

        // write sorted data
        for cur_offset in 0..pi_vec.len() {
            if pi_vec[cur_offset].partition_id > cur_partition_id
                || cur_offset - cur_slice_start >= self.batch_size
            {
                if cur_slice_start < cur_offset {
                    write_sub_batch!(cur_slice_start..cur_offset);
                    cur_slice_start = cur_offset;
                }
                while pi_vec[cur_offset].partition_id > cur_partition_id {
                    offsets.push(offset);
                    cur_partition_id += 1;
                }
            }
        }
        if cur_slice_start < pi_vec.len() {
            write_sub_batch!(cur_slice_start..);
        }

        // add one extra offset at last to ease partition length computation
        offsets.resize(self.num_output_partitions + 1, offset);
        Ok(offsets)
    }

    fn spill_buffered_batches(&self, buffered_batches: &[RecordBatch]) -> Result<ShuffleSpill> {
        // get sorted PI vec
        let pi_vec = self.build_sorted_pi_vec(buffered_batches)?;

        // write to in-mem spill
        let spill = try_new_spill()?;
        let offsets =
            self.write_buffered_batches(buffered_batches, pi_vec, &mut spill.get_buf_writer())?;
        spill.complete()?;

        Ok(ShuffleSpill { spill, offsets })
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
        let mut batches = self.buffered_batches.lock().await;
        let mut spills = self.spills.lock().await;

        if !batches.is_empty() {
            spills.push(self.spill_buffered_batches(&std::mem::take(&mut *batches))?);
        }
        drop(spills);
        drop(batches);

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
        self.buffered_batches.lock().await.push(input.clone());

        let mem_increase =
            input.get_array_memory_size() + input.num_rows() * std::mem::size_of::<PI>();
        self.update_mem_used_with_diff(mem_increase as isize)
            .await?;

        // we are likely to spill more frequently because the cost of spilling a shuffle
        // repartition is lower than other consumers.
        if self.mem_used_percent() > 0.5 {
            self.spill().await?;
        }
        Ok(())
    }

    async fn shuffle_write(&self) -> Result<()> {
        self.set_spillable(false);
        let mut spills = std::mem::take(&mut *self.spills.lock().await);
        let mut batches = std::mem::take(&mut *self.buffered_batches.lock().await);

        log::info!(
            "sort repartitioner starts outputting with {} ({} spills)",
            self.name(),
            spills.len(),
        );

        // no spills - directly write current batches into final file
        if spills.is_empty() {
            let mut output_data = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&self.output_data_file)?;

            let pi_vec = self.build_sorted_pi_vec(&batches)?;
            let offsets = self.write_buffered_batches(&batches, pi_vec, &mut output_data)?;
            batches.clear();
            output_data.sync_data()?;
            output_data.flush()?;

            let mut output_index = File::create(&self.output_index_file)?;
            for offset in offsets {
                output_index.write_all(&(offset as i64).to_le_bytes()[..])?;
            }
            output_index.sync_data()?;
            output_index.flush()?;
            self.update_mem_used(0).await?;
            return Ok(());
        }

        // define spill cursor. partial-ord is reversed because we
        // need to find mininum using a binary heap
        #[derive(Derivative)]
        #[derivative(PartialOrd, PartialEq, Ord, Eq)]
        struct SpillCursor {
            cur: usize,

            #[derivative(PartialOrd = "ignore")]
            #[derivative(PartialEq = "ignore")]
            #[derivative(Ord = "ignore")]
            reader: BufReader<Box<dyn Read + Send>>,

            #[derivative(PartialOrd = "ignore")]
            #[derivative(PartialEq = "ignore")]
            #[derivative(Ord = "ignore")]
            offsets: Vec<u64>,
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

        // write current buffered batches into a spill
        spills.push(self.spill_buffered_batches(&batches)?);
        batches.clear();

        // use loser tree to select partitions from spills
        let mut cursors: LoserTree<SpillCursor> = LoserTree::new_by(
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
            |c1: &SpillCursor, c2: &SpillCursor| match (c1, c2) {
                (c1, _) if c1.finished() => false,
                (_, c2) if c2.finished() => true,
                (c1, c2) => c1 < c2,
            },
        );
        let raw_spills: Vec<Box<dyn Spill>> = spills.into_iter().map(|spill| spill.spill).collect();

        let data_file = self.output_data_file.clone();
        let index_file = self.output_index_file.clone();

        let num_output_partitions = self.num_output_partitions;
        let mut offsets = vec![0];
        let mut output_data = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(data_file)?;
        let mut cur_partition_id = 0;

        // append partition in each spills
        tokio::task::spawn_blocking(move || {
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
        .map_err(|e| DataFusionError::Execution(format!("shuffle write error: {:?}", e)))??;

        // update disk spill size
        let spill_disk_usage = raw_spills
            .iter()
            .map(|spill| spill.get_disk_usage().unwrap_or(0))
            .sum::<u64>();
        self.metrics.record_spill(spill_disk_usage as usize);
        self.update_mem_used(0).await?;
        Ok(())
    }
}

#[derive(Derivative)]
#[derivative(Clone, Copy, Default, PartialOrd, PartialEq, Ord, Eq)]
pub struct PI {
    pub partition_id: u32,
    pub hash: u32,

    #[derivative(PartialOrd = "ignore")]
    #[derivative(PartialEq = "ignore")]
    #[derivative(Ord = "ignore")]
    pub batch_idx: u32,

    #[derivative(PartialOrd = "ignore")]
    #[derivative(PartialEq = "ignore")]
    #[derivative(Ord = "ignore")]
    pub row_idx: u32,
}
