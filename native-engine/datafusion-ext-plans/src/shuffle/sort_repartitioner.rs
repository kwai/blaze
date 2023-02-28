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

use crate::shuffle::{
    evaluate_hashes, evaluate_partition_ids, ShuffleRepartitioner, ShuffleSpill,
};
use arrow::compute;
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytesize::ByteSize;
use datafusion::common::Result;
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_manager::ConsumerType;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::{MemoryConsumer, MemoryConsumerId, MemoryManager};
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::Partitioning;
use datafusion_ext_commons::io::write_one_batch;
use datafusion_ext_commons::loser_tree::LoserTree;
use derivative::Derivative;
use futures::lock::Mutex;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Cursor, Read, Seek, Write};
use std::sync::Arc;
use voracious_radix_sort::{RadixSort, Radixable};
use crate::spill::{get_spills_disk_usage, OnHeapSpill};

pub struct SortShuffleRepartitioner {
    memory_consumer_id: MemoryConsumerId,
    output_data_file: String,
    output_index_file: String,
    schema: SchemaRef,
    buffered_batches: Mutex<Vec<RecordBatch>>,
    spills: Mutex<Vec<ShuffleSpill>>,
    partitioning: Partitioning,
    num_output_partitions: usize,
    runtime: Arc<RuntimeEnv>,
    batch_size: usize,
    metrics: BaselineMetrics,
}

impl Debug for SortShuffleRepartitioner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SortShuffleRepartitioner")
            .field("id", &self.id())
            .field("memory_used", &self.mem_used())
            .field("spilled_bytes", &self.spilled_bytes())
            .field("spilled_count", &self.spill_count())
            .finish()
    }
}

impl SortShuffleRepartitioner {
    pub fn new(
        partition_id: usize,
        output_data_file: String,
        output_index_file: String,
        schema: SchemaRef,
        partitioning: Partitioning,
        metrics: BaselineMetrics,
        context: Arc<TaskContext>,
    ) -> Self {
        let num_output_partitions = partitioning.partition_count();
        let runtime = context.runtime_env();
        let batch_size = context.session_config().batch_size();
        let repartitioner = Self {
            memory_consumer_id: MemoryConsumerId::new(partition_id),
            output_data_file,
            output_index_file,
            schema,
            buffered_batches: Mutex::default(),
            spills: Mutex::default(),
            partitioning,
            num_output_partitions,
            runtime,
            batch_size,
            metrics,
        };
        repartitioner.runtime.register_requester(repartitioner.id());
        repartitioner
    }

    async fn spill_buffered_batches(&self) -> Result<ShuffleSpill> {
        let buffered_batches = std::mem::take(
            &mut *self.buffered_batches.lock().await
        );

        // combine all buffered batches
        let num_output_partitions = self.num_output_partitions;
        let num_buffered_rows = buffered_batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>();

        let mut pi_vec = Vec::with_capacity(num_buffered_rows);
        for (batch_idx, batch) in buffered_batches.iter().enumerate() {
            let hashes = evaluate_hashes(&self.partitioning, &batch)?;
            let partition_ids = evaluate_partition_ids(&hashes, num_output_partitions);

            // compute partition ids and sorted indices by counting sort
            pi_vec.extend(hashes
                .into_iter()
                .zip(partition_ids.into_iter())
                .enumerate()
                .map(|(i, (hash, partition_id))| PI {
                    partition_id,
                    hash,
                    batch_idx: batch_idx as u32,
                    row_idx: i as u32,
                }));
        }
        pi_vec.voracious_sort();

        // write to in-mem spill
        let mut buffered_columns = vec![vec![]; buffered_batches[0].num_columns()];
        buffered_batches.iter().for_each(|batch| batch
            .columns()
            .iter()
            .enumerate()
            .for_each(|(col_idx, col)| buffered_columns[col_idx].push(col.as_ref())));

        let mut cur_partition_id = 0;
        let mut cur_slice_start = 0;
        let mut cur_spill = OnHeapSpill::try_new()?;
        let mut cur_spill_writer = BufWriter::with_capacity(65536, &mut cur_spill);
        let mut cur_spill_offsets = vec![0];
        let mut offset = 0;

        macro_rules! write_sub_batch {
            ($range:expr) => {{
                let sub_pi_vec = &pi_vec[$range];
                let sub_indices = sub_pi_vec
                    .iter()
                    .map(|pi| (pi.batch_idx as usize, pi.row_idx as usize))
                    .collect::<Vec<_>>();

                let sub_batch = RecordBatch::try_new(
                    self.schema.clone(),
                    buffered_columns
                        .iter()
                        .map(|columns| compute::interleave(columns, &sub_indices))
                        .collect::<ArrowResult<Vec<_>>>()?,
                )?;
                let mut buf = vec![];
                write_one_batch(&sub_batch, &mut Cursor::new(&mut buf), true)?;
                offset += buf.len() as u64;
                cur_spill_writer.write(&buf)?;
            }};
        }

        // write sorted data into in-mem spill
        for cur_offset in 0..pi_vec.len() {
            if pi_vec[cur_offset].partition_id > cur_partition_id
                || cur_offset - cur_slice_start >= self.batch_size
            {
                if cur_slice_start < cur_offset {
                    write_sub_batch!(cur_slice_start..cur_offset);
                    cur_slice_start = cur_offset;
                }
                while pi_vec[cur_offset].partition_id > cur_partition_id {
                    cur_spill_offsets.push(offset);
                    cur_partition_id += 1;
                }
            }
        }
        if cur_slice_start < pi_vec.len() {
            write_sub_batch!(cur_slice_start..);
        }

        // add one extra offset at last to ease partition length computation
        cur_spill_offsets.resize(num_output_partitions + 1, offset);

        drop(cur_spill_writer);
        cur_spill.complete()?;

        Ok(ShuffleSpill {
            spill: cur_spill,
            offsets: cur_spill_offsets,
        })
    }

    fn spilled_bytes(&self) -> usize {
        self.metrics.spilled_bytes().value()
    }

    fn spill_count(&self) -> usize {
        self.metrics.spill_count().value()
    }
}

#[async_trait]
impl MemoryConsumer for SortShuffleRepartitioner {
    fn name(&self) -> String {
        "sort epartitioner".to_string()
    }

    fn id(&self) -> &MemoryConsumerId {
        &self.memory_consumer_id
    }

    fn memory_manager(&self) -> Arc<MemoryManager> {
        self.runtime.memory_manager.clone()
    }

    fn type_(&self) -> &ConsumerType {
        &ConsumerType::Requesting
    }

    async fn spill(&self) -> Result<usize> {
        log::info!(
            "sort repartitioner start spilling, used={}, {}",
            ByteSize(self.mem_used() as u64),
            self.memory_manager(),
        );
        self.spills.lock().await.push(self.spill_buffered_batches().await?);

        let freed = self.metrics.mem_used().set(0);
        Ok(freed)
    }

    fn mem_used(&self) -> usize {
        self.metrics.mem_used().value()
    }
}

#[async_trait]
impl ShuffleRepartitioner for SortShuffleRepartitioner {
    fn name(&self) -> &str {
        "sort repartitioner"
    }

    async fn insert_batch(&self, input: RecordBatch) -> Result<()> {
        let mem_increase = input.get_array_memory_size();
        self.grow(mem_increase);
        self.metrics.mem_used().add(mem_increase);

        let mut buffered_batches = self.buffered_batches.lock().await;
        buffered_batches.push(input);
        drop(buffered_batches);

        // try grow after inserted
        self.try_grow(0).await?;
        Ok(())
    }

    async fn shuffle_write(&self) -> Result<()> {
        let mut spills = self.spills.lock().await;

        // spill all buffered batches
        if self.mem_used() > 0 {
            spills.push(self.spill_buffered_batches().await?);
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
            spill_id: i32,

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

        // use loser tree to select partitions from spills
        let mut spills: LoserTree<SpillCursor> = LoserTree::new_by(
            std::mem::take(&mut *spills)
                .into_iter()
                .map(|spill| {
                    let mut cursor = SpillCursor {
                        cur: 0,
                        spill_id: spill.spill.id(),
                        reader: spill.spill.into_buf_reader(),
                        offsets: spill.offsets,
                    };
                    cursor.skip_empty_partitions();
                    cursor
                })
                .filter(|spill| !spill.finished())
                .collect(),
            |c1: &SpillCursor, c2: &SpillCursor| match (c1, c2) {
                (c1, _2) if c1.finished() => false,
                (_1, c2) if c2.finished() => true,
                (c1, c2) => c1 < c2,
            },
        );

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
        if spills.len() > 0 {
            loop {
                let mut min_spill = spills.peek_mut();
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
                std::io::copy(
                    &mut reader.take(spill_range.len() as u64),
                    &mut output_data,
                )?;

                // forward partition id in min_spill
                min_spill.cur += 1;
                min_spill.skip_empty_partitions();
            }
        }
        output_data.flush()?;

        // add one extra offset at last to ease partition length computation
        offsets.resize(num_output_partitions + 1, output_data.stream_position()?);

        let mut output_index = File::create(index_file)?;
        for offset in offsets {
            output_index.write_all(&(offset as i64).to_le_bytes()[..])?;
        }
        output_index.flush()?;

        // update disk spill size
        let spill_ids = spills
            .values()
            .iter()
            .map(|cursor| cursor.spill_id)
            .collect::<Vec<_>>();
        let spill_disk_usage = get_spills_disk_usage(&spill_ids)?;
        self.metrics.record_spill(spill_disk_usage as usize);

        let used = self.metrics.mem_used().set(0);
        self.shrink(used);
        Ok(())
    }
}

impl Drop for SortShuffleRepartitioner {
    fn drop(&mut self) {
        self.runtime.drop_consumer(self.id(), self.mem_used());
    }
}

#[derive(Derivative)]
#[derivative(Clone, Copy, Default, PartialOrd, PartialEq, Ord, Eq)]
struct PI {
    partition_id: u32,
    hash: u32,

    #[derivative(PartialOrd = "ignore")]
    #[derivative(PartialEq = "ignore")]
    #[derivative(Ord = "ignore")]
    batch_idx: u32,

    #[derivative(PartialOrd = "ignore")]
    #[derivative(PartialEq = "ignore")]
    #[derivative(Ord = "ignore")]
    row_idx: u32,
}
impl Radixable<u64> for PI {
    type Key = u64;

    fn key(&self) -> Self::Key {
        (self.partition_id as u64) << 32 | self.hash as u64
    }
}
