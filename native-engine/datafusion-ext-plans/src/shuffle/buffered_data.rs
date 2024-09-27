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

use std::io::Write;

use arrow::{array::ArrayRef, record_batch::RecordBatch};
use blaze_jni_bridge::jni_call;
use count_write::CountWrite;
use datafusion::{
    common::Result,
    physical_plan::{metrics::Time, Partitioning},
};
use datafusion_ext_commons::{
    array_size::ArraySize,
    compute_suggested_batch_size_for_output,
    ds::rdx_tournament_tree::{KeyForRadixTournamentTree, RadixTournamentTree},
    streams::coalesce_stream::coalesce_arrays_unchecked,
};
use jni::objects::GlobalRef;
use unchecked_index::UncheckedIndex;

use crate::{
    assume,
    common::{
        batch_selection::take_batch, ipc_compression::IpcCompressionWriter,
        timer_helper::TimerHelper,
    },
    shuffle::{evaluate_hashes, evaluate_partition_ids, rss::RssWriter},
    unchecked,
};

pub struct BufferedData {
    partition_id: usize,
    sorted_batches: Vec<RecordBatch>,
    sorted_parts: Vec<Vec<PartitionInBatch>>,
    num_rows: usize,
    staging_mem_used: usize,
    sorted_mem_used: usize,
    sort_time: Time,
}

impl BufferedData {
    pub fn new(partition_id: usize, sort_time: Time) -> Self {
        Self {
            partition_id,
            sorted_batches: vec![],
            sorted_parts: vec![],
            num_rows: 0,
            staging_mem_used: 0,
            sorted_mem_used: 0,
            sort_time,
        }
    }

    pub fn drain(&mut self) -> Self {
        std::mem::replace(self, Self::new(self.partition_id, self.sort_time.clone()))
    }

    pub fn add_batch(&mut self, batch: RecordBatch, partitioning: &Partitioning) -> Result<()> {
        self.num_rows += batch.num_rows();

        let (parts, sorted_batch) = self
            .sort_time
            .with_timer(|| sort_batch_by_partition_id(batch, partitioning))?;
        self.sorted_mem_used +=
            sorted_batch.get_array_mem_size() + parts.len() * size_of::<PartitionInBatch>();
        self.sorted_batches.push(sorted_batch);
        self.sorted_parts.push(parts);
        Ok(())
    }

    // write buffered data to spill/target file, returns uncompressed size and
    // offsets to each partition
    pub fn write<W: Write>(self, mut w: W, partitioning: &Partitioning) -> Result<Vec<u64>> {
        let partition_id = self.partition_id;
        log::info!(
            "[partition={partition_id}] draining all buffered data, total_mem={}",
            self.mem_used()
        );

        if self.num_rows == 0 {
            return Ok(vec![0; partitioning.partition_count() + 1]);
        }
        let mut writer = IpcCompressionWriter::new(CountWrite::from(&mut w));
        let mut offsets = vec![];
        let mut offset = 0;
        let mut iter = self.into_sorted_batches(partitioning)?;

        while (iter.cur_part_id() as usize) < partitioning.partition_count() {
            let cur_part_id = iter.cur_part_id();
            while offsets.len() <= cur_part_id as usize {
                offsets.push(offset); // fill offsets of empty partitions
            }

            // write all batches with this part id
            while iter.cur_part_id() == cur_part_id {
                let (num_rows, cols) = iter.next_batch();
                writer.write_batch(num_rows, &cols)?;
            }
            writer.finish_current_buf()?;
            offset = writer.inner().count();
            offsets.push(offset);
        }
        while offsets.len() <= partitioning.partition_count() {
            offsets.push(offset); // fill offsets of empty partitions
        }
        let compressed_size = offsets.last().cloned().unwrap_or_default();

        log::info!("[partition={partition_id}] all buffered data drained, compressed_size={compressed_size}");
        Ok(offsets)
    }

    // write buffered data to rss, returns uncompressed size
    pub fn write_rss(
        self,
        rss_partition_writer: GlobalRef,
        partitioning: &Partitioning,
    ) -> Result<()> {
        let partition_id = self.partition_id;
        log::info!(
            "[partition={partition_id}] draining all buffered data to rss, total_mem={}",
            self.mem_used()
        );

        if self.num_rows == 0 {
            return Ok(());
        }
        let mut iter = self.into_sorted_batches(partitioning)?;
        let mut writer = IpcCompressionWriter::new(RssWriter::new(rss_partition_writer.clone(), 0));

        while (iter.cur_part_id() as usize) < partitioning.partition_count() {
            let cur_part_id = iter.cur_part_id();
            writer.set_output(RssWriter::new(
                rss_partition_writer.clone(),
                cur_part_id as usize,
            ));

            // write all batches with this part id
            while iter.cur_part_id() == cur_part_id {
                let (num_rows, cols) = iter.next_batch();
                writer.write_batch(num_rows, &cols)?;
            }
            writer.finish_current_buf()?;
        }
        jni_call!(BlazeRssPartitionWriterBase(rss_partition_writer.as_obj()).flush() -> ())?;

        log::info!("[partition={partition_id}] all buffered data drained to rss");
        Ok(())
    }

    fn into_sorted_batches(
        self,
        partitioning: &Partitioning,
    ) -> Result<PartitionedBatchesIterator> {
        let sub_batch_size =
            compute_suggested_batch_size_for_output(self.mem_used(), self.num_rows);

        Ok(PartitionedBatchesIterator {
            batches: unchecked!(self.sorted_batches.clone()),
            cursors: RadixTournamentTree::new(
                self.sorted_parts
                    .into_iter()
                    .enumerate()
                    .map(|(idx, partition_indices)| PartCursor {
                        idx,
                        parts: partition_indices,
                        parts_idx: 0,
                    })
                    .collect(),
                partitioning.partition_count(),
            ),
            num_output_rows: 0,
            num_rows: self.num_rows,
            num_cols: self.sorted_batches[0].schema().fields().len(),
            batch_size: sub_batch_size,
        })
    }

    pub fn mem_used(&self) -> usize {
        self.staging_mem_used + self.sorted_mem_used
    }
}

struct PartitionedBatchesIterator {
    batches: UncheckedIndex<Vec<RecordBatch>>,
    cursors: RadixTournamentTree<PartCursor>,
    num_output_rows: usize,
    num_rows: usize,
    num_cols: usize,
    batch_size: usize,
}

impl PartitionedBatchesIterator {
    pub fn cur_part_id(&self) -> u32 {
        self.cursors.peek().rdx() as u32
    }

    fn next_batch(&mut self) -> (usize, Vec<ArrayRef>) {
        let cur_batch_size = self.batch_size.min(self.num_rows - self.num_output_rows);
        let cur_part_id = self.cur_part_id();
        let mut slices = vec![vec![]; self.num_cols];
        let mut slices_len = 0;

        // add rows with same parition id under this cursor
        while slices_len < cur_batch_size {
            let mut min_cursor = self.cursors.peek_mut();
            if min_cursor.rdx() as u32 != cur_part_id {
                break;
            }

            let cur_part = min_cursor.parts[min_cursor.parts_idx];
            for i in 0..self.num_cols {
                slices[i].push(
                    self.batches[min_cursor.idx]
                        .column(i)
                        .slice(cur_part.start as usize, cur_part.len as usize),
                );
            }
            slices_len += cur_part.len as usize;
            min_cursor.parts_idx += 1;
        }

        let output_slices = slices
            .into_iter()
            .map(|s| coalesce_arrays_unchecked(s[0].data_type(), &s))
            .collect::<Vec<_>>();

        self.num_output_rows += slices_len;
        (slices_len, output_slices)
    }
}

struct PartCursor {
    idx: usize,
    parts: Vec<PartitionInBatch>,
    parts_idx: usize,
}

impl KeyForRadixTournamentTree for PartCursor {
    fn rdx(&self) -> usize {
        if self.parts_idx < self.parts.len() {
            return self.parts[self.parts_idx].part_id as usize;
        }
        u32::MAX as usize
    }
}

#[derive(Clone, Copy, Default)]
struct PartitionInBatch {
    part_id: u32,
    start: u32,
    len: u32,
}

fn sort_batch_by_partition_id(
    batch: RecordBatch,
    partitioning: &Partitioning,
) -> Result<(Vec<PartitionInBatch>, RecordBatch)> {
    let num_partitions = partitioning.partition_count();
    let num_rows = batch.num_rows();

    // compute partition indices
    let hashes = evaluate_hashes(partitioning, &batch)
        .expect(&format!("error evaluating hashes with {partitioning}"));
    let part_ids = evaluate_partition_ids(hashes, partitioning.partition_count());

    // compute partitions
    let mut partitions = vec![PartitionInBatch::default(); num_partitions];
    let mut start = 0;

    for &part_id in &part_ids {
        assume!((part_id as usize) < partitions.len());
        partitions[part_id as usize].len += 1;
    }
    for (part_id, part) in &mut partitions.iter_mut().enumerate() {
        part.part_id = part_id as u32;
        part.start = start;
        start += part.len;
    }

    // bucket sort
    let mut sorted_row_indices = vec![0; num_rows];
    let mut bucket_starts = partitions.iter().map(|part| part.start).collect::<Vec<_>>();

    for (row_idx, part_id) in part_ids.into_iter().enumerate() {
        let start = bucket_starts[part_id as usize];

        assume!((part_id as usize) < bucket_starts.len());
        assume!((start as usize) < sorted_row_indices.len());
        bucket_starts[part_id as usize] += 1;
        sorted_row_indices[start as usize] = row_idx as u32;
    }

    // remove empty partitions
    partitions.retain(|part| part.len > 0);
    partitions.shrink_to_fit();

    let sorted_batch = take_batch(batch, sorted_row_indices)?;
    return Ok((partitions, sorted_batch));
}
