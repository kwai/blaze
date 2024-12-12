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
use blaze_jni_bridge::{is_task_running, jni_call};
use count_write::CountWrite;
use datafusion::{
    common::Result,
    physical_plan::{metrics::Time, Partitioning},
};
use datafusion_ext_commons::{
    algorithm::rdx_tournament_tree::{KeyForRadixTournamentTree, RadixTournamentTree},
    arrow::{array_size::ArraySize, coalesce::coalesce_arrays_unchecked, selection::take_batch},
    assume, compute_suggested_batch_size_for_output, df_execution_err, unchecked,
};
use jni::objects::GlobalRef;
use unchecked_index::UncheckedIndex;

use crate::{
    common::{ipc_compression::IpcCompressionWriter, timer_helper::TimerHelper},
    shuffle::{
        evaluate_hashes, evaluate_partition_ids, evaluate_robin_partition_ids, rss::RssWriter,
    },
};

pub struct BufferedData {
    partition_id: usize,
    sorted_batches: Vec<RecordBatch>,
    sorted_parts: Vec<Vec<PartitionInBatch>>,
    num_rows: usize,
    mem_used: usize,
    sort_time: Time,
}

impl BufferedData {
    pub fn new(partition_id: usize, sort_time: Time) -> Self {
        Self {
            partition_id,
            sorted_batches: vec![],
            sorted_parts: vec![],
            num_rows: 0,
            mem_used: 0,
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
            .with_timer(|| sort_batch_by_partition_id(batch, partitioning, self.num_rows))?;
        self.mem_used +=
            sorted_batch.get_array_mem_size() + parts.len() * size_of::<PartitionInBatch>();
        log::warn!("add batch: num_rows: {}, mem_used: {} ...", self.num_rows, self.mem_used);
        self.sorted_batches.push(sorted_batch);
        self.sorted_parts.push(parts);
        Ok(())
    }

    // write buffered data to spill/target file, returns uncompressed size and
    // offsets to each partition
    pub fn write<W: Write>(self, mut w: W, partitioning: &Partitioning) -> Result<Vec<u64>> {
        log::info!("draining all buffered data, total_mem={}", self.mem_used());

        if self.num_rows == 0 {
            return Ok(vec![0; partitioning.partition_count() + 1]);
        }
        let mut writer = IpcCompressionWriter::new(CountWrite::from(&mut w));
        let mut offsets = vec![];
        let mut offset = 0;
        let mut iter = self.into_sorted_batches(partitioning)?;

        while (iter.cur_part_id() as usize) < partitioning.partition_count() {
            if !is_task_running() {
                df_execution_err!("task completed/killed")?;
            }
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

        log::info!("all buffered data drained, compressed_size={compressed_size}");
        Ok(offsets)
    }

    // write buffered data to rss, returns uncompressed size
    pub fn write_rss(
        self,
        rss_partition_writer: GlobalRef,
        partitioning: &Partitioning,
    ) -> Result<()> {
        if self.num_rows == 0 {
            return Ok(());
        }
        log::info!(
            "draining all buffered data to rss, total_mem={}",
            self.mem_used()
        );
        let mut iter = self.into_sorted_batches(partitioning)?;
        let mut writer = IpcCompressionWriter::new(RssWriter::new(rss_partition_writer.clone(), 0));

        while (iter.cur_part_id() as usize) < partitioning.partition_count() {
            if !is_task_running() {
                df_execution_err!("task completed/killed")?;
            }
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
        log::info!("all buffered data drained to rss");
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
                    .map(|(idx, partition_indices)| {
                        let mut cur = PartCursor {
                            idx,
                            parts: partition_indices,
                            parts_idx: 0,
                        };
                        cur.skip_empty_parts();
                        cur
                    })
                    .collect(),
                partitioning.partition_count(),
            ),
            num_output_rows: 0,
            num_rows: self.num_rows,
            num_cols: self.sorted_batches[0].num_columns(),
            batch_size: sub_batch_size,
        })
    }

    pub fn mem_used(&self) -> usize {
        self.mem_used
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

            // forward to next non-empty partition
            min_cursor.parts_idx += 1;
            min_cursor.skip_empty_parts();
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

impl PartCursor {
    fn skip_empty_parts(&mut self) {
        while self.parts_idx < self.parts.len() && self.parts[self.parts_idx].len == 0 {
            self.parts_idx += 1;
        }
    }
}

impl KeyForRadixTournamentTree for PartCursor {
    fn rdx(&self) -> usize {
        self.parts_idx.min(self.parts.len())
    }
}

#[derive(Clone, Copy, Default)]
struct PartitionInBatch {
    start: u32,
    len: u32,
}

fn sort_batch_by_partition_id(
    batch: RecordBatch,
    partitioning: &Partitioning,
    sum_num_rows: usize,
) -> Result<(Vec<PartitionInBatch>, RecordBatch)> {
    let num_partitions = partitioning.partition_count();
    let num_rows = batch.num_rows();

    let part_ids: Vec<u32> = match partitioning {
        Partitioning::Hash(..) => {
            // compute partition indices
            let hashes = evaluate_hashes(partitioning, &batch)
                .expect(&format!("error evaluating hashes with {partitioning}"));
            evaluate_partition_ids(hashes, partitioning.partition_count())
        }
        Partitioning::RoundRobinBatch(..) => evaluate_robin_partition_ids(partitioning, &batch, sum_num_rows-num_rows),
        _ => unreachable!("unsupported partitioning: {:?}", partitioning),
    };

    // compute partitions
    let mut partitions = vec![PartitionInBatch::default(); num_partitions];
    let mut start = 0;

    for &part_id in &part_ids {
        assume!((part_id as usize) < partitions.len());
        partitions[part_id as usize].len += 1;
    }
    for part in &mut partitions {
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
    let sorted_batch = take_batch(batch, sorted_row_indices)?;
    return Ok((partitions, sorted_batch));
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::{
        common::Result,
        physical_expr::{expressions::Column, Partitioning, PhysicalExpr},
    };

    use crate::shuffle::buffered_data::sort_batch_by_partition_id;

    fn build_table_i32(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Int32, false),
            Field::new(b.0, DataType::Int32, false),
            Field::new(c.0, DataType::Int32, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(a.1.clone())),
                Arc::new(Int32Array::from(b.1.clone())),
                Arc::new(Int32Array::from(c.1.clone())),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn sort_partition_test() -> Result<()> {
        let record_batch = build_table_i32(
            // ("a", &vec![9, 8, 7, 6, 5, 4, 3, 2, 1, 0]),
            ("a", &vec![19, 18, 17, 16, 15, 14, 13, 12, 11, 10]),
            ("b", &vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            ("c", &vec![5, 6, 7, 8, 9, 0, 1, 2, 3, 4]),
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let partition_exprs_a: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new_with_schema("a", &schema).unwrap()), // Partition by column "a"
        ];

        let partition_exprs_b: Vec<Arc<dyn PhysicalExpr>> =
            vec![Arc::new(Column::new_with_schema("b", &schema).unwrap())];

        let partition_exprs_ab: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new_with_schema("a", &schema).unwrap()),
            Arc::new(Column::new_with_schema("b", &schema).unwrap()),
        ];

        let partition_exprs_abc: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new_with_schema("a", &schema).unwrap()),
            Arc::new(Column::new_with_schema("b", &schema).unwrap()),
            Arc::new(Column::new_with_schema("c", &schema).unwrap()),
        ];

        let unknown_partitioning = Partitioning::UnknownPartitioning(5);

        let round_robin_partitioning = Partitioning::RoundRobinBatch(4);

        let hash_partitioning_a = Partitioning::Hash(partition_exprs_a, 4);
        let hash_partitioning_ab = Partitioning::Hash(partition_exprs_ab, 3);
        let hash_partitioning_abc = Partitioning::Hash(partition_exprs_abc, 3);

        let result = sort_batch_by_partition_id(record_batch, &round_robin_partitioning, 3);

        Ok(())
    }
}
