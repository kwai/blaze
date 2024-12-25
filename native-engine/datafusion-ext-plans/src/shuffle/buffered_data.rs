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

use arrow::record_batch::RecordBatch;
use blaze_jni_bridge::{is_task_running, jni_call};
use bytesize::ByteSize;
use count_write::CountWrite;
use datafusion::{
    common::Result,
    physical_plan::{metrics::Time, Partitioning},
};
use datafusion_ext_commons::{
    algorithm::{
        rdx_tournament_tree::{KeyForRadixTournamentTree, RadixTournamentTree},
        rdxsort::radix_sort_by_key,
    },
    arrow::{
        array_size::ArraySize,
        selection::{create_batch_interleaver, BatchInterleaver},
    },
    compute_suggested_batch_size_for_output, df_execution_err,
};
use jni::objects::GlobalRef;

use crate::{
    common::{ipc_compression::IpcCompressionWriter, timer_helper::TimerHelper},
    shuffle::{
        evaluate_hashes, evaluate_partition_ids, evaluate_robin_partition_ids, rss::RssWriter,
    },
};

pub struct BufferedData {
    partition_id: usize,
    partitioning: Partitioning,
    staging_batches: Vec<RecordBatch>,
    staging_num_rows: usize,
    staging_mem_used: usize,
    sorted_batches: Vec<RecordBatch>,
    sorted_offsets: Vec<Vec<u32>>,
    num_rows: usize,
    sorted_mem_used: usize,
    sort_time: Time,
}

impl BufferedData {
    pub fn new(partitioning: Partitioning, partition_id: usize, sort_time: Time) -> Self {
        Self {
            partition_id,
            partitioning,
            staging_batches: vec![],
            staging_num_rows: 0,
            staging_mem_used: 0,
            sorted_batches: vec![],
            sorted_offsets: vec![],
            num_rows: 0,
            sorted_mem_used: 0,
            sort_time,
        }
    }

    pub fn drain(&mut self) -> Self {
        std::mem::replace(
            self,
            Self::new(
                self.partitioning.clone(),
                self.partition_id,
                self.sort_time.clone(),
            ),
        )
    }

<<<<<<< HEAD
    pub fn add_batch(&mut self, batch: RecordBatch, partitioning: &Partitioning) -> Result<()> {
        let current_num_rows = self.num_rows;
        self.num_rows += batch.num_rows();
        let (parts, sorted_batch) = self.sort_time.with_timer(|| {
            sort_batch_by_partition_id(batch, partitioning, current_num_rows, self.partition_id)
        })?;
        self.mem_used +=
            sorted_batch.get_array_mem_size() + parts.len() * size_of::<PartitionInBatch>();
=======
    pub fn add_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.num_rows += batch.num_rows();
        self.staging_num_rows += batch.num_rows();
        self.staging_mem_used += batch.get_array_mem_size();
        self.staging_batches.push(batch);

        let suggested_batch_size =
            compute_suggested_batch_size_for_output(self.staging_mem_used, self.staging_num_rows);
        if self.staging_mem_used > suggested_batch_size {
            self.flush_staging()?;
        }
        Ok(())
    }

    fn flush_staging(&mut self) -> Result<()> {
        let sorted_num_rows = self.num_rows - self.staging_num_rows;
        let staging_batches = std::mem::take(&mut self.staging_batches);
        let (offsets, sorted_batch) = self.sort_time.with_timer(|| {
            sort_batches_by_partition_id(
                staging_batches,
                &self.partitioning,
                sorted_num_rows,
                self.partition_id,
            )
        })?;
        self.staging_num_rows = 0;
        self.staging_mem_used = 0;

        self.sorted_mem_used += sorted_batch.get_array_mem_size() + offsets.len() * 4;
>>>>>>> master
        self.sorted_batches.push(sorted_batch);
        self.sorted_offsets.push(offsets);
        Ok(())
    }

    // write buffered data to spill/target file, returns uncompressed size and
    // offsets to each partition
    pub fn write<W: Write>(mut self, mut w: W) -> Result<Vec<u64>> {
        if !self.staging_batches.is_empty() {
            self.flush_staging()?;
        }

        let mem_used = ByteSize(self.mem_used() as u64);
        log::info!("draining all buffered data, total_mem={mem_used}");

        if self.num_rows == 0 {
            return Ok(vec![0; self.partitioning.partition_count() + 1]);
        }
        let num_partitions = self.partitioning.partition_count();
        let mut writer = IpcCompressionWriter::new(CountWrite::from(&mut w));
        let mut offsets = vec![];
        let mut offset = 0;
        let mut iter = self.into_sorted_batches()?;

        while !iter.finished() {
            if !is_task_running() {
                df_execution_err!("task completed/killed")?;
            }
            let cur_part_id = iter.cur_part_id();
            while offsets.len() <= cur_part_id as usize {
                offsets.push(offset); // fill offsets of empty partitions
            }

            // write all batches with this part id
            while iter.cur_part_id() == cur_part_id {
                let batch = iter.next_batch()?;
                writer.write_batch(batch.num_rows(), batch.columns())?;
            }
            writer.finish_current_buf()?;
            offset = writer.inner().count();
            offsets.push(offset);
        }
        while offsets.len() <= num_partitions {
            offsets.push(offset); // fill offsets of empty partitions
        }

        let compressed_size = ByteSize(offsets.last().cloned().unwrap_or_default() as u64);
        log::info!("all buffered data drained, compressed_size={compressed_size}");
        Ok(offsets)
    }

    // write buffered data to rss, returns uncompressed size
    pub fn write_rss(mut self, rss_partition_writer: GlobalRef) -> Result<()> {
        if !self.staging_batches.is_empty() {
            self.flush_staging()?;
        }

        let mem_used = ByteSize(self.mem_used() as u64);
        log::info!("draining all buffered data to rss, total_mem={mem_used}");

        if self.num_rows == 0 {
            return Ok(());
        }
        let mut iter = self.into_sorted_batches()?;
        let mut writer = IpcCompressionWriter::new(RssWriter::new(rss_partition_writer.clone(), 0));

        while !iter.finished() {
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
                let batch = iter.next_batch()?;
                writer.write_batch(batch.num_rows(), batch.columns())?;
            }
            writer.finish_current_buf()?;
        }
        jni_call!(BlazeRssPartitionWriterBase(rss_partition_writer.as_obj()).flush() -> ())?;
        log::info!("all buffered data drained to rss");
        Ok(())
    }

    fn into_sorted_batches(self) -> Result<PartitionedBatchesIterator> {
        let sub_batch_size =
            compute_suggested_batch_size_for_output(self.mem_used(), self.num_rows);
        Ok(PartitionedBatchesIterator {
            batch_interleaver: create_batch_interleaver(&self.sorted_batches, true)?,
            cursors: RadixTournamentTree::new(
                self.sorted_offsets
                    .into_iter()
                    .enumerate()
                    .map(|(idx, offsets)| {
                        let mut cur = PartCursor {
                            idx,
                            offsets,
                            parts_idx: 0,
                        };
                        cur.skip_empty_parts();
                        cur
                    })
                    .collect(),
                self.partitioning.partition_count(),
            ),
            num_output_rows: 0,
            num_rows: self.num_rows,
            batch_size: sub_batch_size,
        })
    }

    pub fn mem_used(&self) -> usize {
        self.sorted_mem_used + self.staging_mem_used
    }

    pub fn is_empty(&self) -> bool {
        self.sorted_batches.is_empty() && self.staging_batches.is_empty()
    }
}

struct PartitionedBatchesIterator {
    batch_interleaver: BatchInterleaver,
    cursors: RadixTournamentTree<PartCursor>,
    num_output_rows: usize,
    num_rows: usize,
    batch_size: usize,
}

impl PartitionedBatchesIterator {
    pub fn cur_part_id(&self) -> u32 {
        self.cursors.peek().rdx() as u32
    }

    pub fn finished(&self) -> bool {
        self.num_output_rows >= self.num_rows
    }

    pub fn next_batch(&mut self) -> Result<RecordBatch> {
        let cur_batch_size = self.batch_size.min(self.num_rows - self.num_output_rows);
        let cur_part_id = self.cur_part_id();
        let mut indices = Vec::with_capacity(cur_batch_size);

        // add rows with same parition id under this cursor
        while indices.len() < cur_batch_size {
            let mut min_cursor = self.cursors.peek_mut();
            if min_cursor.rdx() as u32 != cur_part_id {
                break;
            }
            let batch_idx = min_cursor.idx;
            let min_offsets = &min_cursor.offsets;
            let min_parts_idx = min_cursor.parts_idx;
            let cur_offset_range = min_offsets[min_parts_idx]..min_offsets[min_parts_idx + 1];
            indices.extend(cur_offset_range.map(|offset| (batch_idx, offset as usize)));

            // forward to next non-empty partition
            min_cursor.parts_idx += 1;
            min_cursor.skip_empty_parts();
        }

        let batch_interleaver = &mut self.batch_interleaver;
        let output_batch = batch_interleaver(&indices)?;
        self.num_output_rows += output_batch.num_rows();
        Ok(output_batch)
    }
}

struct PartCursor {
    idx: usize,
    offsets: Vec<u32>,
    parts_idx: usize,
}

impl PartCursor {
    fn skip_empty_parts(&mut self) {
        if self.parts_idx < self.num_partitions() {
            if self.offsets[self.parts_idx + 1] == self.offsets[self.parts_idx] {
                self.parts_idx += 1;
                self.skip_empty_parts();
            }
        }
    }

    fn num_partitions(&self) -> usize {
        self.offsets.len() - 1
    }
}

impl KeyForRadixTournamentTree for PartCursor {
    fn rdx(&self) -> usize {
        self.parts_idx
    }
}

fn sort_batches_by_partition_id(
    batches: Vec<RecordBatch>,
    partitioning: &Partitioning,
    current_num_rows: usize,
    partition_id: usize,
<<<<<<< HEAD
) -> Result<(Vec<PartitionInBatch>, RecordBatch)> {
=======
) -> Result<(Vec<u32>, RecordBatch)> {
>>>>>>> master
    let num_partitions = partitioning.partition_count();
    let mut round_robin_start_rows =
        (partition_id * 1000193 + current_num_rows) % partitioning.partition_count();

<<<<<<< HEAD
    let part_ids: Vec<u32> = match partitioning {
        Partitioning::Hash(..) => {
            // compute partition indices
            let hashes = evaluate_hashes(partitioning, &batch)
                .expect(&format!("error evaluating hashes with {partitioning}"));
            evaluate_partition_ids(hashes, partitioning.partition_count())
        }
        Partitioning::RoundRobinBatch(..) => {
            let start_rows =
                (partition_id * 1000193 + current_num_rows) % partitioning.partition_count();
            evaluate_robin_partition_ids(partitioning, &batch, start_rows)
        }
        _ => unreachable!("unsupported partitioning: {:?}", partitioning),
    };
=======
    // compute partition indices
    let mut partition_indices = batches
        .iter()
        .enumerate()
        .flat_map(|(batch_idx, batch)| {
            let part_ids: Vec<u32>;

            match partitioning {
                Partitioning::Hash(..) => {
                    // compute partition indices
                    let hashes = evaluate_hashes(partitioning, &batch)
                        .expect(&format!("error evaluating hashes with {partitioning}"));
                    part_ids = evaluate_partition_ids(hashes, partitioning.partition_count());
                }
                Partitioning::RoundRobinBatch(..) => {
                    part_ids =
                        evaluate_robin_partition_ids(partitioning, &batch, round_robin_start_rows);
                    round_robin_start_rows += batch.num_rows();
                    round_robin_start_rows %= partitioning.partition_count();
                }
                _ => unreachable!("unsupported partitioning: {:?}", partitioning),
            };
            part_ids
                .into_iter()
                .enumerate()
                .map(move |(row_idx, part_id)| (part_id, batch_idx as u32, row_idx as u32))
        })
        .collect::<Vec<_>>();

    // sort
    let mut part_counts = vec![0; num_partitions];
    radix_sort_by_key(
        &mut partition_indices,
        &mut part_counts,
        |&(part_id, ..)| part_id as usize,
    );
>>>>>>> master

    // compute partitions
    let mut partition_offsets = Vec::with_capacity(num_partitions + 1);
    let mut offset = 0;
    for part_count in part_counts {
        partition_offsets.push(offset);
        offset += part_count as u32;
    }
    partition_offsets.push(offset);

    // get sorted batch
    let batches_interleaver = create_batch_interleaver(&batches, true)?;
    let sorted_batch = batches_interleaver(
        &partition_indices
            .into_iter()
            .map(|(_, batch_idx, row_idx)| (batch_idx as usize, row_idx as usize))
            .collect::<Vec<_>>(),
    )?;
    return Ok((partition_offsets, sorted_batch));
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::{assert_batches_eq, common::Result, physical_expr::Partitioning};

    use super::*;

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
    async fn test_round_robin() -> Result<()> {
        let record_batch = build_table_i32(
            ("a", &vec![19, 18, 17, 16, 15, 14, 13, 12, 11, 10]),
            ("b", &vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            ("c", &vec![5, 6, 7, 8, 9, 0, 1, 2, 3, 4]),
        );

        let round_robin_partitioning = Partitioning::RoundRobinBatch(4);
        let (_parts, sorted_batch) =
            sort_batches_by_partition_id(vec![record_batch], &round_robin_partitioning, 3, 0)?;

        let expected = vec![
            "+----+---+---+",
            "| a  | b | c |",
            "+----+---+---+",
            "| 18 | 1 | 6 |",
            "| 14 | 5 | 0 |",
            "| 10 | 9 | 4 |",
            "| 17 | 2 | 7 |",
            "| 13 | 6 | 1 |",
            "| 16 | 3 | 8 |",
            "| 12 | 7 | 2 |",
            "| 19 | 0 | 5 |",
            "| 15 | 4 | 9 |",
            "| 11 | 8 | 3 |",
            "+----+---+---+",
        ];
        assert_batches_eq!(expected, &vec![sorted_batch]);
        Ok(())
    }
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
        assert_batches_eq,
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

        let round_robin_partitioning = Partitioning::RoundRobinBatch(4);
        let hash_partitioning_a = Partitioning::Hash(partition_exprs_a, 4);

        let (parts, sorted_batch) =
            sort_batch_by_partition_id(record_batch, &round_robin_partitioning, 3, 0)?;

        let expected = vec![
            "+----+---+---+",
            "| a  | b | c |",
            "+----+---+---+",
            "| 18 | 1 | 6 |",
            "| 14 | 5 | 0 |",
            "| 10 | 9 | 4 |",
            "| 17 | 2 | 7 |",
            "| 13 | 6 | 1 |",
            "| 16 | 3 | 8 |",
            "| 12 | 7 | 2 |",
            "| 19 | 0 | 5 |",
            "| 15 | 4 | 9 |",
            "| 11 | 8 | 3 |",
            "+----+---+---+",
        ];
        assert_batches_eq!(expected, &vec![sorted_batch]);
        Ok(())
    }
}
