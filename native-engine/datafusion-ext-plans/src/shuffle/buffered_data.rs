use std::io::{Cursor, Write};

use arrow::record_batch::RecordBatch;
use datafusion::common;
use datafusion_ext_commons::{
    io::write_one_batch,
    loser_tree::{ComparableForLoserTree, LoserTree},
};

use crate::common::BatchesInterleaver;

#[derive(Default)]
pub struct BufferedData {
    pub sorted_batches: Vec<RecordBatch>,
    pub sorted_partition_indices: Vec<Vec<u32>>,
    pub num_rows: usize,
    pub batch_mem_size: usize,
}

impl BufferedData {
    // write batch to spill/target file, returns offsets to each partition
    pub fn write<W: Write>(
        self,
        mut w: W,
        batch_size: usize,
        num_partitions: usize,
        uncompressed_size: &mut usize,
    ) -> common::Result<Vec<u64>> {
        let mut cur_partition_id = 0;
        let mut offsets = vec![0];
        let mut cur_offset = 0;

        for (partition_id, batch) in self.into_sorted_batches(batch_size) {
            while cur_partition_id < partition_id {
                offsets.push(cur_offset);
                cur_partition_id += 1;
            }
            let mut buf = vec![];
            write_one_batch(
                &batch,
                &mut Cursor::new(&mut buf),
                true,
                Some(uncompressed_size),
            )?;
            w.write_all(&buf)?;
            cur_offset += buf.len() as u64;
        }
        while cur_partition_id < num_partitions as u32 {
            offsets.push(cur_offset);
            cur_partition_id += 1;
        }
        Ok(offsets)
    }

    pub fn into_sorted_batches(
        self,
        batch_size: usize,
    ) -> impl Iterator<Item = (u32, RecordBatch)> {
        if self.num_rows == 0 {
            let empty: Box<dyn Iterator<Item = (u32, RecordBatch)>> = Box::new(std::iter::empty());
            return empty;
        }

        struct Cursor {
            idx: usize,
            partition_indices: Vec<u32>,
            row_idx: usize,
        }

        impl Cursor {
            fn cur_partition_id(&self) -> u32 {
                *self
                    .partition_indices
                    .get(self.row_idx)
                    .unwrap_or(&u32::MAX)
            }
        }

        impl ComparableForLoserTree for Cursor {
            fn lt(&self, other: &Self) -> bool {
                self.cur_partition_id() < other.cur_partition_id()
            }
        }

        struct PartitionedBatchesIterator {
            batches: Vec<RecordBatch>,
            cursors: LoserTree<Cursor>,
            num_output_rows: usize,
            num_rows: usize,
            batch_size: usize,
        }

        impl Iterator for PartitionedBatchesIterator {
            type Item = (u32, RecordBatch);

            fn next(&mut self) -> Option<Self::Item> {
                if self.num_output_rows >= self.num_rows {
                    return None;
                }
                let cur_batch_size = self.batch_size.min(self.num_rows - self.num_output_rows);
                let mut indices = Vec::with_capacity(cur_batch_size);

                // first row
                let mut min_cursor = self.cursors.peek_mut();
                let cur_partition_id = min_cursor.cur_partition_id();
                indices.push((min_cursor.idx, min_cursor.row_idx));
                min_cursor.row_idx += 1;
                drop(min_cursor);

                // rest rows
                while indices.len() < cur_batch_size {
                    let mut min_cursor = self.cursors.peek_mut();
                    if min_cursor.cur_partition_id() != cur_partition_id {
                        break;
                    }

                    // add rows with same parition id under this cursor
                    while indices.len() < cur_batch_size
                        && min_cursor.cur_partition_id() == cur_partition_id
                    {
                        indices.push((min_cursor.idx, min_cursor.row_idx));
                        min_cursor.row_idx += 1;
                    }
                }
                let output_batch = BatchesInterleaver::new(self.batches[0].schema(), &self.batches)
                    .interleave(&indices)
                    .expect("error merging sorted batches: interleaving error");
                self.num_output_rows += output_batch.num_rows();
                Some((cur_partition_id, output_batch))
            }
        }

        let suggested_batch_size = (1 << 25) / (self.batch_mem_size / self.num_rows + 1);
        let batch_size = batch_size.min(suggested_batch_size);

        Box::new(PartitionedBatchesIterator {
            batches: self.sorted_batches.clone(),
            cursors: LoserTree::new(
                self.sorted_partition_indices
                    .into_iter()
                    .enumerate()
                    .map(|(idx, partition_indices)| Cursor {
                        idx,
                        partition_indices,
                        row_idx: 0,
                    })
                    .collect(),
            ),
            num_output_rows: 0,
            num_rows: self.num_rows,
            batch_size,
        })
    }

    pub fn mem_used(&self) -> usize {
        self.batch_mem_size + 4 * self.num_rows
    }
}
