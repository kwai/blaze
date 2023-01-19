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

use crate::agg::agg_helper::{AggContext, AggRecord, radix_sort_records};
use arrow::array::{Array, ArrayRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use arrow::row::RowConverter;
use async_trait::async_trait;
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_manager::ConsumerType;
use datafusion::execution::{
    DiskManager, MemoryConsumer, MemoryConsumerId, MemoryManager,
};
use datafusion::physical_plan::metrics::{BaselineMetrics, Time};
use datafusion_ext_commons::io::{
    read_bytes_slice, read_len, read_one_batch, write_len, write_one_batch,
};
use futures::lock::Mutex;
use futures::TryFutureExt;
use hashbrown::HashMap;
use lz4_flex::frame::FrameDecoder;
use std::io::{BufReader, BufWriter, Cursor, Read, Seek, Write};
use std::mem::size_of;
use std::sync::Arc;
use arrow::datatypes::{Field, Schema};
use tokio::sync::mpsc::Sender;
use datafusion_ext_commons::loser_tree::LoserTree;

pub struct AggTables {
    in_mem: Mutex<InMemTable>,
    spilled: Mutex<Vec<SpilledTable>>,
    id: MemoryConsumerId,
    agg_ctx: Arc<AggContext>,
    context: Arc<TaskContext>,
    metrics: BaselineMetrics,
}

impl AggTables {
    pub fn new(
        agg_ctx: Arc<AggContext>,
        partition_id: usize,
        metrics: BaselineMetrics,
        context: Arc<TaskContext>,
    ) -> Self {
        let tables = Self {
            in_mem: Mutex::new(InMemTable {
                is_hash: true, // only the first im-mem table uses hash
                ..Default::default()
            }),
            spilled: Mutex::default(),
            id: MemoryConsumerId::new(partition_id),
            agg_ctx,
            context,
            metrics,
        };
        tables.context.runtime_env().register_requester(tables.id());
        tables
    }

    pub async fn update_in_mem(
        &self,
        process: impl FnOnce(&mut InMemTable) -> Result<()>,
    ) -> Result<()> {
        let mut in_mem = self.in_mem.lock().await;
        let old_mem_used = in_mem.mem_used();

        process(&mut in_mem)?;
        let new_mem_used = in_mem.mem_used();
        drop(in_mem);

        // NOTE: the memory usage is already changed before we call try_grow(),
        // so we first call grow() to increase memory usage, and then call
        // try_grow(0) to spill if necessary.
        if new_mem_used > old_mem_used {
            let mem_increased = new_mem_used - old_mem_used;
            self.grow(mem_increased);
            self.metrics.mem_used().add(mem_increased);
            self.try_grow(0).await?;
        } else if new_mem_used < old_mem_used {
            let mem_freed = old_mem_used - new_mem_used;
            self.shrink(mem_freed);
            self.metrics.mem_used().sub(mem_freed);
        }
        Ok(())
    }

    pub async fn output(
        &self,
        mut grouping_row_converter: RowConverter,
        elapsed_time: Time,
        sender: Sender<ArrowResult<RecordBatch>>,
    ) -> Result<()> {
        let mut in_mem_locked = self.in_mem.lock().await;
        let mut spilled_locked = self.spilled.lock().await;
        let mut timer = elapsed_time.timer();

        let batch_size = self.context.session_config().batch_size();
        let in_mem = std::mem::take(&mut *in_mem_locked);
        let spilled = std::mem::take(&mut *spilled_locked);

        let num_mem_spills = spilled.iter().filter(|spill| spill.mem_size > 0).count();
        let num_disk_spills = spilled.len() - num_mem_spills;

        log::info!(
            "aggregate exec starts outputing with mem-spills={}, disk-spills={}",
            num_mem_spills,
            num_disk_spills,
        );

        // only one in-mem table, directly output it
        if spilled.is_empty() {
            for chunk in in_mem
                .grouping_mappings
                .into_iter()
                .map(|(_1, _2)| AggRecord::new(_1, _2))
                .collect::<Vec<_>>()
                .chunks_mut(batch_size)
            {
                let batch = self
                    .agg_ctx
                    .convert_records_to_batch(&mut grouping_row_converter, chunk)?;
                timer.stop();

                sender.send(Ok(batch)).map_err(|err| {
                    DataFusionError::Execution(format!("{:?}", err))
                }).await?;
                timer.restart();
            }
            return Ok(());
        }

        // convert all tables into cursors
        let mut cursors = vec![];
        for table in spilled {
            cursors.push(TableCursor::try_new_spilled(table)?);
        }
        if in_mem.num_records() > 0 {
            cursors.push(TableCursor::try_new_sorted(in_mem)?);
        }

        let mut staging_records = Vec::with_capacity(batch_size);
        let mut current_record: Option<AggRecord> = None;

        macro_rules! flush_staging {
            () => {{
                let mut records = std::mem::take(&mut staging_records);
                let batch = self
                    .agg_ctx
                    .convert_records_to_batch(
                        &mut grouping_row_converter,
                        &mut records,
                    )?;
                timer.stop();

                sender.send(Ok(batch)).map_err(|err| {
                    DataFusionError::Execution(format!("{:?}", err))
                }).await?;
                timer.restart();
            }};
        }

        // create a tournament loser tree to do the merging
        // the mem-table and at least one spill should be in the tree
        let mut cursors: LoserTree<TableCursor> = LoserTree::new_by(
            cursors,
            |c1: &TableCursor, c2: &TableCursor| match (c1.peek(), c2.peek()) {
                (None, _) => false,
                (_, None) => true,
                (Some(c1), Some(c2)) => c1 < c2,
            });
        assert!(cursors.len() > 0);

        loop {
            // extract min cursor with the loser tree
            let mut min_cursor = cursors.peek_mut();
            if min_cursor.peek().is_none() {
                // all cursors are finished
                break;
            }
            let mut min_record = min_cursor.next()?.unwrap();

            // merge min record into current record
            match current_record.as_mut() {
                None => current_record = Some(min_record),
                Some(current_record) => {
                    if &min_record == current_record {
                        // update entry
                        let mut acc_offset = 0;
                        for agg in &self.agg_ctx.aggs {
                            agg.agg.partial_merge_scalar(
                                &mut current_record.accums[acc_offset..],
                                &mut min_record.accums[acc_offset..],
                            )?;
                            acc_offset += agg.agg.accums_initial().len();
                        }
                    } else {
                        staging_records
                            .push(std::mem::replace(current_record, min_record));
                        if staging_records.len() >= batch_size {
                            flush_staging!();
                        }
                    }
                }
            }
        }
        if let Some(record) = current_record {
            staging_records.push(record);
        }
        if !staging_records.is_empty() {
            flush_staging!();
        }

        let used = self.metrics.mem_used().set(0);
        self.shrink(used);
        Ok(())
    }
}

#[async_trait]
impl MemoryConsumer for AggTables {
    fn name(&self) -> String {
        "AggTables".to_string()
    }

    fn id(&self) -> &MemoryConsumerId {
        &self.id
    }

    fn memory_manager(&self) -> Arc<MemoryManager> {
        self.context.runtime_env().memory_manager.clone()
    }

    fn type_(&self) -> &ConsumerType {
        &ConsumerType::Requesting
    }

    async fn spill(&self) -> Result<usize> {
        let current_used = self.mem_used();
        log::info!(
            "agg tables start spilling, used={:.2} MB, {}",
            current_used as f64 / 1e6,
            self.memory_manager(),
        );
        let mut in_mem = self.in_mem.lock().await;
        let mut spilled = self.spilled.lock().await;
        let mut freed = 0isize;

        // first try spill in-mem into spilled
        let in_mem = std::mem::take(&mut *in_mem);
        let in_mem_used = in_mem.mem_used();
        freed += in_mem_used as isize;

        let spill_num_rows = in_mem.num_records();
        let spill = in_mem.try_into_mem_spilled(&self.agg_ctx)?;
        let spill_mem_used = spill.mem_size;
        freed -= spill_mem_used as isize;
        spilled.push(spill);
        log::info!(
            "aggregate table (num_rows={}) spilled into memory ({:.2} MB into {:.2} MB), freed={:.2} MB",
            spill_num_rows,
            in_mem_used as f64 / 1e6,
            spill_mem_used as f64 / 1e6,
            freed as f64 / 1e6
        );

        // move mem-spilled into disk-spilled if necessary
        while (freed as usize) < current_used / 2 {
            let max_spill_idx = spilled
                .iter_mut()
                .enumerate()
                .max_by_key(|(_, spill)| spill.mem_size)
                .unwrap()
                .0;
            let spill_count = spilled.len();
            spilled.swap(max_spill_idx, spill_count - 1);
            let max_spill = spilled.pop().unwrap();
            let max_spill_mem_size = max_spill.mem_size;

            self.metrics.record_spill(max_spill_mem_size);
            freed += max_spill.mem_size as isize;

            // move max_spill into file
            let disk_manager = &self.context.runtime_env().disk_manager;
            spilled.push(max_spill.try_into_disk_spill(disk_manager)?);

            log::info!(
                "aggregate table spilled into file, freed={:.2} MB",
                max_spill_mem_size as f64 / 1e6
            );
        }

        let freed = freed as usize;
        self.metrics.mem_used().sub(freed);
        Ok(freed)
    }

    fn mem_used(&self) -> usize {
        self.metrics.mem_used().value()
    }
}

impl Drop for AggTables {
    fn drop(&mut self) {
        self.context
            .runtime_env()
            .drop_consumer(self.id(), self.mem_used());
    }
}

/// Unordered in-mem hash table which can be updated
#[derive(Default)]
pub struct InMemTable {
    is_hash: bool,
    grouping_mappings: HashMap<Box<[u8]>, Box<[ScalarValue]>>,
    unsorted: Vec<AggRecord>,
    data_mem_used: usize,
}

impl InMemTable {
    pub fn mem_used(&self) -> usize {
        // TODO: use more precise mem_used calculation
        let mem = self.data_mem_used
            + size_of::<AggRecord>() * self.unsorted.capacity()
            + size_of::<AggRecord>() * self.grouping_mappings.capacity()
            + size_of::<u64>() * self.grouping_mappings.capacity()
            + size_of::<Self>();

        // NOTE: when spilling in hash mode, the hash table is first transformed
        //  to a sorted vec. this operation requires extra memory. to avoid
        //  oom, we report more memory usage than actually used.
        if self.is_hash {
            mem * 2
        } else {
            mem
        }
    }

    pub fn num_records(&self) -> usize {
        self.grouping_mappings.len() + self.unsorted.len()
    }

    pub fn update(
        &mut self,
        agg_ctx: &Arc<AggContext>,
        grouping_row: Box<[u8]>,
        agg_input_arrays: &[Vec<ArrayRef>],
        row_idx: usize,
    ) -> Result<()> {
        let grouping_row_mem_size = grouping_row.len();

        // get entry from hash/unsorted table
        if self.is_hash {
            let mut is_new = false;
            let entry = self.grouping_mappings
                .entry(grouping_row)
                .or_insert_with(|| {
                    is_new = true;
                    self.data_mem_used += grouping_row_mem_size;
                    agg_ctx.create_initial_accums()

                });
            if !is_new {
                let accums_old_mem_size = agg_ctx.get_accums_mem_size(entry);
                self.data_mem_used -= accums_old_mem_size;
            }
            agg_ctx.partial_update_or_merge_one_row(entry, &agg_input_arrays, row_idx)?;

            let accums_new_mem_size = agg_ctx.get_accums_mem_size(entry);
            self.data_mem_used += accums_new_mem_size;

        } else {
            self.unsorted.push(
                AggRecord::new(grouping_row, agg_ctx.create_initial_accums())
            );
            let entry = &mut self.unsorted.last_mut().unwrap().accums;
            agg_ctx.partial_update_or_merge_one_row(entry, &agg_input_arrays, row_idx)?;

            let accums_new_mem_size = agg_ctx.get_accums_mem_size(entry);
            self.data_mem_used += grouping_row_mem_size;
            self.data_mem_used += accums_new_mem_size;
        };
        Ok(())
    }

    fn into_sorted_vec(self) -> Vec<AggRecord> {
        const USE_RADIX_SORT: bool = true;

        let mut vec = if self.is_hash {
            self.grouping_mappings
                .into_iter()
                .map(|(_1, _2)| AggRecord::new(_1, _2))
                .collect::<Vec<_>>()
        } else {
            self.unsorted
        };

        if USE_RADIX_SORT {
            radix_sort_records(&mut vec);
        } else {
            vec.sort_unstable();
        }
        vec
    }

    fn try_into_mem_spilled(self, agg_ctx: &Arc<AggContext>) -> Result<SpilledTable> {
        let spilled_buf = vec![];
        let zwriter = lz4_flex::frame::FrameEncoder::new(
            BufWriter::with_capacity(65536, spilled_buf)
        );
        let mut writer = BufWriter::with_capacity(65536, zwriter);

        let mut grouping_records = self.into_sorted_vec();
        let default_accum_batch_size = 10000;
        for chunk in grouping_records.chunks_mut(default_accum_batch_size) {
            let num_rows = chunk.len();
            let num_accums = agg_ctx.accums_lens.iter().sum();

            // write accums as batch
            let mut accum_writer = Cursor::new(vec![]);
            let mut accum_columns = Vec::with_capacity(num_accums);
            let mut values = vec![];
            for i in 0..num_accums {
                values.reserve(num_rows);
                for row_idx in 0..num_rows {
                    let value = std::mem::replace(
                        &mut chunk[row_idx].accums[i],
                        ScalarValue::Null,
                    );
                    values.push(value);
                }
                let array = ScalarValue::iter_to_array(std::mem::take(&mut values))?;
                accum_columns.push(array);
            }
            let accum_batch = RecordBatch::try_new_with_options(
                Arc::new(Schema::new(accum_columns
                    .iter()
                    .map(|array: &ArrayRef| {
                        Field::new("", array.data_type().clone(), array.null_count() > 0)
                    })
                    .collect())),
                accum_columns,
                &RecordBatchOptions::new().with_row_count(Some(chunk.len())),
            )?;
            write_one_batch(&accum_batch, &mut accum_writer, false)?;
            writer.write_all(&accum_writer.into_inner())?;

            // write grouping rows
            for record in chunk {
                write_len(record.grouping.as_ref().len(), &mut writer)?;
                writer.write_all(record.grouping.as_ref())?;
            }
        }

        let zwriter = writer.into_inner().map_err(|err| err.into_error())?;
        let mut spilled_buf = zwriter
            .finish()
            .map_err(|err| DataFusionError::Execution(format!("{}", err)))?
            .into_inner()
            .map_err(|err| err.into_error())?;
        spilled_buf.shrink_to_fit();
        let mem_size = spilled_buf.len();

        Ok(SpilledTable {
            spilled_reader: Box::new(Cursor::new(spilled_buf)),
            mem_size,
        })
    }
}

struct SpilledTable {
    spilled_reader: Box<dyn Read + Send>,
    mem_size: usize,
}

impl SpilledTable {
    fn try_into_disk_spill(self, disk_manager: &DiskManager) -> Result<Self> {
        let mut spilled_reader = self.spilled_reader;
        let mut spilled_file = disk_manager.create_tmp_file("AggDiskSpill")?;
        std::io::copy(&mut spilled_reader, &mut spilled_file)?;
        spilled_file.rewind()?;
        Ok(SpilledTable {
            spilled_reader: Box::new(spilled_file),
            mem_size: 0,
        })
    }

    fn try_into_iterator(self) -> Result<SpilledTableIterator> {
        let mut iter = SpilledTableIterator {
            input: BufReader::with_capacity(
                65536,
                FrameDecoder::new(
                    Box::new(BufReader::with_capacity(65536, self.spilled_reader))
                ),
            ),
            cur_accum_batch: None,
            cur_accum_idx: 0,
            cur_record: None,
        };
        iter.next().transpose()?; // first record is always None
        Ok(iter)
    }
}

struct SpilledTableIterator {
    input: BufReader<FrameDecoder<Box<dyn Read + Send>>>,
    cur_accum_batch: Option<RecordBatch>,
    cur_accum_idx: usize,
    cur_record: Option<AggRecord>,
}
impl SpilledTableIterator {
    fn next_impl(&mut self) -> Result<Option<AggRecord>> {
        let current = std::mem::take(&mut self.cur_record);

        // load next accum batch if necessary
        if self.cur_accum_idx
            >= self
                .cur_accum_batch
                .as_ref()
                .map(|batch| batch.num_rows())
                .unwrap_or(0)
        {
            self.cur_accum_idx = 0;
            self.cur_accum_batch = read_one_batch(&mut self.input, None, false)?;
            if self.cur_accum_batch.is_none() {
                self.cur_record = None;
                return Ok(current);
            }
        }

        // read grouping row
        let grouping_row_buf_len = read_len(&mut self.input)?;
        let grouping_row = read_bytes_slice(&mut self.input, grouping_row_buf_len)?;

        // read accums
        let accums: Box<[ScalarValue]> = self.cur_accum_batch
            .as_ref()
            .unwrap()
            .columns()
            .iter()
            .map(|column| ScalarValue::try_from_array(column, self.cur_accum_idx))
            .collect::<Result<_>>()?;
        self.cur_record = Some(AggRecord::new(grouping_row, accums));
        self.cur_accum_idx += 1;
        Ok(current)
    }
}
impl Iterator for SpilledTableIterator {
    type Item = Result<AggRecord>;
    fn next(&mut self) -> Option<Self::Item> {
        self.next_impl().transpose()
    }
}

enum TableCursor {
    Sorted(std::vec::IntoIter<AggRecord>, Option<AggRecord>),
    Spilled(SpilledTableIterator, Option<AggRecord>),
}
impl TableCursor {
    fn try_new_sorted(table: InMemTable) -> Result<Self> {
        let mut iter = table.into_sorted_vec().into_iter();
        let first = iter.next();
        Ok(TableCursor::Sorted(iter, first))
    }

    fn try_new_spilled(table: SpilledTable) -> Result<Self> {
        let mut iter = table.try_into_iterator()?;
        let first = iter.next().transpose()?;
        Ok(TableCursor::Spilled(iter, first))
    }

    fn next(&mut self) -> Result<Option<AggRecord>> {
        match self {
            TableCursor::Sorted(iter, peek) => Ok(std::mem::replace(peek, iter.next())),
            TableCursor::Spilled(iter, peek) => {
                Ok(std::mem::replace(peek, iter.next().transpose()?))
            }
        }
    }

    fn peek(&self) -> Option<&AggRecord> {
        match self {
            TableCursor::Sorted(_, peek) => peek.as_ref(),
            TableCursor::Spilled(_, peek) => peek.as_ref(),
        }
    }
}