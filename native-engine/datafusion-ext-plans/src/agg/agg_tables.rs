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

use std::io::{BufReader, BufWriter, Cursor, Read, Seek, Write};
use std::mem::size_of;
use std::sync::Arc;
use arrow::array::ArrayRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use arrow::row::RowConverter;
use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::execution::{DiskManager, MemoryConsumer, MemoryConsumerId, MemoryManager};
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_manager::ConsumerType;
use datafusion::physical_plan::metrics::{BaselineMetrics, Time};
use futures::lock::Mutex;
use hashbrown::HashMap;
use lz4_flex::frame::FrameDecoder;
use tokio::sync::mpsc::Sender;
use datafusion_ext_commons::io::{read_bytes_slice, read_len, read_one_batch, write_len, write_one_batch};
use crate::agg::agg_helper::{AggContext, AggRecord};
use crate::agg::{AggAccumRef, AggMode};

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

        process(&mut *in_mem)?;
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

        let num_mem_spills = spilled
            .iter()
            .filter(|spill| spill.mem_size > 0)
            .count();
        let num_disk_spills = spilled.len() - num_mem_spills;

        log::info!("aggregate exec starts outputing with mem-spills={}, disk-spills={}",
            num_mem_spills,
            num_disk_spills,
        );

        // only one in-mem table, directly output it
        if spilled.is_empty() {
            for chunk in in_mem.grouping_mappings
                .into_iter()
                .collect::<Vec<_>>()
                .chunks_mut(batch_size)
            {
                let batch = self.agg_ctx.convert_records_to_batch(
                    &mut grouping_row_converter,
                    chunk,
                )?;
                timer.stop();
                sender.send(Ok(batch)).await.unwrap();
                timer.restart();
            }
            return Ok(());
        }

        // convert all tables into cursors
        let mut cursors = vec![];
        for table in spilled {
            cursors.push(TableCursor::try_new_spilled(table, &self.agg_ctx)?);
        }
        if in_mem.num_records() > 0 {
            cursors.push(TableCursor::try_new_sorted(in_mem)?);
        }

        let mut staging_records = Vec::with_capacity(batch_size);
        let mut current_record: Option<AggRecord> = None;

        macro_rules! flush_staging {
            () => {{
                let records = std::mem::take(&mut staging_records);
                let batch = self.agg_ctx.convert_records_to_batch(
                    &mut grouping_row_converter,
                    &records,
                )?;
                timer.stop();
                sender.send(Ok(batch)).await.unwrap();
                timer.restart();
            }}
        }

        // create a tournament loser tree to do the merging
        macro_rules! cursor_lt {
            ($cursor1:expr, $cursor2:expr) => {{
                match ($cursor1.peek(), $cursor2.peek()) {
                    (None, _) => false,
                    (_, None) => true,
                    (Some(c1), Some(c2)) => c1.0 < c2.0,
                }
            }}
        }
        let mut loser_tree = vec![usize::MAX; cursors.len()];
        for i in 0..cursors.len() {
            let mut winner = i;
            let mut cmp_node = (cursors.len() + i) / 2;
            while cmp_node != 0 && loser_tree[cmp_node] != usize::MAX {
                let challenger = loser_tree[cmp_node];
                if cursor_lt!(&cursors[challenger], &cursors[winner]) {
                    loser_tree[cmp_node] = winner;
                    winner = challenger;
                } else {
                    loser_tree[cmp_node] = challenger;
                }
                cmp_node /= 2;
            }
            loser_tree[cmp_node] = winner;
        }

        loop {
            // extract min cursor with the loser tree
            let min_cursor = &mut cursors[loser_tree[0]];
            if min_cursor.peek().is_none() { // all cursors are finished
                break;
            }
            let min_record = min_cursor.next()?.unwrap();

            // merge min record into current record
            match current_record.as_mut() {
                None => current_record = Some(min_record),
                Some(current_record) => {
                    if min_record.0 == current_record.0 {
                        current_record.1
                            .iter_mut()
                            .zip(Vec::from(min_record.1).into_iter())
                            .map(|(a, b)| a.partial_merge(b))
                            .collect::<Result<_>>()?;

                    } else {
                        staging_records.push(std::mem::replace(
                            current_record,
                            min_record,
                        ));
                        if staging_records.len() >= batch_size {
                            flush_staging!();
                        }
                    }
                }
            }

            // adjust loser tree
            let mut winner = loser_tree[0];
            let mut cmp_node = (cursors.len() + winner) / 2;
            while cmp_node != 0 {
                let challenger = loser_tree[cmp_node];
                if cursor_lt!(&cursors[challenger], &cursors[winner]) {
                    loser_tree[cmp_node] = winner;
                    winner = challenger;
                }
                cmp_node /= 2;
            }
            loser_tree[0] = winner;
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
        format!("AggTables")
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
        log::info!(
            "agg tables start spilling, used={:.2} MB, {}",
            self.mem_used() as f64 / 1e6,
            self.memory_manager(),
        );
        let mut in_mem = self.in_mem.lock().await;
        let mut spilled = self.spilled.lock().await;
        let mut freed = 0isize;

        // first try spill in-mem into spilled
        let in_mem = std::mem::take(&mut *in_mem);
        freed += in_mem.mem_used() as isize;

        let spill = in_mem.try_into_mem_spilled(&self.agg_ctx)?;
        freed -= spill.mem_size as isize;
        spilled.push(spill);
        log::info!("aggregate table spilled into memory, freed={:.2} MB",
            freed as f64 / 1e6
        );

        // move mem-spilled into disk-spilled if necessary
        let spilled_count_limit = 5;
        loop {
            let mem_spill_count = spilled
                .iter()
                .filter(|spill| spill.mem_size > 0)
                .count();
            if freed > 0 && mem_spill_count < spilled_count_limit {
                break;
            }

            let max_spill_idx = spilled
                .iter_mut()
                .enumerate()
                .max_by_key(|(_, spill)| spill.mem_size)
                .unwrap()
                .0;
            let spill_count = spilled.len();
            spilled.swap(max_spill_idx, spill_count - 1);
            let max_spill = spilled.pop().unwrap();

            self.metrics.record_spill(max_spill.mem_size);
            freed += max_spill.mem_size as isize;

            // move max_spill into file
            let disk_manager = &self.context.runtime_env().disk_manager;
            spilled.push(max_spill.try_into_disk_spill(disk_manager)?);

            log::info!(
                "aggregate table spilled into file, freed={:.2} MB",
                freed as f64 / 1e6);
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
        self.context.runtime_env().drop_consumer(self.id(), self.mem_used());
    }
}

/// Unordered in-mem hash table which can be updated
#[derive(Default)]
pub struct InMemTable {
    is_hash: bool,
    grouping_mappings: HashMap<Box<[u8]>, Box<[AggAccumRef]>>,
    unsorted: Vec<AggRecord>,
    data_mem_used: usize,
}

impl InMemTable {
    pub fn mem_used(&self) -> usize {
        // TODO: use more precise mem_used calculation
        self.data_mem_used
            + size_of::<AggRecord>() * self.unsorted.capacity()
            + size_of::<AggRecord>() * self.grouping_mappings.capacity()
            + size_of::<u64>() * self.grouping_mappings.capacity()
            + size_of::<Self>()
    }

    pub fn num_records(&self) -> usize {
        self.grouping_mappings.len() + self.unsorted.len()
    }

    pub fn update(
        &mut self,
        agg_ctx: &Arc<AggContext>,
        grouping_row: Box<[u8]>,
        agg_children_projected_arrays: &[Vec<ArrayRef>],
        row_idx: usize,
    ) -> Result<()> {
        let mut mem_diff = 0isize;

        // get entry from hash/unsorted table
        let entry = if self.is_hash {
            let grouping_row_mem_size = grouping_row.as_ref().len();
            let mut is_new = false;

            // find or create a new entry
            let entry = self.grouping_mappings
                .entry(grouping_row)
                .or_insert_with(|| {
                    is_new = true;
                    Box::new([])
                });
            if is_new {
                *entry = agg_ctx.aggs
                    .iter()
                    .map(|agg| agg.agg.create_accum())
                    .collect::<Result<Box<[AggAccumRef]>>>()?;

                self.data_mem_used += grouping_row_mem_size;
                self.data_mem_used += entry
                    .iter()
                    .map(|accum| accum.mem_size())
                    .sum::<usize>();
            }
            entry
        } else {
            let accums = agg_ctx.aggs
                .iter()
                .map(|agg| agg.agg.create_accum())
                .collect::<Result<Box<[AggAccumRef]>>>()?;
            self.unsorted.push((grouping_row, accums));
            &mut self.unsorted.last_mut().unwrap().1
        };

        // update entry
        for (idx, accum) in entry.iter_mut().enumerate() {
            mem_diff -= accum.mem_size() as isize;
            if agg_ctx.aggs[idx].mode == AggMode::Partial {
                accum.partial_update(
                    &agg_children_projected_arrays[idx],
                    row_idx)?;
            } else {
                accum.partial_merge_from_array(
                    &agg_children_projected_arrays[idx],
                    row_idx)?;
            }
            mem_diff += accum.mem_size() as isize;
        }
        self.data_mem_used += mem_diff as usize;
        Ok(())
    }

    fn into_sorted_vec(self) -> Vec<AggRecord> {
        let mut vec = if self.is_hash {
            self.grouping_mappings
                .into_iter()
                .collect::<Vec<_>>()
        } else {
            self.unsorted
        };
        vec.sort_by(|(g1, _), (g2, _)| g1.cmp(g2));
        vec
    }

    fn try_into_mem_spilled(
        self,
        agg_ctx: &Arc<AggContext>,
    ) -> Result<SpilledTable> {

        let spilled_buf = vec![];
        let zwriter = lz4_flex::frame::FrameEncoder::new(spilled_buf);
        let mut writer = BufWriter::with_capacity(65536, zwriter);

        let grouping_records = self.into_sorted_vec();
        let default_accum_batch_size = 10000;
        for chunk in grouping_records.chunks(default_accum_batch_size) {
            // write accums
            let mut accum_writer = Cursor::new(vec![]);
            let agg_columns = agg_ctx.build_agg_columns(chunk)?;
            let agg_batch = RecordBatch::try_new_with_options(
                agg_ctx.agg_schema.clone(),
                agg_columns,
                &RecordBatchOptions::new().with_row_count(Some(chunk.len())),
            )?;
            write_one_batch(&agg_batch, &mut accum_writer, false)?;
            writer.write_all(&accum_writer.into_inner())?;

            // write grouping rows
            for (grouping_row, _) in chunk {
                write_len(grouping_row.as_ref().len(), &mut writer)?;
                writer.write_all(grouping_row.as_ref())?;
            }
        }

        let zwriter = writer
            .into_inner()
            .map_err(|err| DataFusionError::Execution(format!("{}", err)))?;
        let spilled_buf = zwriter
            .finish()
            .map_err(|err| DataFusionError::Execution(format!("{}", err)))?;
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

    fn try_into_iterator(
        self,
        agg_ctx: &Arc<AggContext>,
    ) -> Result<SpilledTableIterator> {
        let mut iter = SpilledTableIterator {
            input: BufReader::with_capacity(65536, FrameDecoder::new(self.spilled_reader)),
            agg_ctx: agg_ctx.clone(),
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
    agg_ctx: Arc<AggContext>,
    cur_accum_batch: Option<RecordBatch>,
    cur_accum_idx: usize,
    cur_record: Option<AggRecord>,
}
impl SpilledTableIterator {
    fn next_impl(&mut self) -> Result<Option<AggRecord>> {
        let current = std::mem::take(&mut self.cur_record);

        // load next accum batch if necessary
        if self.cur_accum_idx >= self.cur_accum_batch
            .as_ref()
            .map(|batch| batch.num_rows())
            .unwrap_or(0)
        {
            self.cur_accum_idx = 0;
            self.cur_accum_batch = read_one_batch(
                &mut self.input, None, false)?;
            if self.cur_accum_batch.is_none() {
                self.cur_record = None;
                return Ok(current);
            }
        }

        // read grouping row
        let grouping_row_buf_len = read_len(&mut self.input)?;
        let grouping_row = read_bytes_slice(
            &mut self.input,
            grouping_row_buf_len,
        )?;

        // read accums
        let mut accums = vec![];
        for agg in &self.agg_ctx.aggs {
            let mut accum = agg.agg.create_accum()?;
            accum.load(
                self.cur_accum_batch.as_ref().unwrap().columns(),
                self.cur_accum_idx,
            )?;
            accums.push(accum);
        }
        self.cur_accum_idx += 1;
        self.cur_record = Some((grouping_row, accums.into()));
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

    fn try_new_spilled(
        table: SpilledTable,
        agg_ctx: &Arc<AggContext>,
    ) -> Result<Self> {
        let mut iter = table.try_into_iterator(agg_ctx)?;
        let first = iter.next().transpose()?;
        Ok(TableCursor::Spilled(iter, first))
    }

    fn next(&mut self) -> Result<Option<AggRecord>> {
        match self {
            TableCursor::Sorted(iter, peek) => {
                Ok(std::mem::replace(peek, iter.next()))
            }
            TableCursor::Spilled(iter, peek) => {
                Ok(std::mem::replace(peek, iter.next().transpose()?))
            }
        }
    }

    fn peek(&self) -> Option<&AggRecord> {
        match self {
            TableCursor::Sorted(_, peek) => {
                peek.as_ref()
            }
            TableCursor::Spilled(_, peek) => {
                peek.as_ref()
            }
        }
    }
}