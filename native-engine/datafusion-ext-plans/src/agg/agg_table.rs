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
    hash::BuildHasher,
    io::Write,
    sync::{Arc, Weak},
};

use arrow::{record_batch::RecordBatch, row::Rows};
use async_trait::async_trait;
use bytesize::ByteSize;
use datafusion::{
    common::{DataFusionError, Result},
    physical_plan::metrics::Time,
};
use datafusion_ext_commons::{
    algorithm::{
        rdx_tournament_tree::{KeyForRadixTournamentTree, RadixTournamentTree},
        rdxsort::radix_sort_by_key,
    },
    batch_size, compute_suggested_batch_size_for_output, df_execution_err, downcast_any,
    io::{read_bytes_slice, read_len, write_len},
};
use futures::lock::Mutex;
use smallvec::SmallVec;

use crate::{
    agg::{acc::AccTable, agg::IdxSelection, agg_ctx::AggContext, agg_hash_map::AggHashMap},
    common::{
        execution_context::{ExecutionContext, WrappedRecordBatchSender},
        timer_helper::TimerHelper,
        SliceAsRawBytes,
    },
    memmgr::{
        spill::{try_new_spill, Spill, SpillCompressedReader, SpillCompressedWriter},
        MemConsumer, MemConsumerInfo, MemManager,
    },
};

pub type OwnedKey = SmallVec<u8, 24>;
const _OWNED_KEY_SIZE_CHECKER: [(); 32] = [(); size_of::<OwnedKey>()];

// reserve memory for each spill
// estimated size: bufread=64KB + lz4dec.src=64KB + lz4dec.dest=64KB +
const SPILL_OFFHEAP_MEM_COST: usize = 200000;

// number of buckets used in merging/spilling
const NUM_SPILL_BUCKETS: usize = 64000;

pub struct AggTable {
    name: String,
    mem_consumer_info: Option<Weak<MemConsumerInfo>>,
    in_mem: Mutex<InMemTable>,
    spills: Mutex<Vec<Box<dyn Spill>>>,
    agg_ctx: Arc<AggContext>,
    exec_ctx: Arc<ExecutionContext>,
    output_time: Time,
}

impl AggTable {
    pub fn new(agg_ctx: Arc<AggContext>, exec_ctx: Arc<ExecutionContext>) -> Self {
        let name = format!("AggTable[partition={}]", exec_ctx.partition_id());
        let hashing_time = exec_ctx.register_timer_metric("hashing_time");
        let merging_time = exec_ctx.register_timer_metric("merging_time");
        let output_time = exec_ctx.register_timer_metric("output_time");
        Self {
            mem_consumer_info: None,
            in_mem: Mutex::new(InMemTable::new(
                name.clone(),
                0,
                agg_ctx.clone(),
                exec_ctx.clone(),
                InMemMode::Hashing,
                hashing_time.clone(),
                merging_time.clone(),
            )),
            spills: Mutex::default(),
            name,
            agg_ctx,
            exec_ctx,
            output_time,
        }
    }

    pub async fn process_input_batch(&self, input_batch: RecordBatch) -> Result<()> {
        let mut in_mem = self.in_mem.lock().await;

        // compute input arrays
        match in_mem.mode {
            InMemMode::Hashing => {
                in_mem.hashing_data.update_batch(input_batch)?;
            }
            InMemMode::Merging => {
                in_mem.merging_data.add_batch(input_batch)?;
            }
        }

        // trigger partial skipping if memory usage is too high
        if self.mem_used_percent() > 0.8 {
            if self.agg_ctx.supports_partial_skipping {
                return df_execution_err!("AGG_TRIGGER_PARTIAL_SKIPPING");
            }
        }

        // check for partial skipping by cardinality ratio
        if in_mem.num_records() >= self.agg_ctx.partial_skipping_min_rows {
            if in_mem.check_trigger_partial_skipping() {
                return df_execution_err!("AGG_TRIGGER_PARTIAL_SKIPPING");
            }
        }

        // update memory usage
        let mem_used = in_mem.mem_used();
        drop(in_mem);
        self.update_mem_used(mem_used).await?;
        Ok(())
    }

    pub async fn has_spill(&self) -> bool {
        !self.spills.lock().await.is_empty()
    }

    pub async fn mode(&self) -> InMemMode {
        self.in_mem.lock().await.mode
    }

    pub async fn renew_in_mem_table(&self, mode: InMemMode) -> InMemTable {
        self.in_mem.lock().await.renew(mode)
    }

    pub async fn output(&self, sender: Arc<WrappedRecordBatchSender>) -> Result<()> {
        let _timer = self.output_time.timer();
        self.set_spillable(false);

        let in_mem = self.renew_in_mem_table(InMemMode::Hashing).await;
        let spills = std::mem::take(&mut *self.spills.lock().await);
        let batch_size = batch_size();

        if in_mem.num_records() == 0 && spills.is_empty() {
            return Ok(()); // no records
        }

        log::info!(
            "{} starts outputting ({} spills + in_mem({:?}): {})",
            self.name(),
            spills.len(),
            in_mem.mode,
            ByteSize(in_mem.mem_used() as u64)
        );

        // only one in-mem table, directly output it
        if spills.is_empty() {
            let num_records = in_mem.num_records();
            let mem_used = in_mem.mem_used();
            let output_batch_size = compute_suggested_batch_size_for_output(mem_used, num_records);
            let mut acc_table = in_mem.hashing_data.acc_table;
            let mut keys = in_mem.hashing_data.map.into_keys();

            // output in reversed order, so we can truncate records and free
            // memory as soon as possible
            for begin in (0..num_records).step_by(output_batch_size).rev() {
                let end = std::cmp::min(begin + output_batch_size, num_records);
                let batch = self.agg_ctx.convert_records_to_batch(
                    &keys[begin..end],
                    &mut acc_table,
                    IdxSelection::Range(begin, end),
                )?;

                // truncate and free memory
                keys.truncate(begin);
                keys.shrink_to_fit();
                acc_table.resize(begin);
                acc_table.shrink_to_fit();

                self.exec_ctx
                    .baseline_metrics()
                    .record_output(batch.num_rows());
                self.output_time
                    .exclude_timer_async(sender.send(batch))
                    .await;

                // free memory of the output batch
                // this is not precise because the used memory is accounted by records and
                // not freed by batches.
                let cur_mem_used = mem_used * keys.len() / num_records;
                self.update_mem_used(cur_mem_used).await?;
            }
            self.update_mem_used(0).await?;
            return Ok(());
        }

        // convert all tables into cursors
        let mut spills = spills;
        let mut cursors = vec![];
        if in_mem.num_records() > 0 {
            let spill = tokio::task::spawn_blocking(|| {
                let mut spill: Box<dyn Spill> = Box::new(vec![]);
                in_mem.try_into_spill(&mut spill)?; // spill staging records
                Ok::<_, DataFusionError>(spill)
            })
            .await
            .expect("tokio error")?;
            let spill_size = downcast_any!(spill, Vec<u8>)?.len();
            self.update_mem_used(spill_size + spills.len() * SPILL_OFFHEAP_MEM_COST)
                .await?;
            spills.push(spill);
        }
        for spill in &mut spills {
            cursors.push(RecordsSpillCursor::try_from_spill(spill, &self.agg_ctx)?);
        }

        // create a radix tournament tree to do the merging
        // the mem-table and at least one spill should be in the tree
        let mut cursors: RadixTournamentTree<RecordsSpillCursor> =
            RadixTournamentTree::new(cursors, NUM_SPILL_BUCKETS);
        assert!(cursors.len() > 0);

        let mut map = AggHashMap::default();
        let mut acc_table = self.agg_ctx.create_acc_table(0);

        while let cur_bucket_idx = cursors.peek().cur_bucket_idx
            && cur_bucket_idx < NUM_SPILL_BUCKETS
        {
            // process current bucket
            while let mut min_cursor = cursors.peek_mut()
                && min_cursor.cur_bucket_idx == cur_bucket_idx
            {
                // merge records of current bucket
                let (mut bucket_acc_table, bucket_key_rows) = min_cursor.read_bucket()?;
                let map_indices = map.upsert_records(bucket_key_rows);
                acc_table.resize(map.len());
                for (agg_idx, agg) in self.agg_ctx.aggs.iter().enumerate() {
                    agg.agg.partial_merge(
                        &mut acc_table.cols_mut()[agg_idx],
                        IdxSelection::IndicesU32(&map_indices),
                        &mut bucket_acc_table.cols_mut()[agg_idx],
                        IdxSelection::Range(0, map_indices.len()),
                    )?;
                }
            }

            // output
            let keys = map.take_keys();
            for begin in (0..keys.len()).step_by(batch_size) {
                let end = std::cmp::min(begin + batch_size, keys.len());
                let batch = self.agg_ctx.convert_records_to_batch(
                    &keys[begin..end],
                    &mut acc_table,
                    IdxSelection::Range(begin, end),
                )?;
                self.exec_ctx
                    .baseline_metrics()
                    .record_output(batch.num_rows());
                sender.send(batch).await;
            }
            acc_table.resize(0);
        }

        assert!(cursors.values().iter().all(|c| !c.has_next_bucket()));
        self.update_mem_used(0).await?;
        Ok(())
    }
}

#[async_trait]
impl MemConsumer for AggTable {
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
        if self.agg_ctx.supports_partial_skipping {
            return df_execution_err!("AGG_SPILL_PARTIAL_SKIPPING");
        }
        let mut in_mem = self.in_mem.lock().await;
        let mut spills = self.spills.lock().await;

        let mut next_in_mem_mode = InMemMode::Merging;
        if in_mem.mode == InMemMode::Hashing {
            // use pre-merging if cardinality is low
            if in_mem.hashing_data.cardinality_ratio() < 0.5 {
                next_in_mem_mode = InMemMode::Hashing
            }
        }
        let cur_in_mem = in_mem.renew(next_in_mem_mode);

        let spill_metrics = self.exec_ctx.spill_metrics().clone();
        let cur_spill = tokio::task::spawn_blocking(move || {
            let mut spill = try_new_spill(&spill_metrics)?;
            cur_in_mem.try_into_spill(&mut spill)?;
            Ok::<_, DataFusionError>(spill)
        })
        .await
        .expect("tokio error")?;
        spills.push(cur_spill);
        drop(spills);
        drop(in_mem);
        self.update_mem_used(0).await?;
        Ok(())
    }
}

impl Drop for AggTable {
    fn drop(&mut self) {
        MemManager::deregister_consumer(self);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InMemMode {
    Hashing,
    Merging,
}

/// Unordered in-mem hash table which can be updated
pub struct InMemTable {
    name: String,
    id: usize,
    agg_ctx: Arc<AggContext>,
    exec_ctx: Arc<ExecutionContext>,
    hashing_data: HashingData,
    merging_data: MergingData,
    mode: InMemMode,
}

impl InMemTable {
    fn new(
        name: String,
        id: usize,
        agg_ctx: Arc<AggContext>,
        exec_ctx: Arc<ExecutionContext>,
        mode: InMemMode,
        hashing_time: Time,
        merging_time: Time,
    ) -> Self {
        Self {
            name,
            id,
            hashing_data: HashingData::new(agg_ctx.clone(), hashing_time),
            merging_data: MergingData::new(agg_ctx.clone(), merging_time),
            agg_ctx,
            exec_ctx,
            mode,
        }
    }

    fn renew(&mut self, mode: InMemMode) -> Self {
        let name = self.name.clone();
        let agg_ctx = self.agg_ctx.clone();
        let task_ctx = self.exec_ctx.clone();
        let id = self.id + 1;
        let hashing_time = self.hashing_data.hashing_time.clone();
        let merging_time = self.merging_data.merging_time.clone();
        std::mem::replace(
            self,
            Self::new(
                name,
                id,
                agg_ctx,
                task_ctx,
                mode,
                hashing_time,
                merging_time,
            ),
        )
    }

    pub fn mem_used(&self) -> usize {
        let hashing_used = self.hashing_data.mem_used();
        let merging_used = self.merging_data.mem_used();
        let sorting_indices_used = self.num_records() * 16;
        hashing_used + merging_used + sorting_indices_used
    }

    pub fn num_records(&self) -> usize {
        self.hashing_data.num_records() + self.merging_data.num_records()
    }

    fn check_trigger_partial_skipping(&mut self) -> bool {
        if self.id == 0 // only works on first table
            && !self.agg_ctx.is_expand_agg
            && self.agg_ctx.supports_partial_skipping
            && self.mode == InMemMode::Hashing
        {
            let cardinality_ratio = self.hashing_data.cardinality_ratio();
            if cardinality_ratio > self.agg_ctx.partial_skipping_ratio {
                log::warn!(
                    "{} cardinality ratio = {cardinality_ratio}, will trigger partial skipping",
                    self.name,
                );
                return true;
            }
        }
        false
    }

    fn try_into_spill(self, spill: &mut Box<dyn Spill>) -> Result<()> {
        match self.mode {
            InMemMode::Hashing => self.hashing_data.try_into_spill(spill),
            InMemMode::Merging => self.merging_data.try_into_spill(spill),
        }
    }
}

pub struct HashingData {
    agg_ctx: Arc<AggContext>,
    acc_table: AccTable,
    map: AggHashMap,
    num_input_records: usize,
    hashing_time: Time,
}

impl HashingData {
    fn new(agg_ctx: Arc<AggContext>, hashing_time: Time) -> Self {
        Self {
            acc_table: agg_ctx.create_acc_table(0),
            map: AggHashMap::default(),
            num_input_records: 0,
            agg_ctx,
            hashing_time,
        }
    }

    fn num_records(&self) -> usize {
        self.map.len()
    }

    fn cardinality_ratio(&self) -> f64 {
        let num_input_records = self.num_input_records;
        let num_records = self.map.len();
        num_records as f64 / num_input_records as f64
    }

    fn mem_used(&self) -> usize {
        self.map.mem_size() + self.acc_table.mem_size()
    }

    fn update_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let _timer = self.hashing_time.timer();

        let num_rows = batch.num_rows();
        self.num_input_records += num_rows;

        let grouping_rows = self.agg_ctx.create_grouping_rows(&batch)?;
        let record_indices = self.map.upsert_records(
            grouping_rows
                .iter()
                .map(|row| row.as_ref().as_raw_bytes())
                .collect(),
        );
        self.acc_table.resize(self.map.len());
        self.agg_ctx.update_batch_to_acc_table(
            &batch,
            &mut self.acc_table,
            IdxSelection::IndicesU32(&record_indices),
        )?;
        Ok(())
    }

    fn try_into_spill(self, spill: &mut Box<dyn Spill>) -> Result<()> {
        // sort all records using radix sort on hashcodes of keys
        let key_rows = self.map.into_keys();
        let acc_table = self.acc_table;
        let mut entries = key_rows
            .iter()
            .enumerate()
            .map(|(record_idx, key)| (bucket_id(key) as u32, record_idx as u32))
            .collect::<Vec<_>>();

        let mut bucket_counts = vec![0; NUM_SPILL_BUCKETS];
        radix_sort_by_key(&mut entries, &mut bucket_counts, |(bucket_id, ..)| {
            *bucket_id as usize
        });

        let mut writer = spill.get_compressed_writer();
        let mut offset = 0;
        for (cur_bucket_id, bucket_count) in bucket_counts.into_iter().enumerate() {
            if bucket_count == 0 {
                continue;
            }
            write_len(cur_bucket_id, &mut writer)?;
            write_len(bucket_count, &mut writer)?;
            write_spill_bucket(
                &mut writer,
                &acc_table,
                entries[offset..][..bucket_count]
                    .iter()
                    .map(|&(_, record_idx)| &key_rows[record_idx as usize]),
                entries[offset..][..bucket_count]
                    .iter()
                    .map(|&(_, record_idx)| record_idx as usize),
            )?;
            offset += bucket_count;
        }
        // EOF
        write_len(NUM_SPILL_BUCKETS, &mut writer)?;
        write_len(0, &mut writer)?;
        writer.finish()?;
        Ok(())
    }
}

pub struct MergingData {
    agg_ctx: Arc<AggContext>,
    acc_table: AccTable,
    key_rows: Vec<Rows>,
    entries: Vec<(u32, u32, u32, u32)>, // (bucket_id, batch_idx, row_idx, acc_idx)
    key_rows_mem_size: usize,
    merging_time: Time,
}

impl MergingData {
    fn new(agg_ctx: Arc<AggContext>, merging_time: Time) -> Self {
        Self {
            acc_table: agg_ctx.create_acc_table(0),
            key_rows: vec![],
            entries: vec![],
            key_rows_mem_size: 0,
            agg_ctx,
            merging_time,
        }
    }

    fn num_records(&self) -> usize {
        self.acc_table.num_records()
    }

    fn mem_used(&self) -> usize {
        let entries_mem_size = self.entries.capacity() * size_of::<(u32, u32, u32, u32)>();
        entries_mem_size + self.key_rows_mem_size + self.acc_table.mem_size()
    }

    fn add_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let _timer = self.merging_time.timer();
        let num_rows = batch.num_rows();
        let num_entries_old = self.entries.len();
        let batch_idx = self.key_rows.len();

        // update acc table
        self.acc_table.resize(num_entries_old + num_rows);
        self.agg_ctx.update_batch_to_acc_table(
            &batch,
            &mut self.acc_table,
            IdxSelection::Range(num_entries_old, num_entries_old + num_rows),
        )?;

        // add key rows
        let grouping_rows = self.agg_ctx.create_grouping_rows(&batch)?;
        let hashes = grouping_rows
            .iter()
            .map(|row| bucket_id(row.as_ref()))
            .collect::<Vec<_>>();

        for (i, hash) in hashes.into_iter().enumerate() {
            self.entries.push((
                hash as u32,
                batch_idx as u32,
                i as u32,
                num_entries_old as u32 + i as u32,
            ));
        }
        self.key_rows_mem_size += grouping_rows.size();
        self.key_rows.push(grouping_rows);
        Ok(())
    }

    fn try_into_spill(self, spill: &mut Box<dyn Spill>) -> Result<()> {
        let mut entries = self.entries;
        let key_rows = self.key_rows;
        let acc_table = self.acc_table;

        let mut bucket_counts = vec![0; NUM_SPILL_BUCKETS];
        radix_sort_by_key(&mut entries, &mut bucket_counts, |(bucket_id, ..)| {
            *bucket_id as usize
        });

        let mut writer = spill.get_compressed_writer();
        let mut offset = 0;
        for (cur_bucket_id, bucket_count) in bucket_counts.into_iter().enumerate() {
            if bucket_count == 0 {
                continue;
            }
            write_len(cur_bucket_id, &mut writer)?;
            write_len(bucket_count, &mut writer)?;
            write_spill_bucket(
                &mut writer,
                &acc_table,
                entries[offset..][..bucket_count]
                    .iter()
                    .map(|&(_, batch_idx, row_idx, _)| {
                        key_rows[batch_idx as usize]
                            .row(row_idx as usize)
                            .as_ref()
                            .as_raw_bytes()
                    }),
                entries[offset..][..bucket_count]
                    .iter()
                    .map(|&(_, _, _, record_idx)| record_idx as usize),
            )?;
            offset += bucket_count;
        }

        // EOF
        write_len(NUM_SPILL_BUCKETS, &mut writer)?;
        write_len(0, &mut writer)?;
        writer.finish()?;
        Ok(())
    }
}

fn write_spill_bucket(
    w: &mut SpillCompressedWriter,
    acc_table: &AccTable,
    key_iter: impl Iterator<Item = impl AsRef<[u8]>>,
    acc_idx_iter: impl Iterator<Item = usize>,
) -> Result<()> {
    // write accs
    let acc_indices: Vec<usize> = acc_idx_iter.collect();
    for col in acc_table.cols() {
        col.spill(IdxSelection::Indices(&acc_indices), w)?;
    }

    // write keys
    for key in key_iter {
        write_len(key.as_ref().len(), w)?;
        w.write_all(key.as_ref())?;
    }
    Ok(())
}

fn read_spill_bucket(
    mut r: &mut SpillCompressedReader,
    num_rows: usize,
    acc_table: &mut AccTable,
    keys: &mut Vec<OwnedKey>,
) -> Result<()> {
    for col in acc_table.cols_mut() {
        col.unspill(num_rows, r)?;
    }

    for _ in 0..num_rows {
        let len = read_len(&mut r)?;
        keys.push(OwnedKey::from_vec(read_bytes_slice(&mut r, len)?.into()));
    }
    Ok(())
}

pub struct RecordsSpillCursor<'a> {
    input: SpillCompressedReader<'a>,
    agg_ctx: Arc<AggContext>,
    cur_bucket_idx: usize,
    cur_bucket_count: usize,
}

impl<'a> RecordsSpillCursor<'a> {
    fn try_from_spill(spill: &'a mut Box<dyn Spill>, agg_ctx: &Arc<AggContext>) -> Result<Self> {
        let mut input = spill.get_compressed_reader();
        Ok(Self {
            agg_ctx: agg_ctx.clone(),
            cur_bucket_idx: read_len(&mut input)?,
            cur_bucket_count: read_len(&mut input)?,
            input,
        })
    }

    fn has_next_bucket(&self) -> bool {
        self.cur_bucket_idx < NUM_SPILL_BUCKETS
    }

    fn read_bucket(&mut self) -> Result<(AccTable, Vec<OwnedKey>)> {
        let mut acc_table = self.agg_ctx.create_acc_table(0);
        let mut keys = vec![];
        read_spill_bucket(
            &mut self.input,
            self.cur_bucket_count,
            &mut acc_table,
            &mut keys,
        )?;

        // load next bucket head
        self.cur_bucket_idx = read_len(&mut self.input).unwrap();
        self.cur_bucket_count = read_len(&mut self.input).unwrap();
        Ok((acc_table, keys))
    }
}

impl<'a> KeyForRadixTournamentTree for RecordsSpillCursor<'a> {
    fn rdx(&self) -> usize {
        self.cur_bucket_idx
    }
}

#[inline]
fn bucket_id(key: impl AsRef<[u8]>) -> u16 {
    const AGG_HASH_SEED_HASHING: i64 = 0xC732BD66;
    const HASHER: foldhash::fast::FixedState =
        foldhash::fast::FixedState::with_seed(AGG_HASH_SEED_HASHING as u64);
    let hash = HASHER.hash_one(key.as_ref()) as u32;
    (hash % NUM_SPILL_BUCKETS as u32) as u16
}
