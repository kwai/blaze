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
    hash::{BuildHasher, Hash, Hasher},
    io::{BufReader, Read, Write},
    mem::{size_of, ManuallyDrop},
    sync::{Arc, Weak},
};

use ahash::RandomState;
use arrow::{record_batch::RecordBatch, row::Rows};
use async_trait::async_trait;
use datafusion::{
    common::Result, execution::context::TaskContext, physical_plan::metrics::BaselineMetrics,
};
use datafusion_ext_commons::{
    bytes_arena::BytesArena,
    df_execution_err,
    io::{read_bytes_slice, read_len, write_len},
    loser_tree::{ComparableForLoserTree, LoserTree},
    rdxsort,
    slim_bytes::SlimBytes,
};
use futures::lock::Mutex;
use hashbrown::{
    hash_map::{Entry, RawEntryMut},
    HashMap,
};
use lz4_flex::frame::FrameDecoder;

use crate::{
    agg::{agg_buf::AggBuf, agg_context::AggContext},
    common::output::WrappedRecordBatchSender,
    memmgr::{
        onheap_spill::{try_new_spill, Spill},
        MemConsumer, MemConsumerInfo, MemManager,
    },
};

// reserve memory for each spill
// estimated size: bufread=64KB + lz4dec.src=64KB + lz4dec.dest=64KB
const SPILL_OFFHEAP_MEM_COST: usize = 200000;

// fixed constant random state used for hashing map keys
const RANDOM_STATE: RandomState = RandomState::with_seeds(
    0x9C6E1CA4E863D6DC,
    0x5E2C7A28DC159691,
    0xB6ED5B7156072B89,
    0xA2B679F7684AC524,
);

pub struct AggTables {
    name: String,
    mem_consumer_info: Option<Weak<MemConsumerInfo>>,
    in_mem: Mutex<InMemTable>,
    spills: Mutex<Vec<Box<dyn Spill>>>,
    agg_ctx: Arc<AggContext>,
    context: Arc<TaskContext>,
    metrics: BaselineMetrics,
}

impl AggTables {
    pub fn new(
        partition_id: usize,
        agg_ctx: Arc<AggContext>,
        metrics: BaselineMetrics,
        context: Arc<TaskContext>,
    ) -> Self {
        Self {
            name: format!("AggTable[partition={}]", partition_id),
            mem_consumer_info: None,
            // only the first im-mem table uses hash
            in_mem: Mutex::new(InMemTable::new(agg_ctx.clone(), InMemMode::Hash)),
            spills: Mutex::default(),
            agg_ctx,
            context,
            metrics,
        }
    }

    pub async fn process_input_batch(&self, input_batch: RecordBatch) -> Result<()> {
        let mut in_mem = self.in_mem.lock().await;

        // compute grouping rows
        let grouping_rows = self.agg_ctx.create_grouping_rows(&input_batch)?;

        // compute input arrays
        let input_arrays = self.agg_ctx.create_input_arrays(&input_batch)?;
        let agg_buf_array = self.agg_ctx.get_input_agg_buf_array(&input_batch)?;
        in_mem.update_entries(grouping_rows, |agg_bufs| {
            let mut mem_diff = 0;
            mem_diff += self
                .agg_ctx
                .partial_batch_update_input(agg_bufs, &input_arrays)?;
            mem_diff += self
                .agg_ctx
                .partial_batch_merge_input(agg_bufs, agg_buf_array)?;
            Ok(mem_diff)
        })?;
        let mem_used = in_mem.mem_used();
        drop(in_mem);

        // if triggered partial skipping, no need to update memory usage and try to
        // spill
        if self.mode().await != InMemMode::PartialSkipped {
            self.update_mem_used(mem_used).await?;
        }
        Ok(())
    }

    pub async fn has_spill(&self) -> bool {
        !self.spills.lock().await.is_empty()
    }

    pub async fn mode(&self) -> InMemMode {
        self.in_mem.lock().await.mode
    }

    pub async fn renew_in_mem_table(&self, mode: InMemMode) -> InMemTable {
        let mut old = self.in_mem.lock().await;
        let new = InMemTable::new(self.agg_ctx.clone(), mode);
        std::mem::replace(&mut *old, new)
    }

    pub async fn process_partial_skipped(
        &self,
        input_batch: RecordBatch,
        baseline_metrics: BaselineMetrics,
        sender: Arc<WrappedRecordBatchSender>,
    ) -> Result<()> {
        self.set_spillable(false);
        let mut timer = baseline_metrics.elapsed_compute().timer();

        let old_in_mem = self.renew_in_mem_table(InMemMode::PartialSkipped).await;
        assert_eq!(old_in_mem.num_records(), 0); // old table must be cleared

        self.process_input_batch(input_batch).await?;
        let in_mem = self.renew_in_mem_table(InMemMode::PartialSkipped).await;
        let records = in_mem
            .unsorted_keys
            .iter()
            .flat_map(|rows| rows.iter())
            .zip(in_mem.unsorted_values.into_iter())
            .collect::<Vec<_>>();
        let batch = self.agg_ctx.convert_records_to_batch(records)?;

        baseline_metrics.record_output(batch.num_rows());
        sender.send(Ok(batch), Some(&mut timer)).await;
        self.update_mem_used(0).await?;
        return Ok(());
    }

    pub async fn output(
        &self,
        baseline_metrics: BaselineMetrics,
        sender: Arc<WrappedRecordBatchSender>,
    ) -> Result<()> {
        self.set_spillable(false);
        let mut timer = baseline_metrics.elapsed_compute().timer();

        let in_mem = self.renew_in_mem_table(InMemMode::PartialSkipped).await;
        let spills = std::mem::take(&mut *self.spills.lock().await);

        let batch_size = self.context.session_config().batch_size();
        log::info!(
            "aggregate exec starts outputting with {} ({} spills)",
            self.name(),
            spills.len(),
        );

        // only one in-mem table, directly output it
        if spills.is_empty() {
            let mut records = in_mem
                .map
                .into_iter()
                .map(|(key_addr, value)| (in_mem.map_keys.get(key_addr), value))
                .collect::<Vec<_>>();

            while !records.is_empty() {
                let chunk = records.split_off(records.len().saturating_sub(batch_size));
                records.shrink_to_fit();

                let batch = self.agg_ctx.convert_records_to_batch(chunk)?;
                let batch_mem_size = batch.get_array_memory_size();

                baseline_metrics.record_output(batch.num_rows());
                sender.send(Ok(batch), Some(&mut timer)).await;

                // free memory of the output batch
                self.update_mem_used_with_diff(-(batch_mem_size as isize))
                    .await?;
            }
            self.update_mem_used(0).await?;
            return Ok(());
        }

        // convert all tables into cursors
        let mut spills = spills;
        let mut cursors = vec![];
        if in_mem.num_records() > 0 {
            // spill staging records
            if let Some(spill) = in_mem.try_into_spill()? {
                spills.push(spill);
            }
            self.update_mem_used(spills.len() * SPILL_OFFHEAP_MEM_COST)
                .await?;
        }
        for spill in &spills {
            cursors.push(SpillCursor::try_from_spill(&spill, &self.agg_ctx)?);
        }
        let mut staging_records = Vec::with_capacity(batch_size);
        let mut current_bucket_idx = 0;
        let mut current_records: HashMap<SlimBytes, AggBuf> = HashMap::new();

        macro_rules! flush_staging {
            () => {{
                let batch = self
                    .agg_ctx
                    .convert_records_to_batch(std::mem::take(&mut staging_records))?;
                baseline_metrics.record_output(batch.num_rows());
                sender.send(Ok(batch), Some(&mut timer)).await;
            }};
        }

        // create a tournament loser tree to do the merging
        // the mem-table and at least one spill should be in the tree
        let mut cursors: LoserTree<SpillCursor> = LoserTree::new(cursors);
        assert!(cursors.len() > 0);

        loop {
            // extract min cursor with the loser tree
            let mut min_cursor = cursors.peek_mut();

            // meets next bucket -- flush records of current bucket
            if min_cursor.cur_bucket_idx > current_bucket_idx {
                for (key, value) in current_records.drain() {
                    staging_records.push((key, value));
                    if staging_records.len() >= batch_size {
                        flush_staging!();
                    }
                }
                current_bucket_idx = min_cursor.cur_bucket_idx;
            }

            // all cursors are finished
            if current_bucket_idx == 65536 {
                break;
            }

            // merge records of current bucket
            for _ in 0..min_cursor.cur_bucket_count {
                let (key, mut value) = min_cursor.next_record()?;
                match current_records.entry(key) {
                    Entry::Occupied(mut view) => {
                        for (idx, agg) in self.agg_ctx.aggs.iter().enumerate() {
                            let addr_offset = self.agg_ctx.agg_buf_addr_offsets[idx];
                            let addrs = &self.agg_ctx.agg_buf_addrs[addr_offset..];
                            agg.agg
                                .partial_merge(view.get_mut(), &mut value, addrs)
                                .map_err(|err| {
                                    err.context("agg: executing partial_merge() error")
                                })?;
                        }
                    }
                    Entry::Vacant(view) => {
                        view.insert(value);
                    }
                }
            }
            min_cursor.next_bucket()?;
        }
        if !staging_records.is_empty() {
            flush_staging!();
        }

        // update disk spill size
        let spill_disk_usage = spills
            .iter()
            .map(|spill| spill.get_disk_usage().unwrap_or(0))
            .sum::<u64>();
        self.metrics.record_spill(spill_disk_usage as usize);
        self.update_mem_used(0).await?;
        Ok(())
    }
}

#[async_trait]
impl MemConsumer for AggTables {
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
        let mut in_mem = self.in_mem.lock().await;
        let mut spills = self.spills.lock().await;

        // do not spill anything if triggered partial skipping
        // regardless minRows configuration
        in_mem.check_trigger_partial_skipping();
        if in_mem.mode != InMemMode::PartialSkipped {
            let replaced_in_mem = InMemTable::new(self.agg_ctx.clone(), InMemMode::Merge);
            spills.extend(std::mem::replace(&mut *in_mem, replaced_in_mem).try_into_spill()?);
            drop(spills);
            drop(in_mem);
            self.update_mem_used(0).await?;
        }
        Ok(())
    }
}

impl Drop for AggTables {
    fn drop(&mut self) {
        MemManager::deregister_consumer(self);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InMemMode {
    Hash,
    Merge,
    PartialSkipped,
}

/// Unordered in-mem hash table which can be updated
pub struct InMemTable {
    agg_ctx: Arc<AggContext>,
    map_keys: Box<BytesArena>,
    map: HashMap<u64, AggBuf, MapKeyHashBuilder>,
    unsorted_keys: Vec<Rows>,
    unsorted_values: Vec<AggBuf>,
    unsorted_keys_mem_used: usize,
    agg_buf_mem_used: usize,
    num_input_records: usize,
    mode: InMemMode,
}

// a hasher for hashing addrs got from map_keys
struct BytesArenaHasher(&'static BytesArena, u64);
impl Hasher for BytesArenaHasher {
    fn write(&mut self, _bytes: &[u8]) {
        unimplemented!()
    }

    fn write_u64(&mut self, i: u64) {
        self.1 = RANDOM_STATE.hash_one(self.0.get(i));
    }

    fn finish(&self) -> u64 {
        self.1
    }
}

// hash builder for map
struct MapKeyHashBuilder(&'static BytesArena);
impl BuildHasher for MapKeyHashBuilder {
    type Hasher = BytesArenaHasher;

    fn build_hasher(&self) -> Self::Hasher {
        BytesArenaHasher(self.0, 0)
    }

    fn hash_one<T: Hash>(&self, x: T) -> u64
    where
        Self: Sized,
        Self::Hasher: Hasher,
    {
        // this hasher is specialized for hashing map keys, so it's safe
        // to convert x to u64 (bytes arena's addr)
        let i = unsafe { *std::mem::transmute::<_, *const u64>(&x) };
        RANDOM_STATE.hash_one(self.0.get(i))
    }
}
unsafe impl Send for MapKeyHashBuilder {}

impl InMemTable {
    fn new(agg_ctx: Arc<AggContext>, mode: InMemMode) -> Self {
        let map_keys: Box<BytesArena> = Box::default();
        let map_key_hash_builder = MapKeyHashBuilder(unsafe {
            // safety: hash builder's lifetime is shorter than map_keys
            std::mem::transmute::<_, &'static BytesArena>(map_keys.as_ref())
        });
        Self {
            agg_ctx,
            map_keys,
            map: HashMap::with_hasher(map_key_hash_builder),
            unsorted_keys: vec![],
            unsorted_values: vec![],
            unsorted_keys_mem_used: 0,
            agg_buf_mem_used: 0,
            num_input_records: 0,
            mode,
        }
    }

    pub fn mem_used(&self) -> usize {
        self.agg_buf_mem_used
            // map memory usage, one byte per entry
            + self.map_keys.mem_size()
            + self.map.capacity() * size_of::<(u64, AggBuf, u8)>()
            // unsorted memory usage
            + self.unsorted_values.capacity() * size_of::<AggBuf>()
            + self.unsorted_keys_mem_used
            // memory usage for sorting
            + self.num_records() * size_of::<(u16, &[u8], AggBuf)>()
    }

    pub fn num_records(&self) -> usize {
        self.map.len() + self.unsorted_values.len()
    }

    pub fn update_entries(
        &mut self,
        key_rows: Rows,
        fn_entries: impl Fn(&mut [AggBuf]) -> Result<usize>,
    ) -> Result<()> {
        let num_input_records = key_rows.num_rows();
        let mem_diff = if self.mode == InMemMode::Hash {
            self.update_hash_entries(key_rows, fn_entries)?
        } else {
            self.update_unsorted_entries(key_rows, fn_entries)?
        };
        self.num_input_records += num_input_records;
        if self.num_input_records >= self.agg_ctx.partial_skipping_min_rows {
            self.check_trigger_partial_skipping();
        }
        Ok(mem_diff)
    }

    fn update_hash_entries(
        &mut self,
        key_rows: Rows,
        fn_entries: impl Fn(&mut [AggBuf]) -> Result<usize>,
    ) -> Result<()> {
        let hashes: Vec<u64> = key_rows
            .iter()
            .map(|row| RANDOM_STATE.hash_one(row.as_ref()))
            .collect();
        let mut agg_bufs = Vec::with_capacity(key_rows.num_rows());

        for (row_idx, row) in key_rows.iter().enumerate() {
            match self
                .map
                .raw_entry_mut()
                .from_hash(hashes[row_idx], |&addr| {
                    self.map_keys.get(addr) == row.as_ref()
                }) {
                RawEntryMut::Occupied(view) => {
                    // safety: agg_buf lives longer than this function call.
                    // items in agg_bufs are later moved into ManuallyDrop to avoid double drop.
                    agg_bufs.push(unsafe { std::ptr::read(view.get() as *const AggBuf) });
                }
                RawEntryMut::Vacant(view) => {
                    let new_key_addr = self.map_keys.add(row.as_ref());
                    let new_entry = self.agg_ctx.initial_agg_buf.clone();
                    // safety: agg_buf lives longer than this function call.
                    // items in agg_bufs are later moved into ManuallyDrop to avoid double drop.
                    agg_bufs.push(unsafe { std::ptr::read(&new_entry as *const AggBuf) });
                    view.insert(new_key_addr, new_entry);
                }
            }
        }

        self.agg_buf_mem_used += fn_entries(&mut agg_bufs)?;
        for agg_buf in agg_bufs {
            let _ = ManuallyDrop::new(agg_buf);
        }
        Ok(())
    }

    fn update_unsorted_entries(
        &mut self,
        key_rows: Rows,
        fn_entries: impl Fn(&mut [AggBuf]) -> Result<usize>,
    ) -> Result<()> {
        let beg = self.unsorted_values.len();
        let len = key_rows.num_rows();
        self.unsorted_values
            .extend((0..len).map(|_| self.agg_ctx.initial_agg_buf.clone()));
        self.agg_buf_mem_used += fn_entries(&mut self.unsorted_values[beg..][..len])?;
        self.unsorted_keys_mem_used += key_rows.size();
        self.unsorted_keys.push(key_rows);
        Ok(())
    }

    fn check_trigger_partial_skipping(&mut self) {
        if self.agg_ctx.supports_partial_skipping && self.mode != InMemMode::PartialSkipped {
            let num_input_records = self.num_input_records;
            let num_records = self.num_records();
            let cardinality_ratio = num_records as f64 / num_input_records as f64;
            if cardinality_ratio > self.agg_ctx.partial_skipping_ratio {
                log::warn!(
                    "Agg: cardinality ratio = {cardinality_ratio}, will trigger partial skipping"
                );
                self.mode = InMemMode::PartialSkipped;
            }
        }
    }

    fn try_into_spill(self) -> Result<Option<Box<dyn Spill>>> {
        if self.map.is_empty() && self.unsorted_values.is_empty() {
            return Ok(None);
        }

        fn write_spill(
            bucket_counts: &[usize],
            bucketed_records: &mut [(u16, impl AsRef<[u8]>, AggBuf)],
        ) -> Result<Box<dyn Spill>> {
            let spill = try_new_spill()?;
            let mut writer = lz4_flex::frame::FrameEncoder::new(spill.get_buf_writer());
            let mut beg = 0;

            for i in 0..65536 {
                if bucket_counts[i] > 0 {
                    // write bucket id and number of records in this bucket
                    write_len(i, &mut writer)?;
                    write_len(bucket_counts[i], &mut writer)?;

                    // write records in this bucket
                    for (_, key, value) in &mut bucketed_records[beg..][..bucket_counts[i]] {
                        // write key
                        let key = key.as_ref();
                        write_len(key.len(), &mut writer)?;
                        writer.write_all(key)?;

                        // write value
                        value.save(&mut writer)?;
                    }
                }
                beg += bucket_counts[i];
            }
            write_len(65536, &mut writer)?; // EOF
            write_len(0, &mut writer)?;
            writer.finish().or_else(|err| df_execution_err!("{err}"))?;
            spill.complete()?;
            Ok(spill)
        }

        // sort all records using radix sort on hashcodes of keys
        let spill = if self.mode == InMemMode::Hash {
            let mut sorted = self
                .map
                .into_iter()
                .map(|(key_addr, value)| {
                    let key = self.map_keys.get(key_addr);
                    let key_hash = RANDOM_STATE.hash_one(key) as u16;
                    (key_hash, key, value)
                })
                .collect::<Vec<_>>();
            let bucket_counts = rdxsort::radix_sort_u16_by(&mut sorted, |(h, ..)| *h);
            write_spill(&bucket_counts, &mut sorted)?
        } else {
            let mut sorted = self
                .unsorted_keys
                .iter()
                .flat_map(|rows| rows.iter())
                .zip(self.unsorted_values.into_iter())
                .map(|(key, value)| (RANDOM_STATE.hash_one(key.as_ref()) as u16, key, value))
                .collect::<Vec<_>>();
            let bucket_counts = rdxsort::radix_sort_u16_by(&mut sorted, |(h, ..)| *h);
            write_spill(&bucket_counts, &mut sorted)?
        };
        Ok(Some(spill))
    }
}

struct SpillCursor {
    agg_ctx: Arc<AggContext>,
    input: FrameDecoder<BufReader<Box<dyn Read + Send>>>,
    pub cur_bucket_idx: usize,
    pub cur_bucket_count: usize,
}

impl ComparableForLoserTree for SpillCursor {
    fn lt(&self, other: &Self) -> bool {
        self.cur_bucket_idx < other.cur_bucket_idx
    }
}

impl SpillCursor {
    fn try_from_spill(spill: &Box<dyn Spill>, agg_ctx: &Arc<AggContext>) -> Result<Self> {
        let input = FrameDecoder::new(spill.get_buf_reader());
        let mut cursor = SpillCursor {
            agg_ctx: agg_ctx.clone(),
            input,
            cur_bucket_idx: 0,
            cur_bucket_count: 0,
        };
        cursor.next_bucket()?;
        Ok(cursor)
    }

    fn next_record(&mut self) -> Result<(SlimBytes, AggBuf)> {
        // read key
        let key_len = read_len(&mut self.input)?;
        let key = read_bytes_slice(&mut self.input, key_len)?.into();

        // read value
        let mut value = self.agg_ctx.initial_agg_buf.clone();
        value.load(&mut self.input)?;
        Ok((key, value))
    }

    fn next_bucket(&mut self) -> Result<()> {
        self.cur_bucket_idx = read_len(&mut self.input)?;
        self.cur_bucket_count = read_len(&mut self.input)?;
        Ok(())
    }
}
