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

use ahash::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};
use std::io::{BufReader, Read, Write};
use std::mem::size_of;
use std::sync::{Arc, Weak};

use arrow::row::{RowConverter, Rows};
use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;

use datafusion::physical_plan::metrics::BaselineMetrics;
use futures::lock::Mutex;
use hashbrown::hash_map::{Entry, RawEntryMut};
use hashbrown::HashMap;
use lz4_flex::frame::FrameDecoder;

use datafusion_ext_commons::io::{read_bytes_slice, read_len, write_len};
use datafusion_ext_commons::loser_tree::LoserTree;

use crate::agg::agg_buf::AggBuf;
use crate::agg::agg_context::AggContext;
use crate::common::bytes_arena::BytesArena;
use crate::common::memory_manager::{MemConsumer, MemConsumerInfo, MemManager};
use crate::common::onheap_spill::{try_new_spill, Spill};
use crate::common::output::WrappedRecordBatchSender;
use crate::common::rdxsort;

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
            in_mem: Mutex::new(InMemTable::new(true)), // only the first im-mem table uses hash
            spills: Mutex::default(),
            agg_ctx,
            context,
            metrics,
        }
    }

    pub async fn update_entries(
        &self,
        key_rows: Rows,
        fn_entry: impl Fn(usize, &mut AggBuf) -> Result<()>,
    ) -> Result<()> {
        let mut in_mem = self.in_mem.lock().await;
        in_mem.update_entries(&self.agg_ctx, key_rows, fn_entry)?;

        let mem_used = in_mem.mem_used();
        drop(in_mem);
        self.update_mem_used(mem_used).await?;
        Ok(())
    }

    pub async fn has_spill(&self) -> bool {
        !self.spills.lock().await.is_empty()
    }

    pub async fn output(
        &self,
        mut grouping_row_converter: RowConverter,
        baseline_metrics: BaselineMetrics,
        sender: Arc<WrappedRecordBatchSender>,
    ) -> Result<()> {
        self.set_spillable(false);
        let mut timer = baseline_metrics.elapsed_compute().timer();

        let in_mem = std::mem::replace(&mut *self.in_mem.lock().await, InMemTable::new(false));
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
                let mut chunk = records.split_off(records.len().saturating_sub(batch_size));
                records.shrink_to_fit();

                let batch = self
                    .agg_ctx
                    .convert_records_to_batch(&mut grouping_row_converter, &mut chunk)?;
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
        let mut current_records: HashMap<Box<[u8]>, AggBuf> = HashMap::new();

        macro_rules! flush_staging {
            () => {{
                let batch = self
                    .agg_ctx
                    .convert_records_to_batch(&mut grouping_row_converter, &mut staging_records)?;
                staging_records.clear();
                baseline_metrics.record_output(batch.num_rows());
                sender.send(Ok(batch), Some(&mut timer)).await;
            }};
        }

        // create a tournament loser tree to do the merging
        // the mem-table and at least one spill should be in the tree
        let mut cursors: LoserTree<SpillCursor> =
            LoserTree::new_by(cursors, |c1, c2| c1.cur_bucket_idx < c2.cur_bucket_idx);
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

        spills.extend(std::mem::replace(&mut *in_mem, InMemTable::new(false)).try_into_spill()?);
        drop(spills);
        drop(in_mem);

        self.update_mem_used(0).await?;
        Ok(())
    }
}

impl Drop for AggTables {
    fn drop(&mut self) {
        MemManager::deregister_consumer(self);
    }
}

/// Unordered in-mem hash table which can be updated
pub struct InMemTable {
    map_keys: Box<BytesArena>,
    map: HashMap<u64, AggBuf, MapKeyHashBuilder>,
    unsorted_keys: Vec<Rows>,
    unsorted_values: Vec<AggBuf>,
    unsorted_keys_mem_used: usize,
    agg_buf_mem_used: usize,
    pub is_hash: bool,
}

// a hasher for hashing addrs got from map_keys
struct BytesArenaHasher(*const BytesArena, u64);
impl Hasher for BytesArenaHasher {
    fn write(&mut self, _bytes: &[u8]) {
        unimplemented!()
    }

    fn write_u64(&mut self, i: u64) {
        // safety - this hasher has the same lifetime with bytes arena
        // and guaranteed to have no read-write conflicts
        let bytes_arena = unsafe { &*self.0 };
        self.1 = RANDOM_STATE.hash_one(bytes_arena.get(i));
    }

    fn finish(&self) -> u64 {
        self.1
    }
}

// hash builder for map
struct MapKeyHashBuilder(*const BytesArena);
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
        // safety - this hasher has the same lifetime with bytes arena
        // and guaranteed to have no read-write conflicts
        // this hasher is specialized for hashing map keys, so it's safe
        // to convert x to u64 (bytes arena's addr)
        let bytes_arena = unsafe { &*self.0 };
        let i = unsafe { *std::mem::transmute::<_, *const u64>(&x) };
        RANDOM_STATE.hash_one(bytes_arena.get(i))
    }
}
unsafe impl Send for MapKeyHashBuilder {}

impl InMemTable {
    fn new(is_hash: bool) -> Self {
        let map_keys: Box<BytesArena> = Box::default();
        let map_key_hash_builder = MapKeyHashBuilder(map_keys.as_ref() as *const BytesArena);
        Self {
            map_keys,
            map: HashMap::with_hasher(map_key_hash_builder),
            unsorted_keys: vec![],
            unsorted_values: vec![],
            unsorted_keys_mem_used: 0,
            agg_buf_mem_used: 0,
            is_hash,
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
        agg_ctx: &Arc<AggContext>,
        key_rows: Rows,
        fn_entry: impl Fn(usize, &mut AggBuf) -> Result<()>,
    ) -> Result<()> {
        if self.is_hash {
            self.update_hash_entries(agg_ctx, key_rows, fn_entry)
        } else {
            self.update_unsorted_entries(agg_ctx, key_rows, fn_entry)
        }
    }

    fn update_hash_entries(
        &mut self,
        agg_ctx: &Arc<AggContext>,
        key_rows: Rows,
        fn_entry: impl Fn(usize, &mut AggBuf) -> Result<()>,
    ) -> Result<()> {
        let hashes: Vec<u64> = key_rows
            .iter()
            .map(|row| RANDOM_STATE.hash_one(row.as_ref()))
            .collect();

        for (row_idx, row) in key_rows.iter().enumerate() {
            match self
                .map
                .raw_entry_mut()
                .from_hash(hashes[row_idx], |&addr| {
                    self.map_keys.get(addr) == row.as_ref()
                }) {
                RawEntryMut::Occupied(mut view) => {
                    self.agg_buf_mem_used -= view.get().mem_size();
                    fn_entry(row_idx, view.get_mut())?;
                    self.agg_buf_mem_used += view.get().mem_size();
                }
                RawEntryMut::Vacant(view) => {
                    let new_key_addr = self.map_keys.add(row.as_ref());
                    let mut new_entry = agg_ctx.initial_agg_buf.clone();
                    fn_entry(row_idx, &mut new_entry)?;
                    self.agg_buf_mem_used += new_entry.mem_size();
                    view.insert(new_key_addr, new_entry);
                }
            }
        }
        Ok(())
    }

    fn update_unsorted_entries(
        &mut self,
        agg_ctx: &Arc<AggContext>,
        key_rows: Rows,
        fn_entry: impl Fn(usize, &mut AggBuf) -> Result<()>,
    ) -> Result<()> {
        for i in 0..key_rows.num_rows() {
            let mut new_entry = agg_ctx.initial_agg_buf.clone();
            fn_entry(i, &mut new_entry)?;
            self.agg_buf_mem_used += new_entry.mem_size();
            self.unsorted_values.push(new_entry);
        }
        self.unsorted_keys_mem_used += key_rows.size();
        self.unsorted_keys.push(key_rows);
        Ok(())
    }

    fn try_into_spill(self) -> Result<Option<Box<dyn Spill>>> {
        if self.map.is_empty() && self.unsorted_values.is_empty() {
            return Ok(None);
        }

        // sort all records using radix sort on hashcodes of keys
        let mut sorted: Vec<(u16, &[u8], AggBuf)> = if self.is_hash {
            self.map
                .into_iter()
                .map(|(key_addr, value)| {
                    let key = self.map_keys.get(key_addr);
                    let key_hash = RANDOM_STATE.hash_one(key) as u16;
                    (key_hash, key, value)
                })
                .collect()
        } else {
            self.unsorted_values
                .into_iter()
                .zip(self.unsorted_keys.iter().flat_map(|rows| {
                    rows.iter().map(|row| {
                        // safety - row bytes has same lifetime with self.unsorted_rows
                        unsafe { std::mem::transmute::<_, &'static [u8]>(row.as_ref()) }
                    })
                }))
                .map(|(value, key)| (RANDOM_STATE.hash_one(key) as u16, key, value))
                .collect()
        };
        let counts = rdxsort::radix_sort_u16_by(&mut sorted, |(h, _, _)| *h);

        let spill = try_new_spill()?;
        let mut writer = lz4_flex::frame::FrameEncoder::new(spill.get_buf_writer());
        let mut beg = 0;

        for i in 0..65536 {
            if counts[i] > 0 {
                // write bucket id and number of records in this bucket
                write_len(i, &mut writer)?;
                write_len(counts[i], &mut writer)?;

                // write records in this bucket
                for (_, key, value) in &mut sorted[beg..][..counts[i]] {
                    // write key
                    write_len(key.len(), &mut writer)?;
                    writer.write_all(key)?;

                    // write value
                    value.save(&mut writer)?;
                }
                beg += counts[i];
            }
        }
        write_len(65536, &mut writer)?; // EOF
        write_len(0, &mut writer)?;

        writer
            .finish()
            .map_err(|err| DataFusionError::Execution(format!("{}", err)))?;
        spill.complete()?;
        Ok(Some(spill))
    }
}

struct SpillCursor {
    agg_ctx: Arc<AggContext>,
    input: FrameDecoder<BufReader<Box<dyn Read + Send>>>,
    pub cur_bucket_idx: usize,
    pub cur_bucket_count: usize,
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

    fn next_record(&mut self) -> Result<(Box<[u8]>, AggBuf)> {
        // read key
        let key_len = read_len(&mut self.input)?;
        let key = read_bytes_slice(&mut self.input, key_len)?;

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
