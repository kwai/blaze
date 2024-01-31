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
    hash::Hasher,
    io::{BufReader, Cursor, Read, Write},
    mem::{size_of, ManuallyDrop},
    sync::{Arc, Weak},
};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use async_trait::async_trait;
use datafusion::{
    common::Result,
    execution::context::TaskContext,
    physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet},
};
use datafusion_ext_commons::{
    bytes_arena::BytesArena,
    df_execution_err,
    io::{read_bytes_slice, read_len, read_one_batch, write_len, write_one_batch},
    loser_tree::{ComparableForLoserTree, LoserTree},
    rdxsort::radix_sort_u16_with_max_key_by,
    slim_bytes::SlimBytes,
};
use futures::lock::Mutex;
use gxhash::GxHasher;
use hashbrown::raw::RawTable;
use lz4_flex::frame::{FrameDecoder, FrameEncoder};

use crate::{
    agg::{acc::AccumStateRow, agg_context::AggContext},
    common::{output::WrappedRecordBatchSender, BatchTaker, BatchesInterleaver},
    memmgr::{
        metrics::SpillMetrics,
        onheap_spill::{try_new_spill, Spill},
        MemConsumer, MemConsumerInfo, MemManager,
    },
};

// reserve memory for each spill
// estimated size: bufread=64KB + lz4dec.src=64KB + lz4dec.dest=64KB
const SPILL_OFFHEAP_MEM_COST: usize = 200000;

// number of buckets used in merging/spilling
const NUM_SPILL_BUCKETS: usize = 16384;

pub struct AggTable {
    name: String,
    mem_consumer_info: Option<Weak<MemConsumerInfo>>,
    in_mem: Mutex<InMemTable>,
    spills: Mutex<Vec<AggSpill>>,
    agg_ctx: Arc<AggContext>,
    context: Arc<TaskContext>,
    spill_metrics: SpillMetrics,
}

impl AggTable {
    pub fn new(
        partition_id: usize,
        agg_ctx: Arc<AggContext>,
        context: Arc<TaskContext>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        let spill_metrics = SpillMetrics::new(&metrics, partition_id);
        Self {
            name: format!("AggTable[partition={}]", partition_id),
            mem_consumer_info: None,
            in_mem: Mutex::new(InMemTable::new(
                0,
                agg_ctx.clone(),
                context.clone(),
                InMemMode::Hashing,
                spill_metrics.clone(),
            )),
            spills: Mutex::default(),
            agg_ctx,
            context,
            spill_metrics: spill_metrics.clone(),
        }
    }

    pub async fn process_input_batch(&self, input_batch: RecordBatch) -> Result<()> {
        let mut in_mem = self.in_mem.lock().await;

        // compute input arrays
        match in_mem.mode {
            InMemMode::Hashing => {
                in_mem
                    .hashing_data
                    .update_batch::<GX_HASH_SEED_HASHING>(input_batch)?;
            }
            InMemMode::Merging => {
                in_mem.merging_data.add_batch(input_batch)?;
            }
            InMemMode::PartialSkipped => {
                unreachable!("in_mem.mode cannot be PartialSkipped");
            }
        }

        // check for partial skipping
        if in_mem.num_records() >= self.agg_ctx.partial_skipping_min_rows {
            in_mem.check_trigger_partial_skipping();
        }
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
        self.in_mem.lock().await.renew(mode)
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

        let grouping_rows = self.agg_ctx.create_grouping_rows(&input_batch)?;
        let mut accs: Vec<AccumStateRow> = (0..input_batch.num_rows())
            .map(|_| self.agg_ctx.initial_acc.clone())
            .collect();

        // partial update
        let input_arrays = self.agg_ctx.create_input_arrays(&input_batch)?;
        self.agg_ctx
            .partial_batch_update_input(&mut accs, &input_arrays)?;

        // partial merge
        let acc_array = self.agg_ctx.get_input_acc_array(&input_batch)?;
        self.agg_ctx
            .partial_batch_merge_input(&mut accs, acc_array)?;

        // create output batch
        let records = grouping_rows.iter().zip(accs).collect();
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
        let sub_batch_size = batch_size / batch_size.ilog2() as usize;

        log::info!(
            "aggregate exec starts outputting with {} ({} spills)",
            self.name(),
            spills.len(),
        );

        // only one in-mem table, directly output it
        if spills.is_empty() {
            assert!(matches!(
                in_mem.mode,
                InMemMode::Hashing | InMemMode::PartialSkipped
            ));
            let mut records = in_mem
                .hashing_data
                .map
                .into_iter()
                .map(|(key_addr, value)| (in_mem.hashing_data.map_keys.get(key_addr), value))
                .collect::<Vec<_>>();

            while !records.is_empty() {
                let chunk = records.split_off(records.len().saturating_sub(sub_batch_size));
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
            spills.push(in_mem.try_into_spill()?); // spill staging records
            self.update_mem_used(spills.len() * SPILL_OFFHEAP_MEM_COST)
                .await?;
        }
        for spill in &spills {
            cursors.push(AggSpillCursor::try_from_spill(&spill, &self.agg_ctx)?);
        }
        let mut current_bucket_idx = 0;
        let mut hashing = HashingData::new(
            self.agg_ctx.clone(),
            self.context.clone(),
            &self.spill_metrics,
        );

        macro_rules! flush_staging {
            ($staging_records:expr) => {{
                let batch = self.agg_ctx.convert_records_to_batch($staging_records)?;
                baseline_metrics.record_output(batch.num_rows());
                sender.send(Ok(batch), Some(&mut timer)).await;
            }};
        }
        macro_rules! flush_bucket {
            () => {{
                if hashing.num_records() > 0 {
                    let mut staging_records = vec![];
                    let cur_hashing = hashing.renew();
                    let map = cur_hashing.map;
                    let map_keys = cur_hashing.map_keys;
                    for (key_addr, value) in map {
                        let key = unsafe {
                            // safety:
                            // map_keys will be append-only while processing the same bucket
                            std::mem::transmute::<_, &[u8]>(map_keys.get(key_addr))
                        };
                        staging_records.push((key, value));
                        if staging_records.len() >= sub_batch_size {
                            flush_staging!(std::mem::take(&mut staging_records));
                        }
                    }

                    // must flush out all staging records because we are dropping map keys
                    if !staging_records.is_empty() {
                        flush_staging!(staging_records);
                    }
                }
            }};
        }

        // create a tournament loser tree to do the merging
        // the mem-table and at least one spill should be in the tree
        let mut cursors: LoserTree<AggSpillCursor> = LoserTree::new(cursors);
        assert!(cursors.len() > 0);

        loop {
            // extract min cursor with the loser tree
            let mut min_cursor = cursors.peek_mut();

            // meets next bucket -- flush records of current bucket
            if min_cursor.cur_bucket_idx() > current_bucket_idx {
                flush_bucket!();
                current_bucket_idx = min_cursor.cur_bucket_idx();
            }

            // all cursors are finished
            if current_bucket_idx == NUM_SPILL_BUCKETS {
                break;
            }

            // merge records of current bucket
            match &mut *min_cursor {
                AggSpillCursor::Records(c) => {
                    while c.cur_bucket_idx == current_bucket_idx {
                        let (key, mut acc) = c.next_record()?;
                        let hash = gx_hash::<GX_HASH_SEED_POST_MERGING>(&key);
                        match hashing.map.find_or_find_insert_slot(
                            hash,
                            |v| key.as_ref() == hashing.map_keys.get(v.0),
                            |v| gx_hash::<GX_HASH_SEED_POST_MERGING>(hashing.map_keys.get(v.0)),
                        ) {
                            Ok(found) => unsafe {
                                // safety - access hashbrown raw table
                                let old_acc = &mut found.as_mut().1;
                                self.agg_ctx.partial_merge(old_acc, &mut acc)?;
                            },
                            Err(slot) => unsafe {
                                // safety - access hashbrown raw table
                                let key_addr = hashing.map_keys.add(key.as_ref());
                                hashing.map.insert_in_slot(hash, slot, (key_addr, acc));
                            },
                        }
                    }
                }
                AggSpillCursor::Batches(c) => {
                    while c.cur_bucket_idx == current_bucket_idx {
                        hashing.update_batch::<GX_HASH_SEED_POST_MERGING>(c.next_batch()?)?;
                    }
                }
            }
        }
        flush_bucket!();
        assert!(cursors
            .values()
            .iter()
            .all(|c| c.cur_bucket_idx() == NUM_SPILL_BUCKETS));
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
        let mut in_mem = self.in_mem.lock().await;
        let mut spills = self.spills.lock().await;

        // do not spill anything if triggered partial skipping
        // regardless minRows configuration
        in_mem.check_trigger_partial_skipping();
        if in_mem.mode != InMemMode::PartialSkipped {
            let mut next_in_mem_mode = InMemMode::Merging;
            if in_mem.mode == InMemMode::Hashing {
                // use pre-merging if cardinality is low
                if in_mem.hashing_data.cardinality_ratio() < 0.5 {
                    next_in_mem_mode = InMemMode::Hashing
                }
            }
            spills.push(in_mem.renew(next_in_mem_mode).try_into_spill()?);
            drop(spills);
            drop(in_mem);

            // reset mem trackers of aggs
            self.agg_ctx
                .aggs
                .iter()
                .for_each(|agg| agg.agg.reset_mem_used());
            self.update_mem_used(0).await?;
        }
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
    PartialSkipped,
}

/// Unordered in-mem hash table which can be updated
pub struct InMemTable {
    id: usize,
    agg_ctx: Arc<AggContext>,
    task_ctx: Arc<TaskContext>,
    hashing_data: HashingData,
    merging_data: MergingData,
    mode: InMemMode,
}

impl InMemTable {
    fn new(
        id: usize,
        agg_ctx: Arc<AggContext>,
        task_ctx: Arc<TaskContext>,
        mode: InMemMode,
        spill_metrics: SpillMetrics,
    ) -> Self {
        Self {
            id,
            hashing_data: HashingData::new(agg_ctx.clone(), task_ctx.clone(), &spill_metrics),
            merging_data: MergingData::new(agg_ctx.clone(), task_ctx.clone(), &spill_metrics),
            agg_ctx,
            task_ctx,
            mode,
        }
    }

    fn renew(&mut self, mode: InMemMode) -> Self {
        let agg_ctx = self.agg_ctx.clone();
        let task_ctx = self.task_ctx.clone();
        let spill_metrics = self.hashing_data.spill_metrics.clone();
        let id = self.id + 1;
        std::mem::replace(self, Self::new(id, agg_ctx, task_ctx, mode, spill_metrics))
    }

    pub fn mem_used(&self) -> usize {
        let acc_used = self.num_records() * self.agg_ctx.initial_acc.mem_size()
            + self
                .agg_ctx
                .aggs
                .iter()
                .map(|agg| agg.agg.mem_used())
                .sum::<usize>();
        let hashing_used = self.hashing_data.mem_used();
        let merging_used = self.merging_data.mem_used();
        acc_used + hashing_used + merging_used
    }

    pub fn num_records(&self) -> usize {
        self.hashing_data.num_records() + self.merging_data.num_records()
    }

    fn check_trigger_partial_skipping(&mut self) {
        if self.id == 0 // only works on first table
            && self.agg_ctx.supports_partial_skipping
            && self.mode == InMemMode::Hashing
        {
            let cardinality_ratio = self.hashing_data.cardinality_ratio();
            if cardinality_ratio > self.agg_ctx.partial_skipping_ratio {
                log::warn!(
                    "Agg: cardinality ratio = {cardinality_ratio}, will trigger partial skipping"
                );
                self.mode = InMemMode::PartialSkipped;
            }
        }
    }

    fn try_into_spill(self) -> Result<AggSpill> {
        match self.mode {
            InMemMode::Hashing => Ok(self.hashing_data.try_into_spill()?),
            InMemMode::Merging => Ok(self.merging_data.try_into_spill()?),
            InMemMode::PartialSkipped => {
                unreachable!("in_mem.mode cannot be PartialSkipped")
            }
        }
    }
}

enum AggSpill {
    BucketedRecords(Box<dyn Spill>), // from spilled hashmap
    BucketedBatches(Box<dyn Spill>), // from sorted merging batches
}

enum AggSpillCursor {
    Records(RecordsSpillCursor),
    Batches(BatchesSpillCursor),
}

impl AggSpillCursor {
    fn try_from_spill(spill: &AggSpill, agg_ctx: &Arc<AggContext>) -> Result<Self> {
        Ok(match spill {
            AggSpill::BucketedRecords(s) => {
                Self::Records(RecordsSpillCursor::try_from_spill(&s, &agg_ctx)?)
            }
            AggSpill::BucketedBatches(s) => {
                Self::Batches(BatchesSpillCursor::try_from_spill(&s, &agg_ctx)?)
            }
        })
    }

    fn cur_bucket_idx(&self) -> usize {
        match self {
            AggSpillCursor::Records(s) => s.cur_bucket_idx,
            AggSpillCursor::Batches(s) => s.cur_bucket_idx,
        }
    }
}

impl ComparableForLoserTree for AggSpillCursor {
    fn lt(&self, other: &Self) -> bool {
        let cmp_key = |c: &Self| match c {
            AggSpillCursor::Records(s) => (s.cur_bucket_idx, 0),
            AggSpillCursor::Batches(s) => (s.cur_bucket_idx, 1),
        };
        cmp_key(self) < cmp_key(other)
    }
}

// hasher used in table
const GX_HASH_SEED_HASHING: i64 = 0x3F6F1B9378DD6AAF;
const GX_HASH_SEED_MERGING: i64 = 0x7A9A2D4E8C19B4EB;
const GX_HASH_SEED_POST_MERGING: i64 = 0x1CE19D40EEED6CA2;

#[inline]
pub fn gx_hash<const SEED: i64>(value: impl AsRef<[u8]>) -> u64 {
    let mut h = GxHasher::with_seed(SEED);
    h.write(value.as_ref());
    h.finish()
}

#[inline]
pub fn gx_merging_bucket_id(value: impl AsRef<[u8]>) -> u16 {
    (gx_hash::<GX_HASH_SEED_MERGING>(value) % NUM_SPILL_BUCKETS as u64) as u16
}
pub struct HashingData {
    agg_ctx: Arc<AggContext>,
    task_ctx: Arc<TaskContext>,
    map_keys: BytesArena,
    map: RawTable<(u64, AccumStateRow)>,
    num_input_records: usize,
    spill_metrics: SpillMetrics,
}

impl HashingData {
    fn new(
        agg_ctx: Arc<AggContext>,
        task_ctx: Arc<TaskContext>,
        spill_metrics: &SpillMetrics,
    ) -> Self {
        Self {
            agg_ctx,
            task_ctx,
            map_keys: Default::default(),
            map: Default::default(),
            num_input_records: 0,
            spill_metrics: spill_metrics.clone(),
        }
    }

    fn renew(&mut self) -> Self {
        let agg_ctx = self.agg_ctx.clone();
        let task_ctx = self.task_ctx.clone();
        let spill_metrics = self.spill_metrics.clone();
        std::mem::replace(self, Self::new(agg_ctx, task_ctx, &spill_metrics))
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
        self.map_keys.mem_size() + self.map.capacity() * size_of::<(u64, AccumStateRow, u8)>()
    }

    fn update_batch<const GX_HASH_SEED: i64>(&mut self, batch: RecordBatch) -> Result<()> {
        let num_rows = batch.num_rows();
        self.num_input_records += num_rows;

        let grouping_rows = self.agg_ctx.create_grouping_rows(&batch)?;
        let hashes: Vec<u64> = grouping_rows
            .iter()
            .map(|row| gx_hash::<GX_HASH_SEED>(row))
            .collect();

        // update hashmap
        let mut accs = Vec::with_capacity(num_rows);
        for (hash, row) in hashes.into_iter().zip(&grouping_rows) {
            let found = self
                .map
                .find_or_find_insert_slot(
                    hash,
                    |v| self.map_keys.get(v.0) == row.as_ref(),
                    |v| gx_hash::<GX_HASH_SEED>(self.map_keys.get(v.0)),
                )
                .unwrap_or_else(|slot| {
                    let key_addr = self.map_keys.add(row.as_ref());
                    let value = self.agg_ctx.initial_acc.clone();
                    let entry = (key_addr, value);
                    unsafe {
                        // safety: inserting slot is ensured to be valid
                        self.map.insert_in_slot(hash, slot, entry)
                    }
                });

            // safety: acc lives longer than this function call.
            // items in accs are later moved into ManuallyDrop to avoid double drop.
            accs.push(unsafe { std::ptr::read(&found.as_mut().1 as *const AccumStateRow) });
        }

        // partial update
        let input_arrays = self.agg_ctx.create_input_arrays(&batch)?;
        self.agg_ctx
            .partial_batch_update_input(&mut accs, &input_arrays)?;

        // partial merge
        let acc_array = self.agg_ctx.get_input_acc_array(&batch)?;
        self.agg_ctx
            .partial_batch_merge_input(&mut accs, acc_array)?;

        // manually drop these agg bufs because they still live in the hashmap
        for unsafe_acc in accs {
            let _ = ManuallyDrop::new(unsafe_acc);
        }
        Ok(())
    }

    fn try_into_spill(self) -> Result<AggSpill> {
        // sort all records using radix sort on hashcodes of keys
        let mut bucketed_records = self
            .map
            .into_iter()
            .map(|(key_addr, value)| {
                let key = self.map_keys.get(key_addr);
                let bucket_id = gx_merging_bucket_id(key);
                (bucket_id, key, value)
            })
            .collect::<Vec<_>>();

        let bucket_counts = radix_sort_u16_with_max_key_by(
            &mut bucketed_records,
            NUM_SPILL_BUCKETS as u16 - 1,
            |v| v.0,
        );

        let spill = try_new_spill(&self.spill_metrics)?;
        let mut writer = FrameEncoder::new(spill.get_buf_writer());
        let mut beg = 0;

        for i in 0..NUM_SPILL_BUCKETS {
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
                    value.save(&mut writer, &self.agg_ctx.acc_dyn_savers)?;
                }
                beg += bucket_counts[i];
            }
        }
        write_len(NUM_SPILL_BUCKETS, &mut writer)?; // EOF
        write_len(0, &mut writer)?;
        writer.finish().or_else(|err| df_execution_err!("{err}"))?;
        spill.complete()?;
        Ok(AggSpill::BucketedRecords(spill))
    }
}

pub struct RecordsSpillCursor {
    input: FrameDecoder<BufReader<Box<dyn Read + Send>>>,
    agg_ctx: Arc<AggContext>,
    cur_bucket_idx: usize,
    cur_bucket_count: usize,
    cur_row_idx: usize,
}

impl RecordsSpillCursor {
    fn try_from_spill(spill: &Box<dyn Spill>, agg_ctx: &Arc<AggContext>) -> Result<Self> {
        let mut input = FrameDecoder::new(spill.get_buf_reader());
        Ok(Self {
            agg_ctx: agg_ctx.clone(),
            cur_bucket_idx: read_len(&mut input)?,
            cur_bucket_count: read_len(&mut input)?,
            input,
            cur_row_idx: 0,
        })
    }

    fn next_record(&mut self) -> Result<(SlimBytes, AccumStateRow)> {
        assert!(self.cur_bucket_idx < NUM_SPILL_BUCKETS);

        // read key
        let key_len = read_len(&mut self.input)?;
        let key = read_bytes_slice(&mut self.input, key_len)?.into();

        // read value
        let mut value = self.agg_ctx.initial_acc.clone();
        value.load(&mut self.input, &self.agg_ctx.acc_dyn_loaders)?;

        // forward next row, load next bucket if current bucket is finished
        self.cur_row_idx += 1;
        if self.cur_row_idx == self.cur_bucket_count {
            assert!(self.cur_bucket_idx < NUM_SPILL_BUCKETS);
            self.cur_row_idx = 0;
            self.cur_bucket_idx = read_len(&mut self.input)?;
            self.cur_bucket_count = read_len(&mut self.input)?;
        }
        Ok((key, value))
    }
}

pub struct MergingData {
    agg_ctx: Arc<AggContext>,
    task_ctx: Arc<TaskContext>,
    key_bucket_indices: Vec<Vec<u16>>,
    sorted_batches: Vec<RecordBatch>,
    sorted_batches_mem_used: usize,
    num_rows: usize,
    spill_metrics: SpillMetrics,
}

impl MergingData {
    fn new(
        agg_ctx: Arc<AggContext>,
        task_ctx: Arc<TaskContext>,
        spill_metrics: &SpillMetrics,
    ) -> Self {
        Self {
            agg_ctx,
            task_ctx,
            key_bucket_indices: Default::default(),
            sorted_batches: Default::default(),
            sorted_batches_mem_used: 0,
            num_rows: 0,
            spill_metrics: spill_metrics.clone(),
        }
    }

    #[allow(dead_code)]
    fn renew(&mut self) -> Self {
        let agg_ctx = self.agg_ctx.clone();
        let task_ctx = self.task_ctx.clone();
        let spill_metrics = self.spill_metrics.clone();
        std::mem::replace(self, Self::new(agg_ctx, task_ctx, &spill_metrics))
    }

    fn num_records(&self) -> usize {
        self.num_rows
    }

    fn add_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let grouping_rows = self.agg_ctx.create_grouping_rows(&batch)?;
        let mut sorted: Vec<(u16, u32)> = grouping_rows
            .iter()
            .map(|row| gx_merging_bucket_id(row))
            .zip(0..batch.num_rows() as u32)
            .collect();
        radix_sort_u16_with_max_key_by(&mut sorted, NUM_SPILL_BUCKETS as u16 - 1, |v| v.0);

        let (key_bucket_indices, row_indices): (Vec<u16>, Vec<u32>) = sorted.into_iter().unzip();
        let sorted_batch = BatchTaker(&batch)
            .take(row_indices)
            .expect("error sorting batch");

        self.num_rows += sorted_batch.num_rows();
        self.sorted_batches_mem_used += sorted_batch.get_array_memory_size();
        self.key_bucket_indices.push(key_bucket_indices);
        self.sorted_batches.push(sorted_batch);
        Ok(())
    }

    fn try_into_spill(self) -> Result<AggSpill> {
        let spill = try_new_spill(&self.spill_metrics)?;
        let mut writer = FrameEncoder::new(spill.get_buf_writer());

        let batch_size = self.task_ctx.session_config().batch_size();
        let sub_batch_size = batch_size / batch_size.ilog2() as usize;
        for (bucket_id, batch) in self.into_sorted_batches(sub_batch_size) {
            // write bucket_id + batch
            write_len(bucket_id as usize, &mut writer)?;

            let mut buf = vec![];
            write_one_batch(&batch, &mut Cursor::new(&mut buf), false, None)?;
            writer.write_all(&buf)?;
        }
        write_len(NUM_SPILL_BUCKETS, &mut writer)?; // EOF
        writer.finish().or_else(|err| df_execution_err!("{err}"))?;
        spill.complete()?;
        Ok(AggSpill::BucketedBatches(spill))
    }

    fn mem_used(&self) -> usize {
        self.sorted_batches_mem_used + 2 * self.num_rows
    }

    fn into_sorted_batches(self, batch_size: usize) -> impl Iterator<Item = (u16, RecordBatch)> {
        struct Cursor {
            idx: usize,
            row_idx: usize,
            keys: Vec<u16>,
            cur_bucket_id: u16,
        }

        impl Cursor {
            fn new(idx: usize, keys: Vec<u16>) -> Self {
                let first_bucket_id = keys.get(0).cloned().unwrap_or(NUM_SPILL_BUCKETS as u16);
                Self {
                    idx,
                    row_idx: 0,
                    keys,
                    cur_bucket_id: first_bucket_id,
                }
            }

            fn forward(&mut self) {
                self.row_idx += 1;
                self.cur_bucket_id = self
                    .keys
                    .get(self.row_idx)
                    .cloned()
                    .unwrap_or(NUM_SPILL_BUCKETS as u16);
            }
        }

        impl ComparableForLoserTree for Cursor {
            #[inline(always)]
            fn lt(&self, other: &Self) -> bool {
                self.cur_bucket_id < other.cur_bucket_id
            }
        }

        struct SortedBatchesIterator {
            cursors: LoserTree<Cursor>,
            batches: Vec<RecordBatch>,
            batch_size: usize,
        }

        impl Iterator for SortedBatchesIterator {
            type Item = (u16, RecordBatch);

            fn next(&mut self) -> Option<Self::Item> {
                let bucket_id = self.cursors.peek_mut().cur_bucket_id;
                if bucket_id as usize == NUM_SPILL_BUCKETS {
                    return None;
                }
                let batch_schema = self.batches[0].schema();

                let mut indices = vec![];
                while indices.len() < self.batch_size {
                    let mut min_cursor = self.cursors.peek_mut();
                    if min_cursor.cur_bucket_id != bucket_id {
                        break; // only collects data in current bucket
                    }
                    while min_cursor.cur_bucket_id == bucket_id {
                        indices.push((min_cursor.idx, min_cursor.row_idx));
                        min_cursor.forward();
                    }
                }
                let batch = BatchesInterleaver::new(batch_schema, &self.batches)
                    .interleave(&indices)
                    .expect("error merging sorted batches: interleaving error");
                Some((bucket_id, batch))
            }
        }

        let cursors = LoserTree::new(
            self.key_bucket_indices
                .into_iter()
                .enumerate()
                .map(|(idx, keys)| Cursor::new(idx, keys))
                .collect(),
        );

        Box::new(SortedBatchesIterator {
            cursors,
            batch_size,
            batches: self.sorted_batches,
        })
    }
}

pub struct BatchesSpillCursor {
    input: FrameDecoder<BufReader<Box<dyn Read + Send>>>,
    cur_bucket_idx: usize,
    schema: SchemaRef,
}

impl BatchesSpillCursor {
    fn try_from_spill(spill: &Box<dyn Spill>, agg_ctx: &Arc<AggContext>) -> Result<Self> {
        let mut input = FrameDecoder::new(spill.get_buf_reader());
        let first_bucket_idx = read_len(&mut input)?;
        Ok(Self {
            input,
            cur_bucket_idx: first_bucket_idx,
            schema: agg_ctx.input_schema.clone(),
        })
    }

    fn next_batch(&mut self) -> Result<RecordBatch> {
        assert!(self.cur_bucket_idx < NUM_SPILL_BUCKETS);

        let batch = read_one_batch(&mut self.input, Some(self.schema.clone()), false)?
            .expect("error reading batch");
        self.cur_bucket_idx = read_len(&mut self.input)?; // read next bucket id
        Ok(batch)
    }
}
