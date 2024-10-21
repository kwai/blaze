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
    io::{Cursor, Read, Write},
    sync::{Arc, Weak},
};

use arrow::{
    array::ArrayRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use async_trait::async_trait;
use bytes::Buf;
use datafusion::{
    common::Result,
    execution::context::TaskContext,
    physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet},
};
use datafusion_ext_commons::{
    array_size::ArraySize,
    batch_size, downcast_any,
    ds::rdx_tournament_tree::{KeyForRadixTournamentTree, RadixTournamentTree},
    io::{read_len, write_len},
    rdxsort::radix_sort_unstable_by_key,
    staging_mem_size_for_partial_sort, suggested_output_batch_mem_size,
};
use futures::lock::Mutex;

use crate::{
    agg::{
        acc::{AccStore, AccumStateRow, OwnedAccumStateRow, RefAccumStateRow},
        agg_context::AggContext,
        agg_hash_map::AggHashMap,
    },
    assume,
    common::output::WrappedRecordBatchSender,
    memmgr::{
        metrics::SpillMetrics,
        spill::{try_new_spill, Spill, SpillCompressedReader},
        MemConsumer, MemConsumerInfo, MemManager,
    },
    unchecked,
};

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
    baseline_metrics: BaselineMetrics,
    spill_metrics: SpillMetrics,
}

impl AggTable {
    pub fn new(
        partition_id: usize,
        agg_ctx: Arc<AggContext>,
        context: Arc<TaskContext>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        let baseline_metrics = BaselineMetrics::new(&metrics, partition_id);
        let spill_metrics = SpillMetrics::new(&metrics, partition_id);
        let name = format!("AggTable[partition={}]", partition_id);
        Self {
            mem_consumer_info: None,
            in_mem: Mutex::new(InMemTable::new(
                name.clone(),
                0,
                agg_ctx.clone(),
                context.clone(),
                InMemMode::Hashing,
            )),
            spills: Mutex::default(),
            name,
            agg_ctx,
            baseline_metrics,
            spill_metrics,
        }
    }

    pub async fn process_input_batch(&self, input_batch: RecordBatch) -> Result<()> {
        // update memory usage before processing
        let mem_used = self.in_mem.lock().await.mem_used() + input_batch.get_array_mem_size() * 2;
        self.update_mem_used(mem_used).await?;

        let mem_used = {
            let mut in_mem = self.in_mem.lock().await;

            // compute input arrays
            match in_mem.mode {
                InMemMode::Hashing => {
                    in_mem.hashing_data.update_batch(input_batch)?;
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
            in_mem.mem_used()
        };

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
        sender: Arc<WrappedRecordBatchSender>,
    ) -> Result<()> {
        self.set_spillable(false);

        let batch_num_rows = input_batch.num_rows();
        let old_in_mem = self.renew_in_mem_table(InMemMode::PartialSkipped).await;
        assert_eq!(old_in_mem.num_records(), 0); // old table must be cleared

        let mut acc_store = AccStore::new(self.agg_ctx.initial_acc.clone());
        let mut accs: Vec<RefAccumStateRow> = (0..batch_num_rows)
            .map(|_| acc_store.new_acc())
            .collect::<Vec<_>>()
            .into_iter()
            .map(|idx| acc_store.get(idx))
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
        let grouping_columns = self
            .agg_ctx
            .groupings
            .iter()
            .map(|grouping| grouping.expr.evaluate(&input_batch))
            .map(|r| r.and_then(|columnar| columnar.into_array(batch_num_rows)))
            .collect::<Result<Vec<ArrayRef>>>()?;
        let agg_columns = self
            .agg_ctx
            .build_agg_columns(accs.into_iter().map(|acc| (&[], acc)).collect())?;
        let output_batch = RecordBatch::try_new_with_options(
            self.agg_ctx.output_schema.clone(),
            [grouping_columns, agg_columns].concat(),
            &RecordBatchOptions::new().with_row_count(Some(batch_num_rows)),
        )?;

        self.baseline_metrics.record_output(output_batch.num_rows());
        sender.send(Ok(output_batch)).await;
        return Ok(());
    }

    pub async fn output(&self, sender: Arc<WrappedRecordBatchSender>) -> Result<()> {
        self.set_spillable(false);

        let mut in_mem = self.renew_in_mem_table(InMemMode::PartialSkipped).await;
        let spills = std::mem::take(&mut *self.spills.lock().await);
        let target_batch_mem_size = suggested_output_batch_mem_size();
        let batch_size = batch_size();

        log::info!(
            "{} starts outputting ({} spills)",
            self.name(),
            spills.len()
        );

        // only one in-mem table, directly output it
        if spills.is_empty() {
            assert!(matches!(
                in_mem.mode,
                InMemMode::Hashing | InMemMode::PartialSkipped
            ));
            let mut cur_mem_used = in_mem.mem_used();
            let (_key_store, mut key_addrs) =
                std::mem::take(&mut in_mem.hashing_data.map).into_keys();

            while !key_addrs.is_empty() {
                let mut records = vec![];
                let mut mem_size = 0;

                for i in (0..key_addrs.len()).rev() {
                    if records.len() >= batch_size || mem_size >= target_batch_mem_size {
                        break;
                    }
                    let key = key_addrs[i];
                    let acc = in_mem.hashing_data.acc_store.get(i as u32);
                    mem_size += key.len();
                    mem_size += acc.mem_size();
                    records.push((key, acc));
                }
                key_addrs.truncate(key_addrs.len() - records.len());
                key_addrs.shrink_to_fit();

                let batch = self.agg_ctx.convert_records_to_batch(records)?;
                let batch_mem_size = batch.get_array_mem_size();

                self.baseline_metrics.record_output(batch.num_rows());
                sender.send(Ok(batch)).await;

                // free memory of the output batch
                // this is not precise because the used memory is accounted by records and
                // not freed by batches.
                let estimated_mem_used = cur_mem_used.saturating_sub(batch_mem_size / 2);
                cur_mem_used = estimated_mem_used;
                self.update_mem_used(estimated_mem_used).await?;
            }
            self.update_mem_used(0).await?;
            return Ok(());
        }

        // convert all tables into cursors
        let mut spills = spills;
        let mut cursors = vec![];
        if in_mem.num_records() > 0 {
            let mut spill: Box<dyn Spill> = Box::new(vec![]);
            in_mem.try_into_spill(&mut spill)?; // spill staging records
            let spill_size = downcast_any!(spill, Vec<u8>)?.len();
            self.update_mem_used(spill_size + spills.len() * SPILL_OFFHEAP_MEM_COST)
                .await?;
            spills.push(spill);
        }
        for spill in &mut spills {
            cursors.push(RecordsSpillCursor::try_from_spill(spill, &self.agg_ctx)?);
        }
        let mut map = AggHashMap::default();
        let mut acc_store = AccStore::new(self.agg_ctx.initial_acc.clone());
        let mut key = vec![];

        // create a radix tournament tree to do the merging
        // the mem-table and at least one spill should be in the tree
        let mut cursors: RadixTournamentTree<RecordsSpillCursor> =
            RadixTournamentTree::new(cursors, NUM_SPILL_BUCKETS);
        assert!(cursors.len() > 0);

        while cursors.peek().cur_bucket_idx < NUM_SPILL_BUCKETS {
            let current_bucket_idx = cursors.peek().cur_bucket_idx;

            loop {
                let mut min_cursor = cursors.peek_mut();
                if min_cursor.cur_bucket_idx > current_bucket_idx {
                    break;
                }

                // merge records of current bucket
                while min_cursor.cur_bucket_idx == current_bucket_idx {
                    let mut acc = min_cursor.next_record(&mut key)?;
                    map.upsert_one_record(&key, |existed, record_idx| {
                        if existed {
                            let mut old_acc = acc_store.get(record_idx);
                            self.agg_ctx.partial_merge(&mut old_acc, &mut acc.as_mut())
                        } else {
                            acc_store.new_acc_from(&acc);
                            Ok(())
                        }
                    })?;
                }
            }

            let (_key_store, key_addrs) = map.take_keys();
            let batch = self.agg_ctx.convert_records_to_batch(
                key_addrs
                    .into_iter()
                    .enumerate()
                    .map(|(record_idx, key)| (key, acc_store.get(record_idx as u32)))
                    .collect(),
            )?;
            acc_store.clear();

            self.baseline_metrics.record_output(batch.num_rows());
            sender.send(Ok(batch)).await;
        }

        assert!(cursors
            .values()
            .iter()
            .all(|c| c.cur_bucket_idx == NUM_SPILL_BUCKETS));
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
            let mut spill = try_new_spill(&self.spill_metrics)?;
            in_mem.renew(next_in_mem_mode).try_into_spill(&mut spill)?;
            spills.push(spill);
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
    name: String,
    id: usize,
    agg_ctx: Arc<AggContext>,
    task_ctx: Arc<TaskContext>,
    hashing_data: HashingData,
    merging_data: MergingData,
    mode: InMemMode,
}

impl InMemTable {
    fn new(
        name: String,
        id: usize,
        agg_ctx: Arc<AggContext>,
        task_ctx: Arc<TaskContext>,
        mode: InMemMode,
    ) -> Self {
        Self {
            name,
            id,
            hashing_data: HashingData::new(agg_ctx.clone()),
            merging_data: MergingData::new(agg_ctx.clone()),
            agg_ctx,
            task_ctx,
            mode,
        }
    }

    fn renew(&mut self, mode: InMemMode) -> Self {
        self.id += 1;
        let name = self.name.clone();
        let agg_ctx = self.agg_ctx.clone();
        let task_ctx = self.task_ctx.clone();
        let id = self.id + 1;
        std::mem::replace(self, Self::new(name, id, agg_ctx, task_ctx, mode))
    }

    pub fn mem_used(&self) -> usize {
        let hashing_used = self.hashing_data.mem_used();
        let merging_used = self.merging_data.mem_used();
        let acc_dyn_mem_used = self.agg_ctx.acc_dyn_mem_used();
        hashing_used + merging_used + acc_dyn_mem_used
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
                    "{} cardinality ratio = {cardinality_ratio}, will trigger partial skipping",
                    self.name,
                );
                self.mode = InMemMode::PartialSkipped;
            }
        }
    }

    fn try_into_spill(self, spill: &mut Box<dyn Spill>) -> Result<()> {
        match self.mode {
            InMemMode::Hashing => self.hashing_data.try_into_spill(spill),
            InMemMode::Merging => self.merging_data.try_into_spill(spill),
            InMemMode::PartialSkipped => {
                unreachable!("in_mem.mode cannot be PartialSkipped")
            }
        }
    }
}

pub struct HashingData {
    agg_ctx: Arc<AggContext>,
    acc_store: AccStore,
    map: AggHashMap,
    num_input_records: usize,
}

impl HashingData {
    fn new(agg_ctx: Arc<AggContext>) -> Self {
        Self {
            acc_store: AccStore::new(agg_ctx.initial_acc.clone()),
            map: AggHashMap::default(),
            num_input_records: 0,
            agg_ctx,
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
        self.map.mem_size() + self.acc_store.mem_size()
    }

    fn update_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let num_rows = batch.num_rows();
        self.num_input_records += num_rows;

        let grouping_rows = self.agg_ctx.create_grouping_rows(&batch)?;
        let mut record_indices = vec![];

        self.map.upsert_records(
            grouping_rows.num_rows(),
            |i| {
                assume!(i < grouping_rows.num_rows());
                grouping_rows.row(i)
            },
            |_, existed, record_idx| {
                if !existed {
                    self.acc_store.new_acc();
                }
                record_indices.push(record_idx);
                Ok(())
            },
        )?;

        let mut accs = record_indices
            .into_iter()
            .map(|record_idx| self.acc_store.get(record_idx))
            .collect::<Vec<_>>();

        // partial update
        let input_arrays = self.agg_ctx.create_input_arrays(&batch)?;
        self.agg_ctx
            .partial_batch_update_input(&mut accs, &input_arrays)?;

        // partial merge
        let acc_array = self.agg_ctx.get_input_acc_array(&batch)?;
        self.agg_ctx
            .partial_batch_merge_input(&mut accs, acc_array)?;
        Ok(())
    }

    fn try_into_spill(self, spill: &mut Box<dyn Spill>) -> Result<()> {
        // sort all records using radix sort on hashcodes of keys
        let (_key_store, key_addrs) = self.map.into_keys();
        let mut bucketed_records = key_addrs
            .iter()
            .enumerate()
            .map(|(record_idx, key)| (record_idx as u32, bucket_id(key)))
            .collect::<Vec<_>>();
        radix_sort_unstable_by_key(&mut bucketed_records, |v| v.1);

        let mut writer = spill.get_compressed_writer();
        let mut beg = 0;

        for i in 0..NUM_SPILL_BUCKETS {
            let bucket_count = bucketed_records[beg..]
                .iter()
                .take_while(|(_, bucket_id)| *bucket_id == i as u16)
                .count();

            if bucket_count > 0 {
                // write bucket id and number of records in this bucket
                write_len(i, &mut writer)?;
                write_len(bucket_count, &mut writer)?;

                // write records in this bucket
                for &(record_idx, _) in &bucketed_records[beg..][..bucket_count] {
                    // write key
                    let key = unchecked!(&key_addrs)[record_idx as usize].as_ref();
                    write_len(key.len(), &mut writer)?;
                    writer.write_all(key)?;

                    // write value
                    let mut acc = self.acc_store.get(record_idx);
                    acc.save(&mut writer, &self.agg_ctx.acc_dyn_savers)?;
                }
                beg += bucket_count;
            }
        }
        write_len(NUM_SPILL_BUCKETS, &mut writer)?; // EOF
        write_len(0, &mut writer)?;
        Ok(())
    }
}

pub struct MergingData {
    agg_ctx: Arc<AggContext>,
    staging_acc_store: AccStore,
    staging_batches: Vec<RecordBatch>,
    raw_records: Vec<Box<[u8]>>,
    bucket_counts: Vec<usize>,
    num_rows: usize,
    staging_mem_used: usize,
    sorted_mem_used: usize,
}

impl MergingData {
    fn new(agg_ctx: Arc<AggContext>) -> Self {
        Self {
            staging_acc_store: AccStore::new(agg_ctx.initial_acc.clone()),
            staging_batches: vec![],
            raw_records: vec![],
            bucket_counts: vec![0; NUM_SPILL_BUCKETS],
            num_rows: 0,
            staging_mem_used: 0,
            sorted_mem_used: 0,
            agg_ctx,
        }
    }

    fn num_records(&self) -> usize {
        self.num_rows
    }

    fn mem_used(&self) -> usize {
        self.staging_mem_used + self.staging_acc_store.mem_size() + self.sorted_mem_used
    }

    fn add_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.num_rows += batch.num_rows();
        self.staging_mem_used += batch.get_array_mem_size();
        self.staging_batches.push(batch);
        if self.staging_mem_used >= staging_mem_size_for_partial_sort() {
            self.flush_staging_batches()?;
        }
        Ok(())
    }

    fn flush_staging_batches(&mut self) -> Result<()> {
        let staging_batches = std::mem::take(&mut self.staging_batches);
        self.staging_mem_used = 0;

        let grouping_rows = staging_batches
            .iter()
            .map(|batch| self.agg_ctx.create_grouping_rows(batch))
            .collect::<Result<Vec<_>>>()?;

        let acc_addrs = staging_batches
            .iter()
            .map(|batch| {
                let acc_addrs = (0..batch.num_rows())
                    .map(|_| self.staging_acc_store.new_acc())
                    .collect::<Vec<_>>();
                let mut accs = acc_addrs
                    .iter()
                    .map(|&acc_addr| self.staging_acc_store.get(acc_addr))
                    .collect::<Vec<_>>();

                // partial update
                let input_arrays = self.agg_ctx.create_input_arrays(&batch)?;
                self.agg_ctx
                    .partial_batch_update_input(&mut accs, &input_arrays)?;

                // partial merge
                let acc_array = self.agg_ctx.get_input_acc_array(&batch)?;
                self.agg_ctx
                    .partial_batch_merge_input(&mut accs, acc_array)?;
                Ok(acc_addrs)
            })
            .collect::<Result<Vec<_>>>()?;

        // sort records
        let mut sorted = grouping_rows
            .iter()
            .enumerate()
            .flat_map(|(batch_idx, rows)| {
                rows.iter().enumerate().map(move |(row_idx, row)| {
                    let bucket_id = bucket_id(&row);
                    (batch_idx as u32, row_idx as u32, bucket_id)
                })
            })
            .collect::<Vec<_>>();
        radix_sort_unstable_by_key(&mut sorted, |v| v.2);

        // store serialized records
        // let acc_store = acc_store.lock();
        let mut raw_records = vec![];
        let mut temp_raw_record = vec![];
        for (batch_idx, row_idx, bucket_id) in sorted {
            self.bucket_counts[bucket_id as usize] += 1;

            let batch_idx = batch_idx as usize;
            let row_idx = row_idx as usize;
            let grouping_row = grouping_rows[batch_idx].row(row_idx);
            let key = grouping_row.as_ref();
            let mut acc = self.staging_acc_store.get(acc_addrs[batch_idx][row_idx]);

            // serialize this record to temp_raw_record
            write_len(key.len(), &mut temp_raw_record)?;
            temp_raw_record.write_all(key)?;
            acc.save(&mut temp_raw_record, &self.agg_ctx.acc_dyn_savers)?;

            // write to raw_records
            write_len(bucket_id as usize, &mut raw_records)?;
            write_len(temp_raw_record.len(), &mut raw_records)?;
            raw_records.write_all(&temp_raw_record)?;
            temp_raw_record.clear();
        }
        write_len(NUM_SPILL_BUCKETS, &mut raw_records)?; // EOF

        self.sorted_mem_used += raw_records.len();
        self.raw_records.push(raw_records.into());

        // under merging mode, there are no inflight accumulators, so reset all
        // agg.mem_used
        for agg in &self.agg_ctx.aggs {
            agg.agg.reset_mem_used();
        }

        // clear acc store, this will retain allocated memory and can be reused later
        self.staging_acc_store.clear();
        Ok(())
    }

    fn try_into_spill(mut self, spill: &mut Box<dyn Spill>) -> Result<()> {
        if !self.staging_batches.is_empty() {
            self.flush_staging_batches()?;
        }
        self.staging_acc_store.clear_and_free();

        struct RawRecordsCursor {
            cur_bucket_id: usize,
            raw: Cursor<Box<[u8]>>,
        }

        impl KeyForRadixTournamentTree for RawRecordsCursor {
            fn rdx(&self) -> usize {
                self.cur_bucket_id
            }
        }

        let mut cursors = RadixTournamentTree::new(
            self.raw_records
                .into_iter()
                .map(|raw_records| {
                    let mut cursor = Cursor::new(raw_records);
                    Ok(RawRecordsCursor {
                        cur_bucket_id: read_len(&mut cursor)?,
                        raw: cursor,
                    })
                })
                .collect::<Result<_>>()?,
            NUM_SPILL_BUCKETS,
        );

        let mut writer = spill.get_compressed_writer();
        for bucket_id in 0..NUM_SPILL_BUCKETS {
            let bucket_count = self.bucket_counts[bucket_id];
            if bucket_count == 0 {
                continue;
            }
            write_len(bucket_id, &mut writer)?;
            write_len(bucket_count, &mut writer)?;

            for _ in 0..bucket_count {
                let mut min_cursor = cursors.peek_mut();

                // write this record and forward cursor
                while min_cursor.cur_bucket_id == bucket_id {
                    let len = read_len(&mut min_cursor.raw)?;
                    let start = min_cursor.raw.position() as usize;
                    writer.write_all(&min_cursor.raw.get_ref()[start..][..len])?;
                    min_cursor.raw.advance(len);
                    min_cursor.cur_bucket_id = read_len(&mut min_cursor.raw)?;
                }
            }
        }
        write_len(NUM_SPILL_BUCKETS, &mut writer)?; // EOF
        write_len(0, &mut writer)?;
        Ok(())
    }
}

pub struct RecordsSpillCursor<'a> {
    input: SpillCompressedReader<'a>,
    agg_ctx: Arc<AggContext>,
    cur_bucket_idx: usize,
    cur_bucket_count: usize,
    cur_row_idx: usize,
}

impl<'a> RecordsSpillCursor<'a> {
    fn try_from_spill(spill: &'a mut Box<dyn Spill>, agg_ctx: &Arc<AggContext>) -> Result<Self> {
        let mut input = spill.get_compressed_reader();
        Ok(Self {
            agg_ctx: agg_ctx.clone(),
            cur_bucket_idx: read_len(&mut input)?,
            cur_bucket_count: read_len(&mut input)?,
            input,
            cur_row_idx: 0,
        })
    }

    fn next_record(&mut self, key_buf: &mut Vec<u8>) -> Result<OwnedAccumStateRow> {
        assert!(self.cur_bucket_idx < NUM_SPILL_BUCKETS);

        // read key
        let key_len = read_len(&mut self.input)?;
        key_buf.resize(key_len, 0);
        self.input.read_exact(key_buf)?;

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
        Ok(value)
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
