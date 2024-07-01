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

//! Defines the External shuffle repartition plan

use std::{
    any::Any,
    collections::HashSet,
    fmt::Formatter,
    io::{Cursor, Read, Write},
    marker::PhantomData,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc, Weak,
    },
};

use arrow::{
    array::ArrayRef,
    datatypes::{Schema, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
    row::{Row, RowConverter, RowParser, Rows, SortField},
};
use async_trait::async_trait;
use datafusion::{
    common::{Result, Statistics},
    execution::context::TaskContext,
    physical_expr::{expressions::Column, PhysicalSortExpr},
    physical_plan::{
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    },
};
use datafusion_ext_commons::{
    array_size::ArraySize,
    compute_suggested_batch_size_for_kway_merge, compute_suggested_batch_size_for_output,
    downcast_any,
    ds::loser_tree::{ComparableForLoserTree, LoserTree},
    io::{read_len, read_one_batch, write_len, write_one_batch},
    streams::coalesce_stream::CoalesceInput,
};
use futures::{lock::Mutex, stream::once, StreamExt, TryStreamExt};
use itertools::Itertools;
use once_cell::sync::OnceCell;
use parking_lot::Mutex as SyncMutex;

use crate::{
    common::{
        batch_selection::{interleave_batches, take_batch},
        batch_statisitcs::{stat_input, InputBatchStatistics},
        column_pruning::ExecuteWithColumnPruning,
        output::{TaskOutputter, WrappedRecordBatchSender},
    },
    memmgr::{
        metrics::SpillMetrics,
        spill::{try_new_spill, Spill, SpillCompressedReader},
        MemConsumer, MemConsumerInfo, MemManager,
    },
};

// reserve memory for each spill
// estimated size: bufread=64KB + lz4dec.src=64KB + lz4dec.dest=64KB
const SPILL_OFFHEAP_MEM_COST: usize = 200000;
const SPILL_MERGING_SIZE: usize = 32;

#[derive(Debug)]
pub struct SortExec {
    input: Arc<dyn ExecutionPlan>,
    exprs: Vec<PhysicalSortExpr>,
    fetch: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
}

impl SortExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        exprs: Vec<PhysicalSortExpr>,
        fetch: Option<usize>,
    ) -> Self {
        let metrics = ExecutionPlanMetricsSet::new();
        Self {
            input,
            exprs,
            fetch,
            metrics,
        }
    }
}

impl DisplayAs for SortExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let exprs = self
            .exprs
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "SortExec: {}", exprs)
    }
}

impl ExecutionPlan for SortExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        Some(&self.exprs)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: children[0].clone(),
            exprs: self.exprs.clone(),
            fetch: self.fetch,
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let projection: Vec<usize> = (0..self.schema().fields().len()).collect();
        self.execute_projected(partition, context, &projection)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        todo!()
    }
}

struct LevelSpill {
    spill: Box<dyn Spill>,
    level: usize,
}

struct ExternalSorter {
    name: String,
    mem_consumer_info: Option<Weak<MemConsumerInfo>>,
    prune_sort_keys_from_batch: Arc<PruneSortKeysFromBatch>,
    limit: usize,
    data: Arc<Mutex<BufferedData>>,
    spills: Mutex<Vec<LevelSpill>>,
    baseline_metrics: BaselineMetrics,
    spill_metrics: SpillMetrics,
    num_total_rows: AtomicUsize,
    mem_total_size: AtomicUsize,
}

#[async_trait]
impl MemConsumer for ExternalSorter {
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
        let mut spill = try_new_spill(&self.spill_metrics)?;
        let data = std::mem::take(&mut *self.data.lock().await);
        let sub_batch_size = compute_suggested_batch_size_for_kway_merge(
            self.mem_total_size(),
            self.num_total_rows(),
        );
        data.try_into_spill(self, &mut spill, sub_batch_size)?;

        self.spills
            .lock()
            .await
            .push(LevelSpill { spill, level: 0 });
        self.update_mem_used(0).await?;

        // merge if there are too many spills
        let mut spills = self.spills.lock().await;
        let mut levels = (0..32).map(|_| vec![]).collect::<Vec<_>>();
        for spill in std::mem::take(&mut *spills) {
            levels[spill.level].push(spill.spill);
        }
        for level in 0..levels.len() {
            if levels[level].len() >= SPILL_MERGING_SIZE {
                let merged = merge_spills(
                    std::mem::take(&mut levels[level]),
                    &self.spill_metrics,
                    sub_batch_size,
                    self.limit,
                    self.prune_sort_keys_from_batch.pruned_schema.clone(),
                )?;
                levels[level + 1].push(merged);
            } else {
                spills.extend(
                    std::mem::take(&mut levels[level])
                        .into_iter()
                        .map(|spill| LevelSpill { spill, level }),
                )
            }
        }
        Ok(())
    }
}

#[derive(Default)]
struct BufferedData {
    sorted_key_stores: Vec<Box<[u8]>>,
    sorted_key_stores_mem_used: usize,
    sorted_batches: Vec<RecordBatch>,
    sorted_batches_mem_used: usize,
    num_rows: usize,
}

impl BufferedData {
    fn try_into_spill(
        self,
        sorter: &ExternalSorter,
        spill: &mut Box<dyn Spill>,
        sub_batch_size: usize,
    ) -> Result<()> {
        let mut writer = spill.get_compressed_writer();
        for (key_collector, batch) in
            self.into_sorted_batches::<SqueezeKeyCollector>(sub_batch_size, sorter)?
        {
            let mut buf = vec![];
            write_one_batch(&batch, &mut Cursor::new(&mut buf))?;
            writer.write_all(&buf)?;
            writer.write_all(&key_collector.store)?;
        }
        Ok(())
    }

    fn mem_used(&self) -> usize {
        self.sorted_batches_mem_used + self.sorted_key_stores_mem_used
    }

    fn add_batch(&mut self, batch: RecordBatch, sorter: &ExternalSorter) -> Result<()> {
        self.num_rows += batch.num_rows();
        let (key_rows, batch) = sorter.prune_sort_keys_from_batch.prune(batch)?;

        // sort the batch and append to sorter
        let mut sorted_key_store = Vec::with_capacity(key_rows.size());
        let mut key_writer = SortedKeysWriter::default();
        let mut num_rows = 0;
        let sorted_batch;

        if !sorter.prune_sort_keys_from_batch.is_all_pruned() {
            let cur_sorted_indices = key_rows
                .iter()
                .enumerate()
                .map(|(row_idx, key)| {
                    let key = unsafe {
                        // safety: keys have the same lifetime with key_rows
                        std::mem::transmute::<_, &'static [u8]>(key.as_ref())
                    };
                    (key, row_idx as u32)
                })
                .sorted_unstable_by_key(|&(key, ..)| key)
                .take(sorter.limit)
                .map(|(key, row_idx)| {
                    num_rows += 1;
                    key_writer.write_key(key, &mut sorted_key_store).unwrap();
                    row_idx as usize
                })
                .collect::<Vec<_>>();
            sorted_batch = take_batch(batch, cur_sorted_indices)?;
        } else {
            key_rows
                .iter()
                .map(|key| unsafe {
                    // safety: keys have the same lifetime with key_rows
                    std::mem::transmute::<_, &'static [u8]>(key.as_ref())
                })
                .sorted_unstable()
                .take(sorter.limit)
                .for_each(|key| {
                    num_rows += 1;
                    key_writer.write_key(key, &mut sorted_key_store).unwrap();
                });
            sorted_batch = create_zero_column_batch(num_rows);
        }
        self.sorted_batches_mem_used += sorted_batch.get_array_mem_size();
        self.sorted_key_stores_mem_used += sorted_key_store.len();

        self.sorted_key_stores.push(sorted_key_store.into());
        self.sorted_batches.push(sorted_batch);
        Ok(())
    }

    fn into_sorted_batches<'a, KC: KeyCollector>(
        self,
        batch_size: usize,
        sorter: &ExternalSorter,
    ) -> Result<impl Iterator<Item = (KC, RecordBatch)>> {
        struct Cursor {
            idx: usize,
            row_idx: usize,
            num_rows: usize,
            sorted_key_store_cursor: std::io::Cursor<Box<[u8]>>,
            key_reader: SortedKeysReader,
        }

        impl Cursor {
            fn new(idx: usize, num_rows: usize, sorted_key_store: Box<[u8]>) -> Self {
                let mut sorted_key_store_cursor = std::io::Cursor::new(sorted_key_store);
                let mut key_reader = SortedKeysReader::default();
                if num_rows > 0 {
                    key_reader
                        .next_key(&mut sorted_key_store_cursor)
                        .expect("error reading first sorted key");
                }

                Self {
                    row_idx: 0,
                    idx,
                    num_rows,
                    sorted_key_store_cursor,
                    key_reader,
                }
            }

            fn finished(&self) -> bool {
                self.row_idx >= self.num_rows
            }

            fn cur_key(&self) -> &[u8] {
                &self.key_reader.cur_key
            }

            fn forward(&mut self) {
                self.row_idx += 1;
                if !self.finished() {
                    self.key_reader
                        .next_key(&mut self.sorted_key_store_cursor)
                        .expect("error reading next sorted key");
                }
            }

            fn is_equal_to_prev_key(&self) -> bool {
                self.key_reader.is_equal_to_prev
            }
        }

        impl ComparableForLoserTree for Cursor {
            #[inline(always)]
            fn lt(&self, other: &Self) -> bool {
                if self.finished() {
                    return false;
                }
                if other.finished() {
                    return true;
                }
                self.cur_key() < other.cur_key()
            }
        }

        struct SortedBatchesIterator<KC: KeyCollector> {
            cursors: LoserTree<Cursor>,
            batches: Vec<RecordBatch>,
            batch_size: usize,
            num_output_rows: usize,
            limit: usize,
            _phantom: PhantomData<KC>,
        }

        impl<KC: KeyCollector> Iterator for SortedBatchesIterator<KC> {
            type Item = (KC, RecordBatch);

            fn next(&mut self) -> Option<Self::Item> {
                if self.num_output_rows >= self.limit {
                    return None;
                }
                let cur_batch_size = self.batch_size.min(self.limit - self.num_output_rows);
                let batch_schema = self.batches[0].schema();
                let is_all_pruned = self.batches[0].num_columns() == 0;
                let mut indices = Vec::with_capacity(cur_batch_size);
                let mut key_collector = KC::default();
                let mut min_cursor = self.cursors.peek_mut();

                for _ in 0..cur_batch_size {
                    assert!(!min_cursor.finished());

                    key_collector.add_key(min_cursor.cur_key());
                    indices.push((min_cursor.idx, min_cursor.row_idx));
                    min_cursor.forward();

                    // fetch next min key from loser tree only if it is different from previous key
                    if min_cursor.finished() || !min_cursor.is_equal_to_prev_key() {
                        min_cursor.adjust();
                    }
                }
                let batch = if !is_all_pruned {
                    interleave_batches(batch_schema, &self.batches, &indices)
                        .expect("error merging sorted batches: interleaving error")
                } else {
                    create_zero_column_batch(cur_batch_size)
                };
                self.num_output_rows += cur_batch_size;
                Some((key_collector, batch))
            }
        }

        let cursors = LoserTree::new(
            self.sorted_key_stores
                .into_iter()
                .zip(&self.sorted_batches)
                .enumerate()
                .map(|(idx, (key_store, batch))| Cursor::new(idx, batch.num_rows(), key_store))
                .collect(),
        );

        Ok(Box::new(SortedBatchesIterator {
            cursors,
            batch_size,
            batches: self.sorted_batches,
            limit: sorter.limit.min(self.num_rows),
            num_output_rows: 0,
            _phantom: PhantomData,
        }))
    }
}

impl ExecuteWithColumnPruning for SortExec {
    fn execute_projected(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
        projection: &[usize],
    ) -> Result<SendableRecordBatchStream> {
        let input_schema = self.input.schema();

        let prune_sort_keys_from_batch = Arc::new(PruneSortKeysFromBatch::try_new(
            input_schema,
            projection,
            &self.exprs,
        )?);

        let external_sorter = Arc::new(ExternalSorter {
            name: format!("ExternalSorter[partition={}]", partition),
            mem_consumer_info: None,
            prune_sort_keys_from_batch,
            limit: self.fetch.unwrap_or(usize::MAX),
            data: Default::default(),
            spills: Default::default(),
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
            spill_metrics: SpillMetrics::new(&self.metrics, partition),
            num_total_rows: Default::default(),
            mem_total_size: Default::default(),
        });
        MemManager::register_consumer(external_sorter.clone(), true);

        let input = stat_input(
            InputBatchStatistics::from_metrics_set_and_blaze_conf(&self.metrics, partition)?,
            self.input.execute(partition, context.clone())?,
        )?;
        let coalesced = context.coalesce_with_default_batch_size(
            input,
            &BaselineMetrics::new(&self.metrics, partition),
        )?;

        let output = Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            once(external_sort(
                coalesced,
                partition,
                context.clone(),
                external_sorter,
                self.metrics.clone(),
            ))
            .try_flatten(),
        ));
        let coalesced = context.coalesce_with_default_batch_size(
            output,
            &BaselineMetrics::new(&self.metrics, partition),
        )?;
        Ok(coalesced)
    }
}

impl Drop for ExternalSorter {
    fn drop(&mut self) {
        MemManager::deregister_consumer(self);
    }
}

async fn external_sort(
    mut input: SendableRecordBatchStream,
    partition_id: usize,
    context: Arc<TaskContext>,
    sorter: Arc<ExternalSorter>,
    metrics: ExecutionPlanMetricsSet,
) -> Result<SendableRecordBatchStream> {
    // insert and sort
    while let Some(batch) = input.next().await.transpose()? {
        sorter
            .insert_batch(batch)
            .await
            .map_err(|err| err.context("sort: executing insert_batch() error"))?;
    }
    let has_spill = sorter.spills.lock().await.is_empty();
    let sorter_cloned = sorter.clone();

    let output = context.output_with_sender("Sort", input.schema(), |sender| async move {
        sorter.output(sender).await?;
        Ok(())
    })?;

    // if running in-memory, buffer output when memory usage is high
    if !has_spill {
        return context.output_bufferable_with_spill(partition_id, sorter_cloned, output, metrics);
    }
    Ok(output)
}

impl ExternalSorter {
    async fn insert_batch(self: &Arc<Self>, batch: RecordBatch) -> Result<()> {
        let _timer = self.baseline_metrics.elapsed_compute().timer();
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.num_total_rows.fetch_add(batch.num_rows(), SeqCst);
        self.mem_total_size
            .fetch_add(batch.get_array_mem_size(), SeqCst);

        // update memory usage before adding to data
        let mem_used = self.data.lock().await.mem_used() + batch.get_array_mem_size() * 2;
        self.update_mem_used(mem_used).await?;

        // add batch to data
        let mem_used = {
            let mut data = self.data.lock().await;
            data.add_batch(batch, self)?;
            data.mem_used()
        };
        self.update_mem_used(mem_used).await?;
        Ok(())
    }

    async fn output(self: Arc<Self>, sender: Arc<WrappedRecordBatchSender>) -> Result<()> {
        let mut timer = self.baseline_metrics.elapsed_compute().timer();
        self.set_spillable(false);

        let data = std::mem::take(&mut *self.data.lock().await);
        let spills = std::mem::take(&mut *self.spills.lock().await);
        log::info!(
            "{} starts outputting ({} spills)",
            self.name(),
            spills.len()
        );

        // no spills -- output in-mem batches
        if spills.is_empty() {
            if data.num_rows == 0 {
                // no data
                return Ok(());
            }
            let sub_batch_size = compute_suggested_batch_size_for_output(
                self.mem_total_size(),
                self.num_total_rows(),
            );

            for (key_store, pruned_batch) in
                data.into_sorted_batches::<SimpleKeyCollector>(sub_batch_size, &self)?
            {
                let batch = self
                    .prune_sort_keys_from_batch
                    .restore(pruned_batch, key_store)?;
                self.baseline_metrics.record_output(batch.num_rows());
                sender.send(Ok(batch), Some(&mut timer)).await;
            }
            self.update_mem_used(0).await?;
            return Ok(());
        }

        // move in-mem batches into spill, so we can free memory as soon as possible
        let mut spills: Vec<Box<dyn Spill>> = spills.into_iter().map(|spill| spill.spill).collect();
        let mut spill: Box<dyn Spill> = Box::new(vec![]);
        let sub_batch_size = compute_suggested_batch_size_for_kway_merge(
            self.mem_total_size(),
            self.num_total_rows(),
        );

        data.try_into_spill(&self, &mut spill, sub_batch_size)?;
        let spill_size = downcast_any!(spill, Vec<u8>)?.len();
        self.update_mem_used(spill_size + spills.len() * SPILL_OFFHEAP_MEM_COST)
            .await?;
        spills.push(spill);

        let mut merger = ExternalMerger::<SimpleKeyCollector>::try_new(
            &mut spills,
            self.prune_sort_keys_from_batch.pruned_schema(),
            sub_batch_size,
            self.limit,
        )?;
        while let Some((key_collector, pruned_batch)) = merger.next().transpose()? {
            let batch = self
                .prune_sort_keys_from_batch
                .restore(pruned_batch, key_collector)?;
            let cursors_mem_used = merger.cursors_mem_used();

            self.update_mem_used(cursors_mem_used).await?;
            self.baseline_metrics.record_output(batch.num_rows());
            sender.send(Ok(batch), Some(&mut timer)).await;
        }
        self.update_mem_used(0).await?;
        Ok(())
    }

    fn num_total_rows(&self) -> usize {
        self.num_total_rows.load(SeqCst)
    }

    fn mem_total_size(&self) -> usize {
        self.mem_total_size.load(SeqCst)
    }
}

struct SpillCursor<'a> {
    id: usize,
    pruned_schema: SchemaRef,
    input: SpillCompressedReader<'a>,
    cur_batch_num_rows: usize,
    cur_loaded_num_rows: usize,
    cur_batches: Vec<RecordBatch>,
    cur_key_reader: SortedKeysReader,
    cur_key_row_idx: usize,
    cur_batch_idx: usize,
    cur_row_idx: usize,
    cur_mem_used: usize,
    finished: bool,
}

impl<'a> ComparableForLoserTree for SpillCursor<'a> {
    #[inline(always)]
    fn lt(&self, other: &Self) -> bool {
        if self.finished {
            return false;
        }
        if other.finished {
            return true;
        }
        self.cur_key() < other.cur_key()
    }
}

impl<'a> SpillCursor<'a> {
    fn try_from_spill(
        id: usize,
        pruned_schema: SchemaRef,
        spill: &'a mut Box<dyn Spill>,
    ) -> Result<Self> {
        let mut iter = SpillCursor {
            id,
            pruned_schema,
            input: spill.get_compressed_reader(),
            cur_batch_num_rows: 0,
            cur_loaded_num_rows: 0,
            cur_batches: vec![],
            cur_key_reader: SortedKeysReader::default(),
            cur_key_row_idx: 0,
            cur_batch_idx: 0,
            cur_row_idx: 0,
            cur_mem_used: 0,
            finished: false,
        };
        iter.next_key()?; // load first record
        Ok(iter)
    }

    fn cur_key(&self) -> &[u8] {
        &self.cur_key_reader.cur_key
    }

    // forwards to next key and returns current key
    fn next_key(&mut self) -> Result<()> {
        assert!(
            !self.finished,
            "calling next_key() on finished sort spill cursor"
        );

        if self.cur_key_row_idx >= self.cur_batches.last().map(|b| b.num_rows()).unwrap_or(0) {
            if !self.load_next_batch()? {
                self.finished = true;
                return Ok(());
            }
        }
        self.cur_key_reader
            .next_key(&mut self.input)
            .expect("error reading next key");
        self.cur_key_row_idx += 1;
        Ok(())
    }

    fn is_equal_to_prev_key(&self) -> bool {
        self.cur_key_reader.is_equal_to_prev
    }

    fn load_next_batch(&mut self) -> Result<bool> {
        if let Some(batch) = read_one_batch(&mut self.input, &self.pruned_schema)? {
            self.cur_mem_used += batch.get_array_mem_size();
            self.cur_batch_num_rows = batch.num_rows();
            self.cur_loaded_num_rows = 0;
            self.cur_batches.push(batch);
            self.cur_key_reader = SortedKeysReader::default();
            self.cur_key_row_idx = 0;
            return Ok(true);
        }
        self.finished = true;
        Ok(false)
    }

    fn next_row(&mut self) -> (usize, usize) {
        let batch_idx = self.cur_batch_idx;
        let row_idx = self.cur_row_idx;

        self.cur_row_idx += 1;
        if self.cur_row_idx >= self.cur_batches[self.cur_batch_idx].num_rows() {
            self.cur_batch_idx += 1;
            self.cur_row_idx = 0;
        }
        (batch_idx, row_idx)
    }

    fn clear_finished_batches(&mut self) {
        if self.cur_batch_idx > 0 {
            for batch in self.cur_batches.drain(..self.cur_batch_idx) {
                self.cur_mem_used -= batch.get_array_mem_size();
            }
            self.cur_batch_idx = 0;
        }
    }
}

struct ExternalMerger<'a, KC: KeyCollector> {
    cursors: LoserTree<SpillCursor<'a>>,
    pruned_schema: SchemaRef,
    sub_batch_size: usize,
    limit: usize,
    num_total_output_rows: usize,
    staging_cursor_ids: Vec<usize>,
    staging_key_collector: KC,
    staging_num_rows: usize,
}

impl<'a, KC: KeyCollector> ExternalMerger<'a, KC> {
    fn try_new(
        spills: &'a mut [Box<dyn Spill>],
        pruned_schema: SchemaRef,
        sub_batch_size: usize,
        limit: usize,
    ) -> Result<Self> {
        Ok(Self {
            cursors: LoserTree::new(
                spills
                    .iter_mut()
                    .enumerate()
                    .map(|(id, spill)| {
                        SpillCursor::try_from_spill(id, pruned_schema.clone(), spill)
                    })
                    .collect::<Result<_>>()?,
            ),
            pruned_schema,
            sub_batch_size,
            limit,
            num_total_output_rows: 0,
            staging_cursor_ids: Vec::with_capacity(sub_batch_size),
            staging_key_collector: KC::default(),
            staging_num_rows: 0,
        })
    }
}

impl<KC: KeyCollector> Iterator for ExternalMerger<'_, KC> {
    type Item = Result<(KC, RecordBatch)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.merge_one().transpose()
    }
}

impl<KC: KeyCollector> ExternalMerger<'_, KC> {
    fn cursors_mem_used(&self) -> usize {
        self.cursors.len() * SPILL_OFFHEAP_MEM_COST
            + self
                .cursors
                .values()
                .iter()
                .map(|cursor| cursor.cur_mem_used)
                .sum::<usize>()
    }

    fn merge_one(&mut self) -> Result<Option<(KC, RecordBatch)>> {
        let pruned_schema = self.pruned_schema.clone();

        // collect merged records to staging
        if self.num_total_output_rows < self.limit {
            let mut min_cursor = self.cursors.peek_mut();

            while self.staging_num_rows < self.sub_batch_size {
                if min_cursor.finished {
                    break;
                }
                if !pruned_schema.fields().is_empty() {
                    self.staging_cursor_ids.push(min_cursor.id);
                }
                self.staging_key_collector.add_key(min_cursor.cur_key());
                self.staging_num_rows += 1;
                min_cursor.next_key()?;

                // fetch next min key from loser tree only if it is different from previous key
                if min_cursor.finished || !min_cursor.is_equal_to_prev_key() {
                    min_cursor.adjust();
                }
            }
        }

        // flush staging
        if !self.staging_key_collector.is_empty() {
            let flushed = self.flush_staging()?;
            for cursor in self.cursors.values_mut() {
                cursor.clear_finished_batches();
            }
            return Ok(Some(flushed));
        }
        Ok(None)
    }

    fn flush_staging(&mut self) -> Result<(KC, RecordBatch)> {
        let num_rows = self
            .staging_num_rows
            .min(self.limit - self.num_total_output_rows);

        // collect keys
        let key_collector = std::mem::take(&mut self.staging_key_collector);
        self.staging_num_rows = 0;

        // collect pruned columns
        let pruned_schema = self.pruned_schema.clone();
        let pruned_batch = if !pruned_schema.fields().is_empty() {
            let mut batches_base_idx = vec![];
            let mut base_idx = 0;
            for cursor in self.cursors.values() {
                batches_base_idx.push(base_idx);
                base_idx += cursor.cur_batches.len();
            }

            let mut batches = vec![];
            for cursor in self.cursors.values() {
                batches.extend(cursor.cur_batches.clone());
            }
            let staging_indices = std::mem::take(&mut self.staging_cursor_ids)
                .iter()
                .take(num_rows)
                .map(|&cursor_id| {
                    let cursor = &mut self.cursors.values_mut()[cursor_id];
                    let base_idx = batches_base_idx[cursor.id];
                    let (batch_idx, row_idx) = cursor.next_row();
                    (base_idx + batch_idx, row_idx)
                })
                .collect::<Vec<_>>();
            interleave_batches(pruned_schema, &batches, &staging_indices)?
        } else {
            RecordBatch::try_new_with_options(
                pruned_schema.clone(),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(num_rows)),
            )?
        };
        self.num_total_output_rows += num_rows;
        Ok((key_collector, pruned_batch))
    }
}

fn merge_spills(
    mut spills: Vec<Box<dyn Spill>>,
    spill_metrics: &SpillMetrics,
    sub_batch_size: usize,
    limit: usize,
    pruned_schema: SchemaRef,
) -> Result<Box<dyn Spill>> {
    assert!(spills.len() >= 1);
    if spills.len() == 1 {
        return Ok(spills.into_iter().next().unwrap());
    }

    let mut output_spill = try_new_spill(spill_metrics)?;
    let mut output_writer = output_spill.get_compressed_writer();
    let mut merger = ExternalMerger::<SqueezeKeyCollector>::try_new(
        &mut spills,
        pruned_schema,
        sub_batch_size,
        limit,
    )?;

    while let Some((key_collector, pruned_batch)) = merger.next().transpose()? {
        let mut buf = vec![];
        write_one_batch(&pruned_batch, &mut Cursor::new(&mut buf))?;
        output_writer.write_all(&buf)?;
        output_writer.write_all(&key_collector.store)?;
    }
    drop(output_writer);
    Ok(output_spill)
}

fn create_zero_column_batch(num_rows: usize) -> RecordBatch {
    static EMPTY_SCHEMA: OnceCell<SchemaRef> = OnceCell::new();
    let empty_schema = EMPTY_SCHEMA
        .get_or_init(|| Arc::new(Schema::empty()))
        .clone();
    RecordBatch::try_new_with_options(
        empty_schema,
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(num_rows)),
    )
    .unwrap()
}

struct PruneSortKeysFromBatch {
    input_projection: Vec<usize>,
    sort_row_converter: Arc<SyncMutex<RowConverter>>,
    sort_row_parser: RowParser,
    key_exprs: Vec<PhysicalSortExpr>,
    key_cols: HashSet<usize>,
    restored_col_mappers: Vec<ColMapper>,
    restored_schema: SchemaRef,
    pruned_schema: SchemaRef,
}

#[derive(Clone, Copy)]
enum ColMapper {
    FromPrunedBatch(usize),
    FromKey(usize),
}

impl PruneSortKeysFromBatch {
    fn try_new(
        input_schema: SchemaRef,
        input_projection: &[usize],
        exprs: &[PhysicalSortExpr],
    ) -> Result<Self> {
        let sort_row_converter = Arc::new(SyncMutex::new(RowConverter::new(
            exprs
                .iter()
                .map(|expr: &PhysicalSortExpr| {
                    Ok(SortField::new_with_options(
                        expr.expr.data_type(&input_schema)?,
                        expr.options,
                    ))
                })
                .collect::<Result<Vec<SortField>>>()?,
        )?));
        let sort_row_parser = sort_row_converter.lock().parser();
        let input_projected_schema = Arc::new(input_schema.project(input_projection)?);

        let mut relation = vec![];
        for (expr_idx, expr) in exprs.iter().enumerate() {
            if let Some(col) = expr.expr.as_any().downcast_ref::<Column>() {
                relation.push((expr_idx, col.index()));
            }
        }

        // compute pruned col indices
        let pruned_cols = relation
            .iter()
            .map(|(_, col_idx)| *col_idx)
            .collect::<HashSet<_>>();

        // compute schema after pruning
        let mut fields_after_pruning = vec![];
        for field_idx in 0..input_projected_schema.fields().len() {
            if !pruned_cols.contains(&field_idx) {
                fields_after_pruning.push(input_projected_schema.field(field_idx).clone());
            }
        }
        let pruned_schema = Arc::new(Schema::new(fields_after_pruning));

        // compute col mappers for restoring
        let restored_schema = input_projected_schema;
        let mut restored_col_mappers = vec![];
        let mut num_pruned_cols = 0;
        for col_idx in 0..restored_schema.fields().len() {
            if let Some(expr_idx) = relation.iter().find(|kv| kv.1 == col_idx).map(|kv| kv.0) {
                restored_col_mappers.push(ColMapper::FromKey(expr_idx));
                num_pruned_cols += 1;
            } else {
                restored_col_mappers.push(ColMapper::FromPrunedBatch(col_idx - num_pruned_cols));
            }
        }

        Ok(Self {
            input_projection: input_projection.to_vec(),
            sort_row_converter,
            sort_row_parser,
            key_exprs: exprs.to_vec(),
            key_cols: pruned_cols,
            restored_col_mappers,
            pruned_schema,
            restored_schema,
        })
    }

    fn is_all_pruned(&self) -> bool {
        self.pruned_schema.fields().is_empty()
    }

    fn pruned_schema(&self) -> SchemaRef {
        self.pruned_schema.clone()
    }

    fn restored_schema(&self) -> SchemaRef {
        self.restored_schema.clone()
    }

    fn prune(&self, batch: RecordBatch) -> Result<(Rows, RecordBatch)> {
        // compute key rows
        let key_cols: Vec<ArrayRef> = self
            .key_exprs
            .iter()
            .map(|expr| {
                expr.expr
                    .evaluate(&batch)
                    .and_then(|cv| cv.into_array(batch.num_rows()))
            })
            .collect::<Result<_>>()?;
        let key_rows = self.sort_row_converter.lock().convert_columns(&key_cols)?;

        let retained_cols = batch
            .project(&self.input_projection)?
            .columns()
            .iter()
            .enumerate()
            .filter(|(col_idx, _)| !self.key_cols.contains(col_idx))
            .map(|(_, col)| col)
            .cloned()
            .collect();
        let pruned_batch = RecordBatch::try_new_with_options(
            self.pruned_schema(),
            retained_cols,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )?;
        Ok((key_rows, pruned_batch))
    }

    fn restore<'a, KC: KeyCollector>(
        &self,
        pruned_batch: RecordBatch,
        key_collector: KC,
    ) -> Result<RecordBatch> {
        // restore key columns
        let key_rows: Box<dyn Iterator<Item = Row<'a>>> =
            if let Some(kc) = key_collector.as_any().downcast_ref::<SimpleKeyCollector>() {
                Box::new(kc.iter_key().map(move |key| unsafe {
                    // safety - row has the same lifetime with key_collector
                    let row = self.sort_row_parser.parse(key);
                    std::mem::transmute::<_, Row<'a>>(row)
                }))
            } else if let Some(kc) = key_collector.as_any().downcast_ref::<SqueezeKeyCollector>() {
                let mut key_data = vec![];
                let mut key_lens = vec![];
                let mut key_reader = SortedKeysReader::default();
                let mut sorted_key_store_cursor = Cursor::new(&kc.store);
                for _ in 0..pruned_batch.num_rows() {
                    key_reader
                        .next_key(&mut sorted_key_store_cursor)
                        .expect("error reading next key");
                    key_data.extend(&key_reader.cur_key);
                    key_lens.push(key_reader.cur_key.len());
                }
                let mut key_offset = 0;
                Box::new(key_lens.into_iter().map(move |key_len| unsafe {
                    // safety - row has the same lifetime with key_collector
                    let key = &key_data[key_offset..][..key_len];
                    let row = self.sort_row_parser.parse(key);
                    key_offset += key_len;
                    std::mem::transmute::<_, Row<'a>>(row)
                }))
            } else {
                unreachable!("unknown KeyCollector type")
            };
        let key_cols = self.sort_row_converter.lock().convert_rows(key_rows)?;

        // restore batch
        let mut restored_fields = vec![];
        for &map in &self.restored_col_mappers {
            match map {
                ColMapper::FromPrunedBatch(idx) => {
                    restored_fields.push(pruned_batch.column(idx).clone());
                }
                ColMapper::FromKey(idx) => {
                    restored_fields.push(key_cols[idx].clone());
                }
            }
        }
        Ok(RecordBatch::try_new_with_options(
            self.restored_schema(),
            restored_fields,
            &RecordBatchOptions::new().with_row_count(Some(pruned_batch.num_rows())),
        )?)
    }
}

trait KeyCollector: Default {
    fn as_any(&self) -> &dyn Any;
    fn add_key(&mut self, key: &[u8]);
    fn is_empty(&self) -> bool;
}

#[derive(Default)]
struct SimpleKeyCollector {
    lens: Vec<u32>,
    store: Vec<u8>,
}

impl SimpleKeyCollector {
    fn iter_key(&self) -> impl Iterator<Item = &[u8]> {
        let mut key_offset = 0;
        self.lens.iter().map(move |&key_len| {
            let key = &self.store[key_offset..][..key_len as usize];
            key_offset += key_len as usize;
            key
        })
    }
}

impl KeyCollector for SimpleKeyCollector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn add_key(&mut self, key: &[u8]) {
        self.lens.push(key.len() as u32);
        self.store.extend(key);
    }

    fn is_empty(&self) -> bool {
        self.lens.is_empty()
    }
}

#[derive(Default)]
struct SqueezeKeyCollector {
    sorted_key_writer: SortedKeysWriter,
    store: Vec<u8>,
}

impl KeyCollector for SqueezeKeyCollector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn add_key(&mut self, key: &[u8]) {
        self.sorted_key_writer
            .write_key(key, &mut self.store)
            .unwrap()
    }

    fn is_empty(&self) -> bool {
        self.store.is_empty()
    }
}

#[derive(Default)]
struct SortedKeysWriter {
    cur_key: Vec<u8>,
}

impl SortedKeysWriter {
    fn write_key(&mut self, key: &[u8], w: &mut impl Write) -> std::io::Result<()> {
        let prefix_len = common_prefix_len(&self.cur_key, key);
        let suffix_len = key.len() - prefix_len;

        if prefix_len == key.len() && suffix_len == 0 {
            write_len(0, w)?; // indicates same record
        } else {
            self.cur_key.resize(key.len(), 0);
            self.cur_key[prefix_len..].copy_from_slice(&key[prefix_len..]);
            write_len(suffix_len + 1, w)?;
            write_len(prefix_len, w)?;
            w.write_all(&key[prefix_len..])?;
        }
        Ok(())
    }
}

#[derive(Default)]
struct SortedKeysReader {
    cur_key: Vec<u8>,
    is_equal_to_prev: bool,
}

impl SortedKeysReader {
    fn next_key(&mut self, r: &mut impl Read) -> std::io::Result<()> {
        let b = read_len(r)?;
        if b > 0 {
            self.is_equal_to_prev = false;
            let suffix_len = b - 1;
            let prefix_len = read_len(r)?;
            self.cur_key.resize(prefix_len + suffix_len, 0);
            r.read_exact(&mut self.cur_key[prefix_len..][..suffix_len])?;
        } else {
            self.is_equal_to_prev = true;
        }
        Ok(())
    }
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let max_len = a.len().min(b.len());
    for i in 0..max_len {
        if unsafe {
            // safety - indices are within bounds
            a.get_unchecked(i) != b.get_unchecked(i)
        } {
            return i;
        }
    }
    max_len
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{
        array::Int32Array,
        compute::SortOptions,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::{
        assert_batches_eq,
        common::Result,
        physical_expr::{expressions::Column, PhysicalSortExpr},
        physical_plan::{common, memory::MemoryExec, ExecutionPlan},
        prelude::SessionContext,
    };

    use crate::{memmgr::MemManager, sort_exec::SortExec};

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

    fn build_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a, b, c);
        let schema = batch.schema();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    #[tokio::test]
    async fn test_sort_i32() -> Result<()> {
        MemManager::init(100);
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let input = build_table(
            ("a", &vec![9, 8, 7, 6, 5, 4, 3, 2, 1, 0]),
            ("b", &vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            ("c", &vec![5, 6, 7, 8, 9, 0, 1, 2, 3, 4]),
        );
        let sort_exprs = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("a", 0)),
            options: SortOptions::default(),
        }];

        let sort = SortExec::new(input, sort_exprs, Some(6));
        let output = sort.execute(0, task_ctx)?;
        let batches = common::collect(output).await?;
        let expected = vec![
            "+---+---+---+",
            "| a | b | c |",
            "+---+---+---+",
            "| 0 | 9 | 4 |",
            "| 1 | 8 | 3 |",
            "| 2 | 7 | 2 |",
            "| 3 | 6 | 1 |",
            "| 4 | 5 | 0 |",
            "| 5 | 4 | 9 |",
            "+---+---+---+",
        ];
        assert_batches_eq!(expected, &batches);

        Ok(())
    }
}

#[cfg(test)]
mod fuzztest {
    use std::sync::Arc;

    use arrow::{compute::SortOptions, record_batch::RecordBatch};
    use datafusion::{
        common::{Result, ScalarValue},
        logical_expr::ColumnarValue,
        physical_expr::{expressions::Column, math_expressions::random, PhysicalSortExpr},
        physical_plan::{coalesce_batches::concat_batches, memory::MemoryExec},
        prelude::{SessionConfig, SessionContext},
    };

    use crate::{memmgr::MemManager, sort_exec::SortExec};

    #[tokio::test]
    async fn fuzztest() -> Result<()> {
        MemManager::init(10000);
        let session_ctx =
            SessionContext::new_with_config(SessionConfig::new().with_batch_size(10000));
        let task_ctx = session_ctx.task_ctx();
        let n = 1234567;

        // generate random batch for fuzzying
        let mut batches = vec![];
        let mut num_rows = 0;
        while num_rows < n {
            let nulls = ScalarValue::Null
                .to_array_of_size((n - num_rows).min(10000))
                .unwrap();
            let rand_key1 = random(&[ColumnarValue::Array(nulls.clone())])?.into_array(0)?;
            let rand_key2 = random(&[ColumnarValue::Array(nulls.clone())])?.into_array(0)?;
            let rand_val1 = random(&[ColumnarValue::Array(nulls.clone())])?.into_array(0)?;
            let rand_val2 = random(&[ColumnarValue::Array(nulls.clone())])?.into_array(0)?;
            let batch = RecordBatch::try_from_iter_with_nullable(vec![
                ("k1", rand_key1, true),
                ("k2", rand_key2, true),
                ("v1", rand_val1, true),
                ("v2", rand_val2, true),
            ])?;
            num_rows += batch.num_rows();
            batches.push(batch);
        }
        let schema = batches[0].schema();
        let sort_exprs = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("k1", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("k2", 1)),
                options: SortOptions::default(),
            },
        ];

        let input = Arc::new(MemoryExec::try_new(
            &[batches.clone()],
            schema.clone(),
            None,
        )?);
        let sort = Arc::new(SortExec::new(input, sort_exprs.clone(), None));
        let output = datafusion::physical_plan::collect(sort, task_ctx.clone()).await?;
        let a = concat_batches(&schema, &output, n)?;

        let input = Arc::new(MemoryExec::try_new(
            &[batches.clone()],
            schema.clone(),
            None,
        )?);
        let sort = Arc::new(datafusion::physical_plan::sorts::sort::SortExec::new(
            sort_exprs.clone(),
            input,
        ));
        let output = datafusion::physical_plan::collect(sort, task_ctx.clone()).await?;
        let b = concat_batches(&schema, &output, n)?;

        assert_eq!(a.num_rows(), b.num_rows());
        assert!(a == b);
        Ok(())
    }
}
