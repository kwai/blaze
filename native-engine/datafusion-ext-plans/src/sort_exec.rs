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
    io::{BufReader, Cursor, Read, Write},
    marker::PhantomData,
    sync::{Arc, Weak},
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
    df_execution_err,
    io::{read_bytes_slice, read_len, read_one_batch, write_len, write_one_batch},
    loser_tree::{ComparableForLoserTree, LoserTree},
    slim_bytes::SlimBytes,
    streams::coalesce_stream::CoalesceInput,
};
use futures::{lock::Mutex, stream::once, StreamExt, TryStreamExt};
use itertools::Itertools;
use lz4_flex::frame::FrameDecoder;
use once_cell::sync::OnceCell;
use parking_lot::Mutex as SyncMutex;

use crate::{
    common::{
        batch_statisitcs::{stat_input, InputBatchStatistics},
        column_pruning::ExecuteWithColumnPruning,
        output::{TaskOutputter, WrappedRecordBatchSender},
        BatchTaker, BatchesInterleaver,
    },
    memmgr::{
        onheap_spill::{try_new_spill, Spill},
        MemConsumer, MemConsumerInfo, MemManager,
    },
};

// reserve memory for each spill
// estimated size: bufread=64KB + lz4dec.src=64KB + lz4dec.dest=64KB +
// batches=~100KB
const SPILL_OFFHEAP_MEM_COST: usize = 300000;

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

    fn statistics(&self) -> Statistics {
        todo!()
    }
}

struct ExternalSorter {
    name: String,
    mem_consumer_info: Option<Weak<MemConsumerInfo>>,
    sub_batch_size: usize,
    prune_sort_keys_from_batch: Arc<PruneSortKeysFromBatch>,
    limit: usize,
    data: Arc<Mutex<BufferedData>>,
    spills: Mutex<Vec<Box<dyn Spill>>>,
    baseline_metrics: BaselineMetrics,
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
        let spill = std::mem::take(&mut *self.data.lock().await)
            .try_into_spill(self.sub_batch_size, self.limit)?;
        self.spills.lock().await.push(spill);
        self.update_mem_used(0).await?;
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct KeyRef {
    start: u32,
    len: u32,
}

impl KeyRef {
    fn new_from_key_store(key_store: &mut Vec<u8>, key: &[u8]) -> Self {
        let start = key_store.len() as u32;
        let len = key.len() as u32;
        key_store.extend_from_slice(key);
        Self { start, len }
    }

    fn ref_key<'a>(&self, key_store: &'a [u8]) -> &'a [u8] {
        let start = self.start as usize;
        let end = start + self.len as usize;
        &key_store[start..end]
    }
}

#[derive(Default)]
struct BufferedData {
    key_stores: Vec<Box<[u8]>>,
    key_stores_mem_used: usize,
    sorted_batches: Vec<RecordBatch>,
    sorted_keys: Vec<Vec<KeyRef>>,
    sorted_batches_mem_used: usize,
    num_rows: usize,
}

impl BufferedData {
    fn try_into_spill(self, sub_batch_size: usize, limit: usize) -> Result<Box<dyn Spill>> {
        let spill = try_new_spill()?;
        let mut writer = lz4_flex::frame::FrameEncoder::new(spill.get_buf_writer());

        for (keys, batch) in self.into_sorted_batches(sub_batch_size, limit) {
            let mut buf = vec![];
            write_one_batch(&batch, &mut Cursor::new(&mut buf), true, None)?;
            writer.write_all(&buf)?;

            for key in keys {
                write_len(key.len(), &mut writer)?;
                writer.write_all(key)?;
            }
        }
        writer.finish().or_else(|err| df_execution_err!("{err}"))?;
        spill.complete()?;
        Ok(spill)
    }

    fn mem_used(&self) -> usize {
        self.sorted_batches_mem_used + self.key_stores_mem_used + 8 * self.num_rows
    }

    fn into_sorted_batches<'a>(
        self,
        batch_size: usize,
        limit: usize,
    ) -> impl Iterator<Item = (Vec<&'a [u8]>, RecordBatch)> {
        struct Cursor {
            idx: usize,
            row_idx: usize,
            keys: Vec<KeyRef>,
            key_store: Box<[u8]>,
        }

        impl ComparableForLoserTree for Cursor {
            #[inline(always)]
            fn lt(&self, other: &Self) -> bool {
                match (self.keys.get(self.row_idx), other.keys.get(other.row_idx)) {
                    (Some(k1), Some(k2)) => {
                        let key1 = k1.ref_key(&self.key_store);
                        let key2 = k2.ref_key(&other.key_store);
                        key1 < key2
                    }
                    (None, _) => false,
                    (_, None) => true,
                }
            }
        }

        struct SortedBatchesIterator<'a> {
            cursors: LoserTree<Cursor>,
            batches: Vec<RecordBatch>,
            batch_size: usize,
            num_output_rows: usize,
            limit: usize,
            _phantom: PhantomData<&'a ()>,
        }

        impl<'a> Iterator for SortedBatchesIterator<'a> {
            type Item = (Vec<&'a [u8]>, RecordBatch);

            fn next(&mut self) -> Option<Self::Item> {
                if self.num_output_rows >= self.limit {
                    return None;
                }
                let cur_batch_size = self.batch_size.min(self.limit - self.num_output_rows);
                let batch_schema = self.batches[0].schema();
                let is_all_pruned = self.batches[0].num_columns() == 0;

                let mut key_refs = Vec::with_capacity(cur_batch_size);
                let mut indices = Vec::with_capacity(cur_batch_size);

                while key_refs.len() < cur_batch_size {
                    let mut min_cursor = self.cursors.peek_mut();
                    key_refs.push(min_cursor.keys[min_cursor.row_idx]);
                    indices.push((min_cursor.idx, min_cursor.row_idx));
                    min_cursor.row_idx += 1;
                }
                let batch = if !is_all_pruned {
                    BatchesInterleaver::new(batch_schema, &self.batches)
                        .interleave(&indices)
                        .expect("error merging sorted batches: interleaving error")
                } else {
                    create_zero_column_batch(key_refs.len())
                };
                let keys = key_refs
                    .into_iter()
                    .zip(&indices)
                    .map(|(key_ref, (idx, _))| unsafe {
                        // safety: cursor have same lifetime with 'a
                        std::mem::transmute::<_, &'a [u8]>(
                            key_ref.ref_key(&self.cursors.values()[*idx].key_store),
                        )
                    })
                    .collect();

                self.num_output_rows += cur_batch_size;
                Some((keys, batch))
            }
        }

        let mut key_stores = self.key_stores;
        let cursors = LoserTree::new(
            self.sorted_keys
                .into_iter()
                .enumerate()
                .map(|(idx, keys)| Cursor {
                    idx,
                    keys,
                    key_store: std::mem::take(&mut key_stores[idx]),
                    row_idx: 0,
                })
                .collect(),
        );

        Box::new(SortedBatchesIterator {
            cursors,
            batch_size,
            batches: self.sorted_batches,
            limit: limit.min(self.num_rows),
            num_output_rows: 0,
            _phantom: PhantomData::default(),
        })
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
        let batch_size = context.session_config().batch_size();
        let sub_batch_size = batch_size / batch_size.ilog2() as usize;

        let prune_sort_keys_from_batch = Arc::new(PruneSortKeysFromBatch::try_new(
            input_schema,
            projection,
            &self.exprs,
        )?);

        let external_sorter = Arc::new(ExternalSorter {
            name: format!("ExternalSorter[partition={}]", partition),
            mem_consumer_info: None,
            sub_batch_size,
            prune_sort_keys_from_batch,
            limit: self.fetch.unwrap_or(usize::MAX),
            data: Default::default(),
            spills: Default::default(),
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
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
            once(external_sort(coalesced, context.clone(), external_sorter)).try_flatten(),
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
    context: Arc<TaskContext>,
    sorter: Arc<ExternalSorter>,
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
        return context.output_bufferable_with_spill(sorter_cloned, output);
    }
    Ok(output)
}

impl ExternalSorter {
    async fn insert_batch(self: &Arc<Self>, batch: RecordBatch) -> Result<()> {
        let _timer = self.baseline_metrics.elapsed_compute().timer();
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let (key_rows, batch) = self.prune_sort_keys_from_batch.prune(batch)?;

        // sort the batch and append to sorter
        let mut key_store = Vec::with_capacity(key_rows.size());
        let sorted_keys;
        let sorted_batch;
        if !self.prune_sort_keys_from_batch.is_all_pruned() {
            let (cur_sorted_keys, cur_sorted_indices): (Vec<KeyRef>, Vec<usize>) = key_rows
                .iter()
                .map(|key| unsafe {
                    // safety: keys have the same lifetime with key_rows
                    std::mem::transmute::<_, &'static [u8]>(key.as_ref())
                })
                .enumerate()
                .sorted_unstable_by_key(|(_idx, key)| unsafe {
                    // safety: keys have the same lifetime with key_rows
                    std::mem::transmute::<_, &'static [u8]>(key.as_ref())
                })
                .take(self.limit)
                .map(|(idx, key)| {
                    let key_ref = KeyRef::new_from_key_store(&mut key_store, key);
                    (key_ref, idx)
                })
                .unzip();
            sorted_keys = cur_sorted_keys;
            sorted_batch = BatchTaker(&batch).take(cur_sorted_indices)?;
        } else {
            sorted_keys = key_rows
                .iter()
                .map(|key| unsafe {
                    // safety: keys have the same lifetime with key_rows
                    std::mem::transmute::<_, &'static [u8]>(key.as_ref())
                })
                .sorted_unstable()
                .take(self.limit)
                .map(|key| KeyRef::new_from_key_store(&mut key_store, key))
                .collect();
            sorted_batch = create_zero_column_batch(sorted_keys.len());
        }

        let mut data = self.data.lock().await;
        data.num_rows += sorted_batch.num_rows();
        data.sorted_batches_mem_used += sorted_batch.get_array_memory_size();
        data.key_stores_mem_used += key_store.len();

        data.key_stores.push(key_store.into());
        data.sorted_batches.push(sorted_batch);
        data.sorted_keys.push(sorted_keys);
        let mem_used = data.mem_used();
        drop(data);

        self.update_mem_used(mem_used).await?;
        Ok(())
    }

    async fn output(self: Arc<Self>, sender: Arc<WrappedRecordBatchSender>) -> Result<()> {
        let mut timer = self.baseline_metrics.elapsed_compute().timer();
        self.set_spillable(false);

        let data = std::mem::take(&mut *self.data.lock().await);
        let mut spills = std::mem::take(&mut *self.spills.lock().await);
        log::info!(
            "sort exec starts outputting with {} ({} spills)",
            self.name(),
            spills.len(),
        );

        // no spills -- output in-mem batches
        if spills.is_empty() {
            for (keys, batch) in data.into_sorted_batches(self.sub_batch_size, self.limit) {
                let batch = self
                    .prune_sort_keys_from_batch
                    .restore(batch, keys.into_iter())?;
                self.baseline_metrics.record_output(batch.num_rows());
                sender.send(Ok(batch), Some(&mut timer)).await;
            }
            self.update_mem_used(0).await?;
            return Ok(());
        }

        // move in-mem batches into spill, so we can free memory as soon as possible
        spills.push(data.try_into_spill(self.sub_batch_size, self.limit)?);

        // adjust mem usage
        self.update_mem_used(spills.len() * SPILL_OFFHEAP_MEM_COST)
            .await?;

        // use loser tree to merge all spills
        let mut cursors: LoserTree<SpillCursor> = LoserTree::new(
            spills
                .iter()
                .enumerate()
                .map(|(id, spill)| SpillCursor::try_from_spill(id, self.clone(), &spill))
                .collect::<Result<_>>()?,
        );

        let mut num_total_output_rows = 0;
        let mut staging_cursor_ids = Vec::with_capacity(self.sub_batch_size);
        let mut staging_keys = Vec::with_capacity(self.sub_batch_size);

        macro_rules! flush_staging {
            () => {{
                if num_total_output_rows < self.limit {
                    let batch_num_rows = staging_keys.len().min(self.limit - num_total_output_rows);
                    let mut batches_base_idx = vec![];
                    let mut base_idx = 0;
                    for cursor in cursors.values() {
                        batches_base_idx.push(base_idx);
                        base_idx += cursor.cur_batches.len();
                    }

                    let pruned_schema = self.prune_sort_keys_from_batch.pruned_schema();
                    let pruned_batch = if !self.prune_sort_keys_from_batch.is_all_pruned() {
                        let mut batches = vec![];
                        for cursor in cursors.values() {
                            batches.extend(cursor.cur_batches.clone());
                        }
                        let staging_indices = std::mem::take(&mut staging_cursor_ids)
                            .iter()
                            .map(|&cursor_id| {
                                let cursor = &mut cursors.values_mut()[cursor_id];
                                let base_idx = batches_base_idx[cursor.id];
                                let (batch_idx, row_idx) = cursor.next_row();
                                (base_idx + batch_idx, row_idx)
                            })
                            .collect::<Vec<_>>();
                        let interleaver = BatchesInterleaver::new(pruned_schema, &batches);
                        interleaver.interleave(&staging_indices)?
                    } else {
                        RecordBatch::try_new_with_options(
                            pruned_schema,
                            vec![],
                            &RecordBatchOptions::new().with_row_count(Some(staging_keys.len())),
                        )?
                    };

                    let mut batch = self
                        .prune_sort_keys_from_batch
                        .restore(pruned_batch, std::mem::take(&mut staging_keys).iter())?;
                    if batch_num_rows < batch.num_rows() {
                        batch = batch.slice(0, batch_num_rows)
                    }

                    num_total_output_rows += batch.num_rows();
                    let _ = num_total_output_rows;
                    self.baseline_metrics.record_output(batch.num_rows());
                    sender.send(Ok(batch), Some(&mut timer)).await;
                }
            }};
        }

        // merge
        while num_total_output_rows < self.limit {
            let mut min_cursor = cursors.peek_mut();
            if min_cursor.finished {
                break;
            }
            if !self.prune_sort_keys_from_batch.is_all_pruned() {
                staging_cursor_ids.push(min_cursor.id);
            }
            staging_keys.push(min_cursor.next_key()?);
            drop(min_cursor);

            if staging_keys.len() >= self.sub_batch_size {
                flush_staging!();

                for cursor in cursors.values_mut() {
                    cursor.clear_finished_batches();
                }
            }
        }
        if !staging_keys.is_empty() {
            flush_staging!();
        }

        // update disk spill size
        let spill_disk_usage = spills
            .iter()
            .map(|spill| spill.get_disk_usage().unwrap_or(0))
            .sum::<u64>();
        self.baseline_metrics
            .record_spill(spill_disk_usage as usize);
        self.update_mem_used(0).await?;
        Ok(())
    }
}

struct SpillCursor {
    id: usize,
    sorter: Arc<ExternalSorter>,
    input: FrameDecoder<BufReader<Box<dyn Read + Send>>>,
    cur_batch_num_rows: usize,
    cur_loaded_num_rows: usize,
    cur_batches: Vec<RecordBatch>,
    cur_batch_idx: usize,
    cur_row_idx: usize,
    cur_key: SlimBytes,
    finished: bool,
}

impl ComparableForLoserTree for SpillCursor {
    #[inline(always)]
    fn lt(&self, other: &Self) -> bool {
        let key1 = (self.finished, &self.cur_key);
        let key2 = (other.finished, &other.cur_key);
        key1 < key2
    }
}

impl SpillCursor {
    fn try_from_spill(
        id: usize,
        sorter: Arc<ExternalSorter>,
        spill: &Box<dyn Spill>,
    ) -> Result<Self> {
        let buf_reader = spill.get_buf_reader();
        let mut iter = SpillCursor {
            id,
            sorter,
            input: FrameDecoder::new(buf_reader),
            cur_batch_num_rows: 0,
            cur_loaded_num_rows: 0,
            cur_batches: vec![],
            cur_batch_idx: 0,
            cur_row_idx: 0,
            cur_key: Default::default(),
            finished: false,
        };
        iter.next_key()?; // load first record into current
        Ok(iter)
    }

    // forwards to next key and returns current key
    fn next_key(&mut self) -> Result<SlimBytes> {
        assert!(
            !self.finished,
            "calling next_key() on finished sort spill cursor"
        );

        if self.cur_loaded_num_rows >= self.cur_batch_num_rows && !self.load_next_batch()? {
            let cur_key = std::mem::take(&mut self.cur_key);
            return Ok(cur_key);
        }
        let sorted_row_len = read_len(&mut self.input)?;
        let cur_key = std::mem::replace(
            &mut self.cur_key,
            read_bytes_slice(&mut self.input, sorted_row_len)?.into(),
        );
        self.cur_loaded_num_rows += 1;
        Ok(cur_key)
    }

    fn load_next_batch(&mut self) -> Result<bool> {
        if let Some(batch) = read_one_batch(
            &mut self.input,
            Some(self.sorter.prune_sort_keys_from_batch.pruned_schema()),
            true,
        )? {
            self.cur_batch_num_rows = batch.num_rows();
            self.cur_loaded_num_rows = 0;
            self.cur_batches.push(batch);
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
            self.cur_batches.drain(..self.cur_batch_idx);
            self.cur_batch_idx = 0;
        }
    }
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
                    .map(|cv| cv.into_array(batch.num_rows()))
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

    fn restore<'a>(
        &self,
        pruned_batch: RecordBatch,
        keys: impl Iterator<Item = impl AsRef<[u8]>>,
    ) -> Result<RecordBatch> {
        let mut restored_fields = vec![];
        let key_cols = self
            .sort_row_converter
            .lock()
            .convert_rows(keys.map(|key| unsafe {
                // safety - row has the same lifetime 'a
                let row = self.sort_row_parser.parse(key.as_ref());
                std::mem::transmute::<_, Row<'a>>(row)
            }))?;

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
        let session_ctx = SessionContext::with_config(SessionConfig::new().with_batch_size(10000));
        let task_ctx = session_ctx.task_ctx();
        let n = 1234567;

        // generate random batch for fuzzying
        let mut batches = vec![];
        let mut num_rows = 0;
        while num_rows < n {
            let nulls = ScalarValue::Null.to_array_of_size((n - num_rows).min(10000));
            let rand_key1 = random(&[ColumnarValue::Array(nulls.clone())])?.into_array(0);
            let rand_key2 = random(&[ColumnarValue::Array(nulls.clone())])?.into_array(0);
            let rand_val1 = random(&[ColumnarValue::Array(nulls.clone())])?.into_array(0);
            let rand_val2 = random(&[ColumnarValue::Array(nulls.clone())])?.into_array(0);
            let batch = RecordBatch::try_from_iter_with_nullable(vec![
                ("k1", rand_key1, false),
                ("k2", rand_key2, false),
                ("v1", rand_val1, false),
                ("v2", rand_val2, false),
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
