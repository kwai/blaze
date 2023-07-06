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

use crate::common::memory_manager::{MemConsumer, MemConsumerInfo, MemManager};
use crate::common::onheap_spill::{Spill, try_new_spill};
use crate::common::BatchesInterleaver;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use async_trait::async_trait;
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use datafusion_ext_commons::io::{
    read_bytes_slice, read_len, read_one_batch, write_len, write_one_batch,
};
use datafusion_ext_commons::loser_tree::LoserTree;
use datafusion_ext_commons::streams::coalesce_stream::CoalesceStream;
use futures::lock::Mutex;
use futures::stream::once;
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use lz4_flex::frame::FrameDecoder;
use parking_lot::Mutex as SyncMutex;
use std::any::Any;
use std::collections::VecDeque;
use std::fmt::Formatter;
use std::io::{BufReader, Cursor, Read, Write};
use std::sync::{Arc, Weak};
use arrow::array::{ArrayRef, UInt32Array};
use crate::common::output::{output_with_sender, WrappedRecordBatchSender};

const NUM_LEVELS: usize = 64;

// reserve memory for each spill
// estimated size: bufread=64KB + lz4dec.src=64KB + lz4dec.dest=64KB + batches=~100KB
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
        let input_schema = self.input.schema();
        let batch_size = context.session_config().batch_size();
        let sub_batch_size = batch_size / batch_size.ilog2() as usize;

        let sort_row_converter = RowConverter::new(
            self.exprs
                .iter()
                .map(|expr: &PhysicalSortExpr| {
                    Ok(SortField::new_with_options(
                        expr.expr.data_type(&input_schema)?,
                        expr.options,
                    ))
                })
                .collect::<Result<Vec<SortField>>>()?,
        )?;

        let external_sorter = Arc::new(ExternalSorter {
            name: format!("ExternalSorter[partition={}]", partition),
            mem_consumer_info: None,
            sub_batch_size,
            exprs: self.exprs.clone(),
            input_schema: self.schema(),
            limit: self.fetch.unwrap_or(usize::MAX),
            sort_row_converter: SyncMutex::new(sort_row_converter),
            levels: Mutex::new(vec![None; NUM_LEVELS]),
            spills: Default::default(),
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
        });
        MemManager::register_consumer(external_sorter.clone(), true);

        let input = self.input.execute(partition, context.clone())?;
        let coalesced = Box::pin(CoalesceStream::new(
            input,
            batch_size,
            BaselineMetrics::new(&self.metrics, partition)
                .elapsed_compute()
                .clone(),
        ));

        let output = Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            once(external_sort(coalesced, context, external_sorter)).try_flatten(),
        ));
        let coalesced = Box::pin(CoalesceStream::new(
            output,
            batch_size,
            BaselineMetrics::new(&self.metrics, partition)
                .elapsed_compute()
                .clone(),
        ));
        Ok(coalesced)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let exprs = self.exprs
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "SortExec: {}", exprs)
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}

struct ExternalSorter {
    name: String,
    mem_consumer_info: Option<Weak<MemConsumerInfo>>,
    sub_batch_size: usize,
    exprs: Vec<PhysicalSortExpr>,
    input_schema: SchemaRef,
    limit: usize,
    sort_row_converter: SyncMutex<RowConverter>,
    levels: Mutex<Vec<Option<SortedBatches>>>,
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
        let mut levels = self.levels.lock().await;

        // spill out the maximum level
        let max_level_in_mem_batches = match max_level_id(&levels) {
            Some(max_level_id) => std::mem::take(&mut levels[max_level_id]).unwrap(),
            None => return Ok(()),
        };
        self.spills
            .lock()
            .await
            .extend(max_level_in_mem_batches.try_into_spill()?);

        let mem_used = levels.iter().flatten().map(|b| b.mem_size()).sum::<usize>();
        drop(levels);

        // adjust memory usage
        self.update_mem_used(mem_used).await?;
        Ok(())
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

    output_with_sender("Sort", context, input.schema(), |sender| async move {
        sorter.output(sender).await?;
        Ok(())
    })
}

impl ExternalSorter {
    async fn insert_batch(self: &Arc<Self>, batch: RecordBatch) -> Result<()> {
        let _timer = self.baseline_metrics.elapsed_compute().timer();
        if batch.num_rows() == 0 {
            return Ok(());
        }

        // create a sorted batches containing the single input batch
        let mut sorted_batches = SortedBatches::from_batch(self.clone(), batch)?;

        // merge sorted batches into levels
        let mut levels = self.levels.lock().await;
        let mut cur_level = 0;
        while let Some(existed) = std::mem::take(&mut levels[cur_level]) {
            sorted_batches.merge(existed)?;
            cur_level += 1;
        }
        levels[cur_level] = Some(sorted_batches);

        // adjust memory usage
        let mem_used = levels.iter().flatten().map(|b| b.mem_size()).sum::<usize>();
        drop(levels);

        self.update_mem_used(mem_used).await?;
        Ok(())
    }

    async fn output(self: Arc<Self>, sender: Arc<WrappedRecordBatchSender>) -> Result<()> {
        let mut timer = self.baseline_metrics.elapsed_compute().timer();
        self.set_spillable(false);

        let mut levels = std::mem::take(&mut *self.levels.lock().await);
        let mut spills = std::mem::take(&mut *self.spills.lock().await);
        log::info!(
            "sort exec starts outputting with {} ({} spills)",
            self.name(),
            spills.len(),
        );

        // in_mem_batches1: batches in max level
        let in_mem_batches1 = match max_level_id(&levels) {
            Some(max_level_id) => std::mem::take(&mut levels[max_level_id]),
            None => None,
        };

        // in_mem_batches2: rest in-mem batches
        let mut in_mem_batches2: Option<SortedBatches> = None;
        for level in levels.into_iter().flatten() {
            if let Some(in_mem_batches2) = &mut in_mem_batches2 {
                in_mem_batches2.merge(level)?;
            } else {
                in_mem_batches2 = Some(level)
            }
        }

        let mut in_mem_batches = [in_mem_batches1, in_mem_batches2]
            .into_iter()
            .flatten()
            .collect::<Vec<SortedBatches>>();
        self.update_mem_used(in_mem_batches.iter().map(|b| b.mem_size()).sum::<usize>()).await?;

        // no spills -- output in-mem batches
        if spills.is_empty() {
            match in_mem_batches.len() {
                0 => {}
                1 => {
                    let batches = in_mem_batches.pop().unwrap().batches;
                    self.update_mem_used(batches
                        .iter()
                        .map(|batch| batch.get_array_memory_size())
                        .sum()).await?;

                    for batch in batches {
                        let batch_mem_size = batch.get_array_memory_size();
                        self.baseline_metrics.record_output(batch.num_rows());
                        sender.send(Ok(batch), Some(&mut timer)).await;
                        self.update_mem_used_with_diff(-(batch_mem_size as isize)).await?;
                    }
                }
                2 => {
                    let in_mem_batches2 = in_mem_batches.pop().unwrap();
                    let in_mem_batches1 = in_mem_batches.pop().unwrap();
                    let mut merge_iter =
                        SortedBatches::merge_into_iter(in_mem_batches1, in_mem_batches2);
                    while let Some((batch, _)) = merge_iter.next().transpose()? {
                        let batch_mem_size = batch.get_array_memory_size();
                        self.baseline_metrics.record_output(batch.num_rows());
                        sender.send(Ok(batch), Some(&mut timer)).await;
                        self.update_mem_used_with_diff(-(batch_mem_size as isize)).await?;
                    }
                }
                _ => unreachable!()
            }
            self.update_mem_used(0).await?;
            return Ok(());
        }

        // move in-mem batches into spill, so we can free memory as soon as possible
        for in_mem_batches in in_mem_batches {
            spills.extend(in_mem_batches.try_into_spill()?);
        }

        // adjust mem usage
        self.update_mem_used(spills.len() * SPILL_OFFHEAP_MEM_COST).await?;

        // use loser tree to merge all spills
        let mut cursors: LoserTree<SpillCursor> = LoserTree::new_by(
            spills
                .iter()
                .enumerate()
                .map(|(id, spill)| SpillCursor::try_from_spill(id, self.clone(), &spill))
                .collect::<Result<_>>()?,
            |c1, c2| {
                let key1 = (c1.finished, &c1.cur_key);
                let key2 = (c2.finished, &c2.cur_key);
                key1 < key2
            },
        );

        let mut num_total_output_rows = 0;
        let mut staging_cursor_ids = Vec::with_capacity(self.sub_batch_size);

        macro_rules! flush_staging {
            () => {{
                if num_total_output_rows < self.limit {
                    flush_staging!(@do_flush);
                }
            }};
            (@do_flush) => {{
                let mut batches_base_idx = vec![];
                let mut base_idx = 0;
                for cursor in cursors.values() {
                    batches_base_idx.push(base_idx);
                    base_idx += cursor.cur_batches.len();
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

                let mut batches = vec![];
                for cursor in cursors.values() {
                    batches.extend(cursor.cur_batches.clone());
                }

                let mut batch = BatchesInterleaver::new(self.input_schema.clone(), &batches)
                    .interleave(&staging_indices)?;
                if num_total_output_rows + batch.num_rows() > self.limit {
                    batch = batch.slice(0, self.limit - num_total_output_rows);
                };
                num_total_output_rows += batch.num_rows();
                let _ = num_total_output_rows;

                self.baseline_metrics.record_output(batch.num_rows());
                sender.send(Ok(batch), Some(&mut timer)).await;
            }};
        }

        // merge
        while num_total_output_rows < self.limit {
            let mut min_cursor = cursors.peek_mut();
            if min_cursor.finished {
                break;
            }
            staging_cursor_ids.push(min_cursor.id);
            min_cursor.next_key()?;
            drop(min_cursor);

            if staging_cursor_ids.len() >= self.sub_batch_size {
                flush_staging!();

                for cursor in cursors.values_mut() {
                    cursor.clear_finished_batches();
                }
            }
        }
        if !staging_cursor_ids.is_empty() {
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

#[derive(Clone)]
struct SortedBatches {
    sorter: Arc<ExternalSorter>,
    batches: Vec<RecordBatch>,
    keys: Vec<Box<[u8]>>,
    batches_num_rows: usize,
    batches_mem_size: usize,
    keys_mem_size: usize,
}

impl SortedBatches {
    fn new_empty(sorter: Arc<ExternalSorter>) -> Self {
        Self {
            sorter,
            batches: vec![],
            keys: vec![],
            batches_num_rows: 0,
            batches_mem_size: 0,
            keys_mem_size: 0,
        }
    }

    fn from_batch(sorter: Arc<ExternalSorter>, batch: RecordBatch) -> Result<Self> {

        // compute key cols
        let key_cols: Vec<ArrayRef> = sorter
            .exprs
            .iter()
            .map(|expr| expr.expr
                .evaluate(&batch)
                .map(|cv| cv.into_array(batch.num_rows())))
            .collect::<Result<_>>()?;

        // sort keys
        let (indices, keys): (Vec<u32>, Vec<Box<[u8]>>) = sorter.sort_row_converter
            .lock()
            .convert_columns(&key_cols)?
            .iter()
            .enumerate()
            .map(|(idx, row)| (idx as u32, Box::<[u8]>::from(row.as_ref())))
            .sorted_unstable_by(|(_, row1), (_, row2)| row1.cmp(row2))
            .take(sorter.limit)
            .unzip();

        let keys_mem_size = keys
            .iter()
            .map(|key| key.len())
            .sum::<usize>();

        // get sorted batch
        let indices = UInt32Array::from(indices);
        let batch = RecordBatch::try_new(
            batch.schema(),
            batch
                .columns()
                .iter()
                .map(|c| Ok(arrow::compute::take(&c, &indices, None)?))
                .collect::<Result<Vec<_>>>()?,
        )?;
        let batches_num_rows = batch.num_rows();
        let batches_mem_size = batch.get_array_memory_size();
        let batches = vec![batch];

        Ok(Self {
            sorter,
            batches,
            keys,
            batches_num_rows,
            batches_mem_size,
            keys_mem_size,
        })
    }

    fn mem_size(&self) -> usize {
        2 * (self.batches_mem_size + self.keys_mem_size + self.batches_num_rows * 8)
    }

    fn merge(&mut self, other: SortedBatches) -> Result<()> {
        let sorter = self.sorter.clone();
        let a = std::mem::replace(self, SortedBatches::new_empty(sorter));
        let b = other;

        let mut new = Self {
            sorter: a.sorter.clone(),
            batches: vec![],
            keys: vec![],
            batches_num_rows: 0,
            batches_mem_size: 0,
            keys_mem_size: 0
        };

        let mut merge_iter = Self::merge_into_iter(a, b);
        while let Some((batch, keys)) = merge_iter.next().transpose()? {
            new.batches_num_rows += batch.num_rows();
            new.batches_mem_size += batch.get_array_memory_size();
            new.keys_mem_size += keys.iter().map(|key| key.len()).sum::<usize>();
            new.batches.push(batch);
            new.keys.extend(keys);
        }
        *self = new;
        Ok(())
    }

    fn merge_into_iter(
        a: SortedBatches,
        b: SortedBatches,
    ) -> Box<dyn Iterator<Item = Result<(RecordBatch, Vec<Box<[u8]>>)>> + Send> {

        struct MergeCursor {
            empty_batch: RecordBatch,
            a_batches: VecDeque<RecordBatch>,
            b_batches: VecDeque<RecordBatch>,
            a_keys: VecDeque<Box<[u8]>>,
            b_keys: VecDeque<Box<[u8]>>,
            a_row_idx: usize,
            b_row_idx: usize,
            limit: usize,
            num_fetched: usize,
            batch_size: usize,
        }
        impl Iterator for MergeCursor {
            type Item = Result<(RecordBatch, Vec<Box<[u8]>>)>;

            fn next(&mut self) -> Option<Self::Item> {
                if self.a_keys.is_empty() && self.b_keys.is_empty() {
                    return None;
                }
                if self.num_fetched >= self.limit {
                    self.a_batches.clear();
                    self.b_batches.clear();
                    self.a_keys.clear();
                    self.b_keys.clear();
                    return None;
                }

                let cur_batch_limit = self.batch_size.min(self.limit - self.num_fetched);
                let mut indices = vec![];
                let mut keys = vec![];
                let mut a_batch_idx = 0;
                let mut b_batch_idx = 0;
                let mut a_row_idx = self.a_row_idx;
                let mut b_row_idx = self.b_row_idx;

                // merge keys and get indices for interleaving
                while indices.len() < cur_batch_limit {
                    let key_a = self.a_keys.front();
                    let key_b = self.b_keys.front();
                    let cmp = match (&key_a, &key_b) {
                        (None, None) => break,
                        (_, None) => 0,
                        (None, _) => 1,
                        (Some(a), Some(b)) => (b < a) as i32,
                    };

                    if cmp == 0 {
                        indices.push((a_batch_idx * 2, a_row_idx));
                        keys.push(self.a_keys.pop_front().unwrap());
                        a_row_idx += 1;
                        if a_row_idx >= self.a_batches[a_batch_idx].num_rows() {
                            a_row_idx = 0;
                            a_batch_idx += 1;
                        }
                    } else {
                        indices.push((b_batch_idx * 2 + 1, b_row_idx));
                        keys.push(self.b_keys.pop_front().unwrap());
                        b_row_idx += 1;
                        if b_row_idx >= self.b_batches[b_batch_idx].num_rows() {
                            b_row_idx = 0;
                            b_batch_idx += 1;
                        }
                    }
                }
                indices.shrink_to_fit();
                keys.shrink_to_fit();

                // get sorted batches
                let mut interleaving = vec![];
                for batch_idx in 0..=std::cmp::max(a_batch_idx, b_batch_idx) {
                    for batch in [self.a_batches.get(batch_idx), self.b_batches.get(batch_idx)] {
                        interleaving.push(batch.unwrap_or(&self.empty_batch).clone());
                    }
                }
                let interleaver = BatchesInterleaver::new(self.empty_batch.schema(), &interleaving);
                let batch = match interleaver.interleave(&indices) {
                    Ok(batch) => batch,
                    Err(err) => return Some(Err(err)),
                };

                // adjust batch and rows indices
                while a_batch_idx > 0 {
                    self.a_batches.pop_front();
                    a_batch_idx -= 1;
                }
                while b_batch_idx > 0 {
                    self.b_batches.pop_front();
                    b_batch_idx -= 1;
                }
                self.num_fetched += batch.num_rows();
                self.a_row_idx = a_row_idx;
                self.b_row_idx = b_row_idx;
                return Some(Ok((batch, keys)));
            }
        }

        let sorter = a.sorter.clone();
        let mc = MergeCursor {
            empty_batch: RecordBatch::new_empty(sorter.input_schema.clone()),
            a_batches: a.batches.into(),
            b_batches: b.batches.into(),
            a_keys: a.keys.into(),
            b_keys: b.keys.into(),
            a_row_idx: 0,
            b_row_idx: 0,
            limit: sorter.limit,
            num_fetched: 0,
            batch_size: sorter.sub_batch_size,
        };
        Box::new(mc)
    }

    fn try_into_spill(self) -> Result<Option<Box<dyn Spill>>> {
        if self.batches_num_rows == 0 {
            return Ok(None);
        }

        let spill = try_new_spill()?;
        let mut writer = lz4_flex::frame::FrameEncoder::new(spill.get_buf_writer());
        let mut keys: VecDeque::<Box<[u8]>> = self.keys.into();

        // write batch1 + keys1, batch2 + keys2, ...
        for batch in self.batches {
            let mut buf = vec![];
            write_one_batch(&batch, &mut Cursor::new(&mut buf), false)?;
            writer.write_all(&buf)?;

            for _ in 0..batch.num_rows() {
                let key = keys.pop_front().unwrap();
                write_len(key.len(), &mut writer)?;
                writer.write_all(&key)?;
            }
        }
        writer.finish().map_err(|err| DataFusionError::Execution(format!("{}", err)))?;
        spill.complete()?;
        Ok(Some(spill))
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
    cur_key: Box<[u8]>,
    finished: bool,
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
            cur_key: Box::default(),
            finished: false,
        };
        iter.next_key()?; // load first record into current
        Ok(iter)
    }

    fn next_key(&mut self) -> Result<()> {
        assert!(
            !self.finished,
            "calling next_key() on finished sort spill cursor"
        );

        if self.cur_loaded_num_rows >= self.cur_batch_num_rows && !self.load_next_batch()? {
            return Ok(());
        }
        let sorted_row_len = read_len(&mut self.input)?;
        self.cur_key = read_bytes_slice(&mut self.input, sorted_row_len)?;
        self.cur_loaded_num_rows += 1;
        Ok(())
    }

    fn load_next_batch(&mut self) -> Result<bool> {
        if let Some(batch) = read_one_batch(
            &mut self.input,
            Some(self.sorter.input_schema.clone()),
            false,
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

fn max_level_id(levels: &[Option<SortedBatches>]) -> Option<usize> {
    levels
        .iter()
        .enumerate()
        .rev()
        .flat_map(|(id, level)| level.as_ref().map(|_| id))
        .next()
}

#[cfg(test)]
mod test {
    use crate::sort_exec::SortExec;
    use arrow::array::Int32Array;
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_eq;
    use datafusion::common::Result;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::PhysicalSortExpr;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::{common, ExecutionPlan};
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

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
    use arrow::compute::SortOptions;
    use arrow::record_batch::RecordBatch;
    use datafusion::common::{Result, ScalarValue};
    use datafusion::logical_expr::ColumnarValue;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::math_expressions::random;
    use datafusion::physical_expr::PhysicalSortExpr;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_ext_commons::concat_batches;
    use crate::common::memory_manager::MemManager;
    use crate::sort_exec::SortExec;

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

        let input = Arc::new(MemoryExec::try_new(&[batches.clone()], schema.clone(), None)?);
        let sort = Arc::new(SortExec::new(input, sort_exprs.clone(), None));
        let output = datafusion::physical_plan::collect(sort, task_ctx.clone()).await?;
        let a = concat_batches(&schema, &output, n)?;

        let input = Arc::new(MemoryExec::try_new(&[batches.clone()], schema.clone(), None)?);
        let sort = Arc::new(
            datafusion::physical_plan::sorts::sort::SortExec::new(sort_exprs.clone(), input)
        );
        let output = datafusion::physical_plan::collect(sort, task_ctx.clone()).await?;
        let b = concat_batches(&schema, &output, n)?;

        assert!(a == b);
        Ok(())
    }
}
