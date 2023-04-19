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

use std::any::Any;
use std::fmt::Formatter;
use std::io::{BufReader, Cursor, Read, Write};
use std::sync::{Arc, Weak};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};
use async_trait::async_trait;
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{StreamExt, TryStreamExt, TryFutureExt};
use futures::lock::Mutex;
use futures::stream::once;
use itertools::Itertools;
use lz4_flex::frame::FrameDecoder;
use parking_lot::{Mutex as SyncMutex};
use datafusion_ext_commons::io::{read_bytes_slice, read_len, read_one_batch, write_len, write_one_batch};
use datafusion_ext_commons::loser_tree::LoserTree;
use datafusion_ext_commons::streams::coalesce_stream::CoalesceStream;
use crate::common::memory_manager::{MemConsumer, MemConsumerInfo, MemManager};
use crate::common::{BatchesInterleaver, output_with_sender, WrappedRecordBatchSender};
use crate::common::onheap_spill::OnHeapSpill;

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

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: children[0].clone(),
            exprs: self.exprs.clone(),
            fetch: self.fetch.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let input_schema = self.input.schema();
        let batch_size = context.session_config().batch_size();
        let squeeze_batch_size = batch_size / batch_size.ilog2() as usize;

        let sort_row_converter = RowConverter::new(
            self.exprs
                .iter()
                .map(|expr: &PhysicalSortExpr| Ok(
                    SortField::new_with_options(
                        expr.expr.data_type(&input_schema)?.clone(),
                        expr.options.clone(),
                    )
                ))
                .collect::<Result<Vec<SortField>>>()?
        )?;

        let external_sorter = Arc::new(ExternalSorter {
            name: format!("ExternalSorter[partition={}]", partition),
            mem_consumer_info: None,
            squeeze_batch_size,
            exprs: self.exprs.clone(),
            input_schema: self.schema(),
            limit: self.fetch.unwrap_or(usize::MAX),
            sort_row_converter: SyncMutex::new(sort_row_converter),
            levels: Mutex::new(vec![None; NUM_LEVELS]),
            spills: Default::default(),
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
        });
        MemManager::register_consumer(external_sorter.clone(), true);

        let input = self.input.execute(partition, context)?;
        let coalesced = Box::pin(CoalesceStream::new(
            input,
            batch_size,
            BaselineMetrics::new(&self.metrics, partition)
                .elapsed_compute()
                .clone(),
        ));

        let output = Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            once(external_sort(coalesced, external_sorter)).try_flatten(),
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

    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "SortExec: {}", self.exprs
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(", "))
            }
        }
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}

struct ExternalSorter {
    name: String,
    mem_consumer_info: Option<Weak<MemConsumerInfo>>,
    squeeze_batch_size: usize,
    exprs: Vec<PhysicalSortExpr>,
    input_schema: SchemaRef,
    limit: usize,
    sort_row_converter: SyncMutex<RowConverter>,
    levels: Mutex<Vec<Option<SortedBatches>>>,
    spills: Mutex<Vec<OnHeapSpill>>,
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
        &self.mem_consumer_info.as_ref().expect("consumer info not set")
    }

    async fn spill(&self) -> Result<()> {
        let mut levels = self.levels.lock().await;

        // merge all batches in levels into one in_mem_batches
        let mut in_mem_batches: Option<SortedBatches> = None;
        for level in std::mem::replace(&mut *levels, vec![None; NUM_LEVELS]) {
            if let Some(existed) = level {
                match &mut in_mem_batches {
                    Some(in_mem_batches) => in_mem_batches.merge(existed),
                    None => in_mem_batches = Some(existed),
                }
            }
        }

        if let Some(in_mem_batches) = in_mem_batches {
            self.spills.lock().await.extend(
                in_mem_batches.try_into_spill(self.squeeze_batch_size)?
            );
        }
        drop(levels);
        self.update_mem_used(0).await?;
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
    sorter: Arc<ExternalSorter>,
) -> Result<SendableRecordBatchStream> {

    // insert and sort
    while let Some(batch) = input.next().await.transpose()? {
        sorter
            .insert_batch(batch)
            .await
            .map_err(|err| err.context("sort: executing insert_batch() error"))?;
    }

    output_with_sender("Sort", input.schema(), |sender| async move {
        sorter
            .output(sender)
            .await
            .map_err(|err| err.context("sort: executing output() error"))?;
        Ok(())
    })
}

impl ExternalSorter {
    async fn insert_batch(self: &Arc<Self>, batch: RecordBatch) -> Result<()> {
        let _timer = self.baseline_metrics.elapsed_compute().timer();

        // create a sorted batches containing the single input batch
        let mut sorted_batches = SortedBatches::from_batch(self.clone(), batch)?;

        // merge sorted batches into levels
        let mem_used = {
            let mut levels = self.levels.lock().await;
            let mut cur_level = 0;
            while let Some(existed) = std::mem::take(&mut levels[cur_level]) {
                // merge and squeeze
                sorted_batches.merge(existed);
                if sorted_batches.batches_num_rows > self.limit * 2 {
                    sorted_batches.squeeze(self.squeeze_batch_size)?;
                }
                cur_level += 1;
            }
            levels[cur_level] = Some(sorted_batches);

            // adjust memory usage
            levels.iter().flatten().map(|b| b.mem_size()).sum::<usize>()
        };
        self.update_mem_used(mem_used).await?;
        Ok(())
    }

    async fn output(
        self: Arc<Self>,
        sender: WrappedRecordBatchSender,
    ) -> Result<()> {
        let mut timer = self.baseline_metrics.elapsed_compute().timer();
        self.set_spillable(false);
        let levels = std::mem::take(&mut *self.levels.lock().await);
        let spills = std::mem::take(&mut *self.spills.lock().await);

        log::info!(
            "sort exec starts outputting with {} ({} spills)",
            self.name(),
            spills.len(),
        );

        // merge all batches in levels
        let mut in_mem_batches: Option<SortedBatches> = None;
        for level in levels {
            if let Some(existed) = level {
                match &mut in_mem_batches {
                    Some(in_mem_batches) => in_mem_batches.merge(existed),
                    None => in_mem_batches = Some(existed),
                }
            }
        }
        if let Some(in_mem_batches) = &mut in_mem_batches {
            in_mem_batches.squeeze(self.squeeze_batch_size)?;
            self.update_mem_used(in_mem_batches.mem_size()).await?;
        }

        // no spills -- output in-mem batches
        if spills.is_empty() {
            if let Some(in_mem_batches) = in_mem_batches {
                let batches = in_mem_batches.batches;
                self.update_mem_used(batches
                    .iter()
                    .map(|batch| batch.get_array_memory_size())
                    .sum()).await?;

                for batch in batches {
                    let batch_mem_size = batch.get_array_memory_size();

                    timer.stop();
                    self.baseline_metrics.record_output(batch.num_rows());
                    sender
                        .send(Ok(batch))
                        .map_err(|err| DataFusionError::Execution(format!("{:?}", err)))
                        .await?;

                    timer.restart();
                    self.update_mem_used_with_diff(-(batch_mem_size as isize)).await?;
                }
            }
            self.update_mem_used(0).await?;
            return Ok(());
        }

        // move in-mem batches into spill, so we can free memory as soon as possible
        let mut spills = spills;
        if let Some(in_mem_batches) = in_mem_batches {
            if let Some(in_mem_spill) =
                in_mem_batches.try_into_spill(self.squeeze_batch_size)?
            {
                spills.push(in_mem_spill);
            }

            // adjust mem usage
            self.update_mem_used(spills.len() * SPILL_OFFHEAP_MEM_COST).await?;
        }

        // use loser tree to merge all spills
        let mut cursors: LoserTree<SpillCursor> = LoserTree::new_by(
            spills
                .iter()
                .enumerate()
                .map(|(id, spill)| {
                    SpillCursor::try_from_spill(id, self.clone(), spill.clone())
                })
                .collect::<Result<_>>()?,
            |c1, c2| {
                let key1 = (c1.finished, &c1.cur_key);
                let key2 = (c2.finished, &c2.cur_key);
                key1 < key2
            });

        let mut num_total_output_rows = 0;
        let mut staging_cursor_ids = Vec::with_capacity(self.squeeze_batch_size);


        macro_rules! flush_staging {
            () => {{
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
                let batch = BatchesInterleaver::new(self.input_schema.clone(), &batches)
                    .interleave(&staging_indices)?;
                timer.stop();

                self.baseline_metrics.record_output(batch.num_rows());
                sender
                    .send(Ok(batch))
                    .map_err(|err| DataFusionError::Execution(format!("{:?}", err)))
                    .await?;
                timer.restart();
            }}
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
            num_total_output_rows += 1;

            if staging_cursor_ids.len() >= self.squeeze_batch_size {
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
        self.baseline_metrics.record_spill(spill_disk_usage as usize);
        self.update_mem_used(0).await?;
        Ok(())
    }
}

#[derive(Default, Clone)]
struct IndexedRow {
    row: Box<[u8]>,
    batch_idx: u32,
    row_idx: u32,
}

impl IndexedRow {
    fn new(row: Box<[u8]>, batch_idx: usize, row_idx: usize) -> Self {
        Self {
            row,
            batch_idx: batch_idx as u32,
            row_idx: row_idx as u32,
        }
    }

    fn set_batch_idx(&mut self, batch_index: usize) {
        self.batch_idx = batch_index as u32;
    }

    fn set_row_idx(&mut self, row_index: usize) {
        self.row_idx = row_index as u32;
    }
    fn batch_idx(&self) -> usize {
        self.batch_idx as usize
    }

    fn row_idx(&self) -> usize {
        self.row_idx as usize
    }
}

#[derive(Clone)]
struct SortedBatches {
    sorter: Arc<ExternalSorter>,
    batches: Vec<RecordBatch>,
    sorted_rows: Vec<IndexedRow>,
    batches_num_rows: usize,
    batches_mem_size: usize,
    row_mem_size: usize,
    squeezed: bool,
}

impl SortedBatches {
    fn new_empty(sorter: Arc<ExternalSorter>) -> Self {
        Self {
            sorter,
            batches: vec![],
            sorted_rows: vec![],
            batches_num_rows: 0,
            batches_mem_size: 0,
            row_mem_size: 0,
            squeezed: false,
        }
    }

    fn from_batch(
        sorter: Arc<ExternalSorter>,
        batch: RecordBatch,
    ) -> Result<Self> {

        let batches_num_rows = batch.num_rows();
        let batches_mem_size = batch.get_array_memory_size();

        let rows: Rows = sorter.sort_row_converter
            .lock()
            .convert_columns(&sorter.exprs
                .iter()
                .map(|expr| {
                    expr.expr.evaluate(&batch).map(|cv| cv.into_array(batch.num_rows()))
                })
                .collect::<Result<Vec<_>>>()?
            )?;

        let mut row_mem_size = 0;
        let sorted_rows: Vec<IndexedRow> = rows
            .iter()
            .enumerate()
            .sorted_unstable_by(|(_, r1), (_, r2)| r1.cmp(r2))
            .take(sorter.limit)
            .map(|(row_idx, row)| {
                let row: Box<[u8]> = row.as_ref().into();
                row_mem_size += row.len();
                IndexedRow::new(row, 0, row_idx)
            })
            .collect();

        let batches = vec![batch];
        let squeezed = false;

        Ok(Self {
            sorter,
            batches,
            sorted_rows,
            batches_num_rows,
            batches_mem_size,
            row_mem_size,
            squeezed,
        })
    }

    fn mem_size(&self) -> usize {
        self.batches_mem_size * 2 + // batches are duplicated during squeezing
            self.sorted_rows.len() * 2 * std::mem::size_of::<IndexedRow>() +
            self.row_mem_size
    }

    fn merge(&mut self, other: SortedBatches) {
        let mut a = std::mem::replace(
            self,
            SortedBatches::new_empty(self.sorter.clone()),
        );
        let mut b = other;

        let num_batches_a = a.batches.len();
        let limit = self.sorter.limit;
        let num_rows_a = a.sorted_rows.len().min(limit);
        let num_rows_b = b.sorted_rows.len().min(limit);
        let num_rows_output = (num_rows_a + num_rows_b).min(limit);
        let batches_mem_size = a.batches_mem_size + b.batches_mem_size;

        let mut sorted_rows = Vec::with_capacity(num_rows_output);
        let mut cur_a = 0;
        let mut cur_b = 0;
        let mut row_mem_size = 0;

        while sorted_rows.len() < num_rows_output {
            match (a.sorted_rows.get_mut(cur_a), b.sorted_rows.get_mut(cur_b)) {
                (Some(row_a), None) => {
                    row_mem_size += row_a.row.len();
                    sorted_rows.push(IndexedRow::new(
                        std::mem::take(&mut row_a.row),
                        row_a.batch_idx(),
                        row_a.row_idx(),
                    ));
                    cur_a += 1;
                }
                (None, Some(row_b)) => {
                    row_mem_size += row_b.row.len();
                    sorted_rows.push(IndexedRow::new(
                        std::mem::take(&mut row_b.row),
                        row_b.batch_idx() + num_batches_a,
                        row_b.row_idx(),
                    ));
                    cur_b += 1;
                }
                (Some(row_a), Some(row_b)) => {
                    if row_a.row < row_b.row {
                        row_mem_size += row_a.row.len();
                        sorted_rows.push(IndexedRow::new(
                            std::mem::take(&mut row_a.row),
                            row_a.batch_idx(),
                            row_a.row_idx(),
                        ));
                        cur_a += 1;
                    } else {
                        row_mem_size += row_b.row.len();
                        sorted_rows.push(IndexedRow::new(
                            std::mem::take(&mut row_b.row),
                            row_b.batch_idx() + num_batches_a,
                            row_b.row_idx(),
                        ));
                        cur_b += 1;
                    }
                }
                (None, None) => unreachable!()
            }
        }

        let batches_num_rows = a.batches_num_rows + b.batches_num_rows;
        let batches = [a.batches, b.batches].concat();
        let squeezed = false;

        *self = SortedBatches {
            sorter: self.sorter.clone(),
            batches,
            sorted_rows,
            batches_num_rows,
            batches_mem_size,
            row_mem_size,
            squeezed,
        };
    }

    fn squeeze(&mut self, batch_size: usize) -> Result<()> {
        if self.squeezed {
            return Ok(());
        }
        let mut squeezed = vec![];
        let interleaver =
            BatchesInterleaver::new(self.batches[0].schema(), &self.batches);

        for chunk in self.sorted_rows.chunks_mut(batch_size) {
            let batch_idx = squeezed.len();
            let indices = chunk
                .iter()
                .map(|row: &IndexedRow| (row.batch_idx(), row.row_idx()))
                .collect::<Vec<_>>();
            squeezed.push(interleaver.interleave(&indices)?);

            for (row_idx, row) in chunk.iter_mut().enumerate() {
                row.set_batch_idx(batch_idx);
                row.set_row_idx(row_idx);
            }
        }
        self.batches_num_rows = self.sorted_rows.len();
        self.batches_mem_size = squeezed
            .iter()
            .map(|batch| batch.get_array_memory_size())
            .sum();
        self.batches = squeezed;
        Ok(())
    }

    fn try_into_spill(
        mut self,
        squeeze_batch_size: usize,
    ) -> Result<Option<OnHeapSpill>> {

        if self.batches_num_rows == 0 {
            return Ok(None);
        }
        self.squeeze(squeeze_batch_size)?;

        let spill = OnHeapSpill::try_new()?;
        let mut writer = lz4_flex::frame::FrameEncoder::new(spill.get_buf_writer());
        let mut cur_rows = 0;

        // write batch1 + rows1, batch2 + rows2, ...
        for batch in self.batches {
            let mut buf = vec![];
            write_one_batch(&batch, &mut Cursor::new(&mut buf), false)?;
            writer.write_all(&buf)?;

            for row in &self.sorted_rows[cur_rows..][..batch.num_rows()] {
                write_len(row.row.len(), &mut writer)?;
                writer.write_all(&row.row)?;
            }
            cur_rows += batch.num_rows();
        }

        writer.finish().map_err(|err| {
            DataFusionError::Execution(format!("{}", err))
        })?;
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
        spill: OnHeapSpill,
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
        assert!(!self.finished, "calling next_key() on finished sort spill cursor");

        if self.cur_loaded_num_rows >= self.cur_batch_num_rows {
            if !self.load_next_batch()? {
                return Ok(());
            }
        }
        let sorted_row_len = read_len(&mut self.input)?;
        self.cur_key = read_bytes_slice(&mut self.input, sorted_row_len)?;
        self.cur_loaded_num_rows += 1;
        Ok(())
    }

    fn load_next_batch(&mut self) -> Result<bool> {
        if let Some(batch) = read_one_batch(
            &mut self.input, Some(self.sorter.input_schema.clone()), false)?
        {
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

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use arrow::array::Int32Array;
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_eq;
    use datafusion::common::Result;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::PhysicalSortExpr;
    use datafusion::physical_plan::{common, ExecutionPlan};
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::prelude::SessionContext;
    use crate::sort_exec::SortExec;

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
        let sort_exprs = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: SortOptions::default(),
            }
        ];

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