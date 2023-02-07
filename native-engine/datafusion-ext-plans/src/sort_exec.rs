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
use std::io::{BufReader, BufWriter, Cursor, Read, Write};
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex as SyncMutex};
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{Field, SchemaRef};
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;
use arrow::row::{OwnedRow, RowConverter, Rows, SortField};
use async_trait::async_trait;
use bytesize::ByteSize;
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::execution::context::TaskContext;
use datafusion::execution::{MemoryConsumer, MemoryConsumerId, MemoryManager};
use datafusion::execution::memory_manager::ConsumerType;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{FutureExt, StreamExt, TryStreamExt, TryFutureExt};
use futures::lock::Mutex;
use futures::stream::once;
use itertools::Itertools;
use lz4_flex::frame::FrameDecoder;
use tokio::sync::mpsc::Sender;
use datafusion_ext_commons::io::{read_bytes_slice, read_len, read_one_batch, write_len, write_one_batch};
use datafusion_ext_commons::loser_tree::LoserTree;
use datafusion_ext_commons::streams::coalesce_stream::CoalesceStream;
use datafusion_ext_commons::streams::receiver_stream::ReceiverStream;
use crate::spill::{dump_spills_statistics, Spill};

const NUM_LEVELS: usize = 64;

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

        let input_row_converter = RowConverter::new(
            input_schema.fields
                .iter()
                .map(|field: &Field| SortField::new(field.data_type().clone()))
                .collect()
        )?;
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
            id: MemoryConsumerId::new(partition),
            context: context.clone(),
            batch_size,
            exprs: self.exprs.clone(),
            input_schema: self.schema(),
            limit: self.fetch.unwrap_or(usize::MAX),
            input_row_converter: SyncMutex::new(input_row_converter),
            sort_row_converter: SyncMutex::new(sort_row_converter),
            levels: Mutex::new(vec![None; NUM_LEVELS]),
            spills: Default::default(),
            metrics: self.metrics.clone(),
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
        });
        context.runtime_env().register_requester(external_sorter.id());

        let input = self.input.execute(partition, context)?;
        let coalesced = Box::pin(CoalesceStream::new(
            input,
            batch_size,
            BaselineMetrics::new(&self.metrics, partition)
                .elapsed_compute()
                .clone(),
        ));
        let stream = external_sort(coalesced, external_sorter);

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            once(stream).try_flatten(),
        )))
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
    id: MemoryConsumerId,
    context: Arc<TaskContext>,
    batch_size: usize,
    exprs: Vec<PhysicalSortExpr>,
    input_schema: SchemaRef,
    limit: usize,
    input_row_converter: SyncMutex<RowConverter>,
    sort_row_converter: SyncMutex<RowConverter>,
    levels: Mutex<Vec<Option<SortedBatches>>>,
    spills: Mutex<Vec<Spill>>,
    metrics: ExecutionPlanMetricsSet,
    baseline_metrics: BaselineMetrics,
}

impl Drop for ExternalSorter {
    fn drop(&mut self) {
        self.context
            .runtime_env()
            .drop_consumer(self.id(), self.mem_used());
    }
}

#[async_trait]
impl MemoryConsumer for ExternalSorter {
    fn name(&self) -> String {
        "ExternalSorter".to_string()
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
        let current_used = self.mem_used();
        log::info!(
            "external sorter start spilling, used={}, {}",
            ByteSize(current_used as u64),
            self.memory_manager(),
        );

        let mut levels = self.levels.lock().await;
        let mut spills = self.spills.lock().await;
        let mut freed = 0isize;

        // merge all batches in levels and spill into l1
        let mut in_mem_batches: Option<SortedBatches> = None;
        for level in std::mem::replace(&mut *levels, vec![None; NUM_LEVELS]) {
            if let Some(existed) = level {
                freed += existed.mem_size() as isize;
                match &mut in_mem_batches {
                    Some(in_mem_batches) => in_mem_batches.merge(existed),
                    None => in_mem_batches = Some(existed),
                }
            }
        }
        let in_mem_spill = in_mem_batches.unwrap().try_into_l1_spill(
            &self.input_schema,
            self.batch_size,
        )?;
        freed -= in_mem_spill.offheap_mem_size() as isize;
        log::info!(
            "external sorter spilled into memory, freed={}",
            ByteSize(freed as u64),
        );
        spills.push(in_mem_spill);

        // move L1 into L2/L3 if necessary
        while freed < current_used as isize / 2 {
            let max_spill_idx = spills
                .iter()
                .enumerate()
                .max_by_key(|(_, spill)| spill.offheap_mem_size())
                .unwrap()
                .0;
            let spill_count = spills.len();
            spills.swap(max_spill_idx, spill_count - 1);
            let max_spill = spills.pop().unwrap();
            let max_spill_mem_size = max_spill.offheap_mem_size();
            freed += max_spill_mem_size as isize;

            let spill = match max_spill.to_l2() {
                Ok(spill) => {
                    log::info!(
                        "external sorter spilled into L2: size={}",
                        ByteSize(max_spill_mem_size as u64),
                    );
                    spill
                }
                Err(DataFusionError::ResourcesExhausted(..)) => {
                    let spill = max_spill
                        .to_l3(&self.context.runtime_env().disk_manager)?;
                    log::info!(
                        "external sorter spilled into L3: size={}",
                        ByteSize(max_spill_mem_size as u64),
                    );
                    self.baseline_metrics.record_spill(max_spill_mem_size);
                    spill
                }
                Err(err) => {
                    return Err(err);
                }
            };
            spills.push(spill);
        }
        let freed = freed as usize;
        self.baseline_metrics.mem_used().sub(freed);
        Ok(freed)
    }

    fn mem_used(&self) -> usize {
        self.baseline_metrics.mem_used().value()
    }
}

async fn external_sort(
    mut input: SendableRecordBatchStream,
    sorter: Arc<ExternalSorter>,
) -> Result<SendableRecordBatchStream> {

    // insert and sort
    while let Some(batch) = input.next().await.transpose()? {
        sorter.insert_batch(batch).await?;
    }

    // output
    let (sender, receiver) = tokio::sync::mpsc::channel(2);
    let output_schema = sorter.input_schema.clone();
    let baseline_metrics = BaselineMetrics::new(&sorter.metrics, sorter.partition_id());

    let join_handle = tokio::task::spawn(async move {
        let err_sender = sender.clone();
        let result = AssertUnwindSafe(async move {
            sorter.output(sender).await.unwrap()
        })
        .catch_unwind()
        .await;

        if let Err(e) = result {
            let err_message = panic_message::panic_message(&e).to_owned();
            err_sender
                .send(Err(ArrowError::ExternalError(Box::new(
                    DataFusionError::Execution(err_message),
                ))))
                .await
                .unwrap();
        }
    });
    Ok(Box::pin(ReceiverStream::new(
        output_schema,
        receiver,
        baseline_metrics,
        vec![join_handle],
    )))
}

impl ExternalSorter {
    async fn insert_batch(self: &Arc<Self>, batch: RecordBatch) -> Result<()> {
        let _timer = self.baseline_metrics.elapsed_compute().timer();

        // create a sorted batches containing the single input batch
        let mut sorted_batches = SortedBatches::from_batch(self.clone(), batch)?;

        // merge sorted batches into levels
        let mut levels = self.levels.lock().await;
        let mut cur_level = 0;
        while let Some(existed) = std::mem::take(&mut levels[cur_level]) {
            self.baseline_metrics.mem_used().sub(existed.mem_size());
            self.shrink(existed.mem_size());

            // merge and squeeze
            sorted_batches.merge(existed);
            if sorted_batches.batches_num_rows > self.limit * 2 {
                sorted_batches.squeeze(self.batch_size)?;
            }
            cur_level += 1;
        }
        let used = sorted_batches.mem_size();
        levels[cur_level] = Some(sorted_batches);
        drop(levels);

        self.try_grow(used).await?; // trigger spill if necessary
        self.baseline_metrics.mem_used().add(used);
        Ok(())
    }

    async fn output(self: Arc<Self>, sender: Sender<ArrowResult<RecordBatch>>) -> Result<()> {
        let mut timer = self.baseline_metrics.elapsed_compute().timer();
        let mut levels = self.levels.lock().await;

        // merge all batches in levels
        let mut in_mem_batches: Option<SortedBatches> = None;
        for level in std::mem::replace(&mut *levels, vec![None; NUM_LEVELS]) {
            if let Some(existed) = level {
                self.baseline_metrics.mem_used().sub(existed.mem_size());
                self.shrink(existed.mem_size());

                match &mut in_mem_batches {
                    Some(in_mem_batches) => in_mem_batches.merge(existed),
                    None => in_mem_batches = Some(existed),
                }
            }
        }
        if let Some(in_mem_batches) = &mut in_mem_batches {
            in_mem_batches.squeeze(self.batch_size)?;
            self.grow(in_mem_batches.mem_size());
            self.baseline_metrics.mem_used().add(in_mem_batches.mem_size());
        };

        drop(levels);
        let mut spills = self.spills.lock().await;

        log::info!(
            "sort exec starts outputing with spills: {}",
            dump_spills_statistics(&*spills),
        );

        // no spills -- output in-mem batches
        if spills.is_empty() {
            if let Some(in_mem_batches) = in_mem_batches {
                for batch in in_mem_batches.batches {
                    let batch = RecordBatch::try_new(
                        self.input_schema.clone(),
                        batch,
                    )?;
                    timer.stop();
                    sender
                        .send(Ok(batch))
                        .map_err(|err| DataFusionError::Execution(format!("{:?}", err)))
                        .await?;
                    timer.restart();
                }
                self.shrink(self.baseline_metrics.mem_used().set(0));
            }
            return Ok(());
        }

        // move in-mem batches into l1-spill, so we can free memory as soon as possible
        if let Some(in_mem_batches) = in_mem_batches {
            let in_mem_size = in_mem_batches.mem_size();
            let in_mem_spill = in_mem_batches.try_into_l1_spill(
                &self.input_schema,
                self.batch_size,
            )?;
            self.grow(in_mem_spill.offheap_mem_size());
            self.baseline_metrics.mem_used().add(in_mem_spill.offheap_mem_size());
            self.baseline_metrics.mem_used().sub(in_mem_size);
            self.shrink(in_mem_size);
            spills.push(in_mem_spill);
        }

        // use loser tree to merge all spills
        let mut cursors: LoserTree<SpillCursor> = LoserTree::new_by(
            std::mem::take(&mut *spills)
                .into_iter()
                .map(|spill| SpillCursor::try_from_spill(self.clone(), spill))
                .collect::<Result<_>>()?,
            |c1, c2| {
                let key1 = (c1.finished, &c1.current_key);
                let key2 = (c2.finished, &c2.current_key);
                key1 < key2
            });

        let mut num_total_output_rows = 0;
        let mut staging_rows = Vec::with_capacity(self.batch_size);

        macro_rules! flush_staging {
            () => {{
                let output_rows = std::mem::take(&mut staging_rows);
                staging_rows.reserve(self.batch_size);

                let batch = RecordBatch::try_new(
                    self.input_schema.clone(),
                    self.input_row_converter
                        .lock()
                        .unwrap()
                        .convert_rows(output_rows.iter().map(|r| r.row()))?,
                )?;
                timer.stop();
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
            staging_rows.push(min_cursor.pop()?.unwrap());
            num_total_output_rows += 1;

            if staging_rows.len() >= self.batch_size {
                flush_staging!();
            }
        }
        if !staging_rows.is_empty() {
            flush_staging!();
        }
        self.shrink(self.baseline_metrics.mem_used().set(0));
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
    batches: Vec<Vec<ArrayRef>>,
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

        let mut batches_mem_size = 0;
        let rows: Rows = sorter.sort_row_converter
            .lock()
            .unwrap()
            .convert_columns(&sorter.exprs
                .iter()
                .map(|expr| expr.expr
                    .evaluate(&batch)
                    .map(|cv| {
                        let array = cv.into_array(batch.num_rows());
                        batches_mem_size += array.get_array_memory_size();
                        array
                    }))
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

        let batches_num_rows = batch.num_rows();
        let batches = vec![batch.columns().iter().cloned().collect()];
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
        // some fields are doubled for squeezing/spilling
        std::mem::size_of_val(self) +
            self.batches_mem_size * 2 +
            self.row_mem_size +
            self.sorted_rows.capacity() * std::mem::size_of::<IndexedRow>() * 2
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
        let mut arrays = vec![vec![]; self.batches[0].len()];
        for batch in &self.batches {
            for (i, array) in batch.iter().enumerate() {
                arrays[i].push(array.as_ref());
            }
        }

        let mut squeezed = vec![];
        let mut batches_mem_size = 0;

        for chunk in self.sorted_rows.chunks_mut(batch_size) {
            let batch_idx = squeezed.len();
            let indices = chunk
                .iter()
                .map(|row: &IndexedRow| (row.batch_idx(), row.row_idx()))
                .collect::<Vec<_>>();

            let arrays = arrays
                .iter()
                .map(|values| arrow::compute::interleave(values, &indices))
                .collect::<ArrowResult<Vec<_>>>()?;
            batches_mem_size += arrays
                .iter()
                .map(|array| array.get_array_memory_size())
                .sum::<usize>();
            squeezed.push(arrays);

            for (row_idx, row) in chunk.iter_mut().enumerate() {
                row.set_batch_idx(batch_idx);
                row.set_row_idx(row_idx);
            }
        }
        self.batches_num_rows = self.sorted_rows.len();
        self.batches_mem_size = batches_mem_size;
        self.batches = squeezed;
        Ok(())
    }

    fn try_into_l1_spill(
        mut self,
        schema: &SchemaRef,
        batch_size: usize,
    ) -> Result<Spill> {

        self.squeeze(batch_size)?;

        let spilled_buf = vec![];
        let zwriter = lz4_flex::frame::FrameEncoder::new(BufWriter::with_capacity(
            65536,
            spilled_buf,
        ));
        let mut writer = BufWriter::with_capacity(65536, zwriter);
        let mut cur_rows = 0;

        // write batch1 + rows1, batch2 + rows2, ...
        write_len(self.batches.len(), &mut writer)?;
        for batch in self.batches {
            let mut buf = vec![];
            let batch = RecordBatch::try_new(
                schema.clone(),
                batch,
            )?;
            write_one_batch(&batch, &mut Cursor::new(&mut buf), false)?;
            writer.write_all(&buf)?;

            for _ in 0..batch.num_rows() {
                let row = &self.sorted_rows[cur_rows];
                cur_rows += 1;
                write_len(row.row.len(), &mut writer)?;
                writer.write_all(&row.row)?;
            }
        }

        let zwriter = writer.into_inner().map_err(|err| err.into_error())?;
        let spilled_buf = zwriter
            .finish()
            .map_err(|err| DataFusionError::Execution(format!("{}", err)))?
            .into_inner()
            .map_err(|err| err.into_error())?;
        Ok(Spill::new_l1(spilled_buf))
    }
}

struct SpillCursor {
    sorter: Arc<ExternalSorter>,
    input: BufReader<FrameDecoder<BufReader<Box<dyn Read + Send>>>>,
    num_batches: usize,
    current_batch_idx: usize,
    current_rows: Option<Rows>,
    current_row_idx: usize,
    current_row: Option<OwnedRow>,
    current_key: Box<[u8]>,
    finished: bool,
}

impl SpillCursor {
    fn try_from_spill(sorter: Arc<ExternalSorter>, spill: Spill) -> Result<Self> {
        let buf_reader = spill.into_buf_reader();
        let mut iter = SpillCursor {
            sorter,
            input: BufReader::with_capacity(65536, FrameDecoder::new(buf_reader)),
            num_batches: 0,
            current_batch_idx: 0,
            current_rows: None,
            current_row_idx: 0,
            current_key: Box::default(),
            current_row: None,
            finished: false,
        };
        iter.num_batches = read_len(&mut iter.input)?;
        iter.pop()?; // load first record into current
        Ok(iter)
    }

    fn pop(&mut self) -> Result<Option<OwnedRow>> {
        if !self.finished {
            if self.current_rows.is_none() { // load first batch
                self.next_batch()?;
            }

            if let Some(rows) = &self.current_rows { // always true
                if self.current_row_idx >= rows.num_rows() { // load next batch
                    if self.current_batch_idx == self.num_batches { // end?
                        self.current_rows = None;
                        self.finished = true;
                        return Ok(std::mem::replace(&mut self.current_row, None));
                    }
                    self.next_batch()?;
                    return self.pop();
                }
                let sorted_row_len = read_len(&mut self.input)?;
                self.current_key = read_bytes_slice(&mut self.input, sorted_row_len)?;
                let row = rows.row(self.current_row_idx).owned();
                self.current_row_idx += 1;
                return Ok(std::mem::replace(&mut self.current_row, Some(row)));
            }
        }
        return Ok(None);
    }

    fn next_batch(&mut self) -> Result<()> {
        let batch = read_one_batch(&mut self.input, None, false)?.unwrap();
        let rows = self.sorter.input_row_converter
            .lock()
            .unwrap()
            .convert_columns(batch.columns())?;
        self.current_rows = Some(rows);
        self.current_row_idx = 0;
        self.current_batch_idx += 1;
        Ok(())
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