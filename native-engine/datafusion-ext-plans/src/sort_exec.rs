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
    collections::{vec_deque::VecDeque, HashSet},
    fmt::Formatter,
    io::{Cursor, Read, Write},
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc, Weak,
    },
    pin::Pin,
};

use arrow::{
    array::ArrayRef,
    datatypes::{Schema, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
    row::{RowConverter, Rows, SortField},
};
use async_trait::async_trait;
use bytesize::ByteSize;
use datafusion::{
    common::{utils::proxy::VecAllocExt, DataFusionError, Result, Statistics},
    execution::context::TaskContext,
    physical_expr::{
        expressions::Column, EquivalenceProperties, PhysicalExprRef, PhysicalSortExpr,
    },
    physical_plan::{
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
        DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, ExecutionPlanProperties,
        PlanProperties, SendableRecordBatchStream,
    },
};
use datafusion_ext_commons::{
    algorithm::loser_tree::{ComparableForLoserTree, LoserTree},
    arrow::{
        array_size::BatchSize,
        selection::{create_batch_interleaver, take_batch, BatchInterleaver},
    },
    compute_suggested_batch_size_for_kway_merge, compute_suggested_batch_size_for_output,
    io::{read_len, read_one_batch, write_len, write_one_batch},
};
use futures::StreamExt;
use itertools::Itertools;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use futures::stream::BoxStream;

use crate::{
    common::{
        column_pruning::ExecuteWithColumnPruning,
        execution_context::{ExecutionContext, WrappedRecordBatchSender},
        timer_helper::TimerHelper,
    },
    memmgr::{
        spill::{try_new_spill, Spill, SpillCompressedReader, SpillCompressedWriter},
        MemConsumer, MemConsumerInfo, MemManager,
    },
};

// reserve memory for each spill
// estimated size: bufread=64KB + lz4dec.src=64KB + lz4dec.dest=64KB
const SPILL_OFFHEAP_MEM_COST: usize = 200000;

// max number of sorted batches to merge
const NUM_MAX_MERGING_BATCHES: usize = 32;

pub static SORT_EXEC_KEY_ROWS_CONVERT_COUNT: AtomicUsize = AtomicUsize::new(0);

trait RowConverterExt {
    fn convert_columns_with_metrics(&self, columns: &[arrow::array::ArrayRef], caller: &'static str) -> arrow::error::Result<arrow::row::Rows>;
}

impl RowConverterExt for arrow::row::RowConverter {
    fn convert_columns_with_metrics(&self, columns: &[arrow::array::ArrayRef], caller: &'static str) -> arrow::error::Result<arrow::row::Rows> {
        if caller == "SortExec" {
            SORT_EXEC_KEY_ROWS_CONVERT_COUNT.fetch_add(1, SeqCst);
        }
        self.convert_columns(columns)
    }
}

/// Trait for outputting key rows along with RecordBatch
pub trait KeyRowsOutput {
    fn key_exprs(&self) -> Vec<PhysicalExprRef>;
    fn execute_with_key_rows_output(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> BoxStream<'static, Result<(RecordBatch, Rows)>>;
    fn as_any(&self) -> &dyn Any;
}

#[derive(Debug)]
pub struct SortExec {
    input: Arc<dyn ExecutionPlan>,
    exprs: Vec<PhysicalSortExpr>,
    fetch: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    record_output: bool,
    props: OnceCell<PlanProperties>,
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
            record_output: true,
            props: OnceCell::new(),
        }
    }

    pub fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub fn create_default_ascending_sort_exec(
    input: Arc<dyn ExecutionPlan>,
    key_exprs: &[PhysicalExprRef],
    execution_plan_metrics: Option<ExecutionPlanMetricsSet>,
    record_output: bool,
) -> Arc<dyn ExecutionPlan> {
    let mut sort_exec = SortExec::new(
        input,
        key_exprs
            .iter()
            .map(|e| PhysicalSortExpr {
                expr: e.clone(),
                options: Default::default(),
            })
            .collect(),
        None,
    );
    if let Some(execution_plan_metrics) = execution_plan_metrics {
        sort_exec.metrics = execution_plan_metrics;
        sort_exec.record_output = record_output;
    }
    Arc::new(sort_exec)
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
    fn name(&self) -> &str {
        "SortExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        self.props.get_or_init(|| {
            PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                self.input.output_partitioning().clone(),
                ExecutionMode::Bounded,
            )
        })
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.exprs.clone(),
            self.fetch,
        )))
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
        Statistics::with_fetch(self.input.statistics()?, self.schema(), self.fetch, 0, 1)
    }
}

impl KeyRowsOutput for SortExec {
    fn key_exprs(&self) -> Vec<PhysicalExprRef> {
        self.exprs.iter().map(|e| e.expr.clone()).collect()
    }

    fn execute_with_key_rows_output(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> BoxStream<'static, Result<(RecordBatch, Rows)>> {
        use std::sync::Arc;
        use futures::stream;

        let projection: Vec<usize> = (0..self.schema().fields().len()).collect();
        let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);
        let input = exec_ctx.execute_projected(&self.input, &projection).unwrap();
        let prune_sort_keys_from_batch = Arc::new(PruneSortKeysFromBatch::try_new(
            self.input.schema(),
            &projection,
            &self.exprs,
        ).unwrap());

        let stream = input;
        let s = stream::unfold((stream, prune_sort_keys_from_batch), |(mut stream, pruner)| async move {
            match stream.next().await {
                Some(Ok(batch)) => {
                    let (key_rows, pruned_batch) = pruner.prune(batch).unwrap();
                    Some((Ok((pruned_batch, key_rows)), (stream, pruner)))
                }
                Some(Err(e)) => Some((Err(e), (stream, pruner))),
                None => None,
            }
        });
        Box::pin(s)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct LevelSpill {
    block: SpillSortedBlock,
    level: usize,
}

struct ExternalSorter {
    exec_ctx: Arc<ExecutionContext>,
    mem_consumer_info: Option<Weak<MemConsumerInfo>>,
    weak: Weak<Self>,
    prune_sort_keys_from_batch: Arc<PruneSortKeysFromBatch>,
    limit: usize,
    record_output: bool,
    in_mem_blocks: Arc<Mutex<Vec<InMemSortedBlock>>>,
    spills: Arc<Mutex<Vec<LevelSpill>>>,
    num_total_rows: AtomicUsize,
    mem_total_size: AtomicUsize,
}

impl ExternalSorter {
    fn sub_batch_size(&self) -> usize {
        compute_suggested_batch_size_for_kway_merge(self.mem_total_size(), self.num_total_rows())
    }

    fn output_batch_size(&self) -> usize {
        compute_suggested_batch_size_for_output(self.mem_total_size(), self.num_total_rows())
    }

    fn pruned_schema(&self) -> SchemaRef {
        self.prune_sort_keys_from_batch.pruned_schema()
    }
}

#[async_trait]
impl MemConsumer for ExternalSorter {
    fn name(&self) -> &str {
        "ExternalSorter"
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
        let spills = self.spills.clone();
        let blocks = std::mem::take(&mut *self.in_mem_blocks.lock());
        let self_arc = self.to_arc();

        tokio::task::spawn_blocking(move || {
            let mut spills = spills.lock();
            let spill = try_new_spill(self_arc.exec_ctx.spill_metrics())?;
            let merged_block = merge_blocks::<_, SqueezeKeyCollector>(
                self_arc.clone(),
                blocks,
                SpillSortedBlockBuilder::new(self_arc.pruned_schema(), spill),
            )?;
            spills.push(LevelSpill {
                block: merged_block,
                level: 0,
            });

            // merge if there are too many spills
            let mut levels = std::mem::take(&mut *spills)
                .into_iter()
                .sorted_unstable_by_key(|spill| spill.level)
                .chunk_by(|spill| spill.level)
                .into_iter()
                .map(|(_, spills)| {
                    spills
                        .into_iter()
                        .map(|spill| spill.block)
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            levels.push(vec![]); // add a new level for possible merging target

            for level in 0..levels.len() {
                if levels[level].len() >= NUM_MAX_MERGING_BATCHES {
                    let spill = try_new_spill(self_arc.exec_ctx.spill_metrics())?;
                    let merged = merge_blocks::<_, SqueezeKeyCollector>(
                        self_arc.clone(),
                        std::mem::take(&mut levels[level]),
                        SpillSortedBlockBuilder::new(self_arc.pruned_schema(), spill),
                    )?;
                    levels[level + 1].push(merged);
                } else {
                    spills.extend(
                        std::mem::take(&mut levels[level])
                            .into_iter()
                            .map(|block| LevelSpill { block, level }),
                    );
                }
            }
            Ok::<_, DataFusionError>(())
        })
            .await
            .expect("tokio error")?;

        self.update_mem_used(0).await?;
        Ok(())
    }
}

trait SortedBlock: Send {
    fn is_single_batch(&self) -> bool;
    fn next_batch(&mut self) -> Result<Option<RecordBatch>>;
    fn next_key(&mut self) -> Result<()>;
    fn cur_key(&self) -> &[u8];
    fn is_equal_to_prev_key(&self) -> bool;
    fn mem_used(&self) -> usize;
}

struct InMemSortedBlock {
    sorted_keys: VecDeque<InMemRowsKeyCollector>,
    sorted_batches: VecDeque<RecordBatch>,
    mem_used: usize,
    cur_row_idx: usize,
}

impl SortedBlock for InMemSortedBlock {
    fn is_single_batch(&self) -> bool {
        self.sorted_batches.len() == 1
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let batch = self.sorted_batches.pop_front();
        if let Some(batch) = batch {
            self.mem_used -= batch.get_batch_mem_size();
            self.mem_used -= self.sorted_keys[0].mem_size();
            self.sorted_keys.pop_front().unwrap();
            self.cur_row_idx = usize::MAX;
            Ok(Some(batch))
        } else {
            Ok(None)
        }
    }

    fn next_key(&mut self) -> Result<()> {
        self.cur_row_idx += 1;
        Ok(())
    }

    fn cur_key(&self) -> &[u8] {
        self.sorted_keys[0].key(self.cur_row_idx)
    }

    fn is_equal_to_prev_key(&self) -> bool {
        self.cur_row_idx > 0 && {
            let key0 = self.sorted_keys[0].key(self.cur_row_idx - 1);
            let key1 = self.sorted_keys[0].key(self.cur_row_idx);
            key0 == key1
        }
    }

    fn mem_used(&self) -> usize {
        self.mem_used
    }
}

struct SpillSortedBlock {
    pruned_schema: SchemaRef,
    spill: Box<dyn Spill>,
    spill_reader: SpillCompressedReader<'static>,
    cur_key_reader: SortedKeysReader,
}

impl SortedBlock for SpillSortedBlock {
    fn is_single_batch(&self) -> bool {
        false
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if let Some((num_rows, cols)) = read_one_batch(&mut self.spill_reader, &self.pruned_schema)?
        {
            let batch = RecordBatch::try_new_with_options(
                self.pruned_schema.clone(),
                cols,
                &RecordBatchOptions::new().with_row_count(Some(num_rows)),
            )?;
            Ok(Some(batch))
        } else {
            Ok(None)
        }
    }

    fn next_key(&mut self) -> Result<()> {
        Ok(self.cur_key_reader.next_key(&mut self.spill_reader)?)
    }

    fn cur_key(&self) -> &[u8] {
        &self.cur_key_reader.cur_key
    }

    fn is_equal_to_prev_key(&self) -> bool {
        self.cur_key_reader.is_equal_to_prev
    }

    fn mem_used(&self) -> usize {
        0
    }
}

trait SortedBlockBuilder<B: SortedBlock, KS: KeyCollector> {
    fn add_batch_and_keys(&mut self, batch: RecordBatch, keys: KS) -> Result<()>;
    fn finish(self) -> Result<B>;
}

#[derive(Default)]
struct InMemSortedBlockBuilder {
    sorted_keys: VecDeque<InMemRowsKeyCollector>,
    sorted_batches: VecDeque<RecordBatch>,
    mem_used: usize,
}

impl SortedBlockBuilder<InMemSortedBlock, InMemRowsKeyCollector> for InMemSortedBlockBuilder {
    fn add_batch_and_keys(
        &mut self,
        batch: RecordBatch,
        keys: InMemRowsKeyCollector,
    ) -> Result<()> {
        self.mem_used += batch.get_batch_mem_size();
        self.mem_used += keys.mem_size();
        self.sorted_keys.push_back(keys);
        self.sorted_batches.push_back(batch);
        Ok(())
    }

    fn finish(mut self) -> Result<InMemSortedBlock> {
        self.sorted_keys
            .push_front(InMemRowsKeyCollector::default()); // for first pop
        Ok(InMemSortedBlock {
            sorted_keys: self.sorted_keys,
            sorted_batches: self.sorted_batches,
            cur_row_idx: 0,
            mem_used: self.mem_used,
        })
    }
}

struct SpillSortedBlockBuilder {
    pruned_schema: SchemaRef,
    spill: Box<dyn Spill>,
    spill_writer: SpillCompressedWriter<'static>,
}

impl SpillSortedBlockBuilder {
    fn new(pruned_schema: SchemaRef, mut spill: Box<dyn Spill>) -> Self {
        let spill_writer = unsafe {
            // safety: bypass lifetime check, spill writer has the same lifetime as spill
            std::mem::transmute(spill.get_compressed_writer())
        };
        Self {
            pruned_schema,
            spill,
            spill_writer,
        }
    }
}

impl SortedBlockBuilder<SpillSortedBlock, SqueezeKeyCollector> for SpillSortedBlockBuilder {
    fn add_batch_and_keys(&mut self, batch: RecordBatch, keys: SqueezeKeyCollector) -> Result<()> {
        write_one_batch(batch.num_rows(), batch.columns(), &mut self.spill_writer)?;
        self.spill_writer.write_all(&keys.store)?;
        Ok(())
    }

    fn finish(self) -> Result<SpillSortedBlock> {
        let spill = self.spill;
        self.spill_writer.finish()?;

        let spill_reader = unsafe {
            // safety: bypass lifetime check, spill reader has the same lifetime as spill
            std::mem::transmute(spill.get_compressed_reader())
        };
        Ok(SpillSortedBlock {
            pruned_schema: self.pruned_schema,
            spill,
            spill_reader,
            cur_key_reader: SortedKeysReader::default(),
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
        let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);

        // if no sort key expr is specified, just forward the input
        if self.exprs.is_empty() {
            return exec_ctx.execute_projected(&self.input, projection);
        }

        let prune_sort_keys_from_batch = Arc::new(PruneSortKeysFromBatch::try_new(
            self.input.schema(),
            projection,
            &self.exprs,
        )?);
        let mut sorter = Arc::new(ExternalSorter {
            exec_ctx: exec_ctx.clone(),
            mem_consumer_info: None,
            weak: Weak::new(),
            prune_sort_keys_from_batch,
            limit: self.fetch.unwrap_or(usize::MAX),
            record_output: self.record_output,
            in_mem_blocks: Default::default(),
            spills: Default::default(),
            num_total_rows: Default::default(),
            mem_total_size: Default::default(),
        });
        MemManager::register_consumer(sorter.clone(), true);

        unsafe {
            // safety: set weak reference to sorter
            let weak = Arc::downgrade(&sorter);
            let sorter_mut = Arc::get_mut_unchecked(&mut sorter);
            sorter_mut.weak = weak;
        }

        let elapsed_compute = exec_ctx.baseline_metrics().elapsed_compute().clone();
        let input = exec_ctx.execute_with_input_stats(&self.input)?;
        let mut coalesced = exec_ctx.coalesce_with_default_batch_size(input);

        let output = exec_ctx
            .clone()
            .output_with_sender("Sort", move |sender| async move {
                let _timer = elapsed_compute.timer();
                sender.exclude_time(&elapsed_compute);

                while let Some(batch) = elapsed_compute
                    .exclude_timer_async(coalesced.next())
                    .await
                    .transpose()?
                {
                    sorter.insert_batch(batch).await?;
                }
                sorter.output(sender).await?;
                Ok(())
            });
        Ok(exec_ctx.coalesce_with_default_batch_size(output))
    }
}

impl Drop for ExternalSorter {
    fn drop(&mut self) {
        MemManager::deregister_consumer(self);
    }
}

impl ExternalSorter {
    async fn insert_batch(self: &Arc<Self>, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.num_total_rows.fetch_add(batch.num_rows(), SeqCst);
        self.mem_total_size
            .fetch_add(batch.get_batch_mem_size(), SeqCst);

        let (keys, batch) = self.prune_sort_keys_from_batch.prune(batch)?;
        let sorted_indices: Vec<u32> = (0..batch.num_rows() as u32)
            .sorted_unstable_by_key(|&row_idx| unsafe {
                // safety: bypass boundary and lifetime checking
                std::mem::transmute::<_, &'static [u8]>(
                    keys.row_unchecked(row_idx as usize).as_ref(),
                )
            })
            .take(self.limit)
            .collect();

        // build keys
        let mut key_collector = InMemRowsKeyCollector::default();
        key_collector.reserve(keys.num_rows(), keys.size());
        for &row_idx in &sorted_indices {
            key_collector.add_key(keys.row(row_idx as usize).as_ref());
        }
        key_collector.freeze();

        // build batch
        let sorted_batch = if !self.prune_sort_keys_from_batch.is_all_pruned() {
            take_batch(batch, sorted_indices)?
        } else {
            create_zero_column_batch(batch.num_rows())
        };

        // add to in-mem blocks
        let mut block_builder = InMemSortedBlockBuilder::default();
        block_builder.add_batch_and_keys(sorted_batch, key_collector)?;
        self.in_mem_blocks.lock().push(block_builder.finish()?);

        let mem_used = self.mem_used();
        self.update_mem_used(mem_used).await?;
        Ok(())
    }

    async fn output(self: &Arc<Self>, sender: Arc<WrappedRecordBatchSender>) -> Result<()> {
        // if external merging is required, we need to spill in-mem data
        // to free memory as soon as possible
        let has_spill = !self.spills.lock().is_empty();
        let has_in_mem_data = !self.in_mem_blocks.lock().is_empty();
        if has_spill && has_in_mem_data {
            log::info!(
                "{} spills rest in-mem data to disk, size={}",
                self.name(),
                ByteSize(self.mem_used() as u64),
            );
            self.force_spill().await?;
        }
        self.set_spillable(false);

        let output_batch_size = self.output_batch_size();
        let spills = self.spills.lock().drain(..).collect::<Vec<_>>();
        log::info!(
            "{} starts outputting ({} spills + in_mem: {})",
            self.name(),
            spills.len(),
            ByteSize(self.mem_used() as u64)
        );

        // no spills -- output in-mem batches
        if spills.is_empty() {
            let in_mem_blocks = std::mem::take(&mut *self.in_mem_blocks.lock());
            if !in_mem_blocks.is_empty() {
                let mut merger = Merger::try_new(self.clone(), in_mem_blocks)?;
                while let Some((key_collector, pruned_batch)) =
                    merger.next::<InMemRowsKeyCollector>(output_batch_size)?
                {
                    let batch = self
                        .prune_sort_keys_from_batch
                        .restore(pruned_batch, key_collector)?;
                    if self.record_output {
                        self.exec_ctx
                            .baseline_metrics()
                            .record_output(batch.num_rows());
                    }
                    sender.send(batch).await;
                }
            }
            self.update_mem_used(0).await?;
            return Ok(());
        }

        let spill_blocks = spills.into_iter().map(|spill| spill.block).collect();
        let mut merger = Merger::try_new(self.to_arc(), spill_blocks)?;
        while let Some((key_collector, pruned_batch)) =
            merger.next::<InMemRowsKeyCollector>(output_batch_size)?
        {
            let batch = self
                .prune_sort_keys_from_batch
                .restore(pruned_batch, key_collector)?;
            let cursors_mem_used = merger.cursors_mem_used();

            self.update_mem_used(cursors_mem_used).await?;
            if self.record_output {
                self.exec_ctx
                    .baseline_metrics()
                    .record_output(batch.num_rows());
            }
            sender.send(batch).await;
        }
        self.update_mem_used(0).await?;
        Ok(())
    }

    fn to_arc(&self) -> Arc<Self> {
        self.weak.upgrade().expect("ExternalSorter.weak not set")
    }

    fn num_total_rows(&self) -> usize {
        self.num_total_rows.load(SeqCst)
    }

    fn mem_total_size(&self) -> usize {
        self.mem_total_size.load(SeqCst)
    }

    fn mem_used(&self) -> usize {
        let in_mem_blocks = self.in_mem_blocks.lock();
        in_mem_blocks
            .iter()
            .map(|block| block.mem_used())
            .sum::<usize>()
    }
}

struct SortedBlockCursor<B: SortedBlock> {
    id: usize,
    input: B,
    cur_batch_num_rows: usize,
    cur_batches: Vec<RecordBatch>,
    cur_batches_changed: bool,
    cur_key_row_idx: usize,
    cur_batch_idx: usize,
    cur_row_idx: usize,
    cur_mem_used: usize,
    finished: bool,
}

impl<B: SortedBlock> ComparableForLoserTree for SortedBlockCursor<B> {
    #[inline(always)]
    fn lt(&self, other: &Self) -> bool {
        if other.finished || self.finished {
            return other.finished;
        }
        self.input.cur_key() < other.input.cur_key()
    }
}

impl<B: SortedBlock> SortedBlockCursor<B> {
    fn try_from_block(id: usize, block: B) -> Result<Self> {
        let mut new = Self {
            id,
            input: block,
            cur_batch_num_rows: 0,
            cur_batches: vec![],
            cur_batches_changed: false,
            cur_key_row_idx: 0,
            cur_batch_idx: 0,
            cur_row_idx: 0,
            cur_mem_used: 0,
            finished: false,
        };
        new.next_key()?; // load first record
        Ok(new)
    }

    fn block(&self) -> &B {
        &self.input
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
        self.input.next_key()?;
        self.cur_key_row_idx += 1;
        Ok(())
    }

    fn load_next_batch(&mut self) -> Result<bool> {
        if let Some(batch) = self.input.next_batch()? {
            self.cur_batch_num_rows = batch.num_rows();
            self.cur_mem_used += batch.get_batch_mem_size();
            self.cur_batches.push(batch);
            self.cur_key_row_idx = 0;
            self.cur_batches_changed = true;
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
                self.cur_batches_changed = true;
                self.cur_mem_used -= batch.get_batch_mem_size();
            }
            self.cur_batch_idx = 0;
        }
    }
}

struct Merger<B: SortedBlock> {
    sorter: Arc<ExternalSorter>,
    cursors: LoserTree<SortedBlockCursor<B>>,
    batch_interleaver: BatchInterleaver,
    single_batch_mode: bool,
    batches_base_indices: Vec<usize>,
    num_total_output_rows: usize,
}

impl<B: SortedBlock> Merger<B> {
    fn try_new(sorter: Arc<ExternalSorter>, blocks: Vec<B>) -> Result<Self> {
        let single_batch_mode = blocks.iter().all(|block| block.is_single_batch());
        let cursors = LoserTree::new(
            blocks
                .into_iter()
                .enumerate()
                .map(|(id, block)| SortedBlockCursor::try_from_block(id, block))
                .collect::<Result<Vec<_>>>()?,
        );
        let batch_interleaver = if single_batch_mode {
            let batches = cursors
                .values()
                .iter()
                .map(|cursor| cursor.cur_batches[0].clone())
                .collect::<Vec<_>>();
            create_batch_interleaver(&batches, true)?
        } else {
            create_batch_interleaver(&[RecordBatch::new_empty(sorter.pruned_schema())], true)?
        };

        Ok(Self {
            sorter,
            cursors,
            batch_interleaver,
            single_batch_mode,
            batches_base_indices: vec![],
            num_total_output_rows: 0,
        })
    }

    fn cursors_mem_used(&self) -> usize {
        self.cursors.len() * SPILL_OFFHEAP_MEM_COST
            + self
            .cursors
            .values()
            .iter()
            .map(|cursor| cursor.cur_mem_used)
            .sum::<usize>()
    }

    fn next<KC: KeyCollector>(&mut self, batch_size: usize) -> Result<Option<(KC, RecordBatch)>> {
        let max_num_rows = batch_size.min(self.sorter.limit - self.num_total_output_rows);
        let mut num_rows = 0;

        // collect merged records to staging
        let mut min_cursor = self.cursors.peek_mut();
        let mut key_collector = KC::default();
        let mut cursor_ids = vec![];

        key_collector.reserve(num_rows, 0);
        while !min_cursor.finished && num_rows < max_num_rows {
            if !self.sorter.prune_sort_keys_from_batch.is_all_pruned() {
                cursor_ids.push(min_cursor.id);
            }
            key_collector.add_key(&min_cursor.block().cur_key());
            min_cursor.next_key()?;
            num_rows += 1;

            // fetch next min key from loser tree only if it is different from previous key
            if min_cursor.finished || !min_cursor.block().is_equal_to_prev_key() {
                min_cursor.adjust();
            }
        }

        if num_rows == 0 {
            return Ok(None);
        }
        key_collector.freeze();
        drop(min_cursor);

        // collect pruned columns
        let pruned_batch = if !self.sorter.prune_sort_keys_from_batch.is_all_pruned() {
            if !self.single_batch_mode
                && self
                .cursors
                .values()
                .iter()
                .any(|cursor| cursor.cur_batches_changed)
            {
                let mut batches_base_indices = vec![];
                let mut batches = vec![];
                for cursor in self.cursors.values_mut() {
                    batches_base_indices.push(batches.len());
                    batches.extend(cursor.cur_batches.clone());
                    cursor.cur_batches_changed = false;
                }
                self.batch_interleaver = create_batch_interleaver(&batches, true)?;
                self.batches_base_indices = batches_base_indices;
            }
            let batch_interleaver = &self.batch_interleaver;

            // interleave output batch
            let staging_indices = if !self.single_batch_mode {
                cursor_ids
                    .into_iter()
                    .map(|cursor_id| {
                        let cursor = &mut self.cursors.values_mut()[cursor_id];
                        let (batch_idx, row_idx) = cursor.next_row();
                        (self.batches_base_indices[cursor.id] + batch_idx, row_idx)
                    })
                    .collect::<Vec<_>>()
            } else {
                cursor_ids
                    .into_iter()
                    .map(|cursor_id| {
                        let cursor = &mut self.cursors.values_mut()[cursor_id];
                        let (_, row_idx) = cursor.next_row();
                        (cursor.id, row_idx)
                    })
                    .collect::<Vec<_>>()
            };
            batch_interleaver(&staging_indices)?
        } else {
            create_zero_column_batch(num_rows)
        };
        self.num_total_output_rows += num_rows;

        for cursor in self.cursors.values_mut() {
            cursor.clear_finished_batches();
        }
        Ok(Some((key_collector, pruned_batch)))
    }
}

fn merge_blocks<B: SortedBlock, KC: KeyCollector>(
    sorter: Arc<ExternalSorter>,
    blocks: Vec<impl SortedBlock>,
    mut block_builder: impl SortedBlockBuilder<B, KC>,
) -> Result<B> {
    assert!(blocks.len() >= 1);
    let sub_batch_size = sorter.sub_batch_size();
    let mut merger = Merger::try_new(sorter, blocks)?;
    while let Some((key_collector, pruned_batch)) = merger.next::<KC>(sub_batch_size)? {
        block_builder.add_batch_and_keys(pruned_batch, key_collector)?;
    }
    Ok(block_builder.finish()?)
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
    sort_row_converter: Arc<Mutex<RowConverter>>,
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
        let sort_row_converter = Arc::new(Mutex::new(RowConverter::new(
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
        let key_rows = self.sort_row_converter.lock().convert_columns_with_metrics(&key_cols, "SortExec")?;

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
        let num_rows = pruned_batch.num_rows();

        // restore key columns
        let key_rows = key_collector.into_rows(num_rows, &*self.sort_row_converter.lock())?;
        let key_cols = self.sort_row_converter.lock().convert_rows(&key_rows)?;

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
    fn reserve(&mut self, num_rows: usize, data_size: usize);
    fn add_key(&mut self, key: &[u8]);
    fn freeze(&mut self);
    fn mem_size(&self) -> usize;
    fn into_rows(self, num_rows: usize, row_converter: &RowConverter) -> Result<Rows>;
}

#[derive(Default)]
struct InMemRowsKeyCollector {
    buffer: Vec<u8>,
    offsets: Vec<usize>,
}

impl InMemRowsKeyCollector {
    fn key(&self, i: usize) -> &[u8] {
        unsafe {
            // safety: performance critical, bypass bounds check
            let start = *self.offsets.get_unchecked(i);
            let end = *self.offsets.get_unchecked(i + 1);
            self.buffer.get_unchecked(start..end)
        }
    }
}

impl KeyCollector for InMemRowsKeyCollector {
    fn reserve(&mut self, num_rows: usize, data_size: usize) {
        self.offsets.reserve(num_rows + 1);
        self.buffer.reserve(data_size);
    }

    fn add_key(&mut self, key: &[u8]) {
        self.offsets.push(self.buffer.len());
        self.buffer.extend_from_slice(key);
    }

    fn freeze(&mut self) {
        self.offsets.push(self.buffer.len());
        self.offsets.shrink_to_fit();
        self.buffer.shrink_to_fit();
    }

    fn mem_size(&self) -> usize {
        self.offsets.allocated_size() + self.buffer.allocated_size()
    }

    fn into_rows(self, num_rows: usize, row_converter: &RowConverter) -> Result<Rows> {
        assert_eq!(self.offsets.len() - 1, num_rows);
        let mut rows = row_converter.empty_rows(0, 0);
        unsafe {
            // safety: acces rows.buffer/offsets
            struct XRows {
                buffer: Vec<u8>,
                offsets: Vec<usize>,
            }
            let xrows = std::mem::transmute::<_, &mut XRows>(&mut rows);
            xrows.buffer = self.buffer;
            xrows.offsets = self.offsets;
        }
        Ok(rows)
    }
}

#[derive(Default)]
struct SqueezeKeyCollector {
    sorted_key_writer: SortedKeysWriter,
    store: Vec<u8>,
}

impl KeyCollector for SqueezeKeyCollector {
    fn reserve(&mut self, _num_rows: usize, _data_size: usize) {
        // do nothing because we cannot get squeezed data size at this moment
    }

    fn add_key(&mut self, key: &[u8]) {
        self.sorted_key_writer
            .write_key(key, &mut self.store)
            .unwrap();
    }

    fn freeze(&mut self) {
        self.store.shrink_to_fit();
    }

    fn mem_size(&self) -> usize {
        self.store.allocated_size()
    }

    fn into_rows(self, num_rows: usize, row_converter: &RowConverter) -> Result<Rows> {
        let mut sorted_key_reader = SortedKeysReader::default();
        let mut r = Cursor::new(self.store);
        let mut simple_key_collector = InMemRowsKeyCollector::default();

        for _ in 0..num_rows {
            sorted_key_reader.next_key(&mut r)?;
            simple_key_collector.add_key(&sorted_key_reader.cur_key);
        }
        simple_key_collector.into_rows(num_rows, row_converter)
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
            let write_buf = unsafe {
                // safety: bypass bounds check in performance critical code
                self.cur_key
                    .reserve(key.len().saturating_sub(self.cur_key.len()));
                self.cur_key.set_len(key.len());
                let write_buf = self
                    .cur_key
                    .get_unchecked_mut(prefix_len..key.len())
                    .as_mut();
                write_buf.copy_from_slice(key.get_unchecked(prefix_len..));
                write_buf
            };
            write_len(suffix_len + 1, w)?;
            write_len(prefix_len, w)?;
            w.write_all(write_buf)?;
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
            let read_buf = unsafe {
                // safety: bypass bounds check in performance critical code
                let new_key_len = prefix_len + suffix_len;
                self.cur_key
                    .reserve(new_key_len.saturating_sub(self.cur_key.len()));
                self.cur_key.set_len(new_key_len);
                self.cur_key
                    .get_unchecked_mut(prefix_len..new_key_len)
                    .as_mut()
            };
            r.read_exact(read_buf)?;
        } else {
            self.is_equal_to_prev = true;
        }
        Ok(())
    }
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let min_len = a.len().min(b.len());
    let mut lcp = 0;

    macro_rules! pread {
        ($v:expr, $off:expr, $nb:expr) => {{
            // safety: bypass bounds check in performance critical code
            unsafe { std::ptr::read_unaligned($v.as_ptr().add($off) as *const [u8; $nb]) }
        }};
    }

    while lcp + 8 < min_len && pread!(a, lcp, 8) == pread!(b, lcp, 8) {
        lcp += 8;
    }
    lcp += (lcp + 4 < min_len && pread!(a, lcp, 4) == pread!(b, lcp, 4)) as usize * 4;
    lcp += (lcp + 2 < min_len && pread!(a, lcp, 2) == pread!(b, lcp, 2)) as usize * 2;
    lcp += (lcp + 1 < min_len && pread!(a, lcp, 1) == pread!(b, lcp, 1)) as usize;
    lcp
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
    use std::{sync::Arc, time::Instant};

    use arrow::{
        array::{ArrayRef, StringArray, UInt32Array},
        compute::{concat_batches, SortOptions},
        record_batch::RecordBatch,
    };
    use datafusion::{
        common::{stats::Precision, Result},
        physical_expr::{expressions::Column, PhysicalSortExpr},
        physical_plan::{memory::MemoryExec, ExecutionPlan},
        prelude::{SessionConfig, SessionContext},
    };

    use crate::{memmgr::MemManager, sort_exec::SortExec};

    #[tokio::test]
    async fn fuzztest_in_mem_sorting() -> Result<()> {
        let time_start = Instant::now();
        fuzztest_with_mem_conf(1000000000).await?;
        eprintln!("fuzztest_in_mem_sorting_time: {:?}", time_start.elapsed());
        Ok(())
    }

    #[tokio::test]
    async fn fuzztest_external_sorting() -> Result<()> {
        let time_start = Instant::now();
        fuzztest_with_mem_conf(10000).await?;
        eprintln!("fuzztest_external_sorting_time: {:?}", time_start.elapsed());
        Ok(())
    }

    async fn fuzztest_with_mem_conf(mem: usize) -> Result<()> {
        MemManager::init(mem);
        let session_ctx =
            SessionContext::new_with_config(SessionConfig::new().with_batch_size(10000));
        let task_ctx = session_ctx.task_ctx();
        let n = 1234567;

        // generate random batch for fuzzying
        let mut batches = vec![];
        let mut num_rows = 0;
        while num_rows < n {
            let rand_key1: ArrayRef = Arc::new(
                std::iter::repeat_with(|| Some(format!("{}", rand::random::<u32>())))
                    .take((n - num_rows).min(10000))
                    .collect::<StringArray>(),
            );
            let rand_key2: ArrayRef = Arc::new(
                std::iter::repeat_with(|| rand::random::<u32>())
                    .take((n - num_rows).min(10000))
                    .collect::<UInt32Array>(),
            );
            let rand_val1: ArrayRef = Arc::new(
                std::iter::repeat_with(|| rand::random::<u32>())
                    .take((n - num_rows).min(10000))
                    .collect::<UInt32Array>(),
            );
            let rand_val2: ArrayRef = Arc::new(
                std::iter::repeat_with(|| rand::random::<u32>())
                    .take((n - num_rows).min(10000))
                    .collect::<UInt32Array>(),
            );
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
        let output = datafusion::physical_plan::collect(sort.clone(), task_ctx.clone()).await?;
        let a = concat_batches(&schema, &output)?;
        let a_row_count = sort.clone().statistics()?.num_rows;

        let input = Arc::new(MemoryExec::try_new(
            &[batches.clone()],
            schema.clone(),
            None,
        )?);
        let sort = Arc::new(datafusion::physical_plan::sorts::sort::SortExec::new(
            sort_exprs.clone(),
            input,
        ));
        let output = datafusion::physical_plan::collect(sort.clone(), task_ctx.clone()).await?;
        let b = concat_batches(&schema, &output)?;
        let b_row_count = sort.clone().statistics()?.num_rows;

        assert_eq!(a.num_rows(), b.num_rows());
        assert_eq!(a_row_count, b_row_count);
        assert_eq!(a_row_count, Precision::Exact(n));
        assert!(a == b);
        Ok(())
    }
}
