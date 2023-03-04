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

use std::io::{BufReader, BufWriter, Read, Write};
use std::mem::size_of;
use std::sync::Arc;

use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use arrow::row::RowConverter;
use async_trait::async_trait;
use bytesize::ByteSize;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_manager::ConsumerType;
use datafusion::execution::{MemoryConsumer, MemoryConsumerId, MemoryManager};
use datafusion::physical_plan::metrics::BaselineMetrics;
use futures::lock::Mutex;
use futures::TryFutureExt;
use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use lz4_flex::frame::FrameDecoder;
use tokio::sync::mpsc::Sender;

use datafusion_ext_commons::io::{read_bytes_slice, read_len, write_len};
use datafusion_ext_commons::loser_tree::LoserTree;

use crate::agg::agg_buf::AggBuf;
use crate::agg::agg_context::AggContext;
use crate::agg::AggRecord;
use crate::spill::{get_spills_disk_usage, OnHeapSpill};

// reserve memory for each spill
// estimated size: bufread=64KB + lz4dec.src=64KB + lz4dec.dest=64KB
const SPILL_OFFHEAP_MEM_COST: usize = 200000;

pub struct AggTables {
    in_mem: Mutex<InMemTable>,
    spills: Mutex<Vec<OnHeapSpill>>,
    id: MemoryConsumerId,
    agg_ctx: Arc<AggContext>,
    context: Arc<TaskContext>,
    metrics: BaselineMetrics,
}

impl AggTables {
    pub fn new(
        agg_ctx: Arc<AggContext>,
        partition_id: usize,
        metrics: BaselineMetrics,
        context: Arc<TaskContext>,
    ) -> Self {
        let tables = Self {
            in_mem: Mutex::new(InMemTable {
                is_hash: true, // only the first im-mem table uses hash
                ..Default::default()
            }),
            spills: Mutex::default(),
            id: MemoryConsumerId::new(partition_id),
            agg_ctx,
            context,
            metrics,
        };
        tables.context.runtime_env().register_requester(tables.id());
        tables
    }

    pub async fn update_in_mem(
        &self,
        process: impl FnOnce(&mut InMemTable) -> Result<()>,
    ) -> Result<()> {
        let mut in_mem = self.in_mem.lock().await;
        let old_mem_used = in_mem.mem_used();

        process(&mut in_mem)?;
        let new_mem_used = in_mem.mem_used();
        drop(in_mem);

        // NOTE: the memory usage is already changed before we call try_grow(),
        // so we first call grow() to increase memory usage, and then call
        // try_grow(0) to spill if necessary.
        if new_mem_used > old_mem_used {
            let mem_increased = new_mem_used - old_mem_used;
            self.grow(mem_increased);
            self.metrics.mem_used().add(mem_increased);
            self.try_grow(0).await?;
        } else if new_mem_used < old_mem_used {
            let mem_freed = old_mem_used - new_mem_used;
            self.shrink(mem_freed);
            self.metrics.mem_used().sub(mem_freed);
        }
        Ok(())
    }

    pub async fn output(
        &self,
        mut grouping_row_converter: RowConverter,
        baseline_metrics: BaselineMetrics,
        sender: Sender<ArrowResult<RecordBatch>>,
    ) -> Result<()> {
        let mut in_mem_locked = self.in_mem.lock().await;
        let mut spills_locked = self.spills.lock().await;
        let mut timer = baseline_metrics.elapsed_compute().timer();

        let batch_size = self.context.session_config().batch_size();
        let in_mem = std::mem::take(&mut *in_mem_locked);
        let spills = std::mem::take(&mut *spills_locked);
        log::info!("aggregate exec starts outputing with {} spills", spills.len());

        // only one in-mem table, directly output it
        if spills.is_empty() {
            let mut records = in_mem
                .grouping_mappings
                .into_iter()
                .map(|(_1, _2)| AggRecord::new(_1, _2))
                .collect::<Vec<_>>();

            while !records.is_empty() {
                let mut chunk =
                    records.split_off(records.len().saturating_sub(batch_size));
                records.shrink_to_fit();

                let batch = self
                    .agg_ctx
                    .convert_records_to_batch(&mut grouping_row_converter, &mut chunk)?;
                let batch_mem_size = batch.get_array_memory_size();

                timer.stop();
                baseline_metrics.record_output(batch.num_rows());
                sender
                    .send(Ok(batch))
                    .map_err(|err| DataFusionError::Execution(format!("{:?}", err)))
                    .await?;

                timer.restart();

                // free memory of the output batch
                let mem_freed = batch_mem_size.min(self.mem_used());
                self.metrics.mem_used().sub(mem_freed);
                self.shrink(mem_freed);
            }
            return Ok(());
        }

        // convert all tables into cursors
        let mut cursors = vec![];
        if in_mem.num_records() > 0 {
            // spill staging records
            if let Some(spill) = in_mem.try_into_spill()? {
                cursors.push(SpillCursor::try_from_spill(spill, &self.agg_ctx)?);
            }

            // adjust mem usage
            let cur_mem_used = spills.len() * SPILL_OFFHEAP_MEM_COST;
            match self.metrics.mem_used().value() {
                v if v > cur_mem_used => self.shrink(v - cur_mem_used),
                v if v < cur_mem_used => self.grow(cur_mem_used - v),
                _ => {}
            }
            self.metrics.mem_used().set(cur_mem_used);
        }
        for spill in spills {
            cursors.push(SpillCursor::try_from_spill(spill, &self.agg_ctx)?);
        }
        let mut staging_records = Vec::with_capacity(batch_size);
        let mut current_record: Option<AggRecord> = None;

        macro_rules! flush_staging {
            () => {{
                let mut records = std::mem::take(&mut staging_records);
                let batch = self.agg_ctx.convert_records_to_batch(
                    &mut grouping_row_converter,
                    &mut records,
                )?;
                timer.stop();

                baseline_metrics.record_output(batch.num_rows());
                sender
                    .send(Ok(batch))
                    .map_err(|err| DataFusionError::Execution(format!("{:?}", err)))
                    .await?;
                timer.restart();
            }};
        }

        // create a tournament loser tree to do the merging
        // the mem-table and at least one spill should be in the tree
        let mut cursors: LoserTree<SpillCursor> =
            LoserTree::new_by(cursors, |c1: &SpillCursor, c2: &SpillCursor| {
                match (c1.peek(), c2.peek()) {
                    (None, _) => false,
                    (_, None) => true,
                    (Some(c1), Some(c2)) => c1 < c2,
                }
            });
        assert!(cursors.len() > 0);

        loop {
            // extract min cursor with the loser tree
            let mut min_cursor = cursors.peek_mut();
            if min_cursor.peek().is_none() {
                // all cursors are finished
                break;
            }
            let mut min_record = min_cursor.pop()?.unwrap();

            // merge min record into current record
            match current_record.as_mut() {
                None => current_record = Some(min_record),
                Some(current_record) => {
                    if &min_record == current_record {
                        // update entry
                        for (idx, agg) in self.agg_ctx.aggs.iter().enumerate() {
                            let addr_offset = self.agg_ctx.agg_buf_addr_offsets[idx];
                            let addrs = &self.agg_ctx.agg_buf_addrs[addr_offset..];
                            agg.agg.partial_merge(
                                &mut current_record.agg_buf,
                                &mut min_record.agg_buf,
                                addrs,
                            )?;
                        }
                    } else {
                        let finished = std::mem::replace(current_record, min_record);
                        staging_records.push(finished);
                        if staging_records.len() >= batch_size {
                            flush_staging!();
                        }
                    }
                }
            }
        }
        if let Some(record) = current_record {
            staging_records.push(record);
        }
        if !staging_records.is_empty() {
            flush_staging!();
        }

        // update disk spill size
        let spill_ids = cursors
            .values()
            .iter()
            .map(|cursor| cursor.spill_id)
            .collect::<Vec<_>>();
        let spill_disk_usage = get_spills_disk_usage(&spill_ids)?;
        self.metrics.record_spill(spill_disk_usage as usize);

        let used = self.metrics.mem_used().set(0);
        self.shrink(used);
        Ok(())
    }
}

#[async_trait]
impl MemoryConsumer for AggTables {
    fn name(&self) -> String {
        "AggTables".to_string()
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
        if let Some(spill) = std::mem::take(&mut *self.in_mem.lock().await)
            .try_into_spill()?
        {
            self.spills.lock().await.push(spill);
            let freed = self.metrics.mem_used().set(0);

            log::info!(
                "agg tables spilled {}, {}",
                ByteSize(freed as u64),
                self.memory_manager(),
            );
            return Ok(freed);
        }
        Ok(0)
    }

    fn mem_used(&self) -> usize {
        self.metrics.mem_used().value()
    }
}

impl Drop for AggTables {
    fn drop(&mut self) {
        self.context
            .runtime_env()
            .drop_consumer(self.id(), self.mem_used());
    }
}

/// Unordered in-mem hash table which can be updated
#[derive(Default)]
pub struct InMemTable {
    is_hash: bool,
    grouping_mappings: HashMap<Box<[u8]>, AggBuf>,
    unsorted: Vec<AggRecord>,
    data_mem_used: usize,
}

impl InMemTable {
    pub fn mem_used(&self) -> usize {
        // TODO: use more precise mem_used calculation
        self.data_mem_used
            + size_of::<AggRecord>() * self.unsorted.capacity()
            + size_of::<AggRecord>() * self.grouping_mappings.capacity()
    }

    pub fn num_records(&self) -> usize {
        self.grouping_mappings.len() + self.unsorted.len()
    }

    pub fn with_entry_mut(
        &mut self,
        agg_ctx: &Arc<AggContext>,
        grouping_row: Box<[u8]>,
        fn_entry: impl FnOnce(&mut AggBuf) -> Result<()>,
    ) -> Result<()> {
        let grouping_row_mem_size = grouping_row.len();

        // get entry from hash/unsorted table
        let entry = if self.is_hash {
            match self.grouping_mappings.entry(grouping_row) {
                Entry::Occupied(e) => {
                    let e = e.into_mut();
                    self.data_mem_used -= e.mem_size();
                    e
                }
                Entry::Vacant(e) => {
                    self.data_mem_used += grouping_row_mem_size;
                    e.insert(agg_ctx.initial_agg_buf.clone())
                }
            }
        } else {
            self.data_mem_used += grouping_row_mem_size;
            self.unsorted.push(AggRecord::new(
                grouping_row,
                agg_ctx.initial_agg_buf.clone(),
            ));
            &mut self.unsorted.last_mut().unwrap().agg_buf
        };
        fn_entry(entry)?;
        self.data_mem_used += entry.mem_size();
        Ok(())
    }

    fn into_rev_sorted_vec(mut self) -> Vec<AggRecord> {
        let mut vec = if self.is_hash {
            self.grouping_mappings.shrink_to_fit();
            self.grouping_mappings
                .into_iter()
                .map(|(_1, _2)| AggRecord::new(_1, _2))
                .collect::<Vec<_>>()
        } else {
            self.unsorted.shrink_to_fit();
            self.unsorted
        };
        vec.sort_unstable_by(|_1, _2| _2.cmp(_1));
        vec
    }

    fn try_into_spill(self) -> Result<Option<OnHeapSpill>> {
        if self.grouping_mappings.is_empty() && self.unsorted.is_empty() {
            return Ok(None);
        }

        let spill = OnHeapSpill::try_new()?;
        let mut writer = lz4_flex::frame::FrameEncoder::new(
            BufWriter::with_capacity(65536, spill)
        );
        let mut sorted = self.into_rev_sorted_vec();

        while let Some(record) = sorted.pop() {
            // write grouping row
            write_len(record.grouping.as_ref().len() + 1, &mut writer)?;
            writer.write_all(record.grouping.as_ref())?;

            // write agg buf
            record.agg_buf.save(&mut writer)?;

            // release memory in time
            if sorted.len() + 1000 < sorted.capacity() {
                sorted.shrink_to_fit();
            }
        }
        write_len(0, &mut writer)?; // EOF

        let mut spill = writer
            .finish()
            .map_err(|err| DataFusionError::Execution(format!("{}", err)))?
            .into_inner()
            .map_err(|err| err.into_error())?;

        spill.complete()?;
        Ok(Some(spill))
    }
}

struct SpillCursor {
    agg_ctx: Arc<AggContext>,
    spill_id: i32,
    input: FrameDecoder<BufReader<Box<dyn Read + Send>>>,
    current: Option<AggRecord>,
}
impl SpillCursor {
    fn try_from_spill(spill: OnHeapSpill, agg_ctx: &Arc<AggContext>) -> Result<Self> {
        let spill_id = spill.id();
        let buf_reader = spill.into_buf_reader();
        let mut iter = SpillCursor {
            agg_ctx: agg_ctx.clone(),
            spill_id,
            input: FrameDecoder::new(buf_reader),
            current: None,
        };
        iter.pop()?; // load first record into current
        Ok(iter)
    }

    fn peek(&self) -> &Option<AggRecord> {
        &self.current
    }

    fn pop(&mut self) -> Result<Option<AggRecord>> {
        // read grouping
        let prefix = read_len(&mut self.input)?;
        if prefix == 0 {
            // EOF
            return Ok(std::mem::replace(&mut self.current, None));
        }
        let grouping_buf_len = prefix - 1;
        let grouping = read_bytes_slice(&mut self.input, grouping_buf_len)?;

        // read agg buf
        let mut agg_buf = self.agg_ctx.initial_agg_buf.clone();
        agg_buf.load(&mut self.input)?;

        Ok(std::mem::replace(
            &mut self.current,
            Some(AggRecord::new(grouping, agg_buf)),
        ))
    }
}
