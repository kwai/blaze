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

use std::io::{BufReader, Read, Write};
use std::mem::size_of;
use std::sync::{Arc, Weak};

use arrow::row::RowConverter;
use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::metrics::BaselineMetrics;
use futures::lock::Mutex;
use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use lz4_flex::frame::FrameDecoder;

use datafusion_ext_commons::io::{read_bytes_slice, read_len, write_len};
use datafusion_ext_commons::loser_tree::LoserTree;

use crate::agg::agg_buf::AggBuf;
use crate::agg::agg_context::AggContext;
use crate::agg::AggRecord;
use crate::common::memory_manager::{MemConsumer, MemConsumerInfo, MemManager};
use crate::common::onheap_spill::{Spill, try_new_spill};
use crate::common::output::WrappedRecordBatchSender;

// reserve memory for each spill
// estimated size: bufread=64KB + lz4dec.src=64KB + lz4dec.dest=64KB
const SPILL_OFFHEAP_MEM_COST: usize = 200000;

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
            in_mem: Mutex::new(InMemTable {
                is_hash: true, // only the first im-mem table uses hash
                ..Default::default()
            }),
            spills: Mutex::default(),
            agg_ctx,
            context,
            metrics,
        }
    }

    pub async fn has_spill(&self) -> bool {
        !self.spills.lock().await.is_empty()
    }

    pub async fn update_in_mem(
        &self,
        process: impl FnOnce(&mut InMemTable) -> Result<()>,
    ) -> Result<()> {
        let mem_used = {
            let mut in_mem = self.in_mem.lock().await;
            process(&mut in_mem)?;
            in_mem.mem_used()
        };
        self.update_mem_used(mem_used).await?;
        Ok(())
    }

    pub async fn output(
        &self,
        mut grouping_row_converter: RowConverter,
        baseline_metrics: BaselineMetrics,
        sender: Arc<WrappedRecordBatchSender>,
    ) -> Result<()> {
        self.set_spillable(false);
        let mut timer = baseline_metrics.elapsed_compute().timer();

        let in_mem = std::mem::take(&mut *self.in_mem.lock().await);
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
                .grouping_mappings
                .into_iter()
                .map(|(_1, _2)| AggRecord::new(_1, _2))
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
                self.update_mem_used_with_diff(-(batch_mem_size as isize)).await?;
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
        let mut current_record: Option<AggRecord> = None;

        macro_rules! flush_staging {
            () => {{
                let mut records = std::mem::take(&mut staging_records);
                let batch = self
                    .agg_ctx
                    .convert_records_to_batch(&mut grouping_row_converter, &mut records)?;
                baseline_metrics.record_output(batch.num_rows());
                sender.send(Ok(batch), Some(&mut timer)).await;
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
                            agg.agg
                                .partial_merge(
                                    &mut current_record.agg_buf,
                                    &mut min_record.agg_buf,
                                    addrs,
                                )
                                .map_err(|err| {
                                    err.context("agg: executing partial_merge() error")
                                })?;
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

        spills.extend(std::mem::take(&mut *in_mem).try_into_spill()?);
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
#[derive(Default)]
pub struct InMemTable {
    is_hash: bool,
    grouping_mappings: HashMap<Box<[u8]>, AggBuf>,
    unsorted: Vec<AggRecord>,
    data_mem_used: usize,
}

impl InMemTable {
    pub fn mem_used(&self) -> usize {
        // includes amortized new size
        // hash table is first transformed to sorted table
        self.data_mem_used
            + self.unsorted.len() * 2 * size_of::<AggRecord>()
            + self.grouping_mappings.len()
                * (
                    1 // one-byte payload per entry according to hashbrown's doc
                    + size_of::<AggRecord>() // hashmap entries
                    + size_of::<AggRecord>()
                    // hashmap is sorted into vec during spill
                )
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

    fn try_into_spill(self) -> Result<Option<Box<dyn Spill>>> {
        if self.grouping_mappings.is_empty() && self.unsorted.is_empty() {
            return Ok(None);
        }

        let spill = try_new_spill()?;
        let mut writer = lz4_flex::frame::FrameEncoder::new(spill.get_buf_writer());
        let mut sorted = self.into_rev_sorted_vec();

        while let Some(mut record) = sorted.pop() {
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
    current: Option<AggRecord>,
}
impl SpillCursor {
    fn try_from_spill(spill: &Box<dyn Spill>, agg_ctx: &Arc<AggContext>) -> Result<Self> {
        let buf_reader = spill.get_buf_reader();
        let mut iter = SpillCursor {
            agg_ctx: agg_ctx.clone(),
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
