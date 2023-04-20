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

//! Defines the sort-based shuffle writer

use crate::common::memory_manager::{MemConsumer, MemConsumerInfo, MemManager};
use crate::common::onheap_spill::OnHeapSpill;
use crate::shuffle::{evaluate_hashes, evaluate_partition_ids, ShuffleRepartitioner, ShuffleSpill};
use arrow::array::*;
use arrow::datatypes::*;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::Partitioning;
use datafusion_ext_commons::array_builder::{builder_extend, make_batch, new_array_builders};
use datafusion_ext_commons::concat_batches;
use datafusion_ext_commons::io::write_one_batch;
use futures::lock::Mutex;
use itertools::Itertools;
use std::fs::{File, OpenOptions};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Weak};

pub struct BucketShuffleRepartitioner {
    name: String,
    mem_consumer_info: Option<Weak<MemConsumerInfo>>,
    output_data_file: String,
    output_index_file: String,
    buffered_partitions: Mutex<Vec<PartitionBuffer>>,
    spills: Mutex<Vec<ShuffleSpill>>,
    partitioning: Partitioning,
    num_output_partitions: usize,
    metrics: BaselineMetrics,
}

impl BucketShuffleRepartitioner {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        partition_id: usize,
        output_data_file: String,
        output_index_file: String,
        schema: SchemaRef,
        partitioning: Partitioning,
        metrics: BaselineMetrics,
        context: Arc<TaskContext>,
    ) -> Self {
        let num_output_partitions = partitioning.partition_count();
        let batch_size = context.session_config().batch_size();

        Self {
            name: format!("BucketShufflePartitioner[partition={}]", partition_id),
            mem_consumer_info: None,
            output_data_file,
            output_index_file,
            buffered_partitions: Mutex::new(
                (0..num_output_partitions)
                    .map(|_| PartitionBuffer::new(schema.clone(), batch_size))
                    .collect::<Vec<_>>(),
            ),
            spills: Mutex::new(vec![]),
            partitioning,
            num_output_partitions,
            metrics,
        }
    }
}

#[async_trait]
impl ShuffleRepartitioner for BucketShuffleRepartitioner {
    async fn insert_batch(&self, input: RecordBatch) -> Result<()> {
        // compute partition ids
        let num_output_partitions = self.num_output_partitions;
        let hashes = evaluate_hashes(&self.partitioning, &input)?;
        let partition_ids = evaluate_partition_ids(&hashes, num_output_partitions);

        // count each partition size
        let mut partition_counters = vec![0usize; num_output_partitions];
        for &partition_id in &partition_ids {
            partition_counters[partition_id as usize] += 1
        }

        // accumulate partition counters into partition ends
        let mut partition_ends = partition_counters;
        let mut accum = 0;
        partition_ends.iter_mut().for_each(|v| {
            *v += accum;
            accum = *v;
        });

        // calculate shuffled partition ids
        let mut shuffled_partition_ids = vec![0usize; input.num_rows()];
        for (index, &partition_id) in partition_ids.iter().enumerate().rev() {
            partition_ends[partition_id as usize] -= 1;
            let end = partition_ends[partition_id as usize];
            shuffled_partition_ids[end] = index;
        }

        // after calculating, partition ends become partition starts
        let mut partition_starts = partition_ends;
        partition_starts.push(input.num_rows());

        let mut mem_diff = 0;
        for (partition_id, (&start, &end)) in partition_starts
            .iter()
            .tuple_windows()
            .enumerate()
            .filter(|(_, (start, end))| start < end)
        {
            let mut buffered_partitions = self.buffered_partitions.lock().await;
            let output = &mut buffered_partitions[partition_id];

            if end - start < output.staging_size {
                mem_diff +=
                    output.append_rows(input.columns(), &shuffled_partition_ids[start..end])?;
            } else {
                // for bigger slice, we can use column based operation
                // to build batches and directly append to output.
                // so that we can get rid of column <-> row conversion.
                let indices = PrimitiveArray::from_iter(
                    shuffled_partition_ids[start..end]
                        .iter()
                        .map(|&idx| idx as u64),
                );
                let batch = RecordBatch::try_new(
                    input.schema(),
                    input
                        .columns()
                        .iter()
                        .map(|c| arrow::compute::take(c, &indices, None))
                        .collect::<ArrowResult<Vec<ArrayRef>>>()?,
                )?;
                mem_diff += output.append_batch(batch)?;
            }
            drop(buffered_partitions);
        }
        self.update_mem_used_with_diff(mem_diff).await?;
        Ok(())
    }

    async fn shuffle_write(&self) -> Result<()> {
        self.set_spillable(false);
        let spills = std::mem::take(&mut *self.spills.lock().await);
        let mut buffered_partitions = std::mem::take(&mut *self.buffered_partitions.lock().await);

        log::info!(
            "bucket partitioner start writing with {} ({} spills)",
            self.name(),
            spills.len(),
        );

        let mut output_batches: Vec<Vec<u8>> = vec![vec![]; self.num_output_partitions];
        for i in 0..self.num_output_partitions {
            buffered_partitions[i].flush()?;
            output_batches[i] = std::mem::take(&mut buffered_partitions[i].frozen);
        }

        let raw_spills: Vec<OnHeapSpill> = spills.iter().map(|spill| spill.spill.clone()).collect();

        let mut spill_readers = spills
            .into_iter()
            .map(|spill| (spill.spill.get_buf_reader(), spill.offsets))
            .collect::<Vec<_>>();

        let data_file = self.output_data_file.clone();
        let index_file = self.output_index_file.clone();
        let num_output_partitions = self.num_output_partitions;
        tokio::task::spawn_blocking(move || {
            let mut offsets = vec![0; num_output_partitions + 1];
            let mut output_data = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(data_file)?;

            for i in 0..num_output_partitions {
                offsets[i] = output_data.stream_position()?;
                output_data.write_all(&std::mem::take(&mut output_batches[i]))?;

                // append partition in each spills
                for (reader, offsets) in &mut spill_readers {
                    let length = offsets[i + 1] - offsets[i];
                    if length > 0 {
                        std::io::copy(&mut reader.take(length), &mut output_data)?;
                    }
                }
            }
            output_data.sync_data()?;
            output_data.flush()?;

            // add one extra offset at last to ease partition length computation
            offsets[num_output_partitions] = output_data.stream_position()?;
            let mut output_index = File::create(index_file)?;
            for offset in offsets {
                output_index.write_all(&(offset as i64).to_le_bytes()[..])?;
            }
            output_index.sync_data()?;
            output_index.flush()?;
            Ok::<(), DataFusionError>(())
        })
        .await
        .map_err(|e| DataFusionError::Execution(format!("shuffle write error: {:?}", e)))??;

        // update disk spill size
        let spill_disk_usage = raw_spills
            .iter()
            .map(|spill| spill.get_disk_usage().unwrap_or(0))
            .sum::<u64>();
        self.metrics.record_spill(spill_disk_usage as usize);
        self.update_mem_used(0).await?;
        Ok(())
    }
}

/// consumes buffered partitions and produces a spill
fn spill_buffered_partitions(
    buffered_partitions: &mut [PartitionBuffer],
    num_output_partitions: usize,
) -> Result<Option<ShuffleSpill>> {
    // no data to spill
    if buffered_partitions
        .iter()
        .all(|p| p.frozen.is_empty() && p.num_staging_rows == 0 && p.num_active_rows == 0)
    {
        return Ok(None);
    }

    let mut output_batches: Vec<Vec<u8>> = vec![vec![]; num_output_partitions];
    let spill = OnHeapSpill::try_new()?;
    let mut spill_writer = spill.get_buf_writer();

    for i in 0..num_output_partitions {
        buffered_partitions[i].flush()?;
        output_batches[i] = std::mem::take(&mut buffered_partitions[i].frozen);
    }

    let mut offsets = vec![0; num_output_partitions + 1];
    let mut pos = 0;
    for i in 0..num_output_partitions {
        offsets[i] = pos;
        pos += output_batches[i].len() as u64;
        spill_writer.write_all(&std::mem::take(&mut output_batches[i]))?;
    }
    drop(spill_writer);
    spill.complete()?;

    // add one extra offset at last to ease partition length computation
    offsets[num_output_partitions] = pos;
    Ok(Some(ShuffleSpill { spill, offsets }))
}

#[async_trait]
impl MemConsumer for BucketShuffleRepartitioner {
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
        let mut partitions = self.buffered_partitions.lock().await;
        let mut spills = self.spills.lock().await;

        spills.extend(spill_buffered_partitions(
            &mut partitions,
            self.num_output_partitions,
        )?);
        drop(spills);
        drop(partitions);

        self.update_mem_used(0).await?;
        Ok(())
    }
}

impl Drop for BucketShuffleRepartitioner {
    fn drop(&mut self) {
        MemManager::deregister_consumer(self);
    }
}

struct PartitionBuffer {
    schema: SchemaRef,
    frozen: Vec<u8>,
    staging: Vec<RecordBatch>,
    active: Vec<Box<dyn ArrayBuilder>>,
    active_slots_mem_size: usize,
    num_active_rows: usize,
    num_staging_rows: usize,
    batch_size: usize,
    staging_size: usize,
}

impl PartitionBuffer {
    fn new(schema: SchemaRef, batch_size: usize) -> Self {
        let staging_size = batch_size / (batch_size as f64 + 1.0).log2() as usize;
        Self {
            schema,
            frozen: vec![],
            staging: vec![],
            active: vec![],
            active_slots_mem_size: 0,
            num_active_rows: 0,
            num_staging_rows: 0,
            batch_size,
            staging_size,
        }
    }

    fn append_rows(&mut self, columns: &[ArrayRef], indices: &[usize]) -> Result<isize> {
        let mut mem_diff = 0;
        let mut start = 0;

        while start < indices.len() {
            // lazy init because some partition may be empty
            if self.active.is_empty() {
                self.active = new_array_builders(&self.schema, self.staging_size);
                if self.active_slots_mem_size == 0 {
                    self.active_slots_mem_size = self
                        .active
                        .iter()
                        .zip(self.schema.fields())
                        .map(|(_ab, field)| slot_size(self.staging_size, field.data_type()))
                        .sum::<usize>();
                }
                mem_diff += self.active_slots_mem_size as isize;
            }

            let extend_len =
                (indices.len() - start).min(self.staging_size.saturating_sub(self.num_active_rows));
            self.active
                .iter_mut()
                .zip(columns)
                .for_each(|(builder, column)| {
                    builder_extend(
                        builder.as_mut(),
                        column,
                        &indices[start..][..extend_len],
                        column.data_type(),
                    );
                });
            self.num_active_rows += extend_len;
            if self.num_active_rows >= self.staging_size {
                mem_diff += self.flush_to_staging()?;
            }
            start += extend_len;
        }
        Ok(mem_diff)
    }

    /// append a whole batch directly to staging
    /// this will break the appending order when mixing with append_rows(), but
    /// it does not affect the shuffle output result.
    fn append_batch(&mut self, batch: RecordBatch) -> Result<isize> {
        let mut mem_diff = batch.get_array_memory_size() as isize;
        self.num_staging_rows += batch.num_rows();
        self.staging.push(batch);

        // staging -> frozen
        if self.num_staging_rows >= self.batch_size {
            mem_diff += self.flush()?;
        }
        Ok(mem_diff)
    }

    /// flush active data into one staging batch
    fn flush_to_staging(&mut self) -> Result<isize> {
        if self.num_active_rows == 0 {
            return Ok(0);
        }
        let mut mem_diff = 0isize;

        // active -> staging
        let active = std::mem::take(&mut self.active);
        self.num_active_rows = 0;
        mem_diff -= self.active_slots_mem_size as isize;

        let staging_batch = make_batch(self.schema.clone(), active)?;
        mem_diff += self.append_batch(staging_batch)?;
        Ok(mem_diff)
    }

    /// flush all active and staging data into frozen bytes
    fn flush(&mut self) -> Result<isize> {
        let mut mem_diff = 0isize;

        if self.num_active_rows > 0 {
            mem_diff += self.flush_to_staging()?;
        }
        if self.staging.is_empty() {
            return Ok(mem_diff);
        }

        mem_diff -= self
            .staging
            .iter()
            .map(|batch| batch.get_array_memory_size() as isize)
            .sum::<isize>();
        let frozen_batch = concat_batches(
            &self.schema,
            &std::mem::take(&mut self.staging),
            self.num_staging_rows,
        )?;
        self.num_staging_rows = 0;

        let frozen_capacity_old = self.frozen.capacity();
        let mut cursor = Cursor::new(&mut self.frozen);
        cursor.seek(SeekFrom::End(0))?;
        write_one_batch(&frozen_batch, &mut cursor, true)?;

        mem_diff += (self.frozen.capacity() - frozen_capacity_old) as isize;
        Ok(mem_diff)
    }
}

fn slot_size(len: usize, data_type: &DataType) -> usize {
    match data_type {
        DataType::Null => 0,
        DataType::Boolean => len / 8,
        DataType::Int8 => len,
        DataType::Int16 => len * 2,
        DataType::Int32 => len * 4,
        DataType::Int64 => len * 8,
        DataType::UInt8 => len,
        DataType::UInt16 => len * 2,
        DataType::UInt32 => len * 4,
        DataType::UInt64 => len * 8,
        DataType::Float32 => len * 4,
        DataType::Float64 => len * 8,
        DataType::Date32 => len * 4,
        DataType::Date64 => len * 8,
        DataType::Timestamp(_, _) => len * 8,
        DataType::Time32(TimeUnit::Second) => len * 4,
        DataType::Time32(TimeUnit::Millisecond) => len * 4,
        DataType::Time64(TimeUnit::Microsecond) => len * 4,
        DataType::Time64(TimeUnit::Nanosecond) => len * 4,
        DataType::Binary => len * 4,
        DataType::LargeBinary => len * 8,
        DataType::Utf8 => len * 4,
        DataType::LargeUtf8 => len * 8,
        DataType::Decimal128(_, _) => len * 16,
        DataType::Dictionary(key_type, _) => match key_type.as_ref() {
            DataType::Int8 => len,
            DataType::Int16 => len * 2,
            DataType::Int32 => len * 4,
            DataType::Int64 => len * 8,
            DataType::UInt8 => len,
            DataType::UInt16 => len * 2,
            DataType::UInt32 => len * 4,
            DataType::UInt64 => len * 8,
            dt => unimplemented!(
                "dictionary key type not supported in shuffle write: {:?}",
                dt
            ),
        },
        DataType::List(fields) => len * slot_size(16, fields.data_type()),
        DataType::Map(field, _) => match field.data_type() {
            DataType::Struct(fields) => {
                let key_type = fields.first().unwrap().data_type();
                let value_type = fields.last().unwrap().data_type();
                len * (slot_size(16, key_type) + slot_size(16, value_type))
            }
            t => {
                unimplemented!("map field type: {:?}not supported in shuffle write", t)
            }
        },
        DataType::Struct(fields) => {
            fields
                .iter()
                .map(|e| slot_size(1, e.data_type()))
                .sum::<usize>()
                * len
        }
        dt => unimplemented!("data type not supported in shuffle write: {:?}", dt),
    }
}
