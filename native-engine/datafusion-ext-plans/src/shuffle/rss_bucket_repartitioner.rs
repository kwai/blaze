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

//! Defines the rss bucket shuffle repartitioner

use crate::shuffle::{evaluate_hashes, evaluate_partition_ids, ShuffleRepartitioner};
use async_trait::async_trait;
use blaze_commons::{jni_call, jni_new_direct_byte_buffer};
use bytesize::ByteSize;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_manager::ConsumerType;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::{MemoryConsumer, MemoryConsumerId, MemoryManager};
use datafusion::physical_plan::coalesce_batches::concat_batches;
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::Partitioning;
use datafusion_ext_commons::array_builder::{
    builder_extend, make_batch, new_array_builders,
};
use datafusion_ext_commons::io::write_one_batch;
use futures::lock::Mutex;
use itertools::Itertools;
use jni::objects::GlobalRef;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::io::Cursor;
use std::sync::Arc;

pub struct RssBucketShuffleRepartitioner {
    id: MemoryConsumerId,
    rss_partition_writer: GlobalRef,
    buffered_partitions: Mutex<Vec<PartitionBuffer>>,
    /// Partitioning scheme to use
    partitioning: Partitioning,
    num_output_partitions: usize,
    runtime: Arc<RuntimeEnv>,
    metrics: BaselineMetrics,
}

impl RssBucketShuffleRepartitioner {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        partition_id: usize,
        rss_partition_writer: GlobalRef,
        schema: SchemaRef,
        partitioning: Partitioning,
        metrics: BaselineMetrics,
        context: Arc<TaskContext>,
    ) -> Self {
        let num_output_partitions = partitioning.partition_count();
        let runtime = context.runtime_env();
        let batch_size = context.session_config().batch_size();
        let repartitioner = Self {
            id: MemoryConsumerId::new(partition_id),
            rss_partition_writer: rss_partition_writer.clone(),
            buffered_partitions: Mutex::new(
                (0..num_output_partitions)
                    .map(|_| {
                        PartitionBuffer::new(
                            schema.clone(),
                            batch_size,
                            rss_partition_writer.clone(),
                        )
                    })
                    .collect::<Vec<_>>(),
            ),
            partitioning,
            num_output_partitions,
            runtime,
            metrics,
        };
        repartitioner.runtime.register_requester(&repartitioner.id);
        repartitioner
    }
}

#[async_trait]
impl ShuffleRepartitioner for RssBucketShuffleRepartitioner {
    fn name(&self) -> &str {
        "bucket rss repartitioner"
    }

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
                mem_diff += output.append_rows(
                    input.columns(),
                    &shuffled_partition_ids[start..end],
                    partition_id,
                )?;
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
                        .map(|c| datafusion::arrow::compute::take(c, &indices, None))
                        .collect::<ArrowResult<Vec<ArrayRef>>>()?,
                )?;
                mem_diff += output.append_batch(batch, partition_id)?;
            }
            drop(buffered_partitions);
        }

        if mem_diff > 0 {
            let mem_increase = mem_diff as usize;
            self.try_grow(mem_increase).await?;
            self.metrics.mem_used().add(mem_increase);
        }
        if mem_diff < 0 {
            let mem_used = self.metrics.mem_used().value();
            let mem_decrease = mem_used.min(-mem_diff as usize);
            self.shrink(mem_decrease);
            self.metrics.mem_used().set(mem_used - mem_decrease);
        }
        Ok(())
    }

    async fn shuffle_write(&self) -> Result<()> {
        let num_output_partitions = self.num_output_partitions;
        let mut buffered_partitions = self.buffered_partitions.lock().await;

        for i in 0..num_output_partitions {
            buffered_partitions[i].flush(i)?;
        }

        let used = self.metrics.mem_used().set(0);
        self.shrink(used);
        Ok(())
    }
}

/// consume the `buffered_partitions` and do spill into a single temp shuffle output file
async fn spill_into(
    buffered_partitions: &mut [PartitionBuffer],
    num_output_partitions: usize,
) -> Result<isize> {
    let mut mem_diff = 0_isize;
    for i in 0..num_output_partitions {
        mem_diff += buffered_partitions[i].flush(i)?;
    }

    Ok(mem_diff)
}

impl Debug for RssBucketShuffleRepartitioner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("BucketShuffleRepartitioner")
            .field("id", &self.id())
            .field("memory_used", &self.mem_used())
            .finish()
    }
}

#[async_trait]
impl MemoryConsumer for RssBucketShuffleRepartitioner {
    fn name(&self) -> String {
        "BucketRssShuffleRepartitioner".to_string()
    }

    fn id(&self) -> &MemoryConsumerId {
        &self.id
    }

    fn memory_manager(&self) -> Arc<MemoryManager> {
        self.runtime.memory_manager.clone()
    }

    fn type_(&self) -> &ConsumerType {
        &ConsumerType::Requesting
    }

    async fn spill(&self) -> Result<usize> {
        log::info!(
            "bucket repartitioner start spilling, used={}",
            ByteSize(self.mem_used() as u64)
        );

        let mut buffered_partitions = self.buffered_partitions.lock().await;
        if buffered_partitions.len() == 0 {
            return Ok(0);
        }

        let mem_freed =
            spill_into(&mut buffered_partitions, self.num_output_partitions).await?;
        let freed = self.metrics.mem_used().set(0);

        log::info!(
            "bucket repartitioner spilled into file, freed={}",
            ByteSize(mem_freed as u64)
        );
        Ok(freed)
    }

    fn mem_used(&self) -> usize {
        self.metrics.mem_used().value()
    }
}

impl Drop for RssBucketShuffleRepartitioner {
    fn drop(&mut self) {
        self.runtime.drop_consumer(self.id(), self.mem_used());
    }
}

struct PartitionBuffer {
    rss_partition_writer_use: GlobalRef,
    schema: SchemaRef,
    staging: Vec<RecordBatch>,
    active: Vec<Box<dyn ArrayBuilder>>,
    active_slots_mem_size: usize,
    num_active_rows: usize,
    num_staging_rows: usize,
    batch_size: usize,
    staging_size: usize,
}

impl PartitionBuffer {
    fn new(
        schema: SchemaRef,
        batch_size: usize,
        rss_partition_writer_use: GlobalRef,
    ) -> Self {
        let staging_size = batch_size / (batch_size as f64 + 1.0).log2() as usize;
        Self {
            rss_partition_writer_use,
            schema,
            staging: vec![],
            active: vec![],
            active_slots_mem_size: 0,
            num_active_rows: 0,
            num_staging_rows: 0,
            batch_size,
            staging_size,
        }
    }

    fn append_rows(
        &mut self,
        columns: &[ArrayRef],
        indices: &[usize],
        partition_id: usize,
    ) -> Result<isize> {
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
                        .map(|(_ab, field)| {
                            slot_size(self.staging_size, field.data_type())
                        })
                        .sum::<usize>();
                }
                mem_diff += self.active_slots_mem_size as isize;
            }

            let extend_len = (indices.len() - start)
                .min(self.staging_size.saturating_sub(self.num_active_rows));
            self.active
                .iter_mut()
                .zip(columns)
                .for_each(|(builder, column)| {
                    builder_extend(
                        builder,
                        column,
                        &indices[start..][..extend_len],
                        column.data_type(),
                    );
                });
            self.num_active_rows += extend_len;
            if self.num_active_rows >= self.staging_size {
                mem_diff += self.flush_to_staging(partition_id)?;
            }
            start += extend_len;
        }
        Ok(mem_diff)
    }

    /// append a whole batch directly to staging
    /// this will break the appending order when mixing with append_rows(), but
    /// it does not affect the shuffle output result.
    fn append_batch(&mut self, batch: RecordBatch, partition_id: usize) -> Result<isize> {
        let mut mem_diff = batch.get_array_memory_size() as isize;
        self.num_staging_rows += batch.num_rows();
        self.staging.push(batch);

        // staging -> frozen
        if self.num_staging_rows >= self.batch_size {
            mem_diff += self.flush(partition_id)?;
        }
        Ok(mem_diff)
    }

    /// flush active data into one staging batch
    fn flush_to_staging(&mut self, partition_id: usize) -> Result<isize> {
        if self.num_active_rows == 0 {
            return Ok(0);
        }
        let mut mem_diff = 0isize;

        // active -> staging
        let active = std::mem::take(&mut self.active);
        self.num_active_rows = 0;
        mem_diff -= self.active_slots_mem_size as isize;

        let staging_batch = make_batch(self.schema.clone(), active)?;
        mem_diff += self.append_batch(staging_batch, partition_id)?;
        Ok(mem_diff)
    }

    /// flush batch to rss service
    fn flush(&mut self, partition_id: usize) -> Result<isize> {
        let mut mem_diff = 0isize;
        if self.num_active_rows > 0 {
            mem_diff += self.flush_to_staging(partition_id)?;
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

        let mut cursor = Cursor::new(vec![]);
        write_one_batch(&frozen_batch, &mut cursor, true)?;

        let mut rss_data = cursor.into_inner();
        let length = rss_data.len();
        let rss_buffer = jni_new_direct_byte_buffer!(&mut rss_data)?;

        if length != 0 {
            jni_call!(SparkRssShuffleWriter(self.rss_partition_writer_use.clone().as_obj()).write(partition_id as i32, rss_buffer, length as i32) -> ())?;
        }

        mem_diff += frozen_batch.get_array_memory_size() as isize;
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
        dt => unimplemented!("data type not supported in shuffle write: {:?}", dt),
    }
}
