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

use std::fmt;
use std::fmt::{Debug, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use async_trait::async_trait;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_manager::ConsumerType;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::{MemoryConsumer, MemoryConsumerId, MemoryManager};
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::Partitioning;
use futures::lock::Mutex;
use datafusion::physical_plan::coalesce_batches::concat_batches;
use itertools::Itertools;
use tokio::task;
use datafusion_ext_commons::array_builder::{builder_extend, make_batch, new_array_builders};
use datafusion_ext_commons::io::write_one_batch;
use crate::shuffle::{evaluate_hashes, evaluate_partition_ids, FileSpillInfo, ShuffleRepartitioner};

pub struct BucketShuffleRepartitioner {
    id: MemoryConsumerId,
    output_data_file: String,
    output_index_file: String,
    buffered_partitions: Mutex<Vec<PartitionBuffer>>,
    spills: Mutex<Vec<FileSpillInfo>>,
    /// Sort expressions
    /// Partitioning scheme to use
    partitioning: Partitioning,
    num_output_partitions: usize,
    runtime: Arc<RuntimeEnv>,
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
        let runtime = context.runtime_env();
        let batch_size = context.session_config().batch_size();
        let repartitioner = Self {
            id: MemoryConsumerId::new(partition_id),
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
            runtime,
            metrics,
        };
        repartitioner.runtime.register_requester(&repartitioner.id);
        repartitioner
    }

    fn spilled_bytes(&self) -> usize {
        self.metrics.spilled_bytes().value()
    }

    fn spill_count(&self) -> usize {
        self.metrics.spill_count().value()
    }
}

#[async_trait]
impl ShuffleRepartitioner for BucketShuffleRepartitioner {
    fn name(&self) -> &str {
        "bucket repartitioner"
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
                        .map(|c| {
                            arrow::compute::take(c, &indices, None)
                        })
                        .collect::<ArrowResult<Vec<ArrayRef>>>()?,
                )?;
                mem_diff += output.append_batch(batch)?;
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
        let mut output_batches: Vec<Vec<u8>> = vec![vec![]; num_output_partitions];

        for i in 0..num_output_partitions {
            buffered_partitions[i].flush()?;
            output_batches[i] = std::mem::take(&mut buffered_partitions[i].frozen);
        }

        let mut spills = self.spills.lock().await;
        let output_spills = spills.drain(..).collect::<Vec<_>>();
        log::info!(
            "bucket partitioner start writing with {} disk spills",
            output_spills.len());

        let data_file = self.output_data_file.clone();
        let index_file = self.output_index_file.clone();

        task::spawn_blocking(move || {
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
                for spill in &output_spills {
                    let length = spill.offsets[i + 1] - spill.offsets[i];
                    if length > 0 {
                        let mut spill_file = File::open(&spill.file.path())?;
                        spill_file.seek(SeekFrom::Start(spill.offsets[i]))?;
                        std::io::copy(&mut spill_file.take(length), &mut output_data)?;
                    }
                }
            }
            output_data.flush()?;

            // add one extra offset at last to ease partition length computation
            offsets[num_output_partitions] = output_data.stream_position()?;
            let mut output_index = File::create(index_file)?;
            for offset in offsets {
                output_index.write_all(&(offset as i64).to_le_bytes()[..])?;
            }
            output_index.flush()?;
            Ok::<(), DataFusionError>(())
        })
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!("shuffle write error: {:?}", e))
        })??;

        let used = self.metrics.mem_used().set(0);
        self.shrink(used);
        Ok(())
    }
}

/// consume the `buffered_partitions` and do spill into a single temp shuffle output file
async fn spill_into(
    buffered_partitions: &mut [PartitionBuffer],
    path: &Path,
    num_output_partitions: usize,
) -> Result<Vec<u64>> {
    let mut output_batches: Vec<Vec<u8>> = vec![vec![]; num_output_partitions];

    for i in 0..num_output_partitions {
        buffered_partitions[i].flush()?;
        output_batches[i] = std::mem::take(&mut buffered_partitions[i].frozen);
    }
    let path = path.to_owned();

    task::spawn_blocking(move || {
        let mut offsets = vec![0; num_output_partitions + 1];
        let mut spill_data = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        for i in 0..num_output_partitions {
            offsets[i] = spill_data.stream_position()?;
            spill_data.write_all(&std::mem::take(&mut output_batches[i]))?;
        }
        // add one extra offset at last to ease partition length computation
        offsets[num_output_partitions] = spill_data.stream_position()?;
        Ok(offsets)
    })
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!("Error occurred while spilling {}", e))
        })?
}

impl Debug for BucketShuffleRepartitioner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("BucketShuffleRepartitioner")
            .field("id", &self.id())
            .field("memory_used", &self.mem_used())
            .field("spilled_bytes", &self.spilled_bytes())
            .field("spilled_count", &self.spill_count())
            .finish()
    }
}

#[async_trait]
impl MemoryConsumer for BucketShuffleRepartitioner {
    fn name(&self) -> String {
        format!("BucketShuffleRepartitioner")
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
            "bucket repartitioner start spilling, used={:.2} MB, {}",
            self.mem_used() as f64 / 1e6,
            self.memory_manager(),
        );

        let mut buffered_partitions = self.buffered_partitions.lock().await;
        // we could always get a chance to free some memory as long as we are holding some
        if buffered_partitions.len() == 0 {
            return Ok(0);
        }

        let spillfile = self.runtime.disk_manager.create_tmp_file("shuffle_spill_file")?;
        let offsets = spill_into(
            &mut buffered_partitions,
            spillfile.path(),
            self.num_output_partitions,
        )
        .await?;

        let mut spills = self.spills.lock().await;
        let freed = self.metrics.mem_used().set(0);

        log::info!(
            "bucket repartitioner spilled into file, freed={:.2} MB",
            freed as f64 / 1e6);

        let file_spill = FileSpillInfo {
            file: spillfile,
            offsets,
        };
        self.metrics.record_spill(file_spill.bytes_size());
        spills.push(file_spill);
        Ok(freed)
    }

    fn mem_used(&self) -> usize {
        self.metrics.mem_used().value()
    }
}

impl Drop for BucketShuffleRepartitioner {
    fn drop(&mut self) {
        self.runtime.drop_consumer(self.id(), self.mem_used());
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
        dt => unimplemented!("data type not supported in shuffle write: {:?}", dt),
    }
}
