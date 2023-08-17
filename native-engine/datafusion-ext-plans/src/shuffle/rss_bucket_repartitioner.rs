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

use crate::common::memory_manager::{MemConsumer, MemConsumerInfo, MemManager};
use crate::shuffle::rss::{rss_flush, rss_write_batch};
use crate::shuffle::{evaluate_hashes, evaluate_partition_ids, ShuffleRepartitioner};
use async_trait::async_trait;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::metrics::Count;
use datafusion::physical_plan::Partitioning;
use datafusion_ext_commons::array_builder::{builder_extend, make_batch, new_array_builders};
use futures::lock::Mutex;
use itertools::Itertools;
use jni::objects::GlobalRef;
use std::sync::{Arc, Weak};

pub struct RssBucketShuffleRepartitioner {
    name: String,
    mem_consumer_info: Option<Weak<MemConsumerInfo>>,
    buffered_partitions: Mutex<Vec<PartitionBuffer>>,
    partitioning: Partitioning,
    rss_partition_writer: GlobalRef,
    num_output_partitions: usize,
}

impl RssBucketShuffleRepartitioner {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        partition_id: usize,
        rss_partition_writer: GlobalRef,
        schema: SchemaRef,
        partitioning: Partitioning,
        data_size_metric: Count,
        context: Arc<TaskContext>,
    ) -> Self {
        let num_output_partitions = partitioning.partition_count();
        let batch_size = context.session_config().batch_size();
        let buffered_partitions = Mutex::new(
            (0..num_output_partitions)
                .map(|i| {
                    PartitionBuffer::new(
                        schema.clone(),
                        batch_size,
                        i,
                        rss_partition_writer.clone(),
                        data_size_metric.clone(),
                    )
                })
                .collect(),
        );
        Self {
            name: format!("RssBucketShufflePartitioner[partition={}]", partition_id),
            mem_consumer_info: None,
            buffered_partitions,
            partitioning,
            rss_partition_writer,
            num_output_partitions,
        }
    }
}

#[async_trait]
impl ShuffleRepartitioner for RssBucketShuffleRepartitioner {
    async fn insert_batch(&self, input: RecordBatch) -> Result<()> {
        // batch records are first shuffled and inserted into array builders, which may
        // have doubled capacity in worse case.
        let mem_increase = input.get_array_memory_size() * 2;
        self.update_mem_used_with_diff(mem_increase as isize)
            .await?;

        // we are likely to spill more frequently because the cost of spilling a shuffle
        // repartition is lower than other consumers.
        // rss shuffle spill has even lower cost than normal shuffle
        if self.mem_used_percent() > 0.25 {
            self.spill().await?;
        }

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

        for (partition_id, (&start, &end)) in partition_starts
            .iter()
            .tuple_windows()
            .enumerate()
            .filter(|(_, (start, end))| start < end)
        {
            let mut buffered_partitions = self.buffered_partitions.lock().await;
            let output = &mut buffered_partitions[partition_id];

            if end - start < output.rss_batch_size {
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
                output.append_batch(batch)?;
            }
            drop(buffered_partitions);
        }
        Ok(())
    }

    async fn shuffle_write(&self) -> Result<()> {
        self.spill().await
    }
}

#[async_trait]
impl MemConsumer for RssBucketShuffleRepartitioner {
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
        for i in 0..self.num_output_partitions {
            partitions[i].flush_to_rss()?;
        }
        rss_flush(&self.rss_partition_writer)?;
        drop(partitions);
        self.update_mem_used(0).await?;
        Ok(())
    }
}

impl Drop for RssBucketShuffleRepartitioner {
    fn drop(&mut self) {
        MemManager::deregister_consumer(self);
    }
}

struct PartitionBuffer {
    partition_id: usize,
    rss_partition_writer: GlobalRef,
    schema: SchemaRef,
    active: Vec<Box<dyn ArrayBuilder>>,
    num_active_rows: usize,
    rss_batch_size: usize,
    data_size_metric: Count,
}

impl PartitionBuffer {
    fn new(
        schema: SchemaRef,
        batch_size: usize,
        partition_id: usize,
        rss_partition_writer: GlobalRef,
        data_size_metric: Count,
    ) -> Self {
        // use smaller batch size for rss to trigger more flushes
        let rss_batch_size = batch_size / (batch_size as f64 + 1.0).log2() as usize;
        Self {
            partition_id,
            rss_partition_writer,
            schema,
            active: vec![],
            num_active_rows: 0,
            rss_batch_size,
            data_size_metric,
        }
    }

    fn append_rows(&mut self, columns: &[ArrayRef], indices: &[usize]) -> Result<()> {
        let mut start = 0;

        while start < indices.len() {
            // lazy init because some partition may be empty
            if self.active.is_empty() {
                self.active = new_array_builders(&self.schema, self.rss_batch_size);
            }

            let extend_len = (indices.len() - start)
                .min(self.rss_batch_size.saturating_sub(self.num_active_rows));
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
            if self.num_active_rows >= self.rss_batch_size {
                self.flush_to_rss()?;
            }
            start += extend_len;
        }
        Ok(())
    }

    /// append a whole batch directly to staging
    /// this will break the appending order when mixing with append_rows(), but
    /// it does not affect the shuffle output result.
    fn append_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let mut num_bytes_written_uncompressed = 0;
        rss_write_batch(
            &self.rss_partition_writer,
            self.partition_id,
            batch,
            &mut num_bytes_written_uncompressed,
        )?;
        self.data_size_metric.add(num_bytes_written_uncompressed);
        Ok(())
    }

    /// flush active data into rss
    fn flush_to_rss(&mut self) -> Result<()> {
        if self.num_active_rows == 0 {
            return Ok(());
        }
        let active = std::mem::take(&mut self.active);
        self.num_active_rows = 0;

        let batch = make_batch(self.schema.clone(), active)?;
        let mut num_bytes_written_uncompressed = 0;
        rss_write_batch(
            &self.rss_partition_writer,
            self.partition_id,
            batch,
            &mut num_bytes_written_uncompressed,
        )?;
        self.data_size_metric.add(num_bytes_written_uncompressed);
        Ok(())
    }
}
