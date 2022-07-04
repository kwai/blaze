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
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::io::{Cursor, Read};
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_manager::ConsumerType;
use datafusion::execution::memory_manager::MemoryConsumer;
use datafusion::execution::memory_manager::MemoryConsumerId;
use datafusion::execution::memory_manager::MemoryManager;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::common::batch_byte_size;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::Statistics;
use futures::lock::Mutex;
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use itertools::Itertools;
use tempfile::NamedTempFile;
use tokio::task;

use crate::spark_hash::{create_hashes, pmod};
use crate::util::array_builder::{make_batch, new_array_builders};
use crate::util::ipc::write_ipc_compressed;

struct PartitionBuffer {
    schema: SchemaRef,
    frozen: Vec<u8>,
    staging: Vec<RecordBatch>,
    active: Vec<Box<dyn ArrayBuilder>>,
    num_active_rows: usize,
    num_staging_rows: usize,
    batch_size: usize,
    staging_size: usize,
}

impl PartitionBuffer {
    fn new(schema: SchemaRef, batch_size: usize) -> Self {
        let staging_size = (batch_size as f64).sqrt() as usize;
        let array_builders = new_array_builders(&schema, staging_size);
        Self {
            schema,
            frozen: vec![],
            staging: vec![],
            active: array_builders,
            num_active_rows: 0,
            num_staging_rows: 0,
            batch_size,
            staging_size,
        }
    }

    fn append_rows(&mut self, columns: &[ArrayRef], indices: &[usize]) -> Result<isize> {
        let mut start = 0;
        let mut rest = indices.len();
        let mut mem_diff = 0;

        while rest > 0 {
            let len = rest.min(self.staging_size.saturating_sub(self.num_active_rows));

            self.active
                .iter_mut()
                .zip(columns)
                .for_each(|(builder, column)| {
                    append_columns(
                        builder,
                        column,
                        &indices[start..][..len],
                        column.data_type()
                    );
                });
            self.num_active_rows += len;
            if self.num_active_rows >= self.staging_size {
                mem_diff += self.flush_to_staging()?;
            }
            start += len;
            rest -= len;
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
        let active = std::mem::replace(
            &mut self.active,
            new_array_builders(&self.schema, self.staging_size),
        );
        self.num_staging_rows += self.num_active_rows;
        self.num_active_rows = 0;

        let staging_batch = make_batch(self.schema.clone(), active)?;
        mem_diff += batch_byte_size(&staging_batch) as isize;
        self.staging.push(staging_batch);

        // staging -> frozen
        if self.num_staging_rows >= self.batch_size {
            mem_diff += self.flush()?;
        }
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

        let frozen_batch = RecordBatch::concat(&self.schema, &self.staging)?;
        mem_diff -= self
            .staging
            .iter()
            .map(|batch| batch_byte_size(batch) as isize)
            .sum::<isize>();
        self.staging.clear();
        self.num_staging_rows = 0;

        let frozen_capacity_old = self.frozen.capacity();
        let mut cursor = Cursor::new(&mut self.frozen);
        cursor.seek(SeekFrom::End(0))?;
        write_ipc_compressed(&frozen_batch, &mut cursor)?;

        mem_diff += (self.frozen.capacity() - frozen_capacity_old) as isize;
        Ok(mem_diff)
    }
}

fn append_columns(
    to: &mut Box<dyn ArrayBuilder>,
    from: &Arc<dyn Array>,
    indices: &[usize],
    data_type: &DataType,
) {
    macro_rules! append {
        ($arrowty:ident) => {{
            type B = paste::paste! {[< $arrowty Builder >]};
            type A = paste::paste! {[< $arrowty Array >]};
            let t = to.as_any_mut().downcast_mut::<B>().unwrap();
            let f = from.as_any().downcast_ref::<A>().unwrap();
            for &i in indices {
                if f.is_valid(i) {
                    t.append_value(f.value(i)).unwrap();
                } else {
                    t.append_null().unwrap();
                }
            }
        }};
    }
    match data_type {
        DataType::Boolean => append!(Boolean),
        DataType::Int8 => append!(Int8),
        DataType::Int16 => append!(Int16),
        DataType::Int32 => append!(Int32),
        DataType::Int64 => append!(Int64),
        DataType::UInt8 => append!(UInt8),
        DataType::UInt16 => append!(UInt16),
        DataType::UInt32 => append!(UInt32),
        DataType::UInt64 => append!(UInt64),
        DataType::Float32 => append!(Float32),
        DataType::Float64 => append!(Float64),
        DataType::Date32 => append!(Date32),
        DataType::Date64 => append!(Date64),
        DataType::Time32(TimeUnit::Second) => append!(Time32Second),
        DataType::Time32(TimeUnit::Millisecond) => append!(Time32Millisecond),
        DataType::Time64(TimeUnit::Microsecond) => append!(Time64Microsecond),
        DataType::Time64(TimeUnit::Nanosecond) => append!(Time64Nanosecond),
        DataType::Utf8 => append!(String),
        DataType::LargeUtf8 => append!(LargeString),
        DataType::Decimal(_, _) => append!(Decimal),
        _ => unimplemented!("data type not supported in shuffle write"),
    }
}

struct SpillInfo {
    file: NamedTempFile,
    offsets: Vec<u64>,
}

struct ShuffleRepartitioner {
    id: MemoryConsumerId,
    output_data_file: String,
    output_index_file: String,
    schema: SchemaRef,
    buffered_partitions: Mutex<Vec<PartitionBuffer>>,
    spills: Mutex<Vec<SpillInfo>>,
    /// Sort expressions
    /// Partitioning scheme to use
    partitioning: Partitioning,
    num_output_partitions: usize,
    runtime: Arc<RuntimeEnv>,
    metrics: BaselineMetrics,
}

impl ShuffleRepartitioner {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        partition_id: usize,
        output_data_file: String,
        output_index_file: String,
        schema: SchemaRef,
        partitioning: Partitioning,
        metrics: BaselineMetrics,
        runtime: Arc<RuntimeEnv>,
        batch_size: usize,
    ) -> Self {
        let num_output_partitions = partitioning.partition_count();
        Self {
            id: MemoryConsumerId::new(partition_id),
            output_data_file,
            output_index_file,
            schema: schema.clone(),
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
        }
    }

    async fn insert_batch(&self, input: RecordBatch) -> Result<()> {
        if input.num_rows() == 0 {
            // skip empty batch
            return Ok(());
        }
        let _timer = self.metrics.elapsed_compute().timer();

        // NOTE: in shuffle writer exec, the output_rows metrics represents the
        // number of rows those are written to output data file.
        self.metrics.record_output(input.num_rows());

        let num_output_partitions = self.num_output_partitions;
        match &self.partitioning {
            Partitioning::Hash(exprs, _) => {
                let hashes_buf = &mut vec![];
                let arrays = exprs
                    .iter()
                    .map(|expr| Ok(expr.evaluate(&input)?.into_array(input.num_rows())))
                    .collect::<Result<Vec<_>>>()?;

                // use identical seed as spark hash partition
                hashes_buf.resize(arrays[0].len(), 42);

                // Hash arrays and compute buckets based on number of partitions
                let partition_ids = create_hashes(&arrays, hashes_buf)?
                    .iter_mut()
                    .map(|hash| pmod(*hash, num_output_partitions) as u64)
                    .collect::<Vec<_>>();

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

                    let part_mem_diff = output.append_rows(
                        input.columns(),
                        &shuffled_partition_ids[start..end],
                    )?;
                    mem_diff += part_mem_diff;
                }

                if mem_diff > 0 {
                    let mem_increase = mem_diff as usize;
                    self.try_grow(mem_increase).await?;
                    self.metrics.mem_used().add(mem_increase);
                }
                if mem_diff < 0 {
                    // if shuffle for current batch triggers a lot of staging
                    // batches to be flushed to compressed bytes, the memory
                    // usage can be less than before.
                    let mem_used = self.metrics.mem_used().value();
                    let mem_decrease = mem_used.min(-mem_diff as usize);
                    self.shrink(mem_decrease);
                    self.metrics.mem_used().set(mem_used - mem_decrease);
                }
            }
            other => {
                // this should be unreachable as long as the validation logic
                // in the constructor is kept up-to-date
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported repartitioning scheme {:?}",
                    other
                )));
            }
        }
        Ok(())
    }

    async fn shuffle_write(&self) -> Result<SendableRecordBatchStream> {
        let _timer = self.metrics.elapsed_compute().timer();
        let num_output_partitions = self.num_output_partitions;
        let mut buffered_partitions = self.buffered_partitions.lock().await;
        let mut output_batches: Vec<Vec<u8>> = vec![vec![]; num_output_partitions];

        for i in 0..num_output_partitions {
            buffered_partitions[i].flush()?;
            output_batches[i] = std::mem::take(&mut buffered_partitions[i].frozen);
        }

        let mut spills = self.spills.lock().await;
        let output_spills = spills.drain(..).collect::<Vec<_>>();

        let data_file = self.output_data_file.clone();
        let index_file = self.output_index_file.clone();

        std::mem::drop(_timer);
        let elapsed_compute = self.metrics.elapsed_compute().clone();

        task::spawn_blocking(move || {
            let _timer = elapsed_compute.timer();
            let mut offsets = vec![0; num_output_partitions + 1];
            let mut output_data = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(data_file)?;

            for i in 0..num_output_partitions {
                offsets[i] = output_data.stream_position()?;
                output_data.write_all(&output_batches[i])?;
                output_batches[i].clear();

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

        // shuffle writer always has empty output
        Ok(Box::pin(MemoryStream::try_new(
            vec![],
            self.schema.clone(),
            None,
        )?))
    }

    fn used(&self) -> usize {
        self.metrics.mem_used().value()
    }

    fn spilled_bytes(&self) -> usize {
        self.metrics.spilled_bytes().value()
    }

    fn spill_count(&self) -> usize {
        self.metrics.spill_count().value()
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
            spill_data.write_all(&output_batches[i])?;
            output_batches[i].clear();
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

impl Debug for ShuffleRepartitioner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShuffleRepartitioner")
            .field("id", &self.id())
            .field("memory_used", &self.used())
            .field("spilled_bytes", &self.spilled_bytes())
            .field("spilled_count", &self.spill_count())
            .finish()
    }
}

#[async_trait]
impl MemoryConsumer for ShuffleRepartitioner {
    fn name(&self) -> String {
        "ShuffleRepartitioner".to_owned()
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
        log::debug!(
            "{}[{}] spilling shuffle data of {} to disk while inserting ({} time(s) so far)",
            self.name(),
            self.id(),
            self.used(),
            self.spill_count()
        );

        let mut buffered_partitions = self.buffered_partitions.lock().await;
        // we could always get a chance to free some memory as long as we are holding some
        if buffered_partitions.len() == 0 {
            return Ok(0);
        }

        let spillfile = self.runtime.disk_manager.create_tmp_file()?;
        let offsets = spill_into(
            &mut *buffered_partitions,
            spillfile.path(),
            self.num_output_partitions,
        )
        .await?;

        let mut spills = self.spills.lock().await;
        let freed = self.metrics.mem_used().set(0);
        self.metrics.record_spill(freed);
        spills.push(SpillInfo {
            file: spillfile,
            offsets,
        });
        Ok(freed)
    }

    fn mem_used(&self) -> usize {
        self.metrics.mem_used().value()
    }
}

impl Drop for ShuffleRepartitioner {
    fn drop(&mut self) {
        self.runtime.drop_consumer(self);
    }
}

/// The shuffle writer operator maps each input partition to M output partitions based on a
/// partitioning scheme. No guarantees are made about the order of the resulting partitions.
#[derive(Debug)]
pub struct ShuffleWriterExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// Partitioning scheme to use
    partitioning: Partitioning,
    /// Output data file path
    output_data_file: String,
    /// Output index file path
    output_index_file: String,
    /// Metrics
    metrics: ExecutionPlanMetricsSet,
}

#[async_trait]
impl ExecutionPlan for ShuffleWriterExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.partitioning.clone()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(ShuffleWriterExec::try_new(
                children[0].clone(),
                self.partitioning.clone(),
                self.output_data_file.clone(),
                self.output_index_file.clone(),
            )?)),
            _ => Err(DataFusionError::Internal(
                "RepartitionExec wrong number of children".to_string(),
            )),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context.clone())?;
        let metrics = BaselineMetrics::new(&self.metrics, 0);

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(
                external_shuffle(
                    input,
                    partition,
                    self.output_data_file.clone(),
                    self.output_index_file.clone(),
                    self.partitioning.clone(),
                    metrics,
                    context,
                )
                .map_err(|e| ArrowError::ExternalError(Box::new(e))),
            )
            .try_flatten(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "ShuffleWriterExec: partitioning={:?}", self.partitioning)
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.input.statistics()
    }
}

impl ShuffleWriterExec {
    /// Create a new ShuffleWriterExec
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
        output_data_file: String,
        output_index_file: String,
    ) -> Result<Self> {
        Ok(ShuffleWriterExec {
            input,
            partitioning,
            metrics: ExecutionPlanMetricsSet::new(),
            output_data_file,
            output_index_file,
        })
    }
}

pub async fn external_shuffle(
    mut input: SendableRecordBatchStream,
    partition_id: usize,
    output_data_file: String,
    output_index_file: String,
    partitioning: Partitioning,
    metrics: BaselineMetrics,
    context: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    let schema = input.schema();
    let repartitioner = Arc::new(ShuffleRepartitioner::new(
        partition_id,
        output_data_file,
        output_index_file,
        schema.clone(),
        partitioning,
        metrics,
        context.runtime_env(),
        context.session_config().batch_size(),
    ));
    context.runtime_env().register_requester(repartitioner.clone());

    while let Some(batch) = input.next().await {
        let batch = batch?;
        repartitioner.insert_batch(batch).await?;
    }
    repartitioner.shuffle_write().await
}
