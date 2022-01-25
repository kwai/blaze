// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Defines the External shuffle repartition plan

use crate::batch_buffer::MutableRecordBatch;
use ahash::RandomState;
use async_trait::async_trait;
use datafusion::arrow::array::*;
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_manager::{
    ConsumerType, MemoryConsumer, MemoryConsumerId, MemoryManager,
};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::from_slice::FromSlice;
use datafusion::physical_plan::common::batch_byte_size;
use datafusion::physical_plan::hash_utils::create_hashes;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::{
    AggregatedMetricsSet, BaselineMetrics, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use futures::lock::Mutex;
use futures::StreamExt;
use log::info;
use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::task;

#[derive(Default)]
struct PartitionBuffer {
    frozen: Vec<RecordBatch>,
    active: Option<MutableRecordBatch>,
}

impl PartitionBuffer {
    fn size_estimation(&self) -> usize {
        let mut res: usize = 0;
        res += self.frozen.iter().map(batch_byte_size).sum::<usize>();
        // TODO estimate size of active as well,
        res
    }

    fn output_all(&mut self) -> Result<Vec<RecordBatch>> {
        let mut output = vec![];
        output.extend(self.frozen.drain(..));
        if let Some(mut mutable) = self.active.take() {
            let result = mutable.output_and_reset()?;
            output.push(result);
            self.active = Some(mutable);
        }
        Ok(output)
    }

    fn output_clean(&mut self) -> Result<Vec<RecordBatch>> {
        let mut output = vec![];
        output.extend(self.frozen.drain(..));
        if let Some(mut mutable) = self.active.take() {
            let result = mutable.output()?;
            output.push(result);
        }
        Ok(output)
    }
}

struct SpillInfo {
    path: String,
    offsets: Vec<u64>,
}

macro_rules! append {
    ($TO:ty, $FROM:ty, $to: ident, $from: ident) => {{
        let to = $to.as_any_mut().downcast_mut::<$TO>().unwrap();
        let from = $from.as_any().downcast_ref::<$FROM>().unwrap();
        for i in from.into_iter() {
            to.append_option(i);
        }
    }};
}

fn append_column(
    to: &mut Box<dyn ArrayBuilder>,
    from: &Arc<dyn Array>,
    data_type: &DataType,
) {
    // output buffered start `buffered_idx`, len `rows_to_output`
    match data_type {
        DataType::Null => unimplemented!(),
        DataType::Boolean => append!(BooleanBuilder, BooleanArray, to, from),
        DataType::Int8 => append!(Int8Builder, Int8Array, to, from),
        DataType::Int16 => append!(Int16Builder, Int16Array, to, from),
        DataType::Int32 => append!(Int32Builder, Int32Array, to, from),
        DataType::Int64 => append!(Int64Builder, Int64Array, to, from),
        DataType::UInt8 => append!(UInt8Builder, UInt8Array, to, from),
        DataType::UInt16 => append!(UInt16Builder, UInt16Array, to, from),
        DataType::UInt32 => append!(UInt32Builder, UInt32Array, to, from),
        DataType::UInt64 => append!(UInt64Builder, UInt64Array, to, from),
        DataType::Float32 => append!(Float32Builder, Float32Array, to, from),
        DataType::Float64 => append!(Float64Builder, Float64Array, to, from),
        DataType::Date32 => append!(Date32Builder, Date32Array, to, from),
        DataType::Date64 => append!(Date64Builder, Date64Array, to, from),
        DataType::Time32(TimeUnit::Second) => {
            append!(Time32SecondBuilder, Time32SecondArray, to, from)
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            append!(Time32MillisecondBuilder, Time32SecondArray, to, from)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            append!(Time64MicrosecondBuilder, Time64MicrosecondArray, to, from)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            append!(Time64NanosecondBuilder, Time64NanosecondArray, to, from)
        }
        DataType::Utf8 => append!(StringBuilder, StringArray, to, from),
        DataType::LargeUtf8 => append!(LargeStringBuilder, LargeStringArray, to, from),
        _ => todo!(),
    }
}

struct ShuffleRepartitioner {
    id: MemoryConsumerId,
    shuffle_id: usize,
    schema: SchemaRef,
    buffered_partitions: Mutex<Vec<PartitionBuffer>>,
    spills: Mutex<Vec<SpillInfo>>,
    /// Sort expressions
    /// Partitioning scheme to use
    partitioning: Partitioning,
    num_output_partitions: usize,
    runtime: Arc<RuntimeEnv>,
    metrics: AggregatedMetricsSet,
    inner_metrics: BaselineMetrics,
    used: AtomicUsize,
    random: RandomState,
}

impl ShuffleRepartitioner {
    pub fn new(
        partition_id: usize,
        shuffle_id: usize,
        schema: SchemaRef,
        partitioning: Partitioning,
        metrics: AggregatedMetricsSet,
        runtime: Arc<RuntimeEnv>,
    ) -> Self {
        let num_output_partitions = partitioning.partition_count();
        let inner_metrics = metrics.new_intermediate_baseline(partition_id);
        Self {
            id: MemoryConsumerId::new(partition_id),
            shuffle_id,
            schema,
            buffered_partitions: Mutex::new(Default::default()),
            spills: Mutex::new(vec![]),
            partitioning,
            num_output_partitions,
            runtime,
            metrics,
            inner_metrics,
            used: AtomicUsize::new(0),
            random: RandomState::with_seeds(0, 0, 0, 0),
        }
    }

    async fn insert_batch(&self, input: RecordBatch) -> Result<()> {
        // TODO: this is a rough estimation of memory consumed for a input batch
        // for example, for first batch seen, we need to open as much output buffer
        // as we encountered in this batch, thus the memory consumption is `rough`.
        let size = batch_byte_size(&input);
        self.try_grow(size).await?;
        self.used.fetch_add(size, Ordering::SeqCst);

        let random_state = self.random.clone();
        let num_output_partitions = self.num_output_partitions;
        let shuffle_batch_size = self.runtime.batch_size() / 16;
        match &self.partitioning {
            Partitioning::Hash(exprs, _) => {
                let hashes_buf = &mut vec![];
                let arrays = exprs
                    .iter()
                    .map(|expr| Ok(expr.evaluate(&input)?.into_array(input.num_rows())))
                    .collect::<Result<Vec<_>>>()?;
                hashes_buf.resize(arrays[0].len(), 0);
                // Hash arrays and compute buckets based on number of partitions
                let hashes = create_hashes(&arrays, &random_state, hashes_buf)?;
                let mut indices = vec![vec![]; num_output_partitions];
                for (index, hash) in hashes.iter().enumerate() {
                    indices[(*hash % num_output_partitions as u64) as usize]
                        .push(index as u64)
                }

                for (num_output_partition, partition_indices) in
                    indices.into_iter().enumerate()
                {
                    let mut buffered_partitions = self.buffered_partitions.lock().await;
                    let output = &mut buffered_partitions[num_output_partition];
                    let indices = UInt64Array::from_slice(&partition_indices);
                    // Produce batches based on indices
                    let columns = input
                        .columns()
                        .iter()
                        .map(|c| {
                            take(c.as_ref(), &indices, None)
                                .map(|x| x.into())
                                .map_err(|e| DataFusionError::Execution(e.to_string()))
                        })
                        .collect::<Result<Vec<Arc<dyn Array>>>>()?;

                    if partition_indices.len() > shuffle_batch_size {
                        let output_batch =
                            RecordBatch::try_new(input.schema().clone(), columns)?;
                        output.frozen.push(output_batch);
                    } else {
                        match output.active {
                            None => {
                                let buffer = MutableRecordBatch::new(
                                    shuffle_batch_size,
                                    self.schema.clone(),
                                );
                                output.active = Some(buffer);
                            }
                            _ => {}
                        }

                        let mut batch = output.active.take().unwrap();
                        batch
                            .arrays
                            .iter_mut()
                            .zip(columns.iter())
                            .zip(self.schema.fields().iter().map(|f| f.data_type()))
                            .for_each(|((to, from), dt)| append_column(to, from, dt));
                        batch.append(partition_indices.len());

                        if batch.is_full() {
                            let result = batch.output_and_reset()?;
                            output.frozen.push(result);
                        }
                        output.active = Some(batch);
                    }
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
        let partition = self.partition_id();
        let num_output_partitions = self.num_output_partitions;
        let data_file = format!("shuffle_{}_{}_0.data", self.shuffle_id, partition);
        let index_file = format!("shuffle_{}_{}_0.index", self.shuffle_id, partition);

        let mut buffered_partitions = self.buffered_partitions.lock().await;
        let mut freed = 0;
        let mut output_batches: Vec<Vec<RecordBatch>> =
            vec![vec![]; num_output_partitions];

        for i in 0..num_output_partitions {
            freed += buffered_partitions[i].size_estimation();
            let partition_batches = buffered_partitions[i].output_clean()?;
            output_batches[i] = partition_batches;
        }

        let mut spills = self.spills.lock().await;
        let output_spills = spills.drain(..).collect::<Vec<_>>();

        let data_file_clone = data_file.clone();
        let index_file_clone = index_file.clone();
        let input_schema = self.schema.clone();
        let res = task::spawn_blocking(move || {
            let mut offset: u64 = 0;
            let mut offsets = vec![0; num_output_partitions + 1];
            let mut output_data = OpenOptions::new()
                .read(true)
                .append(true)
                .open(data_file_clone)?;

            let mut spill_files = vec![];
            for s in 0..output_spills.len() {
                let reader = File::open(&output_spills[s].path)?;
                spill_files.push(reader);
            }

            for i in 0..num_output_partitions {
                offsets[i] = offset;
                let partition_start = output_data.seek(SeekFrom::Current(0))?;

                // write in-mem batches first if any
                let in_mem_batches = &output_batches[i];
                if in_mem_batches.len() > 0 {
                    let mut file_writer =
                        FileWriter::try_new(output_data, input_schema.as_ref())?;
                    for batch in in_mem_batches {
                        file_writer.write(batch)?;
                    }
                    file_writer.finish()?;

                    output_data = file_writer.into_inner()?;
                    let partition_end = output_data.seek(SeekFrom::Current(0))?;
                    let ipc_length: u64 = partition_end - partition_start;
                    output_data.write_all(&ipc_length.to_le_bytes()[..])?;
                    output_data.flush()?;
                    offset = ipc_length + 8;
                }

                // append partition in each spills
                for s in 0..output_spills.len() {
                    let spill = &output_spills[s];
                    let length = spill.offsets[i + 1] - spill.offsets[i];
                    if length > 0 {
                        let mut reader = &spill_files[s];
                        reader.seek(SeekFrom::Start(spill.offsets[i]))?;
                        let mut take = reader.take(length);
                        std::io::copy(&mut take, &mut output_data)?;
                        output_data.flush()?;
                    }
                    offset += length;
                }
            }
            // add one extra offset at last to ease partition length computation
            offsets[num_output_partitions] = offset;

            let mut output_index = File::create(index_file_clone)?;
            for offset in offsets {
                output_index.write_all(&(offset as i64).to_le_bytes()[..])?;
            }
            output_index.flush()?;
            Ok::<(), DataFusionError>(())
        })
        .await;

        if let Err(e) = res {
            return Err(DataFusionError::Execution(format!(
                "Error occurred while writing shuffle output {}",
                e
            )));
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("data", DataType::Utf8, false),
            Field::new("index", DataType::Utf8, false),
        ]));

        let shuffle_result = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from_slice(&[data_file])),
                Arc::new(StringArray::from_slice(&[index_file])),
            ],
        )?;

        Ok(Box::pin(MemoryStream::try_new(
            vec![shuffle_result],
            schema,
            None,
        )?))
    }

    fn used(&self) -> usize {
        self.used.load(Ordering::SeqCst)
    }

    fn spilled_bytes(&self) -> usize {
        self.inner_metrics.spilled_bytes().value()
    }

    fn spill_count(&self) -> usize {
        self.inner_metrics.spill_count().value()
    }
}

/// consume the `buffered_partitions` and do spill into a single temp shuffle output file
async fn spill_into(
    buffered_partitions: &mut Vec<PartitionBuffer>,
    schema: SchemaRef,
    path: &str,
    num_output_partitions: usize,
) -> Result<(usize, Vec<u64>)> {
    let mut freed = 0;
    let mut output_batches: Vec<Vec<RecordBatch>> = vec![vec![]; num_output_partitions];

    for i in 0..num_output_partitions {
        freed += buffered_partitions[i].size_estimation();
        let partition_batches = buffered_partitions[i].output_all()?;
        output_batches[i] = partition_batches;
    }
    let path = path.to_owned();

    let res = task::spawn_blocking(move || {
        let mut offset: u64 = 0;
        let mut offsets = vec![0; num_output_partitions + 1];
        let mut file = OpenOptions::new().read(true).append(true).open(path)?;

        for i in 0..num_output_partitions {
            let partition_start = file.seek(SeekFrom::Current(0))?;
            let partition_batches = &output_batches[i];
            offsets[i] = offset;
            if partition_batches.len() > 0 {
                let mut file_writer = FileWriter::try_new(file, &schema)?;
                for batch in partition_batches {
                    file_writer.write(&batch)?;
                }
                file_writer.finish()?;

                file = file_writer.into_inner()?;
                let partition_end = file.seek(SeekFrom::Current(0))?;
                let ipc_length: u64 = partition_end - partition_start;
                file.write_all(&ipc_length.to_le_bytes()[..])?;
                file.flush()?;
                offset = ipc_length + 8;
            }
        }
        // add one extra offset at last to ease partition length computation
        offsets[num_output_partitions] = offset;
        Ok(offsets)
    })
    .await;

    match res {
        Ok(r) => r.map(|s| (freed, s)),
        Err(e) => Err(DataFusionError::Execution(format!(
            "Error occurred while spilling {}",
            e
        ))),
    }
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
        info!(
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

        let path = self.runtime.disk_manager.create_tmp_file()?;
        let (total_size, offsets) = spill_into(
            &mut *buffered_partitions,
            self.schema.clone(),
            path.as_str(),
            self.num_output_partitions,
        )
        .await?;

        let mut spills = self.spills.lock().await;
        let used = self.used.swap(0, Ordering::SeqCst);
        self.inner_metrics.record_spill(total_size);
        spills.push(SpillInfo { path, offsets });
        Ok(total_size)
    }

    fn mem_used(&self) -> usize {
        self.used.load(Ordering::SeqCst)
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
    /// the shuffle id this map belongs to
    shuffle_id: usize,
    /// Containing all metrics set created during sort
    all_metrics: AggregatedMetricsSet,
}

impl ShuffleWriterExec {
    /// Input execution plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Partitioning scheme to use
    pub fn partitioning(&self) -> &Partitioning {
        &self.partitioning
    }
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

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(ShuffleWriterExec::try_new(
                children[0].clone(),
                self.partitioning.clone(),
                self.shuffle_id,
            )?)),
            _ => Err(DataFusionError::Internal(
                "RepartitionExec wrong number of children".to_string(),
            )),
        }
    }

    async fn execute(
        &self,
        partition: usize,
        runtime: Arc<RuntimeEnv>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, runtime.clone()).await?;
        external_shuffle(
            input,
            partition,
            self.shuffle_id,
            self.partitioning.clone(),
            self.all_metrics.clone(),
            runtime,
        )
        .await
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.all_metrics.aggregate_all())
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
        shuffle_id: usize,
    ) -> Result<Self> {
        Ok(ShuffleWriterExec {
            input,
            partitioning,
            all_metrics: AggregatedMetricsSet::new(),
            shuffle_id,
        })
    }
}

pub async fn external_shuffle(
    mut input: SendableRecordBatchStream,
    partition_id: usize,
    shuffle_id: usize,
    partitioning: Partitioning,
    metrics: AggregatedMetricsSet,
    runtime: Arc<RuntimeEnv>,
) -> Result<SendableRecordBatchStream> {
    let schema = input.schema();
    let repartitioner = Arc::new(ShuffleRepartitioner::new(
        partition_id,
        shuffle_id,
        schema.clone(),
        partitioning,
        metrics,
        runtime.clone(),
    ));
    runtime.register_consumer(&(repartitioner.clone() as Arc<dyn MemoryConsumer>));

    while let Some(batch) = input.next().await {
        let batch = batch?;
        repartitioner.insert_batch(batch).await?;
    }

    repartitioner.shuffle_write().await
}
