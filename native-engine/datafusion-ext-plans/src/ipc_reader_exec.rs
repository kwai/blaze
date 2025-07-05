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

use std::{
    any::Any,
    fmt::{Debug, Formatter},
    fs::File,
    io::{BufReader, Cursor, Read, Seek, SeekFrom},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
};

use arrow::{
    array::{Array, ArrayRef, RecordBatch, RecordBatchOptions},
    datatypes::SchemaRef,
};
use async_trait::async_trait;
use blaze_jni_bridge::{
    jni_call, jni_call_static, jni_get_byte_array_region, jni_get_direct_buffer, jni_get_string,
    jni_new_direct_byte_buffer, jni_new_global_ref, jni_new_string,
};
use datafusion::{
    common::DataFusionError,
    error::Result,
    execution::context::TaskContext,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan,
        Partitioning::UnknownPartitioning,
        PlanProperties, SendableRecordBatchStream, Statistics,
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
    },
};
use datafusion_ext_commons::{
    arrow::{
        array_size::{ArraySize, BatchSize},
        coalesce::coalesce_arrays_unchecked,
    },
    batch_size, df_execution_err, suggested_batch_mem_size,
};
use jni::objects::{GlobalRef, JObject};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;

use crate::common::{execution_context::ExecutionContext, ipc_compression::IpcCompressionReader};

#[derive(Debug, Clone)]
pub struct IpcReaderExec {
    pub num_partitions: usize,
    pub ipc_provider_resource_id: String,
    pub schema: SchemaRef,
    pub metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}
impl IpcReaderExec {
    pub fn new(
        num_partitions: usize,
        ipc_provider_resource_id: String,
        schema: SchemaRef,
    ) -> IpcReaderExec {
        IpcReaderExec {
            num_partitions,
            ipc_provider_resource_id,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
        }
    }
}

impl DisplayAs for IpcReaderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "IpcReader: [{:?}]", &self.schema)
    }
}

#[async_trait]
impl ExecutionPlan for IpcReaderExec {
    fn name(&self) -> &str {
        "IpcReaderExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        self.props.get_or_init(|| {
            PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                UnknownPartitioning(self.num_partitions),
                ExecutionMode::Bounded,
            )
        })
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            self.num_partitions,
            self.ipc_provider_resource_id.clone(),
            self.schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);
        let elapsed_compute = exec_ctx.baseline_metrics().elapsed_compute().clone();
        let _timer = elapsed_compute.timer();

        let blocks_provider = jni_call_static!(
            JniBridge.getResource(
                jni_new_string!(&self.ipc_provider_resource_id)?.as_obj()
            ) -> JObject
        )?;
        assert!(!blocks_provider.as_obj().is_null());

        let blocks_local = jni_call!(ScalaFunction0(blocks_provider.as_obj()).apply() -> JObject)?;
        assert!(!blocks_local.as_obj().is_null());

        let blocks = jni_new_global_ref!(blocks_local.as_obj())?;
        read_ipc(blocks, exec_ctx.clone())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        todo!()
    }
}

fn read_ipc(
    blocks: GlobalRef,
    exec_ctx: Arc<ExecutionContext>,
) -> Result<SendableRecordBatchStream> {
    let size_counter = exec_ctx.register_counter_metric("size");

    Ok(exec_ctx
        .clone()
        .output_with_sender("IpcReader", move |sender| async move {
            sender.exclude_time(exec_ctx.baseline_metrics().elapsed_compute());
            log::info!("start ipc reading");

            let _timer = exec_ctx.baseline_metrics().elapsed_compute().timer();
            let batch_size = batch_size();
            let staging_cols: Arc<Mutex<Vec<Vec<ArrayRef>>>> = Arc::new(Mutex::new(vec![]));
            let staging_num_rows = AtomicUsize::new(0);
            let staging_mem_size = AtomicUsize::new(0);

            while let Some(block) = {
                let blocks = blocks.clone();
                tokio::task::spawn_blocking(move || {
                    if jni_call!(ScalaIterator(blocks.as_obj()).hasNext() -> bool)? {
                        let block = jni_call!(ScalaIterator(blocks.as_obj()).next() -> JObject)?;
                        return Ok(Some(jni_new_global_ref!(block.as_obj())?));
                    }
                    Ok::<_, DataFusionError>(None)
                })
                .await
                .expect("tokio spawn_blocking error")?
            } {
                // get ipc reader
                let block_cloned = block.clone();
                let mut reader = tokio::task::spawn_blocking(|| {
                    let block = block_cloned;
                    if jni_call!(BlazeBlockObject(block.as_obj()).hasFileSegment() -> bool)? {
                        return get_file_reader(block.as_obj());
                    }
                    if jni_call!(BlazeBlockObject(block.as_obj()).hasByteBuffer() -> bool)? {
                        return get_byte_buffer_reader(block.as_obj());
                    }
                    get_channel_reader(block.as_obj())
                })
                .await
                .expect("tokio spawn_blocking error")?;

                while let Some((num_rows, cols)) =
                    reader.read_batch(&exec_ctx.output_schema()).or_else(|e| {
                        // throw FetchFailureException
                        let block = block.clone();
                        let errmsg = jni_new_string!(e.message().as_ref())?;
                        jni_call!(BlazeBlockObject(block.as_obj())
                            .throwFetchFailed(errmsg.as_obj()) -> ())?; // always return error
                        Ok::<_, DataFusionError>(None)
                    })?
                {
                    let (cur_staging_num_rows, cur_staging_mem_size) = {
                        let staging_cols_cloned = staging_cols.clone();
                        let mut staging_cols = staging_cols_cloned.lock();
                        let mut cols_mem_size = 0;
                        staging_cols.resize_with(cols.len(), || vec![]);
                        for (col_idx, col) in cols.into_iter().enumerate() {
                            cols_mem_size += col.get_array_mem_size();
                            staging_cols[col_idx].push(col);
                        }
                        drop(staging_cols);
                        staging_num_rows.fetch_add(num_rows, SeqCst);
                        staging_mem_size.fetch_add(cols_mem_size, SeqCst);
                        (staging_num_rows.load(SeqCst), staging_mem_size.load(SeqCst))
                    };

                    if cur_staging_num_rows >= batch_size
                        || cur_staging_mem_size >= suggested_batch_mem_size()
                    {
                        let coalesced_cols = std::mem::take(&mut *staging_cols.clone().lock())
                            .into_iter()
                            .map(|cols| coalesce_arrays_unchecked(cols[0].data_type(), &cols))
                            .collect::<Vec<_>>();
                        let batch = RecordBatch::try_new_with_options(
                            exec_ctx.output_schema(),
                            coalesced_cols,
                            &RecordBatchOptions::new().with_row_count(Some(cur_staging_num_rows)),
                        )?;
                        staging_num_rows.store(0, SeqCst);
                        staging_mem_size.store(0, SeqCst);
                        size_counter.add(batch.get_batch_mem_size());
                        exec_ctx.baseline_metrics().record_output(batch.num_rows());
                        sender.send(batch).await;
                    }
                }
            }

            let cur_staging_num_rows = staging_num_rows.load(SeqCst);
            if cur_staging_num_rows > 0 {
                let coalesced_cols = std::mem::take(&mut *staging_cols.clone().lock())
                    .into_iter()
                    .map(|cols| coalesce_arrays_unchecked(cols[0].data_type(), &cols))
                    .collect::<Vec<_>>();
                let batch = RecordBatch::try_new_with_options(
                    exec_ctx.output_schema(),
                    coalesced_cols,
                    &RecordBatchOptions::new().with_row_count(Some(cur_staging_num_rows)),
                )?;
                size_counter.add(batch.get_batch_mem_size());
                exec_ctx.baseline_metrics().record_output(batch.num_rows());
                sender.send(batch).await;
            }
            Ok(())
        }))
}

fn get_channel_reader(block: JObject) -> Result<IpcCompressionReader<Box<dyn Read + Send>>> {
    let channel_reader = ReadableByteChannelReader::try_new(block)?;
    Ok(IpcCompressionReader::new(Box::new(
        BufReader::with_capacity(65536, channel_reader),
    )))
}

fn get_file_reader(block: JObject) -> Result<IpcCompressionReader<Box<dyn Read + Send>>> {
    let path = jni_call!(BlazeBlockObject(block).getFilePath() -> JObject)?;
    let path = jni_get_string!(path.as_obj().into())?;
    let offset = jni_call!(BlazeBlockObject(block).getFileOffset() -> i64)?;
    let length = jni_call!(BlazeBlockObject(block).getFileLength() -> i64)?;
    let mut file = File::open(&path)?;
    file.seek(SeekFrom::Start(offset as u64))?;

    Ok(IpcCompressionReader::new(Box::new(
        BufReader::with_capacity(65536, file.take(length as u64)),
    )))
}

fn get_byte_buffer_reader(block: JObject) -> Result<IpcCompressionReader<Box<dyn Read + Send>>> {
    let byte_buffer = jni_call!(BlazeBlockObject(block).getByteBuffer() -> JObject)?;
    if jni_call!(JavaBuffer(byte_buffer.as_obj()).isDirect() -> bool)? {
        let reader = DirectByteBufferReader::try_new(block, byte_buffer.as_obj())?;
        return Ok(IpcCompressionReader::new(Box::new(reader)));
    }
    if jni_call!(JavaBuffer(byte_buffer.as_obj()).hasArray() -> bool)? {
        let reader = HeapByteBufferReader::try_new(block, byte_buffer.as_obj())?;
        return Ok(IpcCompressionReader::new(Box::new(reader)));
    }
    df_execution_err!("ByteBuffer is not direct and do not have array")
}

struct ReadableByteChannelReader {
    channel: GlobalRef,
    closed: bool,
}
impl ReadableByteChannelReader {
    pub fn try_new(block: JObject) -> Result<Self> {
        let channel = jni_call!(BlazeBlockObject(block).getChannel() -> JObject)?;
        let global_ref = jni_new_global_ref!(channel.as_obj())?;
        Ok(Self {
            channel: global_ref,
            closed: false,
        })
    }

    pub fn close(&mut self) -> Result<()> {
        if !self.closed {
            jni_call!(JavaReadableByteChannel(self.channel.as_obj()).close() -> ())?;
            self.closed = true;
        }
        Ok(())
    }

    fn read_impl(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.closed {
            return Ok(0);
        }
        let mut total_read_bytes = 0;
        let buf = jni_new_direct_byte_buffer!(buf)?;

        while jni_call!(JavaBuffer(buf.as_obj()).hasRemaining() -> bool)? {
            let read_bytes = jni_call!(
                JavaReadableByteChannel(self.channel.as_obj()).read(buf.as_obj()) -> i32
            )?;

            if read_bytes < 0 {
                self.close()?;
                break;
            }
            total_read_bytes += read_bytes as usize;
        }
        Ok(total_read_bytes)
    }
}

impl Read for ReadableByteChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.read_impl(buf).map_err(std::io::Error::other)
    }
}

impl Drop for ReadableByteChannelReader {
    fn drop(&mut self) {
        // ensure the channel is closed
        let _ = self.close();
    }
}

struct DirectByteBufferReader {
    block: GlobalRef,
    byte_buffer: GlobalRef,
    inner: Cursor<&'static [u8]>,
}

impl DirectByteBufferReader {
    pub fn try_new(block: JObject, byte_buffer: JObject) -> Result<Self> {
        let block_global_ref = jni_new_global_ref!(block)?;
        let byte_buffer_global_ref = jni_new_global_ref!(byte_buffer)?;
        let data = jni_get_direct_buffer!(byte_buffer_global_ref.as_obj())?;
        let pos = jni_call!(JavaBuffer(byte_buffer).position() -> i32)? as usize;
        let remaining = jni_call!(JavaBuffer(byte_buffer).remaining() -> i32)? as usize;
        Ok(Self {
            block: block_global_ref,
            byte_buffer: byte_buffer_global_ref,
            inner: Cursor::new(&data[pos..][..remaining]),
        })
    }

    pub fn close(&mut self) -> Result<()> {
        jni_call!(JavaAutoCloseable(self.block.as_obj()).close() -> ())?;
        Ok(())
    }
}

impl Read for DirectByteBufferReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

impl Drop for DirectByteBufferReader {
    fn drop(&mut self) {
        // ensure the block is closed
        let _ = self.close();
        let _ = self.byte_buffer;
    }
}

struct HeapByteBufferReader {
    block: GlobalRef,
    byte_array: GlobalRef,
    pos: usize,
    remaining: usize,
}
unsafe impl Send for HeapByteBufferReader {} // jarray is safe to send

impl HeapByteBufferReader {
    pub fn try_new(block: JObject, byte_buffer: JObject) -> Result<Self> {
        let block_global_ref = jni_new_global_ref!(block)?;
        let byte_array = jni_call!(JavaBuffer(byte_buffer).array() -> JObject)?;
        let array_offset = jni_call!(JavaBuffer(byte_buffer).arrayOffset() -> i32)? as usize;
        let pos = jni_call!(JavaBuffer(byte_buffer).position() -> i32)? as usize;
        let remaining = jni_call!(JavaBuffer(byte_buffer).remaining() -> i32)? as usize;
        let byet_array_global_ref = jni_new_global_ref!(byte_array.as_obj())?;
        Ok(Self {
            block: block_global_ref,
            byte_array: byet_array_global_ref,
            pos: array_offset + pos,
            remaining,
        })
    }

    pub fn close(&mut self) -> Result<()> {
        jni_call!(JavaAutoCloseable(self.block.as_obj()).close() -> ())?;
        Ok(())
    }

    fn read_impl(&mut self, buf: &mut [u8]) -> Result<usize> {
        let read_len = buf.len().min(self.remaining);
        if read_len > 0 {
            jni_get_byte_array_region!(self.byte_array.as_obj(), self.pos, &mut buf[..read_len])?;
            self.pos += read_len;
            self.remaining -= read_len;
        }
        Ok(read_len)
    }
}

impl Read for HeapByteBufferReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.read_impl(buf).map_err(std::io::Error::other)
    }
}

impl Drop for HeapByteBufferReader {
    fn drop(&mut self) {
        // ensure the block is closed
        let _ = self.close();
        let _ = self.byte_array;
        let _ = self.block;
    }
}
