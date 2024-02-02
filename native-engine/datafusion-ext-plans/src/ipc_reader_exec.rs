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
    io::{BufReader, Read, Seek, SeekFrom},
    sync::Arc,
};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use async_trait::async_trait;
use blaze_jni_bridge::{
    jni_call, jni_call_static, jni_get_object_class, jni_get_string, jni_new_direct_byte_buffer,
    jni_new_global_ref, jni_new_string,
};
use datafusion::{
    error::{DataFusionError, Result},
    execution::context::TaskContext,
    physical_plan::{
        expressions::PhysicalSortExpr,
        metrics::{BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
        Partitioning::UnknownPartitioning,
        SendableRecordBatchStream, Statistics,
    },
};
use datafusion_ext_commons::{
    df_execution_err, io::read_one_batch, streams::coalesce_stream::CoalesceInput,
};
use futures::{stream::once, TryStreamExt};
use jni::objects::{GlobalRef, JObject};
use parking_lot::Mutex;

use crate::common::output::TaskOutputter;

#[derive(Debug, Clone)]
pub struct IpcReaderExec {
    pub num_partitions: usize,
    pub ipc_provider_resource_id: String,
    pub schema: SchemaRef,
    pub mode: IpcReadMode,
    pub metrics: ExecutionPlanMetricsSet,
}
impl IpcReaderExec {
    pub fn new(
        num_partitions: usize,
        ipc_provider_resource_id: String,
        schema: SchemaRef,
        mode: IpcReadMode,
    ) -> IpcReaderExec {
        IpcReaderExec {
            num_partitions,
            ipc_provider_resource_id,
            schema,
            mode,
            metrics: ExecutionPlanMetricsSet::new(),
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
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        UnknownPartitioning(self.num_partitions)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
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
            self.mode,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let size_counter = MetricBuilder::new(&self.metrics).counter("size", partition);

        let elapsed_compute = baseline_metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer();

        let segments_provider = jni_call_static!(
            JniBridge.getResource(
                jni_new_string!(&self.ipc_provider_resource_id)?.as_obj()
            ) -> JObject
        )?;
        let segments_local =
            jni_call!(ScalaFunction0(segments_provider.as_obj()).apply() -> JObject)?;
        let segments = jni_new_global_ref!(segments_local.as_obj())?;

        let ipc_stream = Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            once(read_ipc(
                context.clone(),
                self.schema(),
                self.mode,
                segments,
                baseline_metrics.clone(),
                size_counter,
            ))
            .try_flatten(),
        ));
        Ok(context.coalesce_with_default_batch_size(ipc_stream, &baseline_metrics)?)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum IpcReadMode {
    ChannelUncompressed,
    Channel,
    ChannelAndFileSegment,
}

pub async fn read_ipc(
    context: Arc<TaskContext>,
    schema: SchemaRef,
    mode: IpcReadMode,
    segments: GlobalRef,
    baseline_metrics: BaselineMetrics,
    size_counter: Count,
) -> Result<SendableRecordBatchStream> {
    context.output_with_sender("IpcReader", schema.clone(), move |sender| async move {
        let mut timer = baseline_metrics.elapsed_compute().timer();
        loop {
            let segments = segments.clone();
            let schema = schema.clone();
            let reader = tokio::task::spawn_blocking(move || {
                if !jni_call!(ScalaIterator(segments.as_obj()).hasNext() -> bool)? {
                    return Ok::<_, DataFusionError>(None);
                }
                let segment = jni_new_global_ref!(
                    jni_call!(ScalaIterator(segments.as_obj()).next() -> JObject)?.as_obj()
                )?;
                let schema = schema.clone();
                let reader = Arc::new(Mutex::new(match mode {
                    IpcReadMode::ChannelUncompressed => {
                        get_channel_reader(Some(schema), segment.as_obj(), false)?
                    }
                    IpcReadMode::Channel => {
                        get_channel_reader(Some(schema), segment.as_obj(), true)?
                    }
                    IpcReadMode::ChannelAndFileSegment => {
                        let segment_class = jni_get_object_class!(segment.as_obj())?;
                        let segment_classname_obj =
                            jni_call!(Class(segment_class.as_obj()).getName() -> JObject)?;
                        let segment_classname =
                            jni_get_string!(segment_classname_obj.as_obj().into())?;

                        if segment_classname == "org.apache.spark.storage.FileSegment" {
                            get_file_segment_reader(Some(schema), segment.as_obj())?
                        } else {
                            get_channel_reader(Some(schema), segment.as_obj(), true)?
                        }
                    }
                }));
                Ok(Some(reader))
            })
            .await
            .or_else(|err| df_execution_err!("{err}"))??;

            if let Some(reader) = reader {
                while let Some(batch) = {
                    let reader_cloned = reader.clone();
                    tokio::task::spawn_blocking(move || reader_cloned.clone().lock().next_batch())
                        .await
                        .or_else(|err| df_execution_err!("{err}"))??
                } {
                    size_counter.add(batch.get_array_memory_size());
                    baseline_metrics.record_output(batch.num_rows());
                    sender.send(Ok(batch), Some(&mut timer)).await;
                }
            } else {
                break; // finished
            }
        }
        Ok(())
    })
}

fn get_channel_reader(
    schema: Option<SchemaRef>,
    channel: JObject,
    compressed: bool,
) -> Result<RecordBatchReader> {
    let global_ref = jni_new_global_ref!(channel)?;
    let channel_reader = ReadableByteChannelReader::new(global_ref);

    Ok(RecordBatchReader::new(
        Box::new(BufReader::with_capacity(65536, channel_reader)),
        schema,
        compressed,
    ))
}

fn get_file_segment_reader(
    schema: Option<SchemaRef>,
    file_segment: JObject,
) -> Result<RecordBatchReader> {
    let file = jni_call!(SparkFileSegment(file_segment).file() -> JObject)?;
    let path = jni_call!(JavaFile(file.as_obj()).getPath() -> JObject)?;
    let path = jni_get_string!(path.as_obj().into())?;
    let offset = jni_call!(SparkFileSegment(file_segment).offset() -> i64)?;
    let length = jni_call!(SparkFileSegment(file_segment).length() -> i64)?;

    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset as u64))?;

    Ok(RecordBatchReader::new(
        Box::new(file.take(length as u64)),
        schema,
        true,
    ))
}

struct ReadableByteChannelReader {
    channel: GlobalRef,
    closed: bool,
}
impl ReadableByteChannelReader {
    pub fn new(channel: GlobalRef) -> Self {
        Self {
            channel,
            closed: false,
        }
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
        let buf = jni_new_direct_byte_buffer!(buf)?;

        while jni_call!(JavaBuffer(buf.as_obj()).hasRemaining() -> bool)? {
            let read_bytes = jni_call!(
                JavaReadableByteChannel(self.channel.as_obj()).read(buf.as_obj()) -> i32
            )?;

            if read_bytes < 0 {
                self.close()?;
                break;
            }
        }
        let position = jni_call!(JavaBuffer(buf.as_obj()).position() -> i32)?;
        Ok(position as usize)
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

pub struct RecordBatchReader {
    input: Box<dyn Read + Send>,
    schema: Option<SchemaRef>,
    compress: bool,
}

impl RecordBatchReader {
    pub fn new(input: Box<dyn Read + Send>, schema: Option<SchemaRef>, compress: bool) -> Self {
        Self {
            input,
            schema,
            compress,
        }
    }

    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        read_one_batch(&mut self.input, self.schema.clone(), self.compress)
    }
}
