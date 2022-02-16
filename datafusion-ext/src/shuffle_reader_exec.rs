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

use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::Statistics;
use futures::Stream;
use jni::errors::Result as JniResult;
use jni::objects::JObject;
use jni::objects::JValue;
use log::info;

use crate::jni_bridge::JavaClasses;
use crate::jni_bridge_call_method;
use crate::jni_bridge_call_static_method;
use crate::util::Util;

#[derive(Debug, Clone)]
pub struct BlazeShuffleReaderExec {
    pub job_id: String,
    pub schema: SchemaRef,
    pub metrics: ExecutionPlanMetricsSet,
}
impl BlazeShuffleReaderExec {
    pub fn new(job_id: String, schema: SchemaRef) -> BlazeShuffleReaderExec {
        BlazeShuffleReaderExec {
            job_id,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

#[async_trait]
impl ExecutionPlan for BlazeShuffleReaderExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "Blaze ShuffleReaderExec does not support with_new_children()".to_owned(),
        ))
    }

    async fn execute(
        &self,
        _partition: usize,
        _runtime: Arc<RuntimeEnv>,
    ) -> Result<SendableRecordBatchStream> {
        let buffers = Util::to_datafusion_external_result(Ok(()).and_then(|_| {
            let env = JavaClasses::get_thread_jnienv();
            let resource_key = format!("ShuffleReader.buffers:{}", self.job_id);
            info!(
                "Shuffle reader FetchIterator resource key: {}",
                &resource_key
            );
            let buffers = jni_bridge_call_static_method!(
                env,
                JniBridge.getResource,
                JValue::Object(env.new_string(&resource_key)?.into())
            )?
            .l()?;
            info!("FetchIterator: {:?}", buffers);
            JniResult::Ok(buffers)
        }))?;
        let schema = self.schema.clone();
        let baseline_metrics = BaselineMetrics::new(&self.metrics, 0);
        Ok(Box::pin(ShuffleReaderStream::new(
            schema,
            buffers,
            baseline_metrics,
        )))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

struct ShuffleReaderStream {
    schema: SchemaRef,
    buffers: JObject<'static>,
    seekable_byte_channels: JObject<'static>,
    seekable_byte_channels_len: usize,
    seekable_byte_channels_pos: usize,
    seekable_byte_channel: JObject<'static>,
    arrow_file_reader: Option<FileReader<SeekableByteChannelReader>>,
    baseline_metrics: BaselineMetrics,
}
unsafe impl Sync for ShuffleReaderStream {} // safety: buffers is safe to be shared
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for ShuffleReaderStream {}

impl ShuffleReaderStream {
    pub fn new(
        schema: SchemaRef,
        buffers: JObject<'static>,
        baseline_metrics: BaselineMetrics,
    ) -> ShuffleReaderStream {
        ShuffleReaderStream {
            schema,
            buffers,
            seekable_byte_channels: JObject::null(),
            seekable_byte_channels_len: 0,
            seekable_byte_channels_pos: 0,
            seekable_byte_channel: JObject::null(),
            arrow_file_reader: None,
            baseline_metrics,
        }
    }

    fn next_seekable_byte_channel(&mut self) -> Result<bool> {
        while self.seekable_byte_channels_pos == self.seekable_byte_channels_len {
            if !self.next_seekable_byte_channels()? {
                self.seekable_byte_channel = JObject::null();
                self.arrow_file_reader = None;
                return Ok(false);
            }
        }

        Util::to_datafusion_external_result(Ok(()).and_then(|_| {
            let env = JavaClasses::get_thread_jnienv();
            self.seekable_byte_channel = jni_bridge_call_method!(
                env,
                JavaList.get,
                self.seekable_byte_channels,
                JValue::Int(self.seekable_byte_channels_pos as i32)
            )?
            .l()?;
            JniResult::Ok(())
        }))?;

        let seekable_byte_channel_reader =
            SeekableByteChannelReader(self.seekable_byte_channel);
        self.arrow_file_reader = Some(FileReader::try_new(seekable_byte_channel_reader)?);
        self.seekable_byte_channels_pos += 1;
        Ok(true)
    }

    fn next_seekable_byte_channels(&mut self) -> Result<bool> {
        if !self.buffers_has_next()? {
            self.seekable_byte_channels = JObject::null();
            self.seekable_byte_channels_len = 0;
            self.seekable_byte_channels_pos = 0;
            return Ok(false);
        }
        Util::to_datafusion_external_result(Ok(()).and_then(|_| {
            let env = JavaClasses::get_thread_jnienv();
            let next =
                jni_bridge_call_method!(env, ScalaIterator.next, self.buffers)?.l()?;
            let next_managed_buffer =
                jni_bridge_call_method!(env, ScalaTuple2._2, next)?.l()?;

            self.seekable_byte_channels = jni_bridge_call_static_method!(
                env,
                SparkBlazeConverters.readManagedBufferToSegmentByteChannelsAsJava,
                JValue::Object(next_managed_buffer)
            )?
            .l()?;
            self.seekable_byte_channels_len =
                jni_bridge_call_method!(env, JavaList.size, self.seekable_byte_channels)?
                    .i()? as usize;
            self.seekable_byte_channels_pos = 0;
            JniResult::Ok(true)
        }))
    }

    fn buffers_has_next(&self) -> Result<bool> {
        Util::to_datafusion_external_result(Ok(()).and_then(|_| {
            let env = JavaClasses::get_thread_jnienv();
            return jni_bridge_call_method!(env, ScalaIterator.hasNext, self.buffers)?
                .z();
        }))
    }
}

impl Stream for ShuffleReaderStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(arrow_file_reader) = &mut self.arrow_file_reader {
            if let Some(record_batch) = arrow_file_reader.next() {
                return self
                    .baseline_metrics
                    .record_poll(Poll::Ready(Some(record_batch)));
            }
        }

        // current arrow file reader reaches EOF, try next ipc
        if self.next_seekable_byte_channel().unwrap() {
            return self.poll_next(cx);
        }
        Poll::Ready(None)
    }
}
impl RecordBatchStream for ShuffleReaderStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

struct SeekableByteChannelReader(JObject<'static>);
impl Read for SeekableByteChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(())
            .and_then(|_| {
                let env = JavaClasses::get_thread_jnienv();
                return JniResult::Ok(
                    jni_bridge_call_method!(
                        env,
                        JavaNioSeekableByteChannel.read,
                        self.0,
                        JValue::Object(env.new_direct_byte_buffer(buf)?.into())
                    )?
                    .i()? as usize,
                );
            })
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "JNI error: SeekableByteChannelReader.jni_read",
                )
            })
    }
}
impl Seek for SeekableByteChannelReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        Ok(())
            .and_then(|_| {
                let env = JavaClasses::get_thread_jnienv();
                match pos {
                    SeekFrom::Start(position) => {
                        jni_bridge_call_method!(
                            env,
                            JavaNioSeekableByteChannel.setPosition,
                            self.0,
                            JValue::Long(position as i64)
                        )?;
                        JniResult::Ok(position)
                    }

                    SeekFrom::End(offset) => {
                        let size = jni_bridge_call_method!(
                            env,
                            JavaNioSeekableByteChannel.size,
                            self.0
                        )?
                        .j()? as u64;
                        let position = size + offset as u64;
                        jni_bridge_call_method!(
                            env,
                            JavaNioSeekableByteChannel.setPosition,
                            self.0,
                            JValue::Long(position as i64)
                        )?;
                        JniResult::Ok(position)
                    }

                    SeekFrom::Current(_) => unimplemented!(),
                }
            })
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "JNI error: SeekableByteChannelReader.jni_seek",
                )
            })
    }
}
