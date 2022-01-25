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
use datafusion::error::DataFusionError;
use datafusion::error::Result;
use datafusion::execution::runtime_env::RuntimeEnv;
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
use jni::JNIEnv;
use jni::JavaVM;

use crate::jni_bridge::JavaClasses;
use crate::util::Util;

pub struct BlazeShuffleReaderExec {
    pub jvm: JavaVM,
    pub job_id: String,
    pub schema: SchemaRef,
}

impl Debug for BlazeShuffleReaderExec {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "BlazeShuffleReaderExec: job_id={}", self.job_id)
    }
}

impl Clone for BlazeShuffleReaderExec {
    fn clone(&self) -> Self {
        Self {
            jvm: Util::jvm_clone(&self.jvm),
            job_id: self.job_id.clone(),
            schema: self.schema.clone(),
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
        let jni_get_buffers = || -> JniResult<JObject<'static>> {
            let env = Util::jni_env_clone(&self.jvm.attach_current_thread_permanently()?);
            let resource_key = format!("ShuffleReader.buffers:{}", self.job_id);
            info!(
                "Shuffle reader FetchIterator resource key: {}",
                &resource_key
            );
            let buffers = env
                .call_static_method_unchecked(
                    JavaClasses::get().cJniBridge.class,
                    JavaClasses::get().cJniBridge.method_get_resource,
                    JavaClasses::get()
                        .cJniBridge
                        .method_get_resource_ret
                        .clone(),
                    &[JValue::Object(env.new_string(resource_key)?.into())],
                )?
                .l()?;
            info!("FetchIterator: {:?}", buffers);
            Ok(buffers)
        };
        let buffers = jni_get_buffers().map_err(|_| {
            Util::wrap_default_data_fusion_io_error(
                "JNI error: BlazeShuffleReaderExec.execute.jni_get_buffers",
            )
        })?;
        let schema = self.schema.clone();

        Ok(Box::pin(ShuffleReaderStream::new(
            Util::jvm_clone(&self.jvm),
            schema,
            buffers,
        )))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

struct ShuffleReaderStream {
    jvm: JavaVM,
    schema: SchemaRef,
    buffers: JObject<'static>,
    seekable_byte_channels: JObject<'static>,
    seekable_byte_channels_len: usize,
    seekable_byte_channels_pos: usize,
    seekable_byte_channel: JObject<'static>,
    arrow_file_reader: Option<FileReader<SeekableByteChannelReader>>,
}
unsafe impl Sync for ShuffleReaderStream {} // safety: buffers is safe to be shared
unsafe impl Send for ShuffleReaderStream {}

impl ShuffleReaderStream {
    pub fn new(
        jvm: JavaVM,
        schema: SchemaRef,
        buffers: JObject<'static>,
    ) -> ShuffleReaderStream {
        ShuffleReaderStream {
            jvm,
            schema,
            buffers,
            seekable_byte_channels: JObject::null(),
            seekable_byte_channels_len: 0,
            seekable_byte_channels_pos: 0,
            seekable_byte_channel: JObject::null(),
            arrow_file_reader: None,
        }
    }

    fn next_seekable_byte_channel(&mut self) -> Result<bool> {
        if self.seekable_byte_channels_pos == self.seekable_byte_channels_len
            && !self.next_seekable_byte_channels()?
        {
            self.seekable_byte_channel = JObject::null();
            self.arrow_file_reader = None;
            return Ok(false);
        }
        let mut jni_next_seekable_byte_channel = || -> JniResult<()> {
            let env: &JNIEnv =
                &Util::jni_env_clone(&self.jvm.attach_current_thread_permanently()?);
            self.seekable_byte_channel = env
                .call_method_unchecked(
                    self.seekable_byte_channels,
                    JavaClasses::get().cJavaList.method_get,
                    JavaClasses::get().cJavaList.method_get_ret.clone(),
                    &[JValue::Int(self.seekable_byte_channels_pos as i32)],
                )?
                .l()?;
            Ok(())
        };

        jni_next_seekable_byte_channel().map_err(|_| {
            Util::wrap_default_data_fusion_io_error(
                "JNI error: ShuffleReaderStream.jni_next_seekable_byte_channel",
            )
        })?;
        let seekable_byte_channel_reader = SeekableByteChannelReader(
            Util::jvm_clone(&self.jvm),
            self.seekable_byte_channel,
        );
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
        let mut jni_next_seekable_byte_channels = || -> JniResult<bool> {
            let env: &JNIEnv =
                &Util::jni_env_clone(&self.jvm.attach_current_thread_permanently()?);
            let next = env
                .call_method_unchecked(
                    self.buffers,
                    JavaClasses::get().cScalaIterator.method_next,
                    JavaClasses::get().cScalaIterator.method_next_ret.clone(),
                    &[],
                )?
                .l()?;
            let next_managed_buffer = env
                .call_method_unchecked(
                    next,
                    JavaClasses::get().cScalaTuple2.method_2,
                    JavaClasses::get().cScalaTuple2.method_2_ret.clone(),
                    &[],
                )?
                .l()?;

            self.seekable_byte_channels = env
                .call_static_method_unchecked(
                    JavaClasses::get().cSparkBlazeConverters.class,
                    JavaClasses::get()
                        .cSparkBlazeConverters
                        .method_read_managed_buffer_to_segment_byte_channels_as_java,
                    JavaClasses::get()
                        .cSparkBlazeConverters
                        .method_read_managed_buffer_to_segment_byte_channels_as_java_ret
                        .clone(),
                    &[JValue::Object(next_managed_buffer)],
                )?
                .l()?;
            self.seekable_byte_channels_len = env
                .call_method_unchecked(
                    self.seekable_byte_channels,
                    JavaClasses::get().cJavaList.method_size,
                    JavaClasses::get().cJavaList.method_size_ret.clone(),
                    &[],
                )?
                .i()? as usize;
            self.seekable_byte_channels_pos = 0;
            Ok(true)
        };
        jni_next_seekable_byte_channels().map_err(|_| {
            Util::wrap_default_data_fusion_io_error(
                "JNI error: ShuffleReaderStream.jni_next_seekable_byte_channels",
            )
        })
    }

    fn buffers_has_next(&self) -> Result<bool> {
        let jni_buffers_has_next = || -> JniResult<bool> {
            let env: &JNIEnv =
                &Util::jni_env_clone(&self.jvm.attach_current_thread_permanently()?);
            return env
                .call_method_unchecked(
                    self.buffers,
                    JavaClasses::get().cScalaIterator.method_has_next,
                    JavaClasses::get()
                        .cScalaIterator
                        .method_has_next_ret
                        .clone(),
                    &[],
                )?
                .z();
        };
        jni_buffers_has_next().map_err(|_| {
            Util::wrap_default_data_fusion_io_error(
                "JNI error: ShuffleReaderStream.jni_buffers_has_next",
            )
        })
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
                return Poll::Ready(Some(record_batch));
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

struct SeekableByteChannelReader(JavaVM, JObject<'static>);
impl Read for SeekableByteChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut jni_read = || -> JniResult<usize> {
            let env = &Util::jni_env_clone(&self.0.attach_current_thread_permanently()?);
            return Ok(env
                .call_method_unchecked(
                    self.1,
                    JavaClasses::get().cJavaNioSeekableByteChannel.method_read,
                    JavaClasses::get()
                        .cJavaNioSeekableByteChannel
                        .method_read_ret
                        .clone(),
                    &[JValue::Object(env.new_direct_byte_buffer(buf)?.into())],
                )?
                .i()? as usize);
        };
        jni_read().map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                "JNI error: SeekableByteChannelReader.jni_read",
            )
        })
    }
}
impl Seek for SeekableByteChannelReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let jni_seek = || -> JniResult<u64> {
            let env = &Util::jni_env_clone(&self.0.attach_current_thread_permanently()?);
            match pos {
                SeekFrom::Start(position) => {
                    env.call_method_unchecked(
                        self.1,
                        JavaClasses::get()
                            .cJavaNioSeekableByteChannel
                            .method_position_set,
                        JavaClasses::get()
                            .cJavaNioSeekableByteChannel
                            .method_position_set_ret
                            .clone(),
                        &[JValue::Long(position as i64)],
                    )?;
                    Ok(position)
                }

                SeekFrom::End(offset) => {
                    let size = env
                        .call_method_unchecked(
                            self.1,
                            JavaClasses::get().cJavaNioSeekableByteChannel.method_size,
                            JavaClasses::get()
                                .cJavaNioSeekableByteChannel
                                .method_size_ret
                                .clone(),
                            &[],
                        )?
                        .j()? as u64;

                    let position = size + offset as u64;
                    env.call_method_unchecked(
                        self.1,
                        JavaClasses::get()
                            .cJavaNioSeekableByteChannel
                            .method_position_set,
                        JavaClasses::get()
                            .cJavaNioSeekableByteChannel
                            .method_position_set_ret
                            .clone(),
                        &[JValue::Long(position as i64)],
                    )?;
                    Ok(position)
                }

                SeekFrom::Current(_) => unimplemented!(),
            }
        };
        jni_seek().map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                "JNI error: SeekableByteChannelReader.jni_seek",
            )
        })
    }
}
