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

use std::fmt::Debug;

use blaze_commons::{
    jni_call, jni_delete_local_ref, jni_get_object_class, jni_get_string,
    jni_new_direct_byte_buffer, jni_new_global_ref, ResultExt,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::physical_plan::common::batch_byte_size;
use datafusion::physical_plan::metrics::{BaselineMetrics, Count};
use datafusion::physical_plan::RecordBatchStream;
use futures::Stream;
use jni::objects::{GlobalRef, JObject};
use jni::sys::{jboolean, jint, jlong, JNI_TRUE};
use std::fs::File;
use std::io::Seek;
use std::io::{BufReader, Read, SeekFrom};
use std::path::Path;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[derive(Debug, Clone, Copy)]
pub enum IpcReadMode {
    /// for ConvertToNative
    ChannelUncompressed,

    /// for BroadcastExchange reader
    Channel,

    /// for ShuffleExchange reader
    ChannelAndFileSegment,
}

pub struct IpcReaderStream {
    schema: SchemaRef,
    mode: IpcReadMode,
    segments: GlobalRef,
    reader: Option<Box<dyn RecordBatchReader>>,
    baseline_metrics: BaselineMetrics,
    size_counter: Count,
}
unsafe impl Sync for IpcReaderStream {} // safety: segments is safe to be shared
unsafe impl Send for IpcReaderStream {} // #[allow(clippy::non_send_fields_in_send_ty)]

impl IpcReaderStream {
    pub fn new(
        schema: SchemaRef,
        segments: GlobalRef,
        mode: IpcReadMode,
        baseline_metrics: BaselineMetrics,
        size_counter: Count,
    ) -> IpcReaderStream {
        IpcReaderStream {
            schema,
            mode,
            segments,
            reader: None,
            baseline_metrics,
            size_counter,
        }
    }

    fn next_segment(&mut self) -> Result<bool> {
        let has_next = jni_call!(
            ScalaIterator(self.segments.as_obj()).hasNext() -> jboolean
        )?;
        if has_next != JNI_TRUE {
            self.reader = None;
            return Ok(false);
        }
        let segment = jni_call!(
            ScalaIterator(self.segments.as_obj()).next() -> JObject
        )?;

        self.reader = Some(match self.mode {
            IpcReadMode::ChannelUncompressed => get_channel_reader(segment, false)?,
            IpcReadMode::Channel => get_channel_reader(segment, true)?,
            IpcReadMode::ChannelAndFileSegment => {
                let segment_class = jni_get_object_class!(segment)?;
                let segment_classname =
                    jni_call!(Class(segment_class).getName() -> JObject)?;
                let segment_classname = jni_get_string!(segment_classname.into())?;
                if segment_classname == "org.apache.spark.storage.FileSegment" {
                    get_file_segment_reader(segment)?
                } else {
                    get_channel_reader(segment, true)?
                }
            }
        });
        Ok(true)
    }
}

fn get_channel_reader(
    channel: JObject,
    compressed: bool,
) -> Result<Box<dyn RecordBatchReader>> {
    let global_ref = jni_new_global_ref!(channel)?;
    jni_delete_local_ref!(channel)?;
    Ok(Box::new(ReadableByteChannelBatchReader::try_new(
        global_ref, compressed,
    )?))
}

fn get_file_segment_reader(file_segment: JObject) -> Result<Box<dyn RecordBatchReader>> {
    let file = jni_call!(SparkFileSegment(file_segment).file() -> JObject)?;
    let path = jni_call!(JavaFile(file).getPath() -> JObject)?;
    let path = jni_get_string!(path.into())?;
    let offset = jni_call!(SparkFileSegment(file_segment).offset() -> jlong)?;
    let length = jni_call!(SparkFileSegment(file_segment).length() -> jlong)?;
    Ok(Box::new(FileSegmentBatchReader::try_new(
        path,
        offset as u64,
        length as u64,
    )?))
}

impl Stream for IpcReaderStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer();

        if let Some(reader) = &mut self.reader {
            if let Some(batch) = reader.next_batch() {
                if let Ok(batch) = batch.as_ref() {
                    self.size_counter.add(batch_byte_size(batch));
                }
                return self.baseline_metrics.record_poll(Poll::Ready(Some(batch)));
            }
        }

        // current arrow file reader reaches EOF, try next ipc
        if self.next_segment()? {
            return self.poll_next(cx);
        }
        Poll::Ready(None)
    }
}
impl RecordBatchStream for IpcReaderStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

trait RecordBatchReader {
    fn next_batch(&mut self) -> Option<ArrowResult<RecordBatch>>;
}

// record batch reader for byte channel
struct ReadableByteChannelBatchReader {
    inner: StreamReader<Box<dyn Read>>,
}

impl ReadableByteChannelBatchReader {
    fn try_new(channel: GlobalRef, compressed: bool) -> ArrowResult<Self> {
        let channel_reader = ReadableByteChannelReader(channel);
        let buffered = BufReader::new(channel_reader);
        let decompressed: Box<dyn Read> = if compressed {
            Box::new(zstd::Decoder::new(buffered)?)
        } else {
            Box::new(buffered)
        };

        Ok(Self {
            inner: StreamReader::try_new(decompressed, None)?,
        })
    }
}
impl RecordBatchReader for ReadableByteChannelBatchReader {
    fn next_batch(&mut self) -> Option<ArrowResult<RecordBatch>> {
        self.inner.next()
    }
}

pub struct ReadableByteChannelReader(pub GlobalRef);

impl Read for ReadableByteChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(jni_call!(
            JavaReadableByteChannel(self.0.as_obj()).read(
                jni_new_direct_byte_buffer!(buf).to_io_result()?
            ) -> jint
        )
        .to_io_result()? as usize)
    }
}
impl Drop for ReadableByteChannelReader {
    fn drop(&mut self) {
        let _ = jni_call!( // ignore errors to avoid double panic problem
            JavaReadableByteChannel(self.0.as_obj()).close() -> ()
        );
    }
}

// record batch reader for file segment
struct FileSegmentBatchReader {
    file: File,
    segment_reader: Option<StreamReader<Box<dyn Read>>>,
    current_ipc_length: u64,
    current_start: u64,
    limit: u64,
}
impl FileSegmentBatchReader {
    fn try_new(path: impl AsRef<Path>, offset: u64, length: u64) -> ArrowResult<Self> {
        Ok(Self {
            file: File::open(path)?,
            segment_reader: None,
            current_ipc_length: 0,
            current_start: offset,
            limit: offset + length,
        })
    }

    fn next_batch_impl(&mut self) -> ArrowResult<Option<RecordBatch>> {
        if let Some(reader) = &mut self.segment_reader {
            if let Some(batch) = reader.next() {
                return Ok(Some(batch?));
            }
        }

        // not first ipc -- update start pos
        if self.segment_reader.is_some() {
            self.current_start += 8 + self.current_ipc_length;
        }

        if self.current_start < self.limit {
            let mut ipc_length_buf = [0u8; 8];

            self.file.seek(SeekFrom::Start(self.current_start))?;
            self.file.read_exact(&mut ipc_length_buf)?;
            self.current_ipc_length = u64::from_le_bytes(ipc_length_buf);

            let ipc = self.file.try_clone()?.take(self.current_ipc_length);
            let zstd_decoder: Box<dyn Read> =
                Box::new(zstd::stream::Decoder::new(BufReader::new(ipc))?);
            self.segment_reader =
                Some(StreamReader::try_new(zstd_decoder, None).unwrap());
            return self.next_batch_impl();
        }
        Ok(None)
    }
}
impl RecordBatchReader for FileSegmentBatchReader {
    fn next_batch(&mut self) -> Option<ArrowResult<RecordBatch>> {
        match self.next_batch_impl() {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }
}
