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

use std::io::{BufReader, Read, Take, Write};

use arrow::{array::ArrayRef, datatypes::SchemaRef};
use blaze_jni_bridge::{conf, conf::StringConf, is_jni_bridge_inited};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use datafusion::common::Result;
use datafusion_ext_commons::{
    df_execution_err,
    io::{read_one_batch, write_one_batch},
};
use once_cell::sync::OnceCell;

pub const DEFAULT_SHUFFLE_COMPRESSION_TARGET_BUF_SIZE: usize = 4194304;
const ZSTD_LEVEL: i32 = 1;

pub struct IpcCompressionWriter<W: Write> {
    output: W,
    shared_buf: VecBuffer,
    block_writer: IoCompressionWriter<VecBufferWrite>,
    block_empty: bool,
}
unsafe impl<W: Write> Send for IpcCompressionWriter<W> {}

impl<W: Write> IpcCompressionWriter<W> {
    pub fn new(output: W) -> Self {
        let mut shared_buf = VecBuffer::default();
        shared_buf.inner_mut().extend_from_slice(&[0u8; 4]);

        let block_writer = IoCompressionWriter::new_with_configured_codec(shared_buf.writer());
        Self {
            output,
            shared_buf,
            block_writer,
            block_empty: true,
        }
    }

    pub fn set_output(&mut self, output: W) {
        assert!(
            self.block_empty,
            "IpcCompressionWriter must be empty while changing output"
        );
        self.output = output;
    }

    pub fn write_batch(&mut self, num_rows: usize, cols: &[ArrayRef]) -> Result<()> {
        if num_rows == 0 {
            return Ok(());
        }
        write_one_batch(num_rows, cols, &mut self.block_writer)?;
        self.block_empty = false;

        let buf_len = self.shared_buf.inner().len();
        if buf_len as f64 >= DEFAULT_SHUFFLE_COMPRESSION_TARGET_BUF_SIZE as f64 * 0.9 {
            self.finish_current_buf()?;
        }
        Ok(())
    }

    pub fn finish_current_buf(&mut self) -> Result<()> {
        if !self.block_empty {
            // finish current buf
            self.block_writer.finish()?;

            // write
            let block_len = self.shared_buf.inner().len() - 4;
            self.shared_buf.inner_mut()[0..4]
                .as_mut()
                .write_u32::<LittleEndian>(block_len as u32)?;
            self.output.write_all(self.shared_buf.inner())?;

            // open next buf
            self.shared_buf.inner_mut().clear();
            self.shared_buf.inner_mut().extend_from_slice(&[0u8; 4]);
            self.block_writer =
                IoCompressionWriter::new_with_configured_codec(self.shared_buf.writer());
            self.block_empty = true;
        }
        Ok(())
    }

    pub fn inner(&self) -> &W {
        &self.output
    }

    pub fn inner_mut(&mut self) -> &mut W {
        &mut self.output
    }
}

pub struct IpcCompressionReader<R: Read + 'static> {
    input: InputState<R>,
}
unsafe impl<R: Read> Send for IpcCompressionReader<R> {}

#[derive(Default)]
enum InputState<R: Read + 'static> {
    #[default]
    Unreachable,
    BlockStart(R),
    BlockContent(IoCompressionReader<Take<R>>),
}

impl<R: Read> IpcCompressionReader<R> {
    pub fn new(input: R) -> Self {
        Self {
            input: InputState::BlockStart(input),
        }
    }

    pub fn read_batch(&mut self, schema: &SchemaRef) -> Result<Option<(usize, Vec<ArrayRef>)>> {
        struct Reader<'a, R: Read + 'static>(&'a mut IpcCompressionReader<R>);
        impl<'a, R: Read> Read for Reader<'a, R> {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                match std::mem::take(&mut self.0.input) {
                    InputState::BlockStart(mut input) => {
                        let block_len = match input.read_u32::<LittleEndian>() {
                            Ok(block_len) => block_len,
                            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                                return Ok(0);
                            }
                            Err(err) => {
                                return Err(err);
                            }
                        };
                        let taken = input.take(block_len as u64);

                        self.0.input = InputState::BlockContent(IoCompressionReader::try_new(
                            io_compression_codec(),
                            taken,
                        )?);
                        self.read(buf)
                    }
                    InputState::BlockContent(mut block_reader) => match block_reader.read(buf) {
                        Ok(len) if len > 0 => {
                            self.0.input = InputState::BlockContent(block_reader);
                            Ok(len)
                        }
                        Ok(_zero) => {
                            let input = block_reader.finish_into_inner()?;
                            self.0.input = InputState::BlockStart(input.into_inner());
                            self.read(buf)
                        }
                        Err(err) => Err(err),
                    },
                    _ => unreachable!(),
                }
            }
        }
        read_one_batch(&mut Reader(self), schema)
    }
}

pub enum IoCompressionWriter<W: Write> {
    LZ4(lz4_flex::frame::FrameEncoder<W>),
    ZSTD(zstd::Encoder<'static, W>),
}

impl<W: Write> IoCompressionWriter<W> {
    pub fn new_with_configured_codec(inner: W) -> Self {
        Self::try_new(io_compression_codec(), inner).expect("error creating compression encoder")
    }

    pub fn try_new(codec: &str, inner: W) -> Result<Self> {
        match codec {
            "lz4" => Ok(Self::LZ4(lz4_flex::frame::FrameEncoder::new(inner))),
            "zstd" => Ok(Self::ZSTD(zstd::Encoder::new(inner, ZSTD_LEVEL)?)),
            _ => df_execution_err!("unsupported codec: {}", codec),
        }
    }

    pub fn finish(&mut self) -> Result<()> {
        match self {
            IoCompressionWriter::LZ4(w) => {
                w.try_finish()
                    .or_else(|_| df_execution_err!("ipc compresion error"))?;
            }
            IoCompressionWriter::ZSTD(w) => {
                w.do_finish()?;
            }
        }
        Ok(())
    }
}

impl<W: Write> Write for IoCompressionWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            IoCompressionWriter::LZ4(w) => w.write(buf),
            IoCompressionWriter::ZSTD(w) => w.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            IoCompressionWriter::LZ4(w) => w.flush(),
            IoCompressionWriter::ZSTD(w) => w.flush(),
        }
    }
}

pub enum IoCompressionReader<R: Read> {
    LZ4(lz4_flex::frame::FrameDecoder<R>),
    ZSTD(zstd::Decoder<'static, BufReader<R>>),
}

impl<R: Read> IoCompressionReader<R> {
    pub fn new_with_configured_codec(inner: R) -> Self {
        Self::try_new(io_compression_codec(), inner).expect("error creating compression encoder")
    }

    pub fn try_new(codec: &str, inner: R) -> Result<Self> {
        match codec {
            "lz4" => Ok(Self::LZ4(lz4_flex::frame::FrameDecoder::new(inner))),
            "zstd" => Ok(Self::ZSTD(zstd::Decoder::new(inner)?)),
            _ => df_execution_err!("unsupported codec: {}", codec),
        }
    }

    pub fn finish_into_inner(self) -> Result<R> {
        match self {
            Self::LZ4(r) => Ok(r.into_inner()),
            Self::ZSTD(r) => Ok(r.finish().into_inner()),
        }
    }
}

impl<R: Read> Read for IoCompressionReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::LZ4(r) => r.read(buf),
            Self::ZSTD(r) => r.read(buf),
        }
    }
}

fn io_compression_codec() -> &'static str {
    static CODEC: OnceCell<String> = OnceCell::new();
    CODEC
        .get_or_try_init(|| {
            if is_jni_bridge_inited() {
                conf::SPARK_IO_COMPRESSION_CODEC.value()
            } else {
                Ok(format!("lz4")) // for testing
            }
        })
        .expect("error reading spark.io.compression.codec")
        .as_str()
}

#[derive(Default)]
struct VecBuffer {
    vec: Box<Vec<u8>>,
}

struct VecBufferWrite {
    unsafe_inner: *mut Vec<u8>,
}

impl Write for VecBufferWrite {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let inner = unsafe { &mut *self.unsafe_inner };
        inner.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl VecBuffer {
    fn inner(&self) -> &Vec<u8> {
        &self.vec
    }

    fn inner_mut(&mut self) -> &mut Vec<u8> {
        &mut self.vec
    }

    fn writer(&mut self) -> VecBufferWrite {
        VecBufferWrite {
            unsafe_inner: &mut *self.vec as *mut Vec<u8>,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, io::Cursor, sync::Arc};

    use arrow::{
        array::StringArray,
        datatypes::{DataType, Field, Schema},
    };

    use super::*;

    #[test]
    fn test_ipc_compression() -> Result<(), Box<dyn Error>> {
        let mut buf = vec![];
        let mut writer = IpcCompressionWriter::new(&mut buf);

        let test_array1: ArrayRef = Arc::new(StringArray::from(vec![Some("hello"), Some("world")]));
        let test_array2: ArrayRef = Arc::new(StringArray::from(vec![Some("foo"), Some("bar")]));
        let schema = Arc::new(Schema::new(vec![Field::new("", DataType::Utf8, false)]));

        writer.write_batch(2, &[test_array1.clone()])?;
        writer.write_batch(2, &[test_array2.clone()])?;
        writer.finish_current_buf()?;

        let mut reader = IpcCompressionReader::new(Cursor::new(buf));
        let (num_rows1, arrays1) = reader.read_batch(&schema)?.unwrap();
        assert_eq!(num_rows1, 2);
        assert_eq!(arrays1, &[test_array1]);
        let (num_rows2, arrays2) = reader.read_batch(&schema)?.unwrap();
        assert_eq!(num_rows2, 2);
        assert_eq!(arrays2, &[test_array2]);
        assert!(reader.read_batch(&schema)?.is_none());
        Ok(())
    }
}
