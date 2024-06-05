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

use std::io::{BufReader, Cursor, Read, Take, Write};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use blaze_jni_bridge::{jni_call_static, jni_get_string, jni_new_string};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use datafusion::{common::Result, error::DataFusionError};
use datafusion_ext_commons::{
    df_execution_err,
    io::{read_one_batch, write_one_batch},
};
use once_cell::sync::OnceCell;

pub const DEFAULT_SHUFFLE_COMPRESSION_TARGET_BUF_SIZE: usize = 4194304;
const ZSTD_LEVEL: i32 = 1;

pub struct IpcCompressionWriter<W: Write> {
    output: W,
    compressed: bool,
    buf: Box<dyn CompressibleBlockWriter>,
    buf_empty: bool,
}
unsafe impl<W: Write> Send for IpcCompressionWriter<W> {}

impl<W: Write> IpcCompressionWriter<W> {
    pub fn new(output: W, compressed: bool) -> Self {
        Self {
            output,
            compressed,
            buf: create_block_writer(compressed),
            buf_empty: true,
        }
    }

    /// Write a batch, returning uncompressed bytes size
    pub fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let mut batch_buf = vec![];
        write_one_batch(&batch, &mut Cursor::new(&mut batch_buf))?;
        self.buf.write_all(&mut batch_buf)?;
        self.buf_empty = false;
        drop(batch_buf);

        if self.buf.buf_len() as f64 >= DEFAULT_SHUFFLE_COMPRESSION_TARGET_BUF_SIZE as f64 * 0.9 {
            self.flush()?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if !self.buf_empty {
            // finish current buf and open next
            let next_buf = create_block_writer(self.compressed);
            let block_data = std::mem::replace(&mut self.buf, next_buf).finish()?;
            self.output.write_all(&block_data)?;
            self.output.flush()?;
            self.buf_empty = true;
        }
        Ok(())
    }

    pub fn finish_into_inner(mut self) -> Result<W> {
        self.flush()?;
        Ok(self.output)
    }
}

pub struct IpcCompressionReader<R: Read + 'static> {
    schema: SchemaRef,
    input: InputState<R>,
}
unsafe impl<R: Read> Send for IpcCompressionReader<R> {}

#[derive(Default)]
enum InputState<R: Read + 'static> {
    #[default]
    Unreachable,
    BlockStart(R),
    BlockContent(Box<dyn CompressibleBlockReader<R>>),
}

impl<R: Read> IpcCompressionReader<R> {
    pub fn new(input: R, schema: SchemaRef) -> Self {
        Self {
            schema,
            input: InputState::BlockStart(input),
        }
    }

    pub fn read_batch(&mut self) -> Result<Option<RecordBatch>> {
        struct Reader<'a, R: Read + 'static>(&'a mut IpcCompressionReader<R>);
        impl<'a, R: Read> Read for Reader<'a, R> {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                match std::mem::take(&mut self.0.input) {
                    InputState::Unreachable => unreachable!(),
                    InputState::BlockStart(input) => {
                        let block_reader = match create_block_reader(input)? {
                            Some(reader) => reader,
                            None => return Ok(0),
                        };
                        self.0.input = InputState::BlockContent(block_reader);
                        self.read(buf)
                    }
                    InputState::BlockContent(mut block_reader) => match block_reader.read(buf) {
                        Ok(len) if len > 0 => {
                            self.0.input = InputState::BlockContent(block_reader);
                            Ok(len)
                        }
                        Ok(_zero) => {
                            let input = block_reader.finish_into_inner()?;
                            self.0.input = InputState::BlockStart(input);
                            self.read(buf)
                        }
                        Err(err) => Err(err),
                    },
                }
            }
        }
        let schema = self.schema.clone();
        read_one_batch(&mut Reader(self), &schema)
    }
}

#[derive(Clone, Copy)]
struct Header {
    compressed: bool,
    block_len: usize,
}

impl Header {
    fn new(compressed: bool, block_len: usize) -> Self {
        Self {
            compressed,
            block_len,
        }
    }

    fn from_u32(value: u32) -> Self {
        let compressed = (value & 0x8000_0000) > 0;
        let block_len = (value & 0x7fff_ffff) as usize;
        Self::new(compressed, block_len)
    }

    fn to_u32(&self) -> u32 {
        (self.compressed as u32) << 31 | (self.block_len as u32)
    }
}

trait CompressibleBlockWriter: Write {
    fn buf_len(&self) -> usize;
    fn finish(self: Box<Self>) -> Result<Vec<u8>>;
}

struct ZWriter(IoCompressionWriter<Vec<u8>>);

impl ZWriter {
    fn new() -> Self {
        Self(
            IoCompressionWriter::try_new(io_compression_codec(), vec![0u8; 4])
                .expect("error creating zstd encoder"),
        )
    }
}

impl Write for ZWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}

impl CompressibleBlockWriter for ZWriter {
    fn buf_len(&self) -> usize {
        self.0.get_ref().len()
    }

    fn finish(self: Box<Self>) -> Result<Vec<u8>> {
        let mut block_data = self.0.finish()?;
        let header = Header::new(true, block_data.len() - 4);
        block_data[0..4]
            .as_mut()
            .write_u32::<LittleEndian>(header.to_u32())?;
        Ok(block_data)
    }
}

struct UncompressedWriter(Vec<u8>);

impl UncompressedWriter {
    fn new() -> Self {
        Self(vec![0u8; 4])
    }
}

impl Write for UncompressedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}

impl CompressibleBlockWriter for UncompressedWriter {
    fn buf_len(&self) -> usize {
        self.0.len()
    }

    fn finish(self: Box<Self>) -> Result<Vec<u8>> {
        let mut block_data = self.0;
        let header = Header::new(false, block_data.len() - 4);
        block_data[0..4]
            .as_mut()
            .write_u32::<LittleEndian>(header.to_u32())?;
        Ok(block_data)
    }
}

trait CompressibleBlockReader<R: Read>: Read {
    fn finish_into_inner(self: Box<Self>) -> Result<R>;
}

impl<R: Read> CompressibleBlockReader<R> for IoCompressionReader<'_, Take<R>> {
    fn finish_into_inner(self: Box<Self>) -> Result<R> {
        let mut r = (*self).finish_into_inner()?;
        std::io::copy(&mut r, &mut std::io::sink())?; // skip to end
        Ok(r.into_inner())
    }
}

impl<R: Read> CompressibleBlockReader<R> for Take<R> {
    fn finish_into_inner(self: Box<Self>) -> Result<R> {
        Ok(self.into_inner())
    }
}

fn create_block_writer(compressed: bool) -> Box<dyn CompressibleBlockWriter> {
    if compressed {
        Box::new(ZWriter::new())
    } else {
        Box::new(UncompressedWriter::new())
    }
}

fn create_block_reader<R: Read + 'static>(
    mut input: R,
) -> Result<Option<Box<dyn CompressibleBlockReader<R>>>> {
    let header = match input.read_u32::<LittleEndian>() {
        Ok(value) => Header::from_u32(value),
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok(None);
        }
        Err(err) => {
            return df_execution_err!("{err}");
        }
    };

    let taken = input.take(header.block_len as u64);
    if !header.compressed {
        return Ok(Some(Box::new(taken)));
    }
    Ok(Some(Box::new(
        IoCompressionReader::try_new(io_compression_codec(), taken)
            .expect("error creating ztd decoder"),
    )))
}

enum IoCompressionWriter<W: Write> {
    LZ4(lz4_flex::frame::FrameEncoder<W>),
    ZSTD(zstd::Encoder<'static, W>),
}

impl<W: Write> IoCompressionWriter<W> {
    fn try_new(codec: &str, inner: W) -> Result<Self> {
        match codec {
            "lz4" => Ok(Self::LZ4(lz4_flex::frame::FrameEncoder::new(inner))),
            "zstd" => Ok(Self::ZSTD(zstd::Encoder::new(inner, ZSTD_LEVEL)?)),
            _ => df_execution_err!("unsupported codec: {}", codec),
        }
    }

    fn get_ref(&self) -> &W {
        match self {
            IoCompressionWriter::LZ4(w) => w.get_ref(),
            IoCompressionWriter::ZSTD(w) => w.get_ref(),
        }
    }

    fn finish(self) -> Result<W> {
        match self {
            IoCompressionWriter::LZ4(w) => Ok(w.finish().or_else(|e| df_execution_err!("{e}"))?),
            IoCompressionWriter::ZSTD(w) => Ok(w.finish().or_else(|e| df_execution_err!("{e}"))?),
        }
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

enum IoCompressionReader<'a, R: Read> {
    LZ4(lz4_flex::frame::FrameDecoder<R>),
    ZSTD(zstd::Decoder<'a, BufReader<R>>),
}

impl<R: Read> IoCompressionReader<'_, R> {
    fn try_new(codec: &str, inner: R) -> Result<Self> {
        match codec {
            "lz4" => Ok(Self::LZ4(lz4_flex::frame::FrameDecoder::new(inner))),
            "zstd" => Ok(Self::ZSTD(zstd::Decoder::new(inner)?)),
            _ => df_execution_err!("unsupported codec: {}", codec),
        }
    }

    fn finish_into_inner(self) -> Result<R> {
        match self {
            Self::LZ4(r) => Ok(r.into_inner()),
            Self::ZSTD(r) => Ok(r.finish().into_inner()),
        }
    }
}

impl<R: Read> Read for IoCompressionReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::LZ4(r) => r.read(buf),
            Self::ZSTD(r) => r.read(buf),
        }
    }
}

fn io_compression_codec() -> &'static str {
    static IO_COMPRESSION_CODEC: OnceCell<String> = OnceCell::new();
    let codec = IO_COMPRESSION_CODEC.get_or_try_init(|| {
        Ok({
            let key = jni_new_string!("spark.io.compression.codec")?;
            let value =
                jni_call_static!(JniBridge.getSparkEnvConfAsString(key.as_obj()) -> JObject)?;
            jni_get_string!(value.as_obj().into())?
        })
    });

    codec
        .map(|value| value.as_ref())
        .unwrap_or_else(|_: DataFusionError| {
            log::warn!("unable to get spark.io.compression.codec, use lz4 as default");
            "lz4"
        })
}
