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

use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use std::io::{Seek, SeekFrom, Write};

pub fn write_ipc_compressed<W: Write + Seek>(
    batch: &RecordBatch,
    output: &mut W,
) -> Result<usize> {
    if batch.num_rows() == 0 {
        return Ok(0);
    }
    let start_pos = output.stream_position()?;

    // write ipc_length placeholder
    output.write_all(&[0u8; 16])?;

    struct CountedWriter<W: Write> {
        inner: W,
        count: usize,
    }
    impl<W: Write> Write for CountedWriter<W> {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let written = self.inner.write(buf)?;
            self.count += written;
            Ok(written)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.inner.flush()
        }
    }

    // write ipc data
    let mut arrow_writer = StreamWriter::try_new(
        CountedWriter {
            inner: zstd::Encoder::new(output, 1)?,
            count: 0,
        },
        &batch.schema(),
    )?;
    arrow_writer.write(batch)?;
    arrow_writer.finish()?;

    let CountedWriter {
        inner: zwriter,
        count: written_length,
    } = arrow_writer.into_inner()?;

    let output = zwriter.finish()?;
    let end_pos = output.stream_position()?;
    let ipc_length_uncompressed = written_length as u64;
    let ipc_length = end_pos - start_pos - 16;

    // fill ipc length
    output.seek(SeekFrom::Start(start_pos))?;
    output.write_all(&ipc_length.to_le_bytes()[..])?;
    output.write_all(&ipc_length_uncompressed.to_le_bytes()[..])?;
    output.seek(SeekFrom::Start(end_pos))?;

    Ok((end_pos - start_pos) as usize)
}
