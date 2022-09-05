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


use datafusion::arrow::datatypes::SchemaRef;

use datafusion::arrow::error::Result as ArrowResult;

use datafusion::arrow::ipc::reader::StreamReader;





use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;

use std::io::Read;
use std::io::{Seek, SeekFrom, Write};

pub fn write_one_batch<W: Write + Seek>(
    batch: &RecordBatch,
    output: &mut W,
    compress: bool,
) -> ArrowResult<usize> {
    if batch.num_rows() == 0 {
        return Ok(0);
    }
    let start_pos = output.stream_position()?;

    // write ipc_length placeholder
    output.write_all(&[0u8; 8])?;

    // write ipc data
    let output = if compress {
        let mut arrow_writer =
            StreamWriter::try_new(zstd::Encoder::new(output, 1)?, &batch.schema())?;
        arrow_writer.write(batch)?;
        arrow_writer.finish()?;
        let zwriter = arrow_writer.into_inner()?;
        zwriter.finish()?
    } else {
        let mut arrow_writer = StreamWriter::try_new(output, &batch.schema())?;
        arrow_writer.write(batch)?;
        arrow_writer.finish()?;
        arrow_writer.into_inner()?
    };

    let end_pos = output.stream_position()?;
    let ipc_length = end_pos - start_pos - 8;

    // fill ipc length
    output.seek(SeekFrom::Start(start_pos))?;
    output.write_all(&ipc_length.to_le_bytes()[..])?;

    output.seek(SeekFrom::Start(end_pos))?;
    Ok((end_pos - start_pos) as usize)
}

pub fn read_one_batch<R: Read>(
    input: &mut R,
    _schema: SchemaRef,
    compress: bool,
    has_length_header: bool,
) -> ArrowResult<RecordBatch> {
    let input: Box<dyn Read> = if has_length_header {
        let mut len_buf = [0u8; 8];
        input.read_exact(&mut len_buf)?;
        let len = u64::from_le_bytes(len_buf);
        Box::new(input.take(len))
    } else {
        Box::new(input)
    };

    // read
    Ok(if compress {
        let mut arrow_reader = StreamReader::try_new(zstd::Decoder::new(input)?, None)?;
        arrow_reader.next().unwrap()?
    } else {
        let mut arrow_reader = StreamReader::try_new(input, None)?;
        arrow_reader.next().unwrap()?
    })
}
