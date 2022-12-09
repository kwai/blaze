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
use std::sync::Arc;
use arrow::datatypes::Schema;

pub fn write_one_batch<W: Write + Seek>(
    batch: &RecordBatch,
    output: &mut W,
    compress: bool,
) -> ArrowResult<usize> {
    let num_rows = batch.num_rows();
    let mem_size = batch.get_array_memory_size();

    if batch.num_rows() == 0 {
        return Ok(0);
    }

    // nameless - reduce column names from serialized data, the names are
    // always available in the read size
    let nameless_schema = Arc::new(Schema::new(batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.clone().with_name(""))
        .collect()));
    let batch = name_batch(batch, &nameless_schema)?;

    // write ipc_length placeholder
    let start_pos = output.stream_position()?;
    output.write_all(&[0u8; 8])?;

    // write ipc data
    let output = if compress {
        let mut arrow_writer =
            StreamWriter::try_new(zstd::Encoder::new(output, 1)?, &batch.schema())?;
        arrow_writer.write(&batch)?;
        arrow_writer.finish()?;
        let zwriter = arrow_writer.into_inner()?;
        zwriter.finish()?
    } else {
        let mut arrow_writer = StreamWriter::try_new(output, &batch.schema())?;
        arrow_writer.write(&batch)?;
        arrow_writer.finish()?;
        arrow_writer.into_inner()?
    };

    let end_pos = output.stream_position()?;
    let ipc_length = end_pos - start_pos - 8;

    // fill ipc length
    output.seek(SeekFrom::Start(start_pos))?;
    output.write_all(&ipc_length.to_le_bytes()[..])?;

    log::info!(
        "write_one_batch: num_rows={}, mem_size={}, ipc_length={}, compressed={}",
        num_rows,
        mem_size,
        ipc_length,
        compress,
    );
    output.seek(SeekFrom::Start(end_pos))?;
    Ok((end_pos - start_pos) as usize)
}

pub fn read_one_batch<R: Read>(
    input: &mut R,
    schema: SchemaRef,
    compress: bool,
) -> ArrowResult<Option<RecordBatch>> {

    // read
    let input: Box<dyn Read> = Box::new(input);
    let nameless_batch = if compress {
        let mut arrow_reader = StreamReader::try_new(zstd::Decoder::new(input)?, None)?;
        arrow_reader.next().unwrap()?
    } else {
        let mut arrow_reader = StreamReader::try_new(input, None)?;
        arrow_reader.next().unwrap()?
    };

    // recover schema name
    Ok(Some(name_batch(&nameless_batch, &schema)?))
}

pub fn name_batch(
    batch: &RecordBatch,
    name_schema: &SchemaRef,
) -> ArrowResult<RecordBatch> {

    let schema = Arc::new(Schema::new(batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| field.clone().with_name(name_schema.field(i).name()))
        .collect()));

    RecordBatch::try_new(
        schema,
        batch.columns().iter() .map(|column| column.clone()).collect(),
    )
}