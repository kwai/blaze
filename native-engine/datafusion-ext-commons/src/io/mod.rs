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

use std::io::{Seek, SeekFrom, Read, Write};
use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;

mod batch_serde;

pub fn write_one_batch<W: Write + Seek>(
    batch: &RecordBatch,
    output: &mut W,
    compress: bool,
) -> ArrowResult<usize> {
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

    // write
    batch_serde::write_batch(&batch, output, compress)?;
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
    schema: Option<SchemaRef>,
    compress: bool,
) -> ArrowResult<Option<RecordBatch>> {

    // read ipc length
    let mut ipc_length_buf = [0u8; 8];
    if let Err(e) = input.read_exact(&mut ipc_length_buf) {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None)
        }
        return Err(e.into());
    }
    let ipc_length = u64::from_le_bytes(ipc_length_buf);
    let mut input = Box::new(input.take(ipc_length));

    // read
    let nameless_batch = batch_serde::read_batch(&mut input, compress)?;

    // consume trailing bytes
    std::io::copy(&mut input, &mut std::io::sink())?;

    // recover schema name
    if let Some(schema) = schema.as_ref() {
        return Ok(Some(name_batch(&nameless_batch, schema)?))
    }
    Ok(Some(nameless_batch))
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

