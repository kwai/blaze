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

use arrow::array::{make_array, Array, ArrayData};
use arrow::record_batch::RecordBatchOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use arrow::datatypes::{DataType, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::Result;

mod batch_serde;

pub fn write_one_batch<W: Write + Seek>(
    batch: &RecordBatch,
    output: &mut W,
    compress: bool,
) -> Result<usize> {
    if batch.num_rows() == 0 {
        return Ok(0);
    }
    // write ipc_length placeholder
    let start_pos = output.stream_position()?;
    output.write_all(&[0u8; 8])?;

    // write
    batch_serde::write_batch(batch, output, compress)?;
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
) -> Result<Option<RecordBatch>> {
    // read ipc length
    let mut ipc_length_buf = [0u8; 8];
    if let Err(e) = input.read_exact(&mut ipc_length_buf) {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
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
        return Ok(Some(name_batch(&nameless_batch, schema)?));
    }
    Ok(Some(nameless_batch))
}

pub fn name_batch(batch: &RecordBatch, name_schema: &SchemaRef) -> Result<RecordBatch> {
    let row_count = batch.num_rows();
    let schema = Arc::new(Schema::new(
        batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, _)| name_schema.field(i).clone())
            .collect::<Fields>(),
    ));
    Ok(RecordBatch::try_new_with_options(
        schema.clone(),
        batch
            .columns()
            .iter()
            .enumerate()
            .map(|(i, column)| {
                let column_data = column.to_data();
                Ok(match schema.field(i).data_type() {
                    DataType::Map(field, _) => {
                        let child_nameless_data = column_data.child_data().get(0).unwrap();
                        let child_data = ArrayData::try_new(
                            field.data_type().clone(),
                            child_nameless_data.len(),
                            child_nameless_data.nulls().map(|nb| nb.buffer().clone()),
                            child_nameless_data.offset(),
                            child_nameless_data.buffers().to_vec(),
                            child_nameless_data.child_data().to_vec(),
                        )?;
                        make_array(ArrayData::try_new(
                            schema.field(i).data_type().clone(),
                            column_data.len(),
                            column_data.nulls().map(|nb| nb.buffer().clone()),
                            column_data.offset(),
                            column_data.buffers().to_vec(),
                            vec![child_data],
                        )?)
                    }
                    _ => make_array(ArrayData::try_new(
                        schema.field(i).data_type().clone(),
                        column_data.len(),
                        column_data.nulls().map(|nb| nb.buffer().clone()),
                        column_data.offset(),
                        column_data.buffers().to_vec(),
                        column_data.child_data().to_vec(),
                    )?),
                })
            })
            .collect::<Result<_>>()?,
        &RecordBatchOptions::new().with_row_count(Some(row_count)),
    )?)
}

pub fn write_len<W: Write>(mut len: usize, output: &mut W) -> Result<()> {
    while len >= 128 {
        let v = len % 128;
        len /= 128;
        write_u8(128 + v as u8, output)?;
    }
    write_u8(len as u8, output)?;
    Ok(())
}

pub fn read_len<R: Read>(input: &mut R) -> Result<usize> {
    let mut len = 0usize;
    let mut factor = 1;
    loop {
        let v = read_u8(input)?;
        if v < 128 {
            len += (v as usize) * factor;
            break;
        }
        len += (v - 128) as usize * factor;
        factor *= 128;
    }
    Ok(len)
}

pub fn write_u8<W: Write>(n: u8, output: &mut W) -> Result<()> {
    output.write_all(&[n])?;
    Ok(())
}

pub fn read_u8<R: Read>(input: &mut R) -> Result<u8> {
    let mut buf = [0; 1];
    input.read_exact(&mut buf)?;
    Ok(buf[0])
}

pub fn read_bytes_slice<R: Read>(input: &mut R, len: usize) -> Result<Box<[u8]>> {
    // safety - assume_init() is safe for [u8]
    let mut byte_slice = unsafe { Box::new_uninit_slice(len).assume_init() };
    input.read_exact(byte_slice.as_mut())?;
    Ok(byte_slice)
}
