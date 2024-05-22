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

use std::io::{Read, Seek, SeekFrom, Write};

use arrow::{
    array::StructArray,
    datatypes::{DataType, SchemaRef},
    record_batch::RecordBatch,
};
pub use batch_serde::{read_array, read_data_type, write_array, write_data_type};
use datafusion::common::{cast::as_struct_array, Result};
pub use scalar_serde::{read_scalar, write_scalar};

mod batch_serde;
mod scalar_serde;

pub fn write_one_batch<W: Write + Seek>(batch: &RecordBatch, output: &mut W) -> Result<()> {
    if batch.num_rows() == 0 {
        return Ok(());
    }
    // write ipc_length placeholder
    let start_pos = output.stream_position()?;
    output.write_all(&[0u8; 8])?;

    // write
    batch_serde::write_batch(batch, output)?;
    let end_pos = output.stream_position()?;
    let ipc_length = end_pos - start_pos - 8;

    // fill ipc length
    output.seek(SeekFrom::Start(start_pos))?;
    output.write_all(&ipc_length.to_le_bytes()[..])?;
    output.seek(SeekFrom::Start(end_pos))?;
    Ok(())
}

pub fn read_one_batch<R: Read>(input: &mut R, schema: &SchemaRef) -> Result<Option<RecordBatch>> {
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
    let nameless_batch = batch_serde::read_batch(&mut input)?;

    // consume trailing bytes
    std::io::copy(&mut input, &mut std::io::sink())?;

    // recover schema name
    return Ok(Some(name_batch(nameless_batch, schema)?));
}

pub fn name_batch(batch: RecordBatch, name_schema: &SchemaRef) -> Result<RecordBatch> {
    Ok(RecordBatch::from(as_struct_array(&crate::cast::cast(
        &StructArray::from(batch),
        &DataType::Struct(name_schema.fields.clone()),
    )?)?))
}

pub fn write_len<W: Write>(mut len: usize, output: &mut W) -> std::io::Result<()> {
    while len >= 128 {
        let v = len % 128;
        len /= 128;
        write_u8(128 + v as u8, output)?;
    }
    write_u8(len as u8, output)?;
    Ok(())
}

pub fn read_len<R: Read>(input: &mut R) -> std::io::Result<usize> {
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

pub fn write_u8<W: Write>(n: u8, output: &mut W) -> std::io::Result<()> {
    output.write_all(&[n])?;
    Ok(())
}

pub fn read_u8<R: Read>(input: &mut R) -> std::io::Result<u8> {
    let mut buf = [0; 1];
    input.read_exact(&mut buf)?;
    Ok(buf[0])
}

pub fn read_bytes_slice<R: Read>(input: &mut R, len: usize) -> std::io::Result<Box<[u8]>> {
    // safety - assume_init() is safe for [u8]
    let mut byte_slice = unsafe { Box::new_uninit_slice(len).assume_init() };
    input.read_exact(byte_slice.as_mut())?;
    Ok(byte_slice)
}
