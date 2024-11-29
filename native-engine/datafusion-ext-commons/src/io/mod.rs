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

use std::io::{Read, Write};

use arrow::{
    array::{Array, ArrayRef, RecordBatchOptions},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
pub use batch_serde::{read_array, write_array};
use datafusion::common::Result;
pub use scalar_serde::{read_scalar, write_scalar};

use crate::arrow::cast::cast;

mod batch_serde;
mod scalar_serde;

pub fn write_raw_slice<T: Sized + Copy>(
    values: &[T],
    mut output: impl Write,
) -> std::io::Result<()> {
    let raw_item_size = size_of::<T>();
    let raw_slice = unsafe {
        // safety: transmute copyable slice to bytes slice
        std::slice::from_raw_parts(values.as_ptr() as *const u8, raw_item_size * values.len())
    };
    output.write_all(raw_slice)
}

pub fn read_raw_slice<T: Sized + Copy>(
    values: &mut [T],
    mut input: impl Read,
) -> std::io::Result<()> {
    let raw_item_size = size_of::<T>();
    let raw_slice = unsafe {
        // safety: transmute copyable slice to bytes slice
        std::slice::from_raw_parts_mut(values.as_mut_ptr() as *mut u8, raw_item_size * values.len())
    };
    input.read_exact(raw_slice)
}

pub fn write_one_batch(num_rows: usize, cols: &[ArrayRef], mut output: impl Write) -> Result<()> {
    assert!(cols.iter().all(|col| col.len() == num_rows));

    let mut batch_data = vec![];
    batch_serde::write_batch(num_rows, cols, &mut batch_data)?;
    write_len(batch_data.len(), &mut output)?;
    output.write_all(&batch_data)?;
    Ok(())
}

pub fn read_one_batch(
    mut input: impl Read,
    schema: &SchemaRef,
) -> Result<Option<(usize, Vec<ArrayRef>)>> {
    let batch_data_len = match read_len(&mut input) {
        Ok(len) => len,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
            return Err(e.into());
        }
    };
    let mut input = input.take(batch_data_len as u64);
    let (num_rows, cols) = batch_serde::read_batch(&mut input, schema)?;

    // consume trailing bytes
    std::io::copy(&mut input, &mut std::io::sink())?;

    assert!(cols.iter().all(|col| col.len() == num_rows));
    return Ok(Some((num_rows, cols)));
}

pub fn recover_named_batch(
    num_rows: usize,
    cols: &[ArrayRef],
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let cols = cols
        .iter()
        .zip(schema.fields())
        .map(|(col, field)| Ok(cast(&col, field.data_type())?))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new_with_options(
        schema,
        cols,
        &RecordBatchOptions::new().with_row_count(Some(num_rows)),
    )?)
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

pub fn read_bytes_into_vec<R: Read>(
    input: &mut R,
    buf: &mut Vec<u8>,
    len: usize,
) -> std::io::Result<()> {
    buf.reserve(len);
    unsafe {
        // safety: space has been reserved
        input.read_exact(std::slice::from_raw_parts_mut(
            buf.as_mut_ptr().add(buf.len()),
            len,
        ))?;
        buf.set_len(buf.len() + len);
    }
    Ok(())
}
pub fn read_bytes_slice<R: Read>(input: &mut R, len: usize) -> std::io::Result<Box<[u8]>> {
    // safety - assume_init() is safe for [u8]
    let mut byte_slice = unsafe { Box::new_uninit_slice(len).assume_init() };
    input.read_exact(byte_slice.as_mut())?;
    Ok(byte_slice)
}
