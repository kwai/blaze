// Copyright 2022 The Auron Authors
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
    array::{ArrayRef, RecordBatchOptions},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
pub use batch_serde::{read_array, write_array};
use datafusion::common::Result;
pub use scalar_serde::{read_scalar, write_scalar};

use crate::{UninitializedInit, arrow::cast::cast};

mod batch_serde;
mod scalar_serde;

pub fn write_one_batch(num_rows: usize, cols: &[ArrayRef], mut output: impl Write) -> Result<()> {
    batch_serde::write_batch(num_rows, cols, &mut output)
}

pub fn read_one_batch(
    mut input: impl Read,
    schema: &SchemaRef,
) -> Result<Option<(usize, Vec<ArrayRef>)>> {
    batch_serde::read_batch(&mut input, schema)
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

pub fn read_bytes_slice<R: Read>(input: &mut R, len: usize) -> std::io::Result<Box<[u8]>> {
    let mut buf = Vec::uninitialized_init(len);
    input.read_exact(buf.as_mut())?;
    Ok(buf.into())
}
