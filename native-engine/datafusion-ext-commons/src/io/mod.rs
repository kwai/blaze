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

use arrow::array::{make_array, Array, ArrayData, ArrayRef, StructArray};

use std::io::{Read, Seek, SeekFrom, Write};


use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::cast::{as_boolean_array, as_list_array, as_map_array, as_struct_array,as_null_array};
use datafusion::common::Result;
use datafusion::error::DataFusionError;

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
        return Ok(Some(name_batch(nameless_batch, schema)?));
    }
    Ok(Some(nameless_batch))
}

pub fn adapt_array_data_type(array: &dyn Array, to_data_type: DataType) -> Result<ArrayRef> {
    let array_data = array.to_data();
    let from_data_type = array_data.data_type();

    if from_data_type == &to_data_type {
        return Ok(make_array(array_data));
    }
    
    macro_rules! adapt_directly {
        ($to_data_type:expr) => {{
            let array_data = ArrayData::try_new(
                $to_data_type,
                array_data.len(),
                array_data.nulls().map(|nb| nb.buffer()).cloned(),
                array_data.offset(),
                array_data.buffers().to_vec(),
                array_data.child_data().to_vec(),
            );
            array_data.map(make_array)
        }}
    }
    match to_data_type {
        ref to_prim_type if to_prim_type.is_primitive() => {
            let from_prim_width = from_data_type.primitive_width();
            let to_prim_width = to_prim_type.primitive_width();
            if to_prim_width != from_prim_width {
                return Err(DataFusionError::Execution(
                    format!("cannot adapt data with type {} to {}", from_data_type, to_data_type)
                ));
            }
            Ok(adapt_directly!(to_data_type)?)
        }
        DataType::Null => {
            let _ = as_null_array(array)?;
            Ok(adapt_directly!(to_data_type)?)
        }
        DataType::Boolean => {
            let _ = as_boolean_array(array)?;
            Ok(adapt_directly!(to_data_type)?)
        }
        DataType::Binary | DataType::Utf8 => {
            if !matches!(from_data_type, DataType::Binary | DataType::Utf8) {
                return Err(DataFusionError::Execution(
                    format!("cannot adapt data with type {} to {}", from_data_type, to_data_type)
                ));
            }
            Ok(adapt_directly!(to_data_type)?)
        }
        DataType::List(ref to_field) => {
            let to_values = adapt_array_data_type(
                as_list_array(array)?.values(),
                to_field.data_type().clone(),
            )?;
            return Ok(make_array(ArrayData::try_new(
                to_data_type,
                array_data.len(),
                array_data.nulls().map(|nb| nb.buffer()).cloned(),
                array_data.offset(),
                array_data.buffers().to_vec(),
                vec![to_values.into_data()],
            )?));
        }
        DataType::Map(ref entries_field, _) => {
            let to_entries = adapt_array_data_type(
                as_map_array(array)?.entries(),
                entries_field.data_type().clone(),
            )?;
            return Ok(make_array(ArrayData::try_new(
                to_data_type,
                array_data.len(),
                array_data.nulls().map(|nb| nb.buffer()).cloned(),
                array_data.offset(),
                array_data.buffers().to_vec(),
                vec![to_entries.into_data()],
            )?));
        }
        DataType::Struct(ref to_fields) => {
            let to_columns = as_struct_array(array)?
                .columns()
                .iter()
                .zip(to_fields.iter())
                .map(|(array, to_field)| {
                    adapt_array_data_type(array, to_field.data_type().clone())
                })
                .collect::<Result<Vec<_>>>()?;

            return Ok(make_array(ArrayData::try_new(
                to_data_type,
                array_data.len(),
                array_data.nulls().map(|nb| nb.buffer()).cloned(),
                array_data.offset(),
                array_data.buffers().to_vec(),
                to_columns.into_iter().map(|array| array.into_data()).collect(),
            )?));
        }
        other => Err(DataFusionError::Execution(
            format!("adapt_array_data_type(): unsupported data type: {}", other)
        )),
    }
}

pub fn name_batch(batch: RecordBatch, name_schema: &SchemaRef) -> Result<RecordBatch> {
    Ok(RecordBatch::from(as_struct_array(
        &adapt_array_data_type(
            &StructArray::from(batch),
            DataType::Struct(name_schema.fields.clone()),
        )?
    )?))
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
