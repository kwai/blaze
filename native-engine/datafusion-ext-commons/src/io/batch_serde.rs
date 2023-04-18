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

use crate::io::{read_bytes_slice, read_len, read_u8, write_len, write_u8};
use arrow::array::*;
use arrow::buffer::{Buffer, MutableBuffer};
use arrow::datatypes::*;
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use bitvec::prelude::BitVec;
use datafusion::common::cast::as_binary_array;
use std::io::{BufReader, BufWriter, Read, Write};
use std::ops::Deref;
use std::sync::Arc;

pub fn write_batch<W: Write>(
    batch: &RecordBatch,
    output: &mut W,
    compress: bool,
) -> ArrowResult<()> {
    let mut output: Box<dyn Write> = if compress {
        Box::new(zstd::Encoder::new(output, 1)?.auto_finish())
    } else {
        Box::new(BufWriter::new(output))
    };
    let schema = batch.schema();

    // write number of columns and rows
    write_len(batch.num_columns(), &mut output)?;
    write_len(batch.num_rows(), &mut output)?;

    // write column data types
    for field in schema.fields() {
        write_data_type(field.data_type(), &mut output)?;
    }

    // write column nullables
    let mut nullables = BitVec::<u8>::with_capacity(batch.num_columns());
    for field in schema.fields() {
        nullables.push(field.is_nullable());
    }
    output.write_all(&nullables.into_vec())?;

    // write whether arrays have null buffers (which may differ from nullables)
    let mut has_null_buffers = BitVec::<u8>::with_capacity(batch.num_columns());
    for array in batch.columns() {
        has_null_buffers.push(array.data().null_buffer().is_some());
    }
    output.write_all(&has_null_buffers.into_vec())?;

    // write columns
    for column in batch.columns() {
        match column.data_type() {
            DataType::Null => {}
            DataType::Boolean => {
                write_boolean_array(as_boolean_array(column), &mut output)?
            }
            DataType::Int8 => write_primitive_array(
                as_primitive_array::<Int8Type>(column),
                &mut output,
            )?,
            DataType::Int16 => write_primitive_array(
                as_primitive_array::<Int16Type>(column),
                &mut output,
            )?,
            DataType::Int32 => write_primitive_array(
                as_primitive_array::<Int32Type>(column),
                &mut output,
            )?,
            DataType::Int64 => write_primitive_array(
                as_primitive_array::<Int64Type>(column),
                &mut output,
            )?,
            DataType::UInt8 => write_primitive_array(
                as_primitive_array::<UInt8Type>(column),
                &mut output,
            )?,
            DataType::UInt16 => write_primitive_array(
                as_primitive_array::<UInt16Type>(column),
                &mut output,
            )?,
            DataType::UInt32 => write_primitive_array(
                as_primitive_array::<UInt32Type>(column),
                &mut output,
            )?,
            DataType::UInt64 => write_primitive_array(
                as_primitive_array::<UInt64Type>(column),
                &mut output,
            )?,
            DataType::Float32 => write_primitive_array(
                as_primitive_array::<Float32Type>(column),
                &mut output,
            )?,
            DataType::Float64 => write_primitive_array(
                as_primitive_array::<Float64Type>(column),
                &mut output,
            )?,
            DataType::Decimal128(_, _) => write_primitive_array(
                as_primitive_array::<Decimal128Type>(column),
                &mut output,
            )?,
            DataType::Utf8 => write_bytes_array(as_string_array(column), &mut output)?,
            DataType::Binary => write_bytes_array(as_binary_array(column)?, &mut output)?,
            DataType::Date32 => write_primitive_array(
                as_primitive_array::<Date32Type>(column),
                &mut output,
            )?,
            DataType::Date64 => write_primitive_array(
                as_primitive_array::<Date64Type>(column),
                &mut output,
            )?,
            DataType::Timestamp(TimeUnit::Microsecond, _) => write_primitive_array(
                as_primitive_array::<TimestampMicrosecondType>(column),
                &mut output,
            )?,
            DataType::List(_field) => write_list_array(
                as_list_array(column),
                &mut output,
            )?,
            DataType::Map(_, _) => write_map_array(
                as_map_array(column),
                &mut output,
            )?,
            DataType::Struct(_) => write_struct_array(
                as_struct_array(column),
                &mut output,
            )?,
            other => {
                return Err(ArrowError::IoError(format!(
                    "unsupported data type: {}",
                    other
                )));
            }
        }
    }
    Ok(())
}

pub fn read_batch<R: Read>(input: &mut R, compress: bool) -> ArrowResult<RecordBatch> {
    let mut input: Box<dyn Read> = if compress {
        Box::new(BufReader::new(zstd::Decoder::new(input)?))
    } else {
        Box::new(BufReader::new(input))
    };

    // read number of columns and rows
    let num_columns = read_len(&mut input)?;
    let num_rows = read_len(&mut input)?;

    // read column data types
    let mut data_types = Vec::with_capacity(num_columns);
    for _ in 0..num_columns {
        data_types.push(read_data_type(&mut input)?);
    }

    // read nullables
    let nullables_bytes = read_bytes_slice(&mut input, (num_columns + 7) / 8)?;
    let nullables = BitVec::<u8>::from_vec(nullables_bytes.into());

    // read whether arrays have null buffers (which may differ from nullables)
    let has_null_buffers_bytes = read_bytes_slice(&mut input, (num_columns + 7) / 8)?;
    let has_null_buffers = BitVec::<u8>::from_vec(has_null_buffers_bytes.into());

    // create schema
    let schema = Arc::new(Schema::new(
        data_types
            .iter()
            .enumerate()
            .map(|(i, data_type)| Field::new("", data_type.clone(), nullables[i]))
            .collect(),
    ));

    // read columns
    let mut columns = vec![];
    for i in 0..num_columns {
        columns.push(match schema.field(i).data_type() {
            DataType::Null => Arc::new(NullArray::new(num_rows)),
            DataType::Boolean => {
                read_boolean_array(num_rows, has_null_buffers[i], &mut input)?
            }
            DataType::Int8 => read_primitive_array::<_, Int8Type>(
                num_rows,
                has_null_buffers[i],
                &mut input,
            )?,
            DataType::Int16 => read_primitive_array::<_, Int16Type>(
                num_rows,
                has_null_buffers[i],
                &mut input,
            )?,
            DataType::Int32 => read_primitive_array::<_, Int32Type>(
                num_rows,
                has_null_buffers[i],
                &mut input,
            )?,
            DataType::Int64 => read_primitive_array::<_, Int64Type>(
                num_rows,
                has_null_buffers[i],
                &mut input,
            )?,
            DataType::UInt8 => read_primitive_array::<_, UInt8Type>(
                num_rows,
                has_null_buffers[i],
                &mut input,
            )?,
            DataType::UInt16 => read_primitive_array::<_, UInt16Type>(
                num_rows,
                has_null_buffers[i],
                &mut input,
            )?,
            DataType::UInt32 => read_primitive_array::<_, UInt32Type>(
                num_rows,
                has_null_buffers[i],
                &mut input,
            )?,
            DataType::UInt64 => read_primitive_array::<_, UInt64Type>(
                num_rows,
                has_null_buffers[i],
                &mut input,
            )?,
            DataType::Float32 => read_primitive_array::<_, Float32Type>(
                num_rows,
                has_null_buffers[i],
                &mut input,
            )?,
            DataType::Float64 => read_primitive_array::<_, Float64Type>(
                num_rows,
                has_null_buffers[i],
                &mut input,
            )?,
            DataType::Decimal128(prec, scale) => {
                let array = read_primitive_array::<_, Decimal128Type>(
                    num_rows,
                    has_null_buffers[i],
                    &mut input,
                )?;
                Arc::new(
                    Decimal128Array::from(array.data().clone())
                        .with_precision_and_scale(*prec, *scale)?,
                )
            }
            DataType::Date32 => read_primitive_array::<_, Date32Type>(
                num_rows,
                has_null_buffers[i],
                &mut input,
            )?,
            DataType::Date64 => read_primitive_array::<_, Date64Type>(
                num_rows,
                has_null_buffers[i],
                &mut input,
            )?,
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                read_primitive_array::<_, TimestampMicrosecondType>(
                    num_rows,
                    has_null_buffers[i],
                    &mut input,
                )?
            }
            DataType::Utf8 => read_bytes_array(
                num_rows,
                has_null_buffers[i],
                &mut input,
                DataType::Utf8,
            )?,
            DataType::Binary => read_bytes_array(
                num_rows,
                has_null_buffers[i],
                &mut input,
                DataType::Binary,
            )?,
            DataType::List(list_field) => read_list_array(
                num_rows,
                has_null_buffers[i],
                &mut input,
                list_field.clone().deref()
            )?,
            DataType::Map(map_field, is_sorted) => read_map_array(
                num_rows,
                has_null_buffers[i],
                &mut input,
                map_field.clone().deref(),
                *is_sorted,
            )?,
            DataType::Struct(fields) => read_struct_array(
                num_rows,
                has_null_buffers[i],
                &mut input,
                fields.clone().deref(),
            )?,
            other => {
                return Err(ArrowError::IoError(format!(
                    "unsupported data type: {}",
                    other
                )));
            }
        });
    }

    // create batch
    Ok(RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(num_rows)),
    )?)
}

fn write_bits_buffer<W: Write>(
    buffer: &Buffer,
    bits_offset: usize,
    bits_len: usize,
    output: &mut W,
) -> ArrowResult<()> {
    let mut out_buffer = vec![0u8; (bits_len + 7) / 8];
    let in_ptr = buffer.as_ptr();
    let out_ptr = out_buffer.as_mut_ptr();
    let mut out_bits_offset = 0;

    for i in 0..bits_len {
        unsafe {
            if arrow::util::bit_util::get_bit_raw(in_ptr, bits_offset + i) {
                arrow::util::bit_util::set_bit_raw(out_ptr, out_bits_offset);
            }
        }
        out_bits_offset += 1;
    }
    output.write_all(&out_buffer)?;
    Ok(())
}

fn read_bits_buffer<R: Read>(input: &mut R, bits_len: usize) -> ArrowResult<Buffer> {
    let buf = read_bytes_slice(input, (bits_len + 7) / 8)?;
    Ok(Buffer::from(buf))
}

fn write_data_type<W: Write>(data_type: &DataType, output: &mut W) -> ArrowResult<()> {
    match data_type {
        DataType::Null => write_u8(1, output)?,
        DataType::Boolean => write_u8(2, output)?,
        DataType::Int8 => write_u8(3, output)?,
        DataType::Int16 => write_u8(4, output)?,
        DataType::Int32 => write_u8(5, output)?,
        DataType::Int64 => write_u8(6, output)?,
        DataType::UInt8 => write_u8(7, output)?,
        DataType::UInt16 => write_u8(8, output)?,
        DataType::UInt32 => write_u8(9, output)?,
        DataType::UInt64 => write_u8(10, output)?,
        DataType::Float32 => write_u8(11, output)?,
        DataType::Float64 => write_u8(12, output)?,
        DataType::Date32 => write_u8(13, output)?,
        DataType::Date64 => write_u8(14, output)?,
        DataType::Timestamp(TimeUnit::Microsecond, None)=> write_u8(15, output)?,
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
            write_u8(16, output)?;
            write_len(tz.as_bytes().len(), output)?;
            output.write_all(tz.as_bytes())?;
        }
        DataType::Decimal128(prec, scale) => {
            write_u8(17, output)?;
            write_u8(*prec, output)?;
            write_u8(*scale as u8, output)?;
        }
        DataType::Utf8 => write_u8(18, output)?,
        DataType::Binary => write_u8(19, output)?,
        DataType::List(field) => {
            write_u8(20, output)?;
            write_data_type(field.data_type(), output)?;
            if field.is_nullable() {
                write_u8(1, output)?;
            } else {
                write_u8(0, output)?;
            }
        },
        DataType::Map(field, is_sorted) => {
            write_u8(21, output)?;
            if *is_sorted {
                write_u8(1, output)?;
            } else {
                write_u8(0, output)?;
            }
            if field.is_nullable() {
                write_u8(1, output)?;
            } else {
                write_u8(0, output)?;
            }
            match field.data_type() {
                DataType::Struct(fields) => {
                    for field in fields {
                        write_data_type(field.data_type(), output)?;
                        if field.is_nullable() {
                            write_u8(1, output)?;
                        } else {
                            write_u8(0, output)?;
                        }
                    }
                }
                other => {
                    return  Err(ArrowError::SchemaError(format!(
                        "Map field data_type must be Struct, but found {:#?}",
                        other
                    )));
                }
            }
        }
        DataType::Struct(fields) => {
            write_u8(22, output)?;
            write_len(fields.len(), output)?;
            for field in fields {
                write_data_type(field.data_type(), output)?;
                if field.is_nullable() {
                    write_u8(1, output)?;
                } else {
                    write_u8(0, output)?;
                }
            }
        }
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "write_data_type: unsupported data type: {:?}",
                other
            )));
        }
    }
    Ok(())
}

fn read_data_type<R: Read>(input: &mut R) -> ArrowResult<DataType> {
    Ok(match read_u8(input)? {
        1 => DataType::Null,
        2 => DataType::Boolean,
        3 => DataType::Int8,
        4 => DataType::Int16,
        5 => DataType::Int32,
        6 => DataType::Int64,
        7 => DataType::UInt8,
        8 => DataType::UInt16,
        9 => DataType::UInt32,
        10 => DataType::UInt64,
        11 => DataType::Float32,
        12 => DataType::Float64,
        13 => DataType::Date32,
        14 => DataType::Date64,
        15 => DataType::Timestamp(TimeUnit::Microsecond, None),
        16 => {
            let tz_len = read_len(input)?;
            let tz_bytes = read_bytes_slice(input, tz_len)?;
            let tz = String::from_utf8_lossy(&tz_bytes).to_string();
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz))
        },
        17 => {
            let prec = read_u8(input)?;
            let scale = read_u8(input)? as i8;
            DataType::Decimal128(prec, scale)
        }
        18 => DataType::Utf8,
        19 => DataType::Binary,
        20 => {
            let child_datatype = read_data_type(input)?;
            let is_nullable = if read_u8(input)? == 1 {true} else {false};
            DataType::List(Box::new(Field::new("item", child_datatype, is_nullable)))
        }
        21 => {
            let is_sorted = if read_u8(input)? == 1 {true} else {false};
            let is_nullable = if read_u8(input)? == 1 {true} else {false};
            let mut fields = Vec::with_capacity(2);
            for _i in 0..2 {
                let field_data_type = read_data_type(input)?;
                let field_is_nullable = if read_u8(input)? == 1 {true} else {false};
                fields.push(Field::new("", field_data_type, field_is_nullable));
            }
            DataType::Map(Box::new(Field::new("entries", DataType::Struct(fields), is_nullable)), is_sorted)

        }
        22 => {
            let field_len = read_len(input)?;
            let mut fields = Vec::new();
            for _i in 0..field_len {
                let field_data_type = read_data_type(input)?;
                let field_is_nullable = if read_u8(input)? == 1 {true} else {false};
                fields.push(Field::new("", field_data_type, field_is_nullable));
            }
            DataType::Struct(fields)
        }
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "read_data_type: unsupported data type: {:?}",
                other
            )));
        }
    })
}

fn write_primitive_array<W: Write, PT: ArrowPrimitiveType>(
    array: &PrimitiveArray<PT>,
    output: &mut W,
) -> ArrowResult<()> {
    let item_size = PT::get_byte_width();
    let offset = array.offset();
    let len = array.len();
    if let Some(null_buffer) = array.data().null_buffer() {
        write_bits_buffer(null_buffer, offset, len, output)?;
    }
    output.write_all(
        &array.data().buffers()[0].as_slice()[item_size * offset..][..item_size * len])?;
    Ok(())
}

fn read_primitive_array<R: Read, PT: ArrowPrimitiveType>(
    num_rows: usize,
    has_null_buffer: bool,
    input: &mut R,
) -> ArrowResult<ArrayRef> {
    let null_buffer: Option<Buffer> = if has_null_buffer {
        Some(read_bits_buffer(input, num_rows)?)
    } else {
        None
    };

    let data_buffers: Vec<Buffer> = {
        let data_buffer_len = num_rows * PT::get_byte_width();
        let data_buffer = Buffer::from(read_bytes_slice(input, data_buffer_len)?);
        vec![data_buffer]
    };

    let array_data = ArrayData::try_new(
        PT::DATA_TYPE,
        num_rows,
        null_buffer,
        0,
        data_buffers,
        vec![],
    )?;
    Ok(make_array(array_data))
}

fn write_child_data<W: Write>(
    data: &ArrayData,
    output: &mut W,
) -> ArrowResult<()> {
    write_len(data.len(), output)?;
    if let Some(null_buffer) = data.null_buffer() {
        write_u8(1, output)?;
        output.write_all(null_buffer.as_slice())?;
    } else {
        write_u8(0, output)?;
    }

    match data.data_type() {
        DataType::Utf8 => {
            let byte_array = StringArray::from(data.clone());
            let mut cur_offset = 0;
            for &offset in byte_array.value_offsets().iter().skip(1) {
                let len = offset - cur_offset;
                write_len(len as usize, output)?;
                cur_offset = offset;
            }
            output.write_all(&byte_array.value_data())?;
        }
        DataType::Binary => {
            let byte_array = BinaryArray::from(data.clone());
            let mut cur_offset = 0;
            for &offset in byte_array.value_offsets().iter().skip(1) {
                let len = offset - cur_offset;
                write_len(len as usize, output)?;
                cur_offset = offset;
            }
            output.write_all(&byte_array.value_data())?;
        }
        DataType::List(_) |
        DataType::Map(_, _) |
        DataType::Struct(_) => {
            return Err(ArrowError::NotYetImplemented(format!(
                "unsupported nesting data type in write child_data",
            )));
        }
        _ => {
            output.write_all(data.buffers()[0].as_slice())?;
        }
    }
    Ok(())
}

fn write_list_array<W: Write>(
    array: &ListArray,
    output: &mut W,
) -> ArrowResult<()> {
    if let Some(null_buffer) = array.data().null_buffer() {
        write_bits_buffer(null_buffer, array.offset(), array.len(), output)?;
    }

    let mut cur_offset = 0;
    for &offset in array.value_offsets().iter().skip(1) {
        let len = offset - cur_offset;
        write_len(len as usize, output)?;
        cur_offset = offset;
    }

    // write list child_data
    if array.data().child_data().len() == 1 {
        write_child_data(array.data().child_data().get(0).unwrap(), output)?;
    } else {
        return Err(ArrowError::InvalidArgumentError("ListArray should contain a single child array (values array)".to_string()))
    }
    Ok(())
}

fn read_list_array<R: Read>(
    num_rows: usize,
    has_null_buffer: bool,
    input: &mut R,
    list_field: &Field ,
) -> ArrowResult<ArrayRef> {
    let null_buffer: Option<Buffer> = if has_null_buffer {
        Some(read_bits_buffer(input, num_rows)?)
    } else {
        None
    };

    let mut cur_offset = 0;
    let mut offsets_buffer = MutableBuffer::new((num_rows + 1) * 4);
    offsets_buffer.push(0u32);
    for _ in 0..num_rows {
        let len = read_len(input)?;
        let offset = cur_offset + len;
        offsets_buffer.push(offset as u32);
        cur_offset = offset;
    }
    let offsets_buffer = vec![offsets_buffer.into()];

    let child_data_len = read_len(input)?;
    let child_has_null_buffer = if read_u8(input)? == 1 {true} else {false};

    let child_data = get_child_array_data(child_data_len, child_has_null_buffer, input, list_field)?;

    let list_array_data = ArrayData::try_new(
        DataType::List(Box::new(list_field.clone())),
        num_rows,
        null_buffer,
        0,
        offsets_buffer,
        vec![child_data],
    )?;
    Ok(make_array(list_array_data))
}

fn write_map_array<W: Write>(
    array: &MapArray,
    output: &mut W,
) -> ArrowResult<()> {
    if let Some(null_buffer) = array.data().null_buffer() {
        write_bits_buffer(null_buffer, array.offset(), array.len(), output)?;
    }

    let mut cur_offset = 0;
    for &offset in array.value_offsets().iter().skip(1) {
        let len = offset - cur_offset;
        write_len(len as usize, output)?;
        cur_offset = offset;
    }

    let struct_data = array.data().child_data().get(0).unwrap();

    write_len(struct_data.len(), output)?;
    if struct_data.null_count() > 0 {
        write_u8(1, output)?;
        if let Some(null_buffer) = struct_data.null_buffer() {
            write_bits_buffer(null_buffer, struct_data.offset(), struct_data.len(), output)?;
        }
    } else {
        write_u8(0, output)?;
    }

    for child in struct_data.child_data() {
        write_child_data(child, output)?;
    }
    Ok(())
}

fn read_map_array<R: Read>(
    num_rows: usize,
    has_null_buffer: bool,
    input: &mut R,
    map_field: &Field,
    is_sorted: bool
) -> ArrowResult<ArrayRef> {
    let null_buffer: Option<Buffer> = if has_null_buffer {
        Some(read_bits_buffer(input, num_rows)?)
    } else {
        None
    };

    let mut cur_offset = 0;
    let mut offsets_buffer = MutableBuffer::new((num_rows + 1) * 4);
    offsets_buffer.push(0u32);
    for _ in 0..num_rows {
        let len = read_len(input)?;
        let offset = cur_offset + len;
        offsets_buffer.push(offset as u32);
        cur_offset = offset;
    }
    let offsets_buffer = vec![offsets_buffer.into()];
    let sturct_num_rows = read_len(input)?;
    let struct_null_buffer = if read_u8(input)? == 1 {
        let null_buffer_len = (sturct_num_rows + 7) / 8;
        let null_buffer = Buffer::from(read_bytes_slice(input, null_buffer_len)?);
        Some(null_buffer)
    } else {
        None
    };
    let mut struct_child_buffer = Vec::with_capacity(2);
    let mut struct_fields = Vec::with_capacity(2);
    if let DataType::Struct(fields) = map_field.data_type() {
        for i in 0..2 {
            let field = fields.get(i).unwrap();
            let child_data_len = read_len(input)?;
            let child_has_null_buffer = if read_u8(input)? == 1 {true} else {false};
            struct_fields.push(field.clone());
            struct_child_buffer.push(get_child_array_data(child_data_len, child_has_null_buffer, input, field)?);
        }
    }

    let child_data = ArrayData::try_new(
        DataType::Struct(struct_fields.clone()),
        sturct_num_rows,
        struct_null_buffer,
        0,
        vec![],
        struct_child_buffer,
    )?;

    let map_data = ArrayData::try_new(
        DataType::Map(Box::new(Field::new(map_field.name(), map_field.data_type().clone(), map_field.is_nullable())), is_sorted),
        num_rows,
        null_buffer,
        0,
        offsets_buffer,
        vec![child_data],
        )?;

    return Ok(make_array(map_data))
}

fn get_child_array_data<R: Read>(
    child_data_len: usize,
    child_has_null_buffer: bool,
    input: &mut R,
    field_context: &Field,
) -> ArrowResult<ArrayData> {
    let child_data = match field_context.data_type() {
        DataType::Null => {
            Arc::new(NullArray::new(child_data_len))
        }
        DataType::Boolean => {
            read_boolean_array(child_data_len, child_has_null_buffer, input)?
        }
        DataType::Int8 => read_primitive_array::<_, Int8Type>(
            child_data_len,
            child_has_null_buffer,
            input,
        )?,
        DataType::Int16 => read_primitive_array::<_, Int16Type>(
            child_data_len,
            child_has_null_buffer,
            input,
        )?,
        DataType::Int32 => read_primitive_array::<_, Int32Type>(
            child_data_len,
            child_has_null_buffer,
            input,
        )?,
        DataType::Int64 => read_primitive_array::<_, Int64Type>(
            child_data_len,
            child_has_null_buffer,
            input,
        )?,
        DataType::UInt8 => read_primitive_array::<_, UInt8Type>(
            child_data_len,
            child_has_null_buffer,
            input,
        )?,
        DataType::UInt16 => read_primitive_array::<_, UInt16Type>(
            child_data_len,
            child_has_null_buffer,
            input,
        )?,
        DataType::UInt32 => read_primitive_array::<_, UInt32Type>(
            child_data_len,
            child_has_null_buffer,
            input,
        )?,
        DataType::UInt64 => read_primitive_array::<_, UInt64Type>(
            child_data_len,
            child_has_null_buffer,
            input,
        )?,
        DataType::Float32 => read_primitive_array::<_, Float32Type>(
            child_data_len,
            child_has_null_buffer,
            input,
        )?,
        DataType::Float64 => read_primitive_array::<_, Float64Type>(
            child_data_len,
            child_has_null_buffer,
            input,
        )?,
        DataType::Decimal128(prec, scale) => {
            let array = read_primitive_array::<_, Decimal128Type>(
                child_data_len,
                child_has_null_buffer,
                input,
            )?;
            Arc::new(
                Decimal128Array::from(array.data().clone())
                    .with_precision_and_scale(*prec, *scale)?,
            )
        }
        DataType::Date32 => read_primitive_array::<_, Date32Type>(
            child_data_len,
            child_has_null_buffer,
            input,
        )?,
        DataType::Date64 => read_primitive_array::<_, Date64Type>(
            child_data_len,
            child_has_null_buffer,
            input,
        )?,
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            read_primitive_array::<_, TimestampMicrosecondType>(
                child_data_len,
                child_has_null_buffer,
                input,
            )?
        }
        DataType::Utf8 => read_bytes_array(
            child_data_len,
            child_has_null_buffer,
            input,
            DataType::Utf8,
        )?,
        DataType::Binary => read_bytes_array(
            child_data_len,
            child_has_null_buffer,
            input,
            DataType::Binary,
        )?,
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "unsupported nesting list data type: {}",
                other
            )));
        }
    }.data().clone();
    Ok(child_data)
}

fn write_struct_array<W: Write>(
    array: &StructArray,
    output: &mut W,
) -> ArrowResult<()> {
    // write struct null_buffer
    if let Some(null_buffer) = array.data().null_buffer() {
        write_bits_buffer(null_buffer, array.offset(), array.len(), output)?;
    }

    // write struct child_data
    let child_data_num = array.data().child_data().len();
    write_len(child_data_num, output)?;
    if !array.data().child_data().is_empty() {
        for child in array.data().child_data() {
            write_child_data(child, output)?;
        }
    }

    Ok(())
}

fn read_struct_array<R: Read>(
    num_rows: usize,
    has_null_buffer: bool,
    input: &mut R,
    fields: &[Field],
) -> ArrowResult<ArrayRef> {
    let null_buffer: Option<Buffer> = if has_null_buffer {
        Some(read_bits_buffer(input, num_rows)?)
    } else {
        None
    };

    let child_data_num = read_len(input)?;
    let mut child_data: Vec<ArrayData> = vec![];

    for i in 0.. child_data_num {
        let child_data_len = read_len(input)?;
        let child_has_null_buffer = if read_u8(input)? == 1 {true} else {false};
        child_data.push(get_child_array_data(child_data_len, child_has_null_buffer, input, fields.get(i).unwrap())?);
    }

    let struct_array_data = ArrayData::try_new(
        DataType::Struct(fields.to_vec()),
        num_rows,
        null_buffer,
        0,
        vec![],
        child_data
    )?;
    Ok(make_array(struct_array_data))
}

fn write_boolean_array<W: Write>(
    array: &BooleanArray,
    output: &mut W,
) -> ArrowResult<()> {
    if let Some(null_buffer) = array.data().null_buffer() {
        write_bits_buffer(null_buffer, array.offset(), array.len(), output)?;
    }
    write_bits_buffer(&array.data().buffers()[0], array.offset(), array.len(), output)?;
    Ok(())
}

fn read_boolean_array<R: Read>(
    num_rows: usize,
    has_null_buffer: bool,
    input: &mut R,
) -> ArrowResult<ArrayRef> {
    let null_buffer: Option<Buffer> = if has_null_buffer {
        Some(read_bits_buffer(input, num_rows)?)
    } else {
        None
    };

    let data_buffers: Vec<Buffer> = {
        let data_buffer = read_bits_buffer(input, num_rows)?;
        vec![data_buffer]
    };

    let array_data = ArrayData::try_new(
        DataType::Boolean,
        num_rows,
        null_buffer,
        0,
        data_buffers,
        vec![],
    )?;
    Ok(make_array(array_data))
}

fn write_bytes_array<T: ByteArrayType<Offset = i32>, W: Write>(
    array: &GenericByteArray<T>,
    output: &mut W,
) -> ArrowResult<()> {
    if let Some(null_buffer) = array.data().null_buffer() {
        write_bits_buffer(null_buffer, array.offset(), array.len(), output)?;
    }

    let first_offset = array.value_offsets().get(0).cloned().unwrap_or_default();
    let mut cur_offset = first_offset;
    for &offset in array.value_offsets().iter().skip(1) {
        let len = offset - cur_offset;
        write_len(len as usize, output)?;
        cur_offset = offset;
    }
    output.write_all(&array.value_data()[first_offset as usize .. cur_offset as usize])?;
    Ok(())
}

fn read_bytes_array<R: Read>(
    num_rows: usize,
    has_null_buffer: bool,
    input: &mut R,
    data_type: DataType,
) -> ArrowResult<ArrayRef> {
    let null_buffer: Option<Buffer> = if has_null_buffer {
        Some(read_bits_buffer(input, num_rows)?)
    } else {
        None
    };

    let mut cur_offset = 0;
    let mut offsets_buffer = MutableBuffer::new((num_rows + 1) * 4);
    offsets_buffer.push(0u32);
    for _ in 0..num_rows {
        let len = read_len(input)?;
        let offset = cur_offset + len;
        offsets_buffer.push(offset as u32);
        cur_offset = offset;
    }
    let offsets_buffer: Buffer = offsets_buffer.into();

    let data_len = cur_offset;
    let data_buffer = Buffer::from(read_bytes_slice(input, data_len)?);
    let array_data = ArrayData::try_new(
        data_type,
        num_rows,
        null_buffer,
        0,
        vec![offsets_buffer, data_buffer],
        vec![],
    )?;
    Ok(make_array(array_data))
}

#[cfg(test)]
mod test {
    use crate::io::batch_serde::{read_batch, write_batch};
    use arrow::array::*;
    use arrow::record_batch::RecordBatch;
    use std::io::Cursor;
    use std::sync::Arc;
    use arrow::buffer::Buffer;
    use arrow::datatypes::{DataType, Field, Int32Type, Schema, ToByteSlice};
    use arrow::datatypes::DataType::{Int32, Int64};
    use arrow::util::bit_util;

    #[test]
    fn test_write_and_read_batch() {
        let array1: ArrayRef = Arc::new(StringArray::from_iter([
            Some("20220101".to_owned()),
            Some("20220102你好🍹".to_owned()),
            Some("你好🍹20220103".to_owned()),
            None,
        ]));
        let array2: ArrayRef = Arc::new(UInt64Array::from_iter([
            Some(1000),
            Some(2000),
            Some(3000),
            None,
        ]));
        let array3: ArrayRef = Arc::new(BooleanArray::from_iter([
            Some(true),
            Some(false),
            None,
            None,
        ]));
        let batch = RecordBatch::try_from_iter_with_nullable(vec![
            ("", array1, true),
            ("", array2, true),
            ("", array3, true),
        ])
        .unwrap();

        // test read after write
        let mut buf = vec![];
        write_batch(&batch, &mut buf, true).unwrap();
        let mut cursor = Cursor::new(buf);
        let decoded_batch = read_batch(&mut cursor, true).unwrap();
        assert_eq!(decoded_batch, batch);

        // test read after write sliced
        let sliced = batch.slice(1, 2);
        let mut buf = vec![];
        write_batch(&sliced, &mut buf, true).unwrap();
        let mut cursor = Cursor::new(buf);
        let decoded_batch = read_batch(&mut cursor, true).unwrap();
        assert_eq!(decoded_batch, sliced);
    }

    #[test]
    fn test_write_and_read_batch_for_list_slice() {

        let data1 = vec![
            Some(vec![Some(0), Some(1), Some(2)]),
            None,
            Some(vec![Some(3), None, Some(5)]),
            Some(vec![Some(6), Some(7)]),
        ];
        let list_array1 = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(data1.clone()));

        let list_array2 = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(data1.clone()));

        let list_array3 = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(data1.clone()));


        let schema = Schema::new(vec![
            Field::new("", DataType::List(Box::new((Field::new("item", DataType::Int32, true)))), true),
            Field::new("", DataType::List(Box::new((Field::new("item", DataType::Int32, true)))), true),
            Field::new("", DataType::List(Box::new((Field::new("item", DataType::Int32, true)))), true),
        ]);

        let record_batch =
            RecordBatch::try_new(Arc::new(schema), vec![list_array1, list_array2, list_array3])
                .unwrap();
        let sliced = record_batch.slice(0,2);

        let mut buf = vec![];
        write_batch(&sliced, &mut buf, true).unwrap();

        let mut cursor = Cursor::new(buf);
        let decoded_batch = read_batch(&mut cursor, true).unwrap();
        assert_eq!(decoded_batch, sliced);
    }

    #[test]
    fn test_write_and_read_batch_for_map_slice() {

        let keys_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from(&[0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice()))
            .build()
            .unwrap();
        let value_data = ArrayData::builder(DataType::UInt32)
            .len(8)
            .add_buffer(Buffer::from(
                &[0u32, 10, 20, 0, 40, 0, 60, 70].to_byte_slice(),
            ))
            .null_bit_buffer(Some(Buffer::from(&[0b11010110])))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let entry_offsets = Buffer::from(&[0, 3, 6, 8].to_byte_slice());

        let keys = Field::new("", DataType::Int32, false);
        let values = Field::new("", DataType::UInt32, true);
        let entry_struct = StructArray::from(vec![
            (keys, make_array(keys_data)),
            (values, make_array(value_data)),
        ]);

        // Construct a map array from the above two
        let map_data_type = DataType::Map(
            Box::new(Field::new(
                "entries",
                entry_struct.data_type().clone(),
                true,
            )),
            false,
        );
        let map_data = ArrayData::builder(map_data_type.clone())
            .len(3)
            .add_buffer(entry_offsets)
            .add_child_data(entry_struct.into_data())
            .build()
            .unwrap();
        let map_array1 = MapArray::from(map_data.clone());
        let map_array2 = MapArray::from(map_data.clone());
        let map_array3 = MapArray::from(map_data.clone());


        let schema = Schema::new(vec![
            Field::new("", map_data_type.clone(), false),
            Field::new("", map_data_type.clone(), false),
            Field::new("", map_data_type.clone(), false),
        ]);


        let record_batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(map_array1), Arc::new(map_array2), Arc::new(map_array3)])
                .unwrap();
        let sliced = record_batch.slice(1,2);
        let mut buf = vec![];
        write_batch(&sliced, &mut buf, true).unwrap();

        let mut cursor = Cursor::new(buf);
        let decoded_batch = read_batch(&mut cursor, true).unwrap();
        assert_eq!(decoded_batch.columns(), sliced.columns());
    }

    #[test]
    fn test_write_and_read_batch_for_struct_slice() {

        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("joe"),
            None,
            None,
            Some("mark"),
        ]));
        let ints: ArrayRef =
            Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)]));

        let arr1 =
            StructArray::try_from(vec![("", strings.clone()), ("", ints.clone())])
                .unwrap();


        let schema = Schema::new(vec![
            Field::new("", arr1.data_type().clone(), true),
            Field::new("", arr1.data_type().clone(), true),
            Field::new("", arr1.data_type().clone(), true),
        ]);


        let record_batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arr1.clone()), Arc::new(arr1.clone()), Arc::new(arr1.clone())])
                .unwrap();
         let sliced = record_batch.slice(1,2);
        let mut buf = vec![];
        write_batch(&sliced, &mut buf, true).unwrap();

        let mut cursor = Cursor::new(buf);
        let decoded_batch = read_batch(&mut cursor, true).unwrap();
        assert_eq!(decoded_batch, sliced);
    }
}
