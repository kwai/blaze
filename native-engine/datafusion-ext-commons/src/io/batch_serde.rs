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
use std::sync::Arc;

pub fn write_batch<W: Write>(
    batch: &RecordBatch,
    output: &mut W,
    compress: bool,
) -> ArrowResult<()> {
    let mut output: Box<dyn Write> = if compress {
        Box::new(BufWriter::new(zstd::Encoder::new(output, 1)?.auto_finish()))
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
        DataType::Decimal128(prec, scale) => {
            write_u8(15, output)?;
            write_u8(*prec, output)?;
            write_u8(*scale as u8, output)?;
        }
        DataType::Utf8 => write_u8(16, output)?,
        DataType::Binary => write_u8(17, output)?,
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
        15 => {
            let prec = read_u8(input)?;
            let scale = read_u8(input)? as i8;
            DataType::Decimal128(prec, scale)
        }
        16 => DataType::Utf8,
        17 => DataType::Binary,
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
    if let Some(null_buffer) = array.data().null_buffer() {
        output.write_all(null_buffer.as_slice())?;
    }
    output.write_all(array.data().buffers()[0].as_slice())?;
    Ok(())
}

fn read_primitive_array<R: Read, PT: ArrowPrimitiveType>(
    num_rows: usize,
    has_null_buffer: bool,
    input: &mut R,
) -> ArrowResult<ArrayRef> {
    let null_buffer: Option<Buffer> = if has_null_buffer {
        let null_buffer_len = (num_rows + 7) / 8;
        Some(Buffer::from(read_bytes_slice(input, null_buffer_len)?))
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

fn write_boolean_array<W: Write>(
    array: &BooleanArray,
    output: &mut W,
) -> ArrowResult<()> {
    if let Some(null_buffer) = array.data().null_buffer() {
        output.write_all(null_buffer.as_slice())?;
    }
    output.write_all(array.data().buffers()[0].as_slice())?;
    Ok(())
}

fn read_boolean_array<R: Read>(
    num_rows: usize,
    has_null_buffer: bool,
    input: &mut R,
) -> ArrowResult<ArrayRef> {
    let null_buffer: Option<Buffer> = if has_null_buffer {
        let null_buffer_len = (num_rows + 7) / 8;
        let null_buffer = Buffer::from(read_bytes_slice(input, null_buffer_len)?);
        Some(null_buffer)
    } else {
        None
    };

    let data_buffers: Vec<Buffer> = {
        let data_buffer_len = (num_rows + 7) / 8;
        let data_buffer = Buffer::from(read_bytes_slice(input, data_buffer_len)?);
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
        output.write_all(null_buffer.as_slice())?;
    }

    let mut cur_offset = 0;
    for &offset in array.value_offsets().iter().skip(1) {
        let len = offset - cur_offset;
        write_len(len as usize, output)?;
        cur_offset = offset;
    }
    output.write_all(&array.value_data())?;
    Ok(())
}

fn read_bytes_array<R: Read>(
    num_rows: usize,
    has_null_buffer: bool,
    input: &mut R,
    data_type: DataType,
) -> ArrowResult<ArrayRef> {
    let null_buffer: Option<Buffer> = if has_null_buffer {
        Some(Buffer::from(read_bytes_slice(input, (num_rows + 7) / 8)?))
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

        let mut buf = vec![];
        write_batch(&batch, &mut buf, true).unwrap();

        let mut cursor = Cursor::new(buf);
        let decoded_batch = read_batch(&mut cursor, true).unwrap();
        assert_eq!(decoded_batch, batch);
    }
}
