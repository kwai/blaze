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

use arrow::datatypes::*;
use datafusion::{common::Result, parquet::data_type::AsBytes, scalar::ScalarValue};

use crate::{
    df_unimplemented_err,
    io::{read_bytes_slice, read_len, read_u8, write_len, write_u8},
};

pub fn write_scalar<W: Write>(value: &ScalarValue, output: &mut W) -> Result<()> {
    fn write_primitive_valid_scalar<W: Write>(buf: &[u8], output: &mut W) -> Result<()> {
        write_len(1 as usize, output)?;
        output.write_all(buf)?;
        Ok(())
    }
    match value {
        ScalarValue::Null => write_u8(0, output)?,
        ScalarValue::Boolean(Some(value)) => write_u8((*value as u8) + 1u8, output)?,
        ScalarValue::Int8(Some(value)) => {
            write_primitive_valid_scalar(value.to_ne_bytes().as_slice(), output)?
        }
        ScalarValue::Int16(Some(value)) => {
            write_primitive_valid_scalar(value.to_ne_bytes().as_slice(), output)?
        }
        ScalarValue::Int32(Some(value)) => {
            write_primitive_valid_scalar(value.to_ne_bytes().as_slice(), output)?
        }
        ScalarValue::Int64(Some(value)) => {
            write_primitive_valid_scalar(value.to_ne_bytes().as_slice(), output)?
        }
        ScalarValue::UInt8(Some(value)) => {
            write_primitive_valid_scalar(value.to_ne_bytes().as_slice(), output)?
        }
        ScalarValue::UInt16(Some(value)) => {
            write_primitive_valid_scalar(value.to_ne_bytes().as_slice(), output)?
        }
        ScalarValue::UInt32(Some(value)) => {
            write_primitive_valid_scalar(value.to_ne_bytes().as_slice(), output)?
        }
        ScalarValue::UInt64(Some(value)) => {
            write_primitive_valid_scalar(value.to_ne_bytes().as_slice(), output)?
        }
        ScalarValue::Float32(Some(value)) => {
            write_primitive_valid_scalar(value.to_ne_bytes().as_slice(), output)?
        }
        ScalarValue::Float64(Some(value)) => {
            write_primitive_valid_scalar(value.to_ne_bytes().as_slice(), output)?
        }
        ScalarValue::Decimal128(Some(value), ..) => {
            write_primitive_valid_scalar(value.to_ne_bytes().as_slice(), output)?
        }
        ScalarValue::Utf8(Some(value)) => {
            let value_bytes = value.as_bytes();
            write_len(value_bytes.len() + 1, output)?;
            output.write_all(value_bytes)?;
        }
        ScalarValue::Binary(Some(value)) => {
            let value_byte = value.as_bytes();
            write_len(value_byte.len() + 1, output)?;
            output.write_all(value_byte)?;
        }
        ScalarValue::Date32(Some(value)) => {
            write_primitive_valid_scalar(value.to_ne_bytes().as_slice(), output)?
        }
        ScalarValue::Date64(Some(value)) => {
            write_primitive_valid_scalar(value.to_ne_bytes().as_slice(), output)?
        }
        ScalarValue::TimestampSecond(Some(value), _) => {
            write_primitive_valid_scalar(value.to_ne_bytes().as_slice(), output)?
        }
        ScalarValue::TimestampMillisecond(Some(value), _) => {
            write_primitive_valid_scalar(value.to_ne_bytes().as_slice(), output)?
        }
        ScalarValue::TimestampMicrosecond(Some(value), _) => {
            write_primitive_valid_scalar(value.to_ne_bytes().as_slice(), output)?
        }
        ScalarValue::TimestampNanosecond(Some(value), _) => {
            write_primitive_valid_scalar(value.to_ne_bytes().as_slice(), output)?
        }
        ScalarValue::List(Some(value), _field) => {
            write_len(value.len() + 1, output)?;
            if value.len() != 0 {
                for element in value {
                    write_scalar(element, output)?;
                }
            }
        }
        ScalarValue::Struct(Some(value), _fields) => {
            write_len(value.len() + 1, output)?;
            if value.len() != 0 {
                for element in value {
                    write_scalar(element, output)?;
                }
            }
        }
        ScalarValue::Map(value, _bool) => {
            write_scalar(value, output)?;
        }
        ScalarValue::Boolean(None)
        | ScalarValue::Int8(None)
        | ScalarValue::Int16(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Int64(None)
        | ScalarValue::UInt8(None)
        | ScalarValue::UInt16(None)
        | ScalarValue::UInt32(None)
        | ScalarValue::UInt64(None)
        | ScalarValue::Float32(None)
        | ScalarValue::Float64(None)
        | ScalarValue::Decimal128(None, ..)
        | ScalarValue::Binary(None)
        | ScalarValue::Utf8(None)
        | ScalarValue::Date32(None)
        | ScalarValue::Date64(None)
        | ScalarValue::TimestampSecond(None, _)
        | ScalarValue::TimestampMillisecond(None, _)
        | ScalarValue::TimestampMicrosecond(None, _)
        | ScalarValue::TimestampNanosecond(None, _)
        | ScalarValue::List(None, _)
        | ScalarValue::Struct(None, ..) => write_len(0 as usize, output)?,
        other => df_unimplemented_err!("unsupported scalarValue type: {other}")?,
    }
    Ok(())
}

pub fn read_scalar<R: Read>(input: &mut R, data_type: &DataType) -> Result<ScalarValue> {
    macro_rules! read_primitive_scalar {
        ($input:ident, $len:expr, $byte_kind:ident) => {{
            let valid = read_len(input)?;
            if valid != 0 {
                let mut buf = [0; $len];
                $input.read_exact(&mut buf)?;
                Some($byte_kind::from_ne_bytes(buf))
            } else {
                None
            }
        }};
    }

    Ok(match data_type {
        DataType::Null => {
            read_u8(input)?;
            ScalarValue::Null
        },
        DataType::Boolean => match read_u8(input)? {
            0u8 => ScalarValue::Boolean(None),
            1u8 => ScalarValue::Boolean(Some(false)),
            _ => ScalarValue::Boolean(Some(true)),
        },
        DataType::Int8 => ScalarValue::Int8(read_primitive_scalar!(input, 1, i8)),
        DataType::Int16 => ScalarValue::Int16(read_primitive_scalar!(input, 2, i16)),
        DataType::Int32 => ScalarValue::Int32(read_primitive_scalar!(input, 4, i32)),
        DataType::Int64 => ScalarValue::Int64(read_primitive_scalar!(input, 8, i64)),
        DataType::UInt8 => ScalarValue::UInt8(read_primitive_scalar!(input, 1, u8)),
        DataType::UInt16 => ScalarValue::UInt16(read_primitive_scalar!(input, 2, u16)),
        DataType::UInt32 => ScalarValue::UInt32(read_primitive_scalar!(input, 4, u32)),
        DataType::UInt64 => ScalarValue::UInt64(read_primitive_scalar!(input, 8, u64)),
        DataType::Float32 => ScalarValue::Float32(read_primitive_scalar!(input, 4, f32)),
        DataType::Float64 => ScalarValue::Float64(read_primitive_scalar!(input, 8, f64)),
        DataType::Decimal128(precision, scale) => {
            ScalarValue::Decimal128(read_primitive_scalar!(input, 16, i128), *precision, *scale)
        }
        DataType::Date32 => ScalarValue::Date32(read_primitive_scalar!(input, 4, i32)),
        DataType::Date64 => ScalarValue::Date64(read_primitive_scalar!(input, 8, i64)),
        DataType::Timestamp(TimeUnit::Second, str) => {
            ScalarValue::TimestampSecond(read_primitive_scalar!(input, 8, i64), str.clone())
        }
        DataType::Timestamp(TimeUnit::Millisecond, str) => {
            ScalarValue::TimestampMillisecond(read_primitive_scalar!(input, 8, i64), str.clone())
        }
        DataType::Timestamp(TimeUnit::Microsecond, str) => {
            ScalarValue::TimestampMicrosecond(read_primitive_scalar!(input, 8, i64), str.clone())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, str) => {
            ScalarValue::TimestampNanosecond(read_primitive_scalar!(input, 8, i64), str.clone())
        }
        DataType::Binary => {
            let data_len = read_len(input)?;
            if data_len > 0 {
                let data_len = data_len - 1;
                ScalarValue::Binary(Some(read_bytes_slice(input, data_len)?.into()))
            } else {
                ScalarValue::Binary(None)
            }
        }
        DataType::Utf8 => {
            let data_len = read_len(input)?;
            if data_len > 0 {
                let data_len = data_len - 1;
                let value_buf = read_bytes_slice(input, data_len)?;
                let value = String::from_utf8_lossy(&value_buf);
                ScalarValue::Utf8(Some(value.into()))
            } else {
                ScalarValue::Utf8(None)
            }
        }
        DataType::List(field) => {
            let data_len = read_len(input)?;
            if data_len > 0 {
                let data_len = data_len - 1;
                let mut list_data: Vec<ScalarValue> = Vec::with_capacity(data_len);
                for _i in 0..data_len {
                    let child_value = read_scalar(input, field.data_type())?;
                    list_data.push(child_value);
                }
                ScalarValue::List(Some(list_data), field.clone())
            } else {
                ScalarValue::List(None, field.clone())
            }
        }
        DataType::Struct(fields) => {
            let data_len = read_len(input)?;
            if data_len > 0 {
                let data_len = data_len - 1;
                let mut struct_data: Vec<ScalarValue> = Vec::with_capacity(data_len);
                for i in 0..data_len {
                    let child_value = read_scalar(input, fields[i].data_type())?;
                    struct_data.push(child_value);
                }
                ScalarValue::Struct(Some(struct_data), fields.clone())
            } else {
                ScalarValue::Struct(None, fields.clone())
            }
        }
        DataType::Map(field, bool) => {
            let map_value = read_scalar(input, field.data_type())?;
            ScalarValue::Map(Box::new(map_value), *bool)
        }
        other => df_unimplemented_err!("unsupported data type: {other}")?,
    })
}

pub fn skip_read_scalar<R: Read + Seek>(input: &mut R, data_type: &DataType) -> Result<usize> {
    let start_pos = input.stream_position()?;

    macro_rules! skip_primitive_scalar {
        ($input:ident, $len:expr) => {{
            let valid = read_len(input)?;
            if valid != 0 {
                input.seek(SeekFrom::Current($len as i64))?;
            }
        }};
    }

    match data_type {
        DataType::Null | DataType::Boolean => {
            input.seek(SeekFrom::Current(1))?;
        },
        DataType::Int8 => (skip_primitive_scalar!(input, 1)),
        DataType::Int16 => (skip_primitive_scalar!(input, 2)),
        DataType::Int32 => (skip_primitive_scalar!(input, 4)),
        DataType::Int64 => (skip_primitive_scalar!(input, 8)),
        DataType::UInt8 => (skip_primitive_scalar!(input, 1)),
        DataType::UInt16 => (skip_primitive_scalar!(input, 2)),
        DataType::UInt32 => (skip_primitive_scalar!(input, 4)),
        DataType::UInt64 => (skip_primitive_scalar!(input, 8)),
        DataType::Float32 => (skip_primitive_scalar!(input, 4)),
        DataType::Float64 => (skip_primitive_scalar!(input, 8)),
        DataType::Decimal128(..) => (skip_primitive_scalar!(input, 16)),
        DataType::Date32 => (skip_primitive_scalar!(input, 4)),
        DataType::Date64 => (skip_primitive_scalar!(input, 8)),
        DataType::Timestamp(..) => (skip_primitive_scalar!(input, 8)),
        DataType::Binary | DataType::Utf8 => {
            let data_len = read_len(input)?;
            if data_len > 0 {
                let data_len = data_len - 1;
                input.seek(SeekFrom::Current(data_len as i64))?;
            }
        }
        DataType::List(field) => {
            let data_len = read_len(input)?;
            if data_len > 0 {
                let data_len = data_len - 1;
                for _i in 0..data_len {
                    skip_read_scalar(input, field.data_type())?;
                }
            }
        }
        DataType::Struct(fields) => {
            let data_len = read_len(input)?;
            if data_len > 0 {
                let data_len = data_len - 1;
                for i in 0..data_len {
                    skip_read_scalar(input, fields[i].data_type())?;
                }
            }
        }
        DataType::Map(field, _) => {
            skip_read_scalar(input, field.data_type())?;
        }
        other => df_unimplemented_err!("unsupported data type: {other}")?,
    }
    let end_pos = input.stream_position()?;
    Ok((end_pos - start_pos) as usize)
}
