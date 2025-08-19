// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    io::{Read, Write},
    sync::Arc,
};

use arrow::{array::AsArray, datatypes::*};
use datafusion::{common::Result, parquet::data_type::AsBytes, scalar::ScalarValue};

use crate::{
    df_unimplemented_err,
    io::{
        batch_serde::TransposeOpt, read_array, read_bytes_slice, read_len, read_u8, write_array,
        write_len, write_u8,
    },
};

pub fn write_scalar<W: Write>(value: &ScalarValue, nullable: bool, output: &mut W) -> Result<()> {
    assert!(nullable || !value.is_null());

    macro_rules! write_prim {
        ($v:expr) => {{
            if nullable {
                if let Some(v) = $v {
                    write_u8(1, output)?;
                    output.write_all(&v.to_ne_bytes())?;
                } else {
                    write_u8(0, output)?;
                }
            } else {
                output.write_all(&$v.unwrap().to_ne_bytes())?;
            }
        }};
    }

    match value {
        ScalarValue::Null => write_u8(0, output)?,
        ScalarValue::Boolean(v) => write_prim!(v.map(|v| v as i8)),
        ScalarValue::Int8(v) => write_prim!(v),
        ScalarValue::Int16(v) => write_prim!(v),
        ScalarValue::Int32(v) => write_prim!(v),
        ScalarValue::Int64(v) => write_prim!(v),
        ScalarValue::UInt8(v) => write_prim!(v),
        ScalarValue::UInt16(v) => write_prim!(v),
        ScalarValue::UInt32(v) => write_prim!(v),
        ScalarValue::UInt64(v) => write_prim!(v),
        ScalarValue::Float32(v) => write_prim!(v),
        ScalarValue::Float64(v) => write_prim!(v),
        ScalarValue::Decimal128(v, ..) => write_prim!(v),
        ScalarValue::Date32(v) => write_prim!(v),
        ScalarValue::Date64(v) => write_prim!(v),
        ScalarValue::TimestampSecond(v, ..) => write_prim!(v),
        ScalarValue::TimestampMillisecond(v, ..) => write_prim!(v),
        ScalarValue::TimestampMicrosecond(v, ..) => write_prim!(v),
        ScalarValue::TimestampNanosecond(v, ..) => write_prim!(v),
        ScalarValue::Utf8(v) => {
            if let Some(v) = v {
                write_len(v.as_bytes().len() + 1, output)?;
                output.write_all(v.as_bytes())?;
            } else {
                write_len(0, output)?;
            }
        }
        ScalarValue::Binary(v) => {
            if let Some(v) = v {
                write_len(v.as_bytes().len() + 1, output)?;
                output.write_all(v.as_bytes())?;
            } else {
                write_len(0, output)?;
            }
        }
        ScalarValue::List(v) => {
            write_array(v.as_ref(), output, &mut TransposeOpt::Disabled)?;
        }
        ScalarValue::Struct(v) => {
            write_array(v.as_ref(), output, &mut TransposeOpt::Disabled)?;
        }
        ScalarValue::Map(v) => {
            write_array(v.as_ref(), output, &mut TransposeOpt::Disabled)?;
        }
        other => df_unimplemented_err!("unsupported scalarValue type: {other}")?,
    }
    Ok(())
}

pub fn read_scalar<R: Read>(
    input: &mut R,
    data_type: &DataType,
    nullable: bool,
) -> Result<ScalarValue> {
    macro_rules! read_prim {
        ($ty:ty) => {{
            if nullable {
                let valid = read_u8(input)? != 0;
                if valid {
                    let mut buf = [0u8; std::mem::size_of::<$ty>()];
                    input.read_exact(&mut buf)?;
                    Some(<$ty>::from_ne_bytes(buf))
                } else {
                    None
                }
            } else {
                let mut buf = [0u8; std::mem::size_of::<$ty>()];
                input.read_exact(&mut buf)?;
                Some(<$ty>::from_ne_bytes(buf))
            }
        }};
    }

    Ok(match data_type {
        DataType::Null => {
            read_u8(input)?;
            ScalarValue::Null
        }
        DataType::Boolean => ScalarValue::Boolean(read_prim!(u8).map(|v| v != 0)),
        DataType::Int8 => ScalarValue::Int8(read_prim!(i8)),
        DataType::Int16 => ScalarValue::Int16(read_prim!(i16)),
        DataType::Int32 => ScalarValue::Int32(read_prim!(i32)),
        DataType::Int64 => ScalarValue::Int64(read_prim!(i64)),
        DataType::UInt8 => ScalarValue::UInt8(read_prim!(u8)),
        DataType::UInt16 => ScalarValue::UInt16(read_prim!(u16)),
        DataType::UInt32 => ScalarValue::UInt32(read_prim!(u32)),
        DataType::UInt64 => ScalarValue::UInt64(read_prim!(u64)),
        DataType::Float32 => ScalarValue::Float32(read_prim!(f32)),
        DataType::Float64 => ScalarValue::Float64(read_prim!(f64)),
        DataType::Decimal128(precision, scale) => {
            ScalarValue::Decimal128(read_prim!(i128), *precision, *scale)
        }
        DataType::Date32 => ScalarValue::Date32(read_prim!(i32)),
        DataType::Date64 => ScalarValue::Date64(read_prim!(i64)),
        DataType::Timestamp(TimeUnit::Second, str) => {
            ScalarValue::TimestampSecond(read_prim!(i64), str.clone())
        }
        DataType::Timestamp(TimeUnit::Millisecond, str) => {
            ScalarValue::TimestampMillisecond(read_prim!(i64), str.clone())
        }
        DataType::Timestamp(TimeUnit::Microsecond, str) => {
            ScalarValue::TimestampMicrosecond(read_prim!(i64), str.clone())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, str) => {
            ScalarValue::TimestampNanosecond(read_prim!(i64), str.clone())
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
        DataType::List(_) => {
            let list = read_array(input, data_type, 1, &mut TransposeOpt::Disabled)?
                .as_list()
                .clone();
            ScalarValue::List(Arc::new(list))
        }
        DataType::Struct(_) => {
            let struct_ = read_array(input, data_type, 1, &mut TransposeOpt::Disabled)?
                .as_struct()
                .clone();
            ScalarValue::Struct(Arc::new(struct_))
        }
        DataType::Map(_, _bool) => {
            let map = read_array(input, data_type, 1, &mut TransposeOpt::Disabled)?
                .as_map()
                .clone();
            ScalarValue::Map(Arc::new(map))
        }
        other => df_unimplemented_err!("unsupported data type: {other}")?,
    })
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use arrow_schema::DataType;
    use datafusion::common::{Result, ScalarValue};

    use crate::io::{read_scalar, write_scalar};

    #[test]
    fn test() -> Result<()> {
        let mut buf = vec![];

        write_scalar(&ScalarValue::from(123), false, &mut buf)?;
        write_scalar(&ScalarValue::from("Wooden"), false, &mut buf)?;
        write_scalar(&ScalarValue::from("Slash"), true, &mut buf)?;
        write_scalar(&ScalarValue::Utf8(None), true, &mut buf)?;
        write_scalar(&ScalarValue::Null, true, &mut buf)?;
        write_scalar(&ScalarValue::from(3.15), false, &mut buf)?;

        let mut cur = Cursor::new(&buf);
        assert_eq!(
            read_scalar(&mut cur, &DataType::Int32, false)?,
            ScalarValue::from(123)
        );
        assert_eq!(
            read_scalar(&mut cur, &DataType::Utf8, false)?,
            ScalarValue::from("Wooden")
        );
        assert_eq!(
            read_scalar(&mut cur, &DataType::Utf8, true)?,
            ScalarValue::from("Slash")
        );
        assert_eq!(
            read_scalar(&mut cur, &DataType::Utf8, true)?,
            ScalarValue::Utf8(None)
        );
        assert_eq!(
            read_scalar(&mut cur, &DataType::Null, true)?,
            ScalarValue::Null
        );
        assert_eq!(
            read_scalar(&mut cur, &DataType::Float64, false)?,
            ScalarValue::from(3.15)
        );
        Ok(())
    }
}
