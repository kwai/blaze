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
    io::{ErrorKind, Read, Write},
    sync::Arc,
};

use arrow::{array::*, buffer::Buffer, datatypes::*};
use datafusion::common::Result;

use crate::{
    SliceAsRawBytes, UninitializedInit, df_unimplemented_err,
    io::{read_bytes_slice, read_len, write_len},
};

pub enum TransposeOpt {
    Disabled,
    Transpose(Box<[u8]>),
}

impl TransposeOpt {
    pub fn with_transpose<'a>(
        num_rows: usize,
        data_types: impl Iterator<Item = &'a DataType>,
    ) -> Self {
        let max_bytes_width = data_types
            .map(Self::data_type_bytes_width)
            .max()
            .unwrap_or(0);
        TransposeOpt::Transpose(Vec::<u8>::uninitialized_init(num_rows * max_bytes_width).into())
    }

    pub fn data_type_bytes_width(dt: &DataType) -> usize {
        match dt {
            DataType::Null => 0,
            DataType::Boolean => 0,
            dt if dt.primitive_width() == Some(1) => 0,
            dt if dt.primitive_width() >= Some(2) => dt.primitive_width().unwrap(),
            DataType::Utf8 | DataType::Binary => 4,
            DataType::List(f) | DataType::Map(f, _) => {
                Self::data_type_bytes_width(f.data_type()).max(4)
            }
            DataType::Struct(fields) => fields
                .iter()
                .map(|f| Self::data_type_bytes_width(f.data_type()))
                .max()
                .unwrap_or(0),
            _ => 0,
        }
    }
}

pub fn write_batch(num_rows: usize, cols: &[ArrayRef], mut output: impl Write) -> Result<()> {
    // write number of columns and rows
    write_len(num_rows, &mut output)?;

    // write columns
    let mut transpose_opt =
        TransposeOpt::with_transpose(num_rows, cols.iter().map(|col| col.data_type()));
    for col in cols {
        write_array(col, &mut output, &mut transpose_opt)?;
    }
    Ok(())
}

pub fn read_batch(
    mut input: impl Read,
    schema: &SchemaRef,
) -> Result<Option<(usize, Vec<ArrayRef>)>> {
    // read number of columns and rows
    let num_rows = match read_len(&mut input) {
        Ok(n) => n,
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    };

    // read columns
    let mut transpose_opt =
        TransposeOpt::with_transpose(num_rows, schema.fields().iter().map(|f| f.data_type()));
    let cols = schema
        .fields()
        .into_iter()
        .map(|field| read_array(&mut input, &field.data_type(), num_rows, &mut transpose_opt))
        .collect::<Result<_>>()?;
    Ok(Some((num_rows, cols)))
}

pub fn write_array<W: Write>(
    array: &dyn Array,
    output: &mut W,
    transpose_opt: &mut TransposeOpt,
) -> Result<()> {
    macro_rules! write_primitive {
        ($ty:ident) => {{
            write_primitive_array(
                as_primitive_array::<paste::paste! {[<$ty Type>]}>(array),
                output,
                transpose_opt,
            )?
        }};
    }
    match array.data_type() {
        DataType::Null => {}
        DataType::Boolean => write_boolean_array(as_boolean_array(array), output)?,
        DataType::Int8 => write_primitive!(Int8),
        DataType::Int16 => write_primitive!(Int16),
        DataType::Int32 => write_primitive!(Int32),
        DataType::Int64 => write_primitive!(Int64),
        DataType::UInt8 => write_primitive!(UInt8),
        DataType::UInt16 => write_primitive!(UInt16),
        DataType::UInt32 => write_primitive!(UInt32),
        DataType::UInt64 => write_primitive!(UInt64),
        DataType::Float32 => write_primitive!(Float32),
        DataType::Float64 => write_primitive!(Float64),
        DataType::Decimal128(..) => write_primitive!(Decimal128),
        DataType::Utf8 => write_bytes_array(as_string_array(array), output, transpose_opt)?,
        DataType::Binary => {
            write_bytes_array(as_generic_binary_array::<i32>(array), output, transpose_opt)?
        }
        DataType::Date32 => write_primitive!(Date32),
        DataType::Date64 => write_primitive!(Date64),
        DataType::Timestamp(TimeUnit::Second, _) => write_primitive!(TimestampSecond),
        DataType::Timestamp(TimeUnit::Millisecond, _) => write_primitive!(TimestampMillisecond),
        DataType::Timestamp(TimeUnit::Microsecond, _) => write_primitive!(TimestampMicrosecond),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => write_primitive!(TimestampNanosecond),
        DataType::List(_field) => write_list_array(as_list_array(array), output, transpose_opt)?,
        DataType::Map(..) => write_map_array(as_map_array(array), output, transpose_opt)?,
        DataType::Struct(_) => write_struct_array(as_struct_array(array), output, transpose_opt)?,
        other => df_unimplemented_err!("unsupported data type: {other}")?,
    }
    Ok(())
}

pub fn read_array<R: Read>(
    input: &mut R,
    data_type: &DataType,
    num_rows: usize,
    transpose_opt: &mut TransposeOpt,
) -> Result<ArrayRef> {
    macro_rules! read_primitive {
        ($ty:ident) => {{ read_primitive_array::<_, paste::paste! {[<$ty Type>]}>(num_rows, input, transpose_opt)? }};
    }
    Ok(match data_type {
        DataType::Null => Arc::new(NullArray::new(num_rows)),
        DataType::Boolean => read_boolean_array(num_rows, input)?,
        DataType::Int8 => read_primitive!(Int8),
        DataType::Int16 => read_primitive!(Int16),
        DataType::Int32 => read_primitive!(Int32),
        DataType::Int64 => read_primitive!(Int64),
        DataType::UInt8 => read_primitive!(UInt8),
        DataType::UInt16 => read_primitive!(UInt16),
        DataType::UInt32 => read_primitive!(UInt32),
        DataType::UInt64 => read_primitive!(UInt64),
        DataType::Float32 => read_primitive!(Float32),
        DataType::Float64 => read_primitive!(Float64),
        DataType::Decimal128(prec, scale) => Arc::new(
            as_primitive_array::<Decimal128Type>(&read_primitive!(Decimal128))
                .clone()
                .with_precision_and_scale(*prec, *scale)?,
        ),
        DataType::Date32 => read_primitive!(Date32),
        DataType::Date64 => read_primitive!(Date64),
        DataType::Timestamp(TimeUnit::Second, _) => read_primitive!(TimestampSecond),
        DataType::Timestamp(TimeUnit::Millisecond, _) => read_primitive!(TimestampMillisecond),
        DataType::Timestamp(TimeUnit::Microsecond, _) => read_primitive!(TimestampMicrosecond),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => read_primitive!(TimestampNanosecond),
        DataType::Utf8 => read_bytes_array(num_rows, input, DataType::Utf8, transpose_opt)?,
        DataType::Binary => read_bytes_array(num_rows, input, DataType::Binary, transpose_opt)?,
        DataType::List(list_field) => read_list_array(num_rows, input, list_field, transpose_opt)?,
        DataType::Map(map_field, is_sorted) => {
            read_map_array(num_rows, input, map_field, *is_sorted, transpose_opt)?
        }
        DataType::Struct(fields) => read_struct_array(num_rows, input, fields, transpose_opt)?,
        other => df_unimplemented_err!("unsupported data type: {other}")?,
    })
}

fn write_bits_buffer<W: Write>(
    buffer: &Buffer,
    bits_offset: usize,
    bits_len: usize,
    output: &mut W,
) -> Result<()> {
    let mut out_buffer = vec![0u8; (bits_len + 7) / 8];
    let in_ptr = buffer.as_ptr();
    let out_ptr = out_buffer.as_mut_ptr();

    for i in 0..bits_len {
        unsafe {
            if arrow::util::bit_util::get_bit_raw(in_ptr, bits_offset + i) {
                arrow::util::bit_util::set_bit_raw(out_ptr, i);
            }
        }
    }
    output.write_all(&out_buffer)?;
    Ok(())
}

fn read_bits_buffer<R: Read>(input: &mut R, bits_len: usize) -> Result<Buffer> {
    let buf = read_bytes_slice(input, (bits_len + 7) / 8)?;
    Ok(Buffer::from_vec(buf.into()))
}

fn write_offsets<W: Write>(
    output: &mut W,
    offsets: &[i32],
    transpose_opt: &mut TransposeOpt,
) -> Result<()> {
    let lens = offsets
        .iter()
        .zip(&offsets[1..])
        .map(|(beg, end)| end - beg)
        .collect::<Vec<_>>();

    if let TransposeOpt::Transpose(buffer) = transpose_opt {
        transpose::transpose(
            lens.as_raw_bytes(),
            buffer.as_raw_bytes_mut()[..4 * lens.len()].as_mut(),
            4,
            lens.len(),
        );
        output.write_all(buffer[..4 * lens.len()].as_ref())?;
    } else {
        output.write_all(lens.as_raw_bytes())?;
    }
    Ok(())
}

fn read_offsets<R: Read>(
    input: &mut R,
    num_rows: usize,
    transpose_opt: &mut TransposeOpt,
) -> Result<Vec<i32>> {
    let mut lens: Vec<i32> = Vec::uninitialized_init(num_rows + 1);

    if let TransposeOpt::Transpose(buffer) = transpose_opt {
        input.read_exact(buffer[..4 * num_rows].as_mut())?;
        transpose::transpose(
            buffer[..4 * num_rows].as_ref(),
            lens[..num_rows].as_raw_bytes_mut(),
            num_rows,
            4,
        );
    } else {
        input.read_exact(lens[..num_rows].as_raw_bytes_mut())?;
    }
    lens[num_rows] = 0;

    let mut offsets = lens;
    let mut cur_offset = 0;
    for offset in &mut offsets {
        cur_offset += *offset;
        *offset = cur_offset - *offset;
    }
    Ok(offsets)
}

fn write_primitive_array<W: Write, PT: ArrowPrimitiveType>(
    array: &PrimitiveArray<PT>,
    output: &mut W,
    transpose_opt: &mut TransposeOpt,
) -> Result<()> {
    let offset = array.offset();
    let len = array.len();
    let array_data = array.to_data();
    if let Some(null_buffer) = array_data.nulls() {
        write_len(1, output)?;
        write_bits_buffer(
            null_buffer.buffer(),
            null_buffer.offset(),
            null_buffer.len(),
            output,
        )?;
    } else {
        write_len(0, output)?;
    }

    let byte_width = PT::Native::get_byte_width();
    if let TransposeOpt::Transpose(buffer) = transpose_opt
        && byte_width > 1
    {
        transpose::transpose(
            array_data.buffer::<PT::Native>(0)[offset..][..len].as_raw_bytes(),
            buffer[..byte_width * array.len()].as_raw_bytes_mut(),
            byte_width,
            array.len(),
        );
        output.write_all(buffer[..byte_width * array.len()].as_ref())?;
    } else {
        output.write_all(array_data.buffer::<PT::Native>(0)[offset..][..len].as_raw_bytes())?;
    }
    Ok(())
}

fn read_primitive_array<R: Read, PT: ArrowPrimitiveType>(
    num_rows: usize,
    input: &mut R,
    transpose_opt: &mut TransposeOpt,
) -> Result<ArrayRef> {
    let has_null_buffer = read_len(input)? == 1;
    let null_buffer: Option<Buffer> = if has_null_buffer {
        Some(read_bits_buffer(input, num_rows)?)
    } else {
        None
    };

    let mut values: Vec<PT::Native> = Vec::uninitialized_init(num_rows);
    let byte_width = PT::Native::get_byte_width();
    if let TransposeOpt::Transpose(buffer) = transpose_opt
        && byte_width > 1
    {
        input.read_exact(buffer[..byte_width * num_rows].as_mut())?;
        transpose::transpose(
            buffer[..byte_width * num_rows].as_ref(),
            values.as_raw_bytes_mut(),
            num_rows,
            byte_width,
        );
    } else {
        input.read_exact(values.as_raw_bytes_mut())?;
    }

    let array_data = ArrayData::try_new(
        PT::DATA_TYPE,
        num_rows,
        null_buffer,
        0,
        vec![Buffer::from_vec(values)],
        vec![],
    )?;
    Ok(make_array(array_data))
}

fn write_list_array<W: Write>(
    array: &ListArray,
    output: &mut W,
    transpose_opt: &mut TransposeOpt,
) -> Result<()> {
    if let Some(null_buffer) = array.to_data().nulls() {
        write_len(1, output)?;
        write_bits_buffer(
            null_buffer.buffer(),
            null_buffer.offset(),
            null_buffer.len(),
            output,
        )?;
    } else {
        write_len(0, output)?;
    }

    let value_offsets = array.value_offsets();
    write_offsets(output, value_offsets, transpose_opt)?;

    let values = array.values().slice(
        value_offsets[0] as usize,
        value_offsets[array.len()] as usize - value_offsets[0] as usize,
    );
    write_array(
        &values,
        output,
        &mut TransposeOpt::with_transpose(values.len(), std::iter::once(&array.value_type())),
    )?;
    Ok(())
}

fn read_list_array<R: Read>(
    num_rows: usize,
    input: &mut R,
    list_field: &FieldRef,
    transpose_opt: &mut TransposeOpt,
) -> Result<ArrayRef> {
    let has_null_buffer = read_len(input)? == 1;
    let null_buffer: Option<Buffer> = if has_null_buffer {
        Some(read_bits_buffer(input, num_rows)?)
    } else {
        None
    };

    let offsets = read_offsets(input, num_rows, transpose_opt)?;
    let values_len = offsets.last().cloned().unwrap() as usize;
    let offsets_buffer: Buffer = Buffer::from_vec(offsets);
    let values = read_array(
        input,
        list_field.data_type(),
        values_len,
        &mut TransposeOpt::with_transpose(values_len, std::iter::once(list_field.data_type())),
    )?;

    let array_data = ArrayData::try_new(
        DataType::List(list_field.clone()),
        num_rows,
        null_buffer,
        0,
        vec![offsets_buffer],
        vec![values.into_data()],
    )?;
    Ok(make_array(array_data))
}

fn write_map_array<W: Write>(
    array: &MapArray,
    output: &mut W,
    transpose_opt: &mut TransposeOpt,
) -> Result<()> {
    let array_data = array.to_data();
    if let Some(null_buffer) = array_data.nulls() {
        write_len(1, output)?;
        write_bits_buffer(
            null_buffer.buffer(),
            null_buffer.offset(),
            null_buffer.len(),
            output,
        )?;
    } else {
        write_len(0, output)?;
    }

    let value_offsets = array.value_offsets();
    write_offsets(output, value_offsets, transpose_opt)?;

    let first_offset = value_offsets.first().cloned().unwrap() as usize;
    let entries_len = value_offsets.last().cloned().unwrap() as usize - first_offset;
    let keys = array.keys().slice(first_offset, entries_len);
    let values = array.values().slice(first_offset, entries_len);

    let mut child_transpose_opt =
        TransposeOpt::with_transpose(entries_len, std::iter::once(array.data_type()));
    write_array(&keys, output, &mut child_transpose_opt)?;
    write_array(&values, output, &mut child_transpose_opt)?;
    Ok(())
}

fn read_map_array<R: Read>(
    num_rows: usize,
    input: &mut R,
    map_field: &FieldRef,
    is_sorted: bool,
    transpose_opt: &mut TransposeOpt,
) -> Result<ArrayRef> {
    let has_null_buffer = read_len(input)? == 1;
    let null_buffer: Option<Buffer> = if has_null_buffer {
        Some(read_bits_buffer(input, num_rows)?)
    } else {
        None
    };

    let offsets = read_offsets(input, num_rows, transpose_opt)?;
    let entries_len = offsets.last().cloned().unwrap() as usize;
    let offsets_buffer = Buffer::from_vec(offsets);

    // build inner struct
    let mut child_transpose_opt =
        TransposeOpt::with_transpose(entries_len, std::iter::once(map_field.data_type()));
    let kv_fields = match map_field.data_type() {
        DataType::Struct(fields) => fields,
        _ => unreachable!(),
    };
    let key_values: Vec<ArrayRef> = kv_fields
        .iter()
        .map(|f| read_array(input, f.data_type(), entries_len, &mut child_transpose_opt))
        .collect::<Result<_>>()?;

    let struct_array_data = ArrayData::try_new(
        DataType::Struct(kv_fields.clone()),
        entries_len,
        None,
        0,
        vec![],
        key_values.into_iter().map(|c| c.into_data()).collect(),
    )?;

    // build map
    let array_data = ArrayData::try_new(
        DataType::Map(map_field.clone(), is_sorted),
        num_rows,
        null_buffer,
        0,
        vec![offsets_buffer],
        vec![struct_array_data],
    )?;
    Ok(make_array(array_data))
}

fn write_struct_array<W: Write>(
    array: &StructArray,
    output: &mut W,
    transpose_opt: &mut TransposeOpt,
) -> Result<()> {
    let array_data = array.to_data();
    if let Some(null_buffer) = array_data.nulls() {
        write_len(1, output)?;
        write_bits_buffer(
            null_buffer.buffer(),
            null_buffer.offset(),
            null_buffer.len(),
            output,
        )?;
    } else {
        write_len(0, output)?;
    }
    for column in array.columns() {
        write_array(&column, output, transpose_opt)?;
    }
    Ok(())
}

fn read_struct_array<R: Read>(
    num_rows: usize,
    input: &mut R,
    fields: &Fields,
    transpose_opt: &mut TransposeOpt,
) -> Result<ArrayRef> {
    let has_null_buffer = read_len(input)? == 1;
    let null_buffer: Option<Buffer> = if has_null_buffer {
        Some(read_bits_buffer(input, num_rows)?)
    } else {
        None
    };

    let child_arrays: Vec<ArrayRef> = fields
        .iter()
        .map(|field| read_array(input, field.data_type(), num_rows, transpose_opt))
        .collect::<Result<_>>()?;

    let array_data = ArrayData::try_new(
        DataType::Struct(fields.clone()),
        num_rows,
        null_buffer,
        0,
        vec![],
        child_arrays.into_iter().map(|c| c.into_data()).collect(),
    )?;
    Ok(make_array(array_data))
}

fn write_boolean_array<W: Write>(array: &BooleanArray, output: &mut W) -> Result<()> {
    let array_data = array.to_data();
    if let Some(null_buffer) = array_data.nulls() {
        write_len(1, output)?;
        write_bits_buffer(
            null_buffer.buffer(),
            null_buffer.offset(),
            null_buffer.len(),
            output,
        )?;
    } else {
        write_len(0, output)?;
    }
    write_bits_buffer(
        &array_data.buffers()[0],
        array.offset(),
        array.len(),
        output,
    )?;
    Ok(())
}

fn read_boolean_array<R: Read>(num_rows: usize, input: &mut R) -> Result<ArrayRef> {
    let has_null_buffer = read_len(input)? == 1;
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
    transpose_opt: &mut TransposeOpt,
) -> Result<()> {
    if let Some(null_buffer) = array.to_data().nulls() {
        write_len(1, output)?;
        write_bits_buffer(
            null_buffer.buffer(),
            null_buffer.offset(),
            null_buffer.len(),
            output,
        )?;
    } else {
        write_len(0, output)?;
    }

    let value_offsets = array.value_offsets();
    write_offsets(output, value_offsets, transpose_opt)?;

    let first_offset = value_offsets.first().cloned().unwrap() as usize;
    let last_offset = value_offsets.last().cloned().unwrap() as usize;
    output.write_all(&array.value_data()[first_offset..last_offset])?;
    Ok(())
}

fn read_bytes_array<R: Read>(
    num_rows: usize,
    input: &mut R,
    data_type: DataType,
    transpose_opt: &mut TransposeOpt,
) -> Result<ArrayRef> {
    let has_null_buffer = read_len(input)? == 1;
    let null_buffer: Option<Buffer> = if has_null_buffer {
        Some(read_bits_buffer(input, num_rows)?)
    } else {
        None
    };

    let offsets = read_offsets(input, num_rows, transpose_opt)?;
    let values_len = offsets.last().cloned().unwrap() as usize;
    let offsets_buffer = Buffer::from_vec(offsets);

    let data_buffer = Buffer::from_vec(read_bytes_slice(input, values_len)?.into());
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
    use std::{io::Cursor, sync::Arc};

    use arrow::{array::*, datatypes::*, record_batch::RecordBatch};
    use datafusion::assert_batches_eq;

    use crate::io::{
        batch_serde::{read_batch, write_batch},
        recover_named_batch,
    };

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
            ("str", array1, true),
            ("u64", array2, true),
            ("bool", array3, true),
        ])
        .unwrap();

        // test read after write
        let mut buf = vec![];
        write_batch(batch.num_rows(), batch.columns(), &mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        let (decoded_num_rows, decoded_cols) =
            read_batch(&mut cursor, &batch.schema()).unwrap().unwrap();
        assert_eq!(
            recover_named_batch(decoded_num_rows, &decoded_cols, batch.schema()).unwrap(),
            batch
        );

        // test read after write sliced
        let sliced = batch.slice(1, 2);
        let mut buf = vec![];
        write_batch(sliced.num_rows(), sliced.columns(), &mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        let (decoded_num_rows, decoded_cols) =
            read_batch(&mut cursor, &batch.schema()).unwrap().unwrap();
        assert_eq!(
            recover_named_batch(decoded_num_rows, &decoded_cols, batch.schema()).unwrap(),
            sliced
        );
    }

    #[test]
    fn test_write_and_read_batch_for_list() {
        let data = vec![
            Some(vec![Some(0), Some(1), Some(2)]),
            None,
            Some(vec![Some(3), None, Some(5)]),
            Some(vec![Some(6), Some(7)]),
        ];
        let list_array: ArrayRef =
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(data));
        let batch = RecordBatch::try_from_iter_with_nullable(vec![
            ("list1", list_array.clone(), true),
            ("list2", list_array.clone(), true),
        ])
        .unwrap();

        assert_batches_eq!(
            vec![
                "+-----------+-----------+",
                "| list1     | list2     |",
                "+-----------+-----------+",
                "| [0, 1, 2] | [0, 1, 2] |",
                "|           |           |",
                "| [3, , 5]  | [3, , 5]  |",
                "| [6, 7]    | [6, 7]    |",
                "+-----------+-----------+",
            ],
            &[batch.clone()]
        );

        // test read after write
        let mut buf = vec![];
        write_batch(batch.num_rows(), batch.columns(), &mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        let (decoded_num_rows, decoded_cols) =
            read_batch(&mut cursor, &batch.schema()).unwrap().unwrap();
        assert_batches_eq!(
            vec![
                "+-----------+-----------+",
                "| list1     | list2     |",
                "+-----------+-----------+",
                "| [0, 1, 2] | [0, 1, 2] |",
                "|           |           |",
                "| [3, , 5]  | [3, , 5]  |",
                "| [6, 7]    | [6, 7]    |",
                "+-----------+-----------+",
            ],
            &[recover_named_batch(decoded_num_rows, &decoded_cols, batch.schema()).unwrap()]
        );

        // test read after write sliced
        let sliced = batch.slice(1, 2);
        let mut buf = vec![];
        write_batch(sliced.num_rows(), sliced.columns(), &mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        let (decoded_num_rows, decoded_cols) =
            read_batch(&mut cursor, &batch.schema()).unwrap().unwrap();
        assert_batches_eq!(
            vec![
                "+----------+----------+",
                "| list1    | list2    |",
                "+----------+----------+",
                "|          |          |",
                "| [3, , 5] | [3, , 5] |",
                "+----------+----------+",
            ],
            &[recover_named_batch(decoded_num_rows, &decoded_cols, sliced.schema()).unwrap()]
        );
    }

    #[test]
    fn test_write_and_read_batch_for_map() {
        let map_array: ArrayRef = Arc::new(
            MapArray::new_from_strings(
                ["00", "11", "22", "33", "44", "55", "66", "77"].into_iter(),
                &StringArray::from(vec![
                    Some("aa"),
                    None,
                    Some("cc"),
                    Some("dd"),
                    Some("ee"),
                    Some("ff"),
                    Some("gg"),
                    Some("hh"),
                ]),
                &[0, 3, 6, 8], // [00,11,22], [33,44,55], [66,77]
            )
            .unwrap(),
        );

        let batch = RecordBatch::try_from_iter_with_nullable(vec![
            ("map1", map_array.clone(), true),
            ("map2", map_array.clone(), true),
        ])
        .unwrap();

        // test read after write
        let mut buf = vec![];
        write_batch(batch.num_rows(), batch.columns(), &mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        let (decoded_num_rows, decoded_cols) =
            read_batch(&mut cursor, &batch.schema()).unwrap().unwrap();
        assert_eq!(
            recover_named_batch(decoded_num_rows, &decoded_cols, batch.schema()).unwrap(),
            batch
        );

        // test read after write sliced
        let sliced = batch.slice(1, 2);
        let mut buf = vec![];
        write_batch(sliced.num_rows(), sliced.columns(), &mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        let (decoded_num_rows, decoded_cols) =
            read_batch(&mut cursor, &batch.schema()).unwrap().unwrap();
        assert_eq!(
            recover_named_batch(decoded_num_rows, &decoded_cols, sliced.schema()).unwrap(),
            sliced
        );
    }

    #[test]
    fn test_write_and_read_batch_for_struct() {
        let c1: ArrayRef = Arc::new(BooleanArray::from(vec![false, false, true, true]));
        let c2: ArrayRef = Arc::new(Int32Array::from(vec![42, 28, 19, 31]));
        let c3: ArrayRef = Arc::new(BooleanArray::from(vec![None, None, None, Some(true)]));
        let c4: ArrayRef = Arc::new(Int32Array::from(vec![None, None, None, Some(31)]));
        let struct_array: ArrayRef = Arc::new(
            StructArray::try_from(vec![("c1", c1), ("c2", c2), ("c3", c3), ("c4", c4)]).unwrap(),
        );

        let batch = RecordBatch::try_from_iter_with_nullable(vec![
            ("struct1", struct_array.clone(), true),
            ("struct2", struct_array.clone(), true),
        ])
        .unwrap();

        // test read after write
        let mut buf = vec![];
        write_batch(batch.num_rows(), batch.columns(), &mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        let (decoded_num_rows, decoded_cols) =
            read_batch(&mut cursor, &batch.schema()).unwrap().unwrap();
        assert_eq!(
            recover_named_batch(decoded_num_rows, &decoded_cols, batch.schema()).unwrap(),
            batch
        );

        // test read after write sliced
        let sliced = batch.slice(1, 2);
        let mut buf = vec![];
        write_batch(sliced.num_rows(), sliced.columns(), &mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        let (decoded_num_rows, decoded_cols) =
            read_batch(&mut cursor, &batch.schema()).unwrap().unwrap();
        assert_eq!(
            recover_named_batch(decoded_num_rows, &decoded_cols, batch.schema()).unwrap(),
            sliced
        );
    }
}
