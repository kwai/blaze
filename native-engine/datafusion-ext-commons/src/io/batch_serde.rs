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

use std::{
    io::{BufReader, BufWriter, Read, Write},
    mem::size_of,
    sync::Arc,
};

use arrow::{
    array::*,
    buffer::{Buffer, MutableBuffer},
    datatypes::*,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use bitvec::prelude::BitVec;
use datafusion::common::Result;
use unchecked_index::unchecked_index;

use crate::{
    df_execution_err, df_unimplemented_err,
    io::{read_bytes_slice, read_len, write_len},
};

pub fn write_batch<W: Write>(batch: &RecordBatch, output: &mut W) -> Result<()> {
    let mut output = BufWriter::new(output);
    let schema = batch.schema();

    // write number of columns and rows
    write_len(batch.num_columns(), &mut output)?;
    write_len(batch.num_rows(), &mut output)?;

    // write column data types
    for field in schema.fields() {
        write_data_type(field.data_type(), &mut output).map_err(|err| {
            err.context(format!(
                "batch_serde error writing data type: {}",
                field.data_type()
            ))
        })?;
    }

    // write column nullables
    let mut nullables = BitVec::<u8>::with_capacity(batch.num_columns());
    for field in schema.fields() {
        nullables.push(field.is_nullable());
    }
    output.write_all(&nullables.into_vec())?;

    // write columns
    for column in batch.columns() {
        write_array(column, &mut output).map_err(|err| {
            err.context(format!(
                "batch_serde error writing column (data_type={})",
                column.data_type()
            ))
        })?;
    }
    Ok(())
}

pub fn read_batch<R: Read>(input: &mut R) -> Result<RecordBatch> {
    let mut input: Box<dyn Read> = Box::new(BufReader::new(input));

    // read number of columns and rows
    let num_columns = read_len(&mut input)?;
    let num_rows = read_len(&mut input)?;

    // read column data types
    let mut data_types = Vec::with_capacity(num_columns);
    for _ in 0..num_columns {
        data_types.push(
            read_data_type(&mut input)
                .map_err(|err| err.context("batch_serde error reading data type"))?,
        );
    }

    // read nullables
    let nullables_bytes = read_bytes_slice(&mut input, (num_columns + 7) / 8)?;
    let nullables = BitVec::<u8>::from_vec(nullables_bytes.into());

    // create schema
    let schema = Arc::new(Schema::new(
        data_types
            .iter()
            .enumerate()
            .map(|(i, data_type)| Field::new("", data_type.clone(), nullables[i]))
            .collect::<Fields>(),
    ));

    // read columns
    let columns = (0..num_columns)
        .map(|i| {
            read_array(&mut input, &data_types[i], num_rows).map_err(|err| {
                err.context(format!(
                    "batch_serde error reading column (data_type={}, num_rows={})",
                    data_types[i], num_rows,
                ))
            })
        })
        .collect::<Result<_>>()?;

    // create batch
    Ok(RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(num_rows)),
    )?)
}

pub fn write_array<W: Write>(array: &dyn Array, output: &mut W) -> Result<()> {
    macro_rules! write_primitive {
        ($ty:ident) => {{
            write_primitive_array(
                as_primitive_array::<paste::paste! {[<$ty Type>]}>(array),
                output,
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
        DataType::Utf8 => write_bytes_array(as_string_array(array), output)?,
        DataType::Binary => write_bytes_array(as_generic_binary_array::<i32>(array), output)?,
        DataType::Date32 => write_primitive!(Date32),
        DataType::Date64 => write_primitive!(Date64),
        DataType::Timestamp(TimeUnit::Second, _) => write_primitive!(TimestampSecond),
        DataType::Timestamp(TimeUnit::Millisecond, _) => write_primitive!(TimestampMillisecond),
        DataType::Timestamp(TimeUnit::Microsecond, _) => write_primitive!(TimestampMicrosecond),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => write_primitive!(TimestampNanosecond),
        DataType::List(_field) => write_list_array(as_list_array(array), output)?,
        DataType::Map(..) => write_map_array(as_map_array(array), output)?,
        DataType::Struct(_) => write_struct_array(as_struct_array(array), output)?,
        other => df_unimplemented_err!("unsupported data type: {other}")?,
    }
    Ok(())
}

pub fn read_array<R: Read>(
    input: &mut R,
    data_type: &DataType,
    num_rows: usize,
) -> Result<ArrayRef> {
    macro_rules! read_primitive {
        ($ty:ident) => {{
            read_primitive_array::<_, paste::paste! {[<$ty Type>]}>(num_rows, input)?
        }};
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
        DataType::Utf8 => read_bytes_array(num_rows, input, DataType::Utf8)?,
        DataType::Binary => read_bytes_array(num_rows, input, DataType::Binary)?,
        DataType::List(list_field) => read_list_array(num_rows, input, list_field)?,
        DataType::Map(map_field, is_sorted) => {
            read_map_array(num_rows, input, map_field, *is_sorted)?
        }
        DataType::Struct(fields) => read_struct_array(num_rows, input, fields)?,
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
    Ok(Buffer::from(buf))
}

fn nameless_field(field: &Field) -> Field {
    Field::new(
        "",
        nameless_data_type(field.data_type()),
        field.is_nullable(),
    )
}

fn nameless_data_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::List(field) => DataType::List(Arc::new(nameless_field(field))),
        DataType::Map(field, sorted) => DataType::Map(Arc::new(nameless_field(field)), *sorted),
        DataType::Struct(fields) => {
            DataType::Struct(fields.iter().map(|field| nameless_field(field)).collect())
        }
        others => others.clone(),
    }
}

pub fn write_data_type<W: Write>(data_type: &DataType, output: &mut W) -> Result<()> {
    let buf = postcard::to_allocvec(&nameless_data_type(data_type))
        .or_else(|err| df_execution_err!("serialize data type error: {err}"))?;
    write_len(buf.len(), output)?;
    output.write_all(&buf)?;
    Ok(())
}

pub fn read_data_type<R: Read>(input: &mut R) -> Result<DataType> {
    let buf_len = read_len(input)?;
    let buf = read_bytes_slice(input, buf_len)?;
    let data_type = postcard::from_bytes(&buf)
        .or_else(|err| df_execution_err!("deserialize data type error: {err}"))?;
    Ok(data_type)
}

fn write_primitive_array<W: Write, PT: ArrowPrimitiveType>(
    array: &PrimitiveArray<PT>,
    output: &mut W,
) -> Result<()> {
    let _item_size = PT::get_byte_width();
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
    write_primitive_raw_array(&array_data.buffer::<PT::Native>(0)[offset..][..len], output)?;
    Ok(())
}

fn read_primitive_array<R: Read, PT: ArrowPrimitiveType>(
    num_rows: usize,
    input: &mut R,
) -> Result<ArrayRef> {
    let has_null_buffer = read_len(input)? == 1;
    let null_buffer: Option<Buffer> = if has_null_buffer {
        Some(read_bits_buffer(input, num_rows)?)
    } else {
        None
    };

    let data_buffers: Vec<Buffer> = {
        let data_buffer =
            Buffer::from_vec(read_primitive_raw_array::<PT::Native, R>(input, num_rows)?);
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

fn write_list_array<W: Write>(array: &ListArray, output: &mut W) -> Result<()> {
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
    for (beg, end) in value_offsets.iter().zip(&value_offsets[1..]) {
        let len = end - beg;
        write_len(len as usize, output)?;
    }
    let values = array.values().slice(
        value_offsets[0] as usize,
        value_offsets[array.len()] as usize - value_offsets[0] as usize,
    );
    write_array(&values, output)?;
    Ok(())
}

fn read_list_array<R: Read>(
    num_rows: usize,
    input: &mut R,
    list_field: &FieldRef,
) -> Result<ArrayRef> {
    let has_null_buffer = read_len(input)? == 1;
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
    let values_len = cur_offset;
    let values = read_array(input, list_field.data_type(), values_len)?;

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

fn write_map_array<W: Write>(array: &MapArray, output: &mut W) -> Result<()> {
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

    let first_offset = array.value_offsets().first().cloned().unwrap_or_default();
    let mut cur_offset = first_offset;
    for &offset in array.value_offsets().iter().skip(1) {
        let len = offset - cur_offset;
        write_len(len as usize, output)?;
        cur_offset = offset;
    }
    let entries_len = cur_offset - first_offset;
    let keys = array
        .keys()
        .slice(first_offset as usize, entries_len as usize);
    let values = array
        .values()
        .slice(first_offset as usize, entries_len as usize);
    write_array(&keys, output)?;
    write_array(&values, output)?;
    Ok(())
}

fn read_map_array<R: Read>(
    num_rows: usize,
    input: &mut R,
    map_field: &FieldRef,
    is_sorted: bool,
) -> Result<ArrayRef> {
    let has_null_buffer = read_len(input)? == 1;
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
    let values_len = cur_offset;

    // build inner struct
    let kv_fields = match map_field.data_type() {
        DataType::Struct(fields) => fields,
        _ => unreachable!(),
    };
    let key_values: Vec<ArrayRef> = kv_fields
        .iter()
        .map(|f| read_array(input, f.data_type(), values_len))
        .collect::<Result<_>>()?;

    let struct_array_data = ArrayData::try_new(
        DataType::Struct(kv_fields.clone()),
        values_len,
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

fn write_struct_array<W: Write>(array: &StructArray, output: &mut W) -> Result<()> {
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
        write_array(&column, output)?;
    }
    Ok(())
}

fn read_struct_array<R: Read>(num_rows: usize, input: &mut R, fields: &Fields) -> Result<ArrayRef> {
    let has_null_buffer = read_len(input)? == 1;
    let null_buffer: Option<Buffer> = if has_null_buffer {
        Some(read_bits_buffer(input, num_rows)?)
    } else {
        None
    };

    let child_arrays: Vec<ArrayRef> = fields
        .iter()
        .map(|field| read_array(input, field.data_type(), num_rows))
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

    // transform offsets to lengths for better compression
    let first_offset = array.value_offsets().first().cloned().unwrap_or_default();
    let mut cur_offset = first_offset;
    let mut lens = vec![];
    for &offset in array.value_offsets().iter().skip(1) {
        let len = offset - cur_offset;
        cur_offset = offset;
        lens.push(len);
    }
    write_primitive_raw_array(&lens, output)?;
    output.write_all(&array.value_data()[first_offset as usize..cur_offset as usize])?;
    Ok(())
}

fn read_bytes_array<R: Read>(
    num_rows: usize,
    input: &mut R,
    data_type: DataType,
) -> Result<ArrayRef> {
    let has_null_buffer = read_len(input)? == 1;
    let null_buffer: Option<Buffer> = if has_null_buffer {
        Some(read_bits_buffer(input, num_rows)?)
    } else {
        None
    };

    let lens = read_primitive_raw_array::<i32, R>(input, num_rows)?;
    let mut cur_offset = 0;
    let mut offsets_buffer = MutableBuffer::new((num_rows + 1) * 4);
    offsets_buffer.push(0u32);
    for len in lens {
        let offset = cur_offset + len;
        cur_offset = offset;
        offsets_buffer.push(offset as u32);
    }
    let offsets_buffer: Buffer = offsets_buffer.into();

    let data_len = cur_offset as usize;
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

fn write_primitive_raw_array<T: Default + Copy + Sized, W: Write>(
    array: &[T],
    output: &mut W,
) -> Result<()> {
    let num_item_bytes = size_of::<T>();
    let num_items = array.len();
    let raw = unsafe {
        // safety: transmute to raw bytes is safe for primitive arrays
        std::slice::from_raw_parts(array.as_ptr() as *const u8, num_item_bytes * num_items)
    };
    let mut raw_out = vec![0u8; raw.len()];

    // write byte-transposed data for better compression ratio
    // for example uint32 array [1,2,3,4,5,6] is stored as raw bytes:
    //  [0,0,0,1,0,0,0,2,0,0,0,3,0,0,0,4,0,0,0,5,0,0,0,6]
    // after transpose it becomes:
    //  [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,2,3,4,5,6]
    // which should have better compression ratio.
    transpose_raw_bytes(&raw, &mut raw_out, num_items, num_item_bytes);
    output.write_all(&mut raw_out)?;
    Ok(())
}

fn read_primitive_raw_array<T: Default + Copy + Sized, R: Read>(
    input: &mut R,
    num_items: usize,
) -> Result<Vec<T>> {
    let mut out = vec![T::default(); num_items];
    let num_item_bytes = size_of::<T>();
    let raw = read_bytes_slice(input, num_item_bytes * num_items)?;
    let mut raw_out =
        unsafe { std::slice::from_raw_parts_mut(out.as_mut_ptr() as *mut u8, raw.len()) };
    transpose_raw_bytes(&raw, &mut raw_out, num_item_bytes, num_items);
    Ok(out)
}

fn transpose_raw_bytes(src: &[u8], dest: &mut [u8], num_items: usize, num_item_bytes: usize) {
    unsafe {
        // safety: ignore boundary checking
        let mut buf = unchecked_index(dest);
        let src = unchecked_index(src);

        for byte_idx in 0..num_item_bytes {
            for i in 0..num_items {
                buf[num_items * byte_idx + i] = src[num_item_bytes * i + byte_idx];
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::{io::Cursor, sync::Arc};

    use arrow::{array::*, datatypes::*, record_batch::RecordBatch};
    use datafusion::assert_batches_eq;

    use crate::io::{
        batch_serde::{
            read_batch, read_primitive_raw_array, write_batch, write_primitive_raw_array,
        },
        name_batch,
    };

    #[test]
    fn test_primitive_raw_bytes() {
        let src = vec![1, 2, 3, 4, 5, 6];
        let mut buf = vec![];
        write_primitive_raw_array(&src, &mut buf).unwrap();
        let out = read_primitive_raw_array::<i32, _>(&mut Cursor::new(&buf), src.len()).unwrap();
        assert_eq!(out, src)
    }

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
        write_batch(&batch, &mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        let decoded_batch = read_batch(&mut cursor).unwrap();
        assert_eq!(name_batch(decoded_batch, &batch.schema()).unwrap(), batch);

        // test read after write sliced
        let sliced = batch.slice(1, 2);
        let mut buf = vec![];
        write_batch(&sliced, &mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        let decoded_batch = read_batch(&mut cursor).unwrap();
        assert_eq!(name_batch(decoded_batch, &sliced.schema()).unwrap(), sliced);
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
        write_batch(&batch, &mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        let decoded_batch = read_batch(&mut cursor).unwrap();
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
            &[name_batch(decoded_batch, &batch.schema()).unwrap()]
        );

        // test read after write sliced
        let sliced = batch.slice(1, 2);
        let mut buf = vec![];
        write_batch(&sliced, &mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        let decoded_batch = read_batch(&mut cursor).unwrap();
        assert_batches_eq!(
            vec![
                "+----------+----------+",
                "| list1    | list2    |",
                "+----------+----------+",
                "|          |          |",
                "| [3, , 5] | [3, , 5] |",
                "+----------+----------+",
            ],
            &[name_batch(decoded_batch, &batch.schema()).unwrap()]
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
        write_batch(&batch, &mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        let decoded_batch = read_batch(&mut cursor).unwrap();
        assert_eq!(name_batch(decoded_batch, &batch.schema()).unwrap(), batch);

        // test read after write sliced
        let sliced = batch.slice(1, 2);
        let mut buf = vec![];
        write_batch(&sliced, &mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        let decoded_batch = read_batch(&mut cursor).unwrap();
        assert_eq!(name_batch(decoded_batch, &sliced.schema()).unwrap(), sliced);
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
        write_batch(&batch, &mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        let decoded_batch = read_batch(&mut cursor).unwrap();
        assert_eq!(name_batch(decoded_batch, &batch.schema()).unwrap(), batch);

        // test read after write sliced
        let sliced = batch.slice(1, 2);
        let mut buf = vec![];
        write_batch(&sliced, &mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        let decoded_batch = read_batch(&mut cursor).unwrap();
        assert_eq!(name_batch(decoded_batch, &sliced.schema()).unwrap(), sliced);
    }
}
