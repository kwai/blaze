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

use arrow::array::*;
use arrow::datatypes::DataType::Struct;
use arrow::datatypes::*;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use paste::paste;
use std::any::Any;
use std::sync::Arc;

pub fn new_array_builders(schema: &SchemaRef, batch_size: usize) -> Vec<Box<dyn ArrayBuilder>> {
    schema
        .fields()
        .iter()
        .map(|field| new_array_builder(field.data_type(), batch_size))
        .collect::<Vec<_>>()
}

pub fn make_batch(
    schema: SchemaRef,
    mut arrays: Vec<Box<dyn ArrayBuilder>>,
) -> ArrowResult<RecordBatch> {
    let columns = arrays.iter_mut().map(|array| array.finish()).collect();
    RecordBatch::try_new(schema, columns)
}

pub fn builder_extend(
    builder: &mut (impl ArrayBuilder + ?Sized),
    array: &ArrayRef,
    indices: &[usize],
    data_type: &DataType,
) {
    macro_rules! append_simple {
        ($arrowty:ident) => {{
            type B = paste::paste! {[< $arrowty Builder >]};
            type A = paste::paste! {[< $arrowty Array >]};
            let t = builder.as_any_mut().downcast_mut::<B>().unwrap();
            let f = array.as_any().downcast_ref::<A>().unwrap();
            for &i in indices {
                if f.is_valid(i) {
                    t.append_value(f.value(i));
                } else {
                    t.append_null();
                }
            }
        }};
    }
    macro_rules! append_decimal {
        ($builderty:ident, $arrowty:ident) => {{
            type B = paste::paste! {[< $builderty Builder >]};
            type A = paste::paste! {[< $arrowty Array >]};
            let t = builder.as_any_mut().downcast_mut::<B>().unwrap();
            let f = array.as_any().downcast_ref::<A>().unwrap();
            for &i in indices {
                if f.is_valid(i) {
                    let _ = t.append_value(f.value(i));
                } else {
                    t.append_null();
                }
            }
        }};
    }

    macro_rules! append_dict {
        ($key_type:expr, $value_type:expr) => {{
            append_dict!(@match_key: $key_type, $value_type)
        }};
        (@match_key: $key_type:expr, $value_type:expr) => {{
            match $key_type.as_ref() {
                DataType::Int8 => append_dict!(@match_value: Int8, $value_type),
                DataType::Int16 => append_dict!(@match_value: Int16, $value_type),
                DataType::Int32 => append_dict!(@match_value: Int32, $value_type),
                DataType::Int64 => append_dict!(@match_value: Int64, $value_type),
                DataType::UInt8 => append_dict!(@match_value: UInt8, $value_type),
                DataType::UInt16=> append_dict!(@match_value: UInt16, $value_type),
                DataType::UInt32 => append_dict!(@match_value: UInt32, $value_type),
                DataType::UInt64 => append_dict!(@match_value: UInt64, $value_type),
                _ => unimplemented!("dictionary key type not supported: {:?}", $value_type),
            }
        }};
        (@match_value: $keyarrowty:ident, $value_type:expr) => {{
            match $value_type.as_ref() {
                DataType::Int8 => append_dict!(@prim: $keyarrowty, Int8),
                DataType::Int16 => append_dict!(@prim: $keyarrowty, Int16),
                DataType::Int32 => append_dict!(@prim: $keyarrowty, Int32),
                DataType::Int64 => append_dict!(@prim: $keyarrowty, Int64),
                DataType::UInt8 => append_dict!(@prim: $keyarrowty, UInt8),
                DataType::UInt16 => append_dict!(@prim: $keyarrowty, UInt16),
                DataType::UInt32 => append_dict!(@prim: $keyarrowty, UInt32),
                DataType::UInt64 => append_dict!(@prim: $keyarrowty, UInt64),
                DataType::Float32 => append_dict!(@prim: $keyarrowty, Float32),
                DataType::Float64 => append_dict!(@prim: $keyarrowty, Float64),
                DataType::Date32 => append_dict!(@prim: $keyarrowty, Date32),
                DataType::Date64 => append_dict!(@prim: $keyarrowty, Date64),
                DataType::Timestamp(TimeUnit::Second, _) => {
                    append_dict!(@prim: $keyarrowty, TimestampSecond)
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    append_dict!(@prim: $keyarrowty, TimestampMillisecond)
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    append_dict!(@prim: $keyarrowty, TimestampMicrosecond)
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    append_dict!(@prim: $keyarrowty, TimestampNanosecond)
                }
                DataType::Utf8 => append_dict!(@str: $keyarrowty, i32),
                DataType::LargeUtf8 => append_dict!(@str: $keyarrowty, i64),
                _ => unimplemented!("dictionary value type not supported: {:?}", $value_type),
            }
        }};
        (@prim: $keyarrowty:ident, $valuearrowty:ident) => {{
            type KeyType = paste! {[< $keyarrowty Type >]};
            type ValueType = paste! {[< $valuearrowty Type >]};
            type B = PrimitiveDictionaryBuilder<KeyType, ValueType>;
            type A = DictionaryArray<KeyType>;
            let t = builder.as_any_mut().downcast_mut::<B>().unwrap();
            let f = array.as_any().downcast_ref::<A>().unwrap();
            let fv = f.values().as_any().downcast_ref::<paste! {[<$valuearrowty Array>]} >().unwrap();
            for &i in indices {
                if f.is_valid(i) {
                    let _ = t.append(fv.value(f.key(i).unwrap()));
                } else {
                    t.append_null();
                }
            }
        }};
        (@bin: $keyarrowty:ident, $strsizety:ty) => {{
            type KeyType = paste! {[< $keyarrowty Type >]};
            type B = BinaryDictionaryBuilder<KeyType>;
            type A = DictionaryArray<KeyType>;
            let t = builder.as_any_mut().downcast_mut::<B>().unwrap();
            let f = array.as_any().downcast_ref::<A>().unwrap();
            let fv = f.values().as_any().downcast_ref::<GenericStringArray<$strsizety>>().unwrap();
            for &i in indices {
                if f.is_valid(i) {
                    t.append(fv.value(f.key(i).unwrap()));
                } else {
                    t.append_null();
                }
            }
        }};
        (@str: $keyarrowty:ident, $strsizety:ty) => {{
            type KeyType = paste! {[< $keyarrowty Type >]};
            type B = StringDictionaryBuilder<KeyType>;
            type A = DictionaryArray<KeyType>;
            let t = builder.as_any_mut().downcast_mut::<B>().unwrap();
            let f = array.as_any().downcast_ref::<A>().unwrap();
            let fv = f.values().as_any().downcast_ref::<GenericStringArray<$strsizety>>().unwrap();
            for &i in indices {
                if f.is_valid(i) {
                    let _ = t.append(fv.value(f.key(i).unwrap()));
                } else {
                    t.append_null();
                }
            }
        }};
    }

    macro_rules! append_map {
        ($key_type:expr, $value_type:expr) => {{
            append_map!(@match_key: $key_type, $value_type)
        }};
        (@match_key: $key_type:expr, $value_type:expr) => {{
            match $key_type {
                DataType::Boolean => append_map!(@match_value: Boolean, $value_type),
                DataType::Int8 => append_map!(@match_value: Int8, $value_type),
                DataType::Int16 => append_map!(@match_value: Int16, $value_type),
                DataType::Int32 => append_map!(@match_value: Int32, $value_type),
                DataType::Int64 => append_map!(@match_value: Int64, $value_type),
                DataType::UInt8 => append_map!(@match_value: UInt8, $value_type),
                DataType::UInt16=> append_map!(@match_value: UInt16, $value_type),
                DataType::UInt32 => append_map!(@match_value: UInt32, $value_type),
                DataType::UInt64 => append_map!(@match_value: UInt64, $value_type),
                DataType::Float32 => append_map!(@match_value: Float32, $value_type),
                DataType::Float64 => append_map!(@match_value: Float64, $value_type),
                DataType::Date32 => append_map!(@match_value: Date32, $value_type),
                DataType::Date64 => append_map!(@match_value: Date64, $value_type),
                DataType::Timestamp(TimeUnit::Second, _) =>
                    append_map!(@match_value: TimestampSecond, $value_type),
                DataType::Timestamp(TimeUnit::Millisecond, _) =>
                    append_map!(@match_value: TimestampMillisecond, $value_type),
                DataType::Timestamp(TimeUnit::Microsecond, _) =>
                    append_map!(@match_value: TimestampMicrosecond, $value_type),
                DataType::Timestamp(TimeUnit::Nanosecond, _) =>
                    append_map!(@match_value: TimestampNanosecond, $value_type),
                DataType::Utf8 => append_map!(@match_value: String, $value_type),
                DataType::LargeUtf8 => append_map!(@match_value: LargeString, $value_type),
                DataType::Binary => append_map!(@match_value: Binary, $value_type),
                DataType::LargeBinary => append_map!(@match_value: LargeBinary, $value_type),
                _ => unimplemented!("map key type not supported: {:?}", $key_type),
            }
        }};
        (@match_value: $keyarrowty:ident, $value_type:expr) => {{
            match $value_type {
                DataType::Boolean => append_map!(@prim: $keyarrowty, Boolean),
                DataType::Int8 => append_map!(@prim: $keyarrowty, Int8),
                DataType::Int16 => append_map!(@prim: $keyarrowty, Int16),
                DataType::Int32 => append_map!(@prim: $keyarrowty, Int32),
                DataType::Int64 => append_map!(@prim: $keyarrowty, Int64),
                DataType::UInt8 => append_map!(@prim: $keyarrowty, UInt8),
                DataType::UInt16 => append_map!(@prim: $keyarrowty, UInt16),
                DataType::UInt32 => append_map!(@prim: $keyarrowty, UInt32),
                DataType::UInt64 => append_map!(@prim: $keyarrowty, UInt64),
                DataType::Float32 => append_map!(@prim: $keyarrowty, Float32),
                DataType::Float64 => append_map!(@prim: $keyarrowty, Float64),
                DataType::Date32 => append_map!(@prim: $keyarrowty, Date32),
                DataType::Date64 => append_map!(@prim: $keyarrowty, Date64),
                DataType::Timestamp(TimeUnit::Second, _) => append_map!(@prim: $keyarrowty, TimestampSecond),
                DataType::Timestamp(TimeUnit::Millisecond, _) => append_map!(@prim: $keyarrowty, TimestampMillisecond),
                DataType::Timestamp(TimeUnit::Microsecond, _) => append_map!(@prim: $keyarrowty, TimestampMicrosecond),
                DataType::Timestamp(TimeUnit::Nanosecond, _) => append_map!(@prim: $keyarrowty, TimestampNanosecond),
                DataType::Utf8 => append_map!(@prim: $keyarrowty, String),
                DataType::LargeUtf8 => append_map!(@prim: $keyarrowty, LargeString),
                DataType::Binary => append_map!(@prim: $keyarrowty, Binary),
                DataType::LargeBinary => append_map!(@prim: $keyarrowty, LargeBinary),
                _ => unimplemented!("map value type not supported: {:?}", $value_type),
            }
        }};
        (@prim: $keyarrowty:ident, $valuearrowty:ident) => {{
            type KeyType = paste! {[< $keyarrowty Builder >]};
            type ValueType = paste! {[< $valuearrowty Builder >]};
            type B = MapBuilder<KeyType, ValueType>;
            let t = builder.as_any_mut().downcast_mut::<B>().unwrap();
            let f = array.as_any().downcast_ref::<MapArray>().unwrap();
            let fo = f.value_offsets();

            for &i in indices {
                if f.is_valid(i) {
                    let first_index = fo[i] as usize;
                    let last_index = fo[i + 1] as usize;
                    if(last_index - first_index > 0) {
                        builder_extend(t.keys(), f.keys(), (first_index..last_index).collect::<Vec<_>>().as_slice(), f.key_type());
                        builder_extend(t.values(), f.values(), (first_index..last_index).collect::<Vec<_>>().as_slice(), f.value_type());
                    }
                    t.append(true).unwrap();
                } else {
                    t.append(false).unwrap();
                }
            }
        }};
    }

    macro_rules! append_list {
        ($data_type:expr) => {{
            append_list!(@match_type: $data_type)
        }};
        (@match_type: $data_type:expr) => {{
            match $data_type {
                DataType::Int8 => append_list!(@prim: Int8),
                DataType::Int16 => append_list!(@prim: Int16),
                DataType::Int32 => append_list!(@prim: Int32),
                DataType::Int64 => append_list!(@prim: Int64),
                DataType::UInt8 => append_list!(@prim: UInt8),
                DataType::UInt16 => append_list!(@prim: UInt16),
                DataType::UInt32 => append_list!(@prim: UInt32),
                DataType::UInt64 => append_list!(@prim: UInt64),
                DataType::Float32 => append_list!(@prim: Float32),
                DataType::Float64 => append_list!(@prim: Float64),
                DataType::Date32 => append_list!(@prim: Date32),
                DataType::Date64 => append_list!(@prim: Date64),
                DataType::Boolean => append_list!(@prim: Boolean),
                DataType::Utf8 => append_list!(@prim: String),
                DataType::LargeUtf8 => append_list!(@prim: LargeString),
                DataType::Timestamp(TimeUnit::Second, _) =>
                    append_list!(@prim: TimestampSecond),
                DataType::Timestamp(TimeUnit::Millisecond, _) =>
                    append_list!(@prim: TimestampMillisecond),
                DataType::Timestamp(TimeUnit::Microsecond, _) =>
                    append_list!(@prim: TimestampMicrosecond),
                DataType::Timestamp(TimeUnit::Nanosecond, _) =>
                    append_list!(@prim: TimestampNanosecond),
                DataType::Time32(TimeUnit::Second) => append_list!(@prim: Time32Second),
                DataType::Time32(TimeUnit::Millisecond) => append_list!(@prim: Time32Millisecond),
                DataType::Time64(TimeUnit::Microsecond) => append_list!(@prim: Time64Microsecond),
                DataType::Time64(TimeUnit::Nanosecond) => append_list!(@prim: Time64Nanosecond),
                DataType::Binary => append_list!(@prim: Binary),
                DataType::LargeBinary => append_list!(@prim: LargeBinary),
                DataType::Decimal128(_, _) => append_list!(@prim: ConfiguredDecimal128),
                DataType::Decimal256(_, _) => append_list!(@prim: ConfiguredDecimal256),
                _ => unimplemented!("list type not supported: {:?}", $data_type),
            }
        }};
        (@prim: $arrowty:ident) => {{
            type ElementType = paste! {[< $arrowty Builder >]};
            type B = ListBuilder<ElementType>;
            type A = ListArray;
            let t = builder.as_any_mut().downcast_mut::<B>().unwrap();
            let f = array.as_any().downcast_ref::<A>().unwrap();
            for &i in indices {
                if f.is_valid(i) {
                    builder_extend(t.values(),&f.value(i),&(0..f.value(i).len()).collect::<Vec<_>>(), f.value(i).data_type());
                    t.append(true);
                } else {
                    t.append(false);
                }
            }
        }};
    }

    macro_rules! append_struct {
        ($fields:expr) => {{
            append_struct!(@make: $fields)
        }};
        (@make: $fields:expr) => {{
            type B = StructBuilder;
            type A = StructArray;
            let t = builder.as_any_mut().downcast_mut::<B>().unwrap();
            let f = array.as_any().downcast_ref::<A>().unwrap();

            for &i in indices {
                if f.is_valid(i) {
                    for j in 0..$fields.len() {
                        let field_builders = unsafe {
                             struct XNullBufferBuilder {
                                _bitmap_builder: Option<BooleanBufferBuilder>,
                                _len: usize,
                                _capacity: usize,
                            }
                            struct XStructBuilder {
                            _fields: Vec<Field>,
                            field_builders: Vec<Box<dyn ArrayBuilder>>,
                            _null_buffer_builder: XNullBufferBuilder,
                            }
                            let t: &mut XStructBuilder = std::mem::transmute(&mut (*t));
                            std::slice::from_raw_parts_mut(t.field_builders.as_mut_ptr(), t.field_builders.len())
                        };

                        builder_extend(field_builders[j].as_mut(), &f.column(j), &[i], $fields[j].data_type());
                    }
                    t.append(true);
                }
                else {
                    builder_append_null(t, &Struct($fields.iter().cloned().collect()));
                }

            }
        }};
    }

    match data_type {
        DataType::Null => {
            builder
                .as_any_mut()
                .downcast_mut::<NullBuilder>()
                .unwrap()
                .extend(indices.len());
        }
        DataType::Boolean => append_simple!(Boolean),
        DataType::Int8 => append_simple!(Int8),
        DataType::Int16 => append_simple!(Int16),
        DataType::Int32 => append_simple!(Int32),
        DataType::Int64 => append_simple!(Int64),
        DataType::UInt8 => append_simple!(UInt8),
        DataType::UInt16 => append_simple!(UInt16),
        DataType::UInt32 => append_simple!(UInt32),
        DataType::UInt64 => append_simple!(UInt64),
        DataType::Float32 => append_simple!(Float32),
        DataType::Float64 => append_simple!(Float64),
        DataType::Date32 => append_simple!(Date32),
        DataType::Date64 => append_simple!(Date64),
        DataType::Timestamp(TimeUnit::Second, _) => {
            append_simple!(TimestampSecond)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            append_simple!(TimestampMillisecond)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            append_simple!(TimestampMicrosecond)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            append_simple!(TimestampNanosecond)
        }
        DataType::Time32(TimeUnit::Second) => append_simple!(Time32Second),
        DataType::Time32(TimeUnit::Millisecond) => append_simple!(Time32Millisecond),
        DataType::Time64(TimeUnit::Microsecond) => append_simple!(Time64Microsecond),
        DataType::Time64(TimeUnit::Nanosecond) => append_simple!(Time64Nanosecond),
        DataType::Binary => append_simple!(Binary),
        DataType::LargeBinary => append_simple!(LargeBinary),
        DataType::Utf8 => append_simple!(String),
        DataType::LargeUtf8 => append_simple!(LargeString),
        DataType::Decimal128(_, _) => append_decimal!(ConfiguredDecimal128, Decimal128),
        DataType::Decimal256(_, _) => append_decimal!(ConfiguredDecimal256, Decimal256),
        DataType::Dictionary(key_type, value_type) => append_dict!(key_type, value_type),
        DataType::List(fields) => append_list!(fields.data_type()),
        DataType::Map(field, _) => {
            if let DataType::Struct(fields) = field.data_type() {
                let key_type = fields.first().unwrap().data_type();
                let value_type = fields.last().unwrap().data_type();
                append_map!(key_type, value_type)
            } else {
                unimplemented!("map field not support {}", field)
            }
        }
        DataType::Struct(fields) => append_struct!(fields),
        dt => unimplemented!("data type not supported in builder_extend: {:?}", dt),
    }
}

pub fn builder_append_null(to: &mut (impl ArrayBuilder + ?Sized), data_type: &DataType) {
    macro_rules! append {
        ($arrowty:ident) => {{
            type B = paste::paste! {[< $arrowty Builder >]};
            let t = to.as_any_mut().downcast_mut::<B>().unwrap();
            t.append_null();
        }};
    }

    macro_rules! append_null_for_list {
        ($data_type:expr) => {{
            append_null_for_list!(@match_type: $data_type)
        }};
        (@match_type: $data_type:expr) => {{
            match $data_type {
                DataType::Int8 => append_null_for_list!(@prim: Int8),
                DataType::Int16 => append_null_for_list!(@prim: Int16),
                DataType::Int32 => append_null_for_list!(@prim: Int32),
                DataType::Int64 => append_null_for_list!(@prim: Int64),
                DataType::UInt8 => append_null_for_list!(@prim: UInt8),
                DataType::UInt16 => append_null_for_list!(@prim: UInt16),
                DataType::UInt32 => append_null_for_list!(@prim: UInt32),
                DataType::UInt64 => append_null_for_list!(@prim: UInt64),
                DataType::Float32 => append_null_for_list!(@prim: Float32),
                DataType::Float64 => append_null_for_list!(@prim: Float64),
                DataType::Date32 => append_null_for_list!(@prim: Date32),
                DataType::Date64 => append_null_for_list!(@prim: Date64),
                DataType::Boolean => append_null_for_list!(@prim: Boolean),
                DataType::Utf8 => append_null_for_list!(@prim: String),
                DataType::LargeUtf8 => append_null_for_list!(@prim: LargeString),
                DataType::Timestamp(TimeUnit::Second, _) => {
                    append_null_for_list!(@prim: TimestampSecond)
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    append_null_for_list!(@prim: TimestampMillisecond)
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    append_null_for_list!(@prim: TimestampMicrosecond)
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    append_null_for_list!(@prim: TimestampNanosecond)
                }
                DataType::Time32(TimeUnit::Second) => append_null_for_list!(@prim: Time32Second),
                DataType::Time32(TimeUnit::Millisecond) => append_null_for_list!(@prim: Time32Millisecond),
                DataType::Time64(TimeUnit::Microsecond) => append_null_for_list!(@prim: Time64Microsecond),
                DataType::Time64(TimeUnit::Nanosecond) => append_null_for_list!(@prim: Time64Nanosecond),
                DataType::Binary => append_null_for_list!(@prim: Binary),
                DataType::LargeBinary => append_null_for_list!(@prim: LargeBinary),
                DataType::Decimal128(_, _) => append_null_for_list!(@prim: ConfiguredDecimal128),
                DataType::Decimal256(_, _) => append_null_for_list!(@prim: ConfiguredDecimal256),
                _ => unimplemented!("list type not supported: {:?}", $data_type),
            }
        }};
        (@prim: $arrowty:ident) => {{
            type ElementType = paste! {[< $arrowty Builder >]};
            type B = ListBuilder<ElementType>;
            let t = to.as_any_mut().downcast_mut::<B>().unwrap();
            t.append(false);
        }};
    }

    macro_rules! append_null_for_map {
        ($key_type:expr, $value_type:expr) => {{
            append_null_for_map!(@match_key: $key_type, $value_type)
        }};
        (@match_key: $key_type:expr, $value_type:expr) => {{
            match $key_type {
                DataType::Boolean => append_null_for_map!(@match_value: Boolean, $value_type),
                DataType::Int8 => append_null_for_map!(@match_value: Int8, $value_type),
                DataType::Int16 => append_null_for_map!(@match_value: Int16, $value_type),
                DataType::Int32 => append_null_for_map!(@match_value: Int32, $value_type),
                DataType::Int64 => append_null_for_map!(@match_value: Int64, $value_type),
                DataType::UInt8 => append_null_for_map!(@match_value: UInt8, $value_type),
                DataType::UInt16=> append_null_for_map!(@match_value: UInt16, $value_type),
                DataType::UInt32 => append_null_for_map!(@match_value: UInt32, $value_type),
                DataType::UInt64 => append_null_for_map!(@match_value: UInt64, $value_type),
                DataType::Float32 => append_null_for_map!(@match_value: Float32, $value_type),
                DataType::Float64 => append_null_for_map!(@match_value: Float64, $value_type),
                DataType::Date32 => append_null_for_map!(@match_value: Date32, $value_type),
                DataType::Date64 => append_null_for_map!(@match_value: Date64, $value_type),
                DataType::Timestamp(TimeUnit::Second, _) =>
                    append_null_for_map!(@match_value: TimestampSecond, $value_type),
                DataType::Timestamp(TimeUnit::Millisecond, _) =>
                    append_null_for_map!(@match_value: TimestampMillisecond, $value_type),
                DataType::Timestamp(TimeUnit::Microsecond, _) =>
                    append_null_for_map!(@match_value: TimestampMicrosecond, $value_type),
                DataType::Timestamp(TimeUnit::Nanosecond, _) =>
                    append_null_for_map!(@match_value: TimestampNanosecond, $value_type),
                DataType::Utf8 => append_null_for_map!(@match_value: String, $value_type),
                DataType::LargeUtf8 => append_null_for_map!(@match_value: LargeString, $value_type),
                DataType::Binary => append_null_for_map!(@match_value: Binary, $value_type),
                DataType::LargeBinary => append_null_for_map!(@match_value: LargeBinary, $value_type),
                _ => unimplemented!("map key type not supported: {:?}", $key_type),
            }
        }};
        (@match_value: $keyarrowty:ident, $value_type:expr) => {{
            match $value_type {
                DataType::Boolean => append_null_for_map!(@prim: $keyarrowty, Boolean),
                DataType::Int8 => append_null_for_map!(@prim: $keyarrowty, Int8),
                DataType::Int16 => append_null_for_map!(@prim: $keyarrowty, Int16),
                DataType::Int32 => append_null_for_map!(@prim: $keyarrowty, Int32),
                DataType::Int64 => append_null_for_map!(@prim: $keyarrowty, Int64),
                DataType::UInt8 => append_null_for_map!(@prim: $keyarrowty, UInt8),
                DataType::UInt16 => append_null_for_map!(@prim: $keyarrowty, UInt16),
                DataType::UInt32 => append_null_for_map!(@prim: $keyarrowty, UInt32),
                DataType::UInt64 => append_null_for_map!(@prim: $keyarrowty, UInt64),
                DataType::Float32 => append_null_for_map!(@prim: $keyarrowty, Float32),
                DataType::Float64 => append_null_for_map!(@prim: $keyarrowty, Float64),
                DataType::Date32 => append_null_for_map!(@prim: $keyarrowty, Date32),
                DataType::Date64 => append_null_for_map!(@prim: $keyarrowty, Date64),
                DataType::Timestamp(TimeUnit::Second, _) => append_null_for_map!(@prim: $keyarrowty, TimestampSecond),
                DataType::Timestamp(TimeUnit::Millisecond, _) => append_null_for_map!(@prim: $keyarrowty, TimestampMillisecond),
                DataType::Timestamp(TimeUnit::Microsecond, _) => append_null_for_map!(@prim: $keyarrowty, TimestampMicrosecond),
                DataType::Timestamp(TimeUnit::Nanosecond, _) => append_null_for_map!(@prim: $keyarrowty, TimestampNanosecond),
                DataType::Utf8 => append_null_for_map!(@prim: $keyarrowty, String),
                DataType::LargeUtf8 => append_null_for_map!(@prim: $keyarrowty, LargeString),
                DataType::Binary => append_null_for_map!(@prim: $keyarrowty, Binary),
                DataType::LargeBinary => append_null_for_map!(@prim: $keyarrowty, LargeBinary),
                _ => unimplemented!("map value type not supported: {:?}", $value_type),
            }
        }};
        (@prim: $keyarrowty:ident, $valuearrowty:ident) => {{
            type KeyType = paste! {[< $keyarrowty Builder >]};
            type ValueType = paste! {[< $valuearrowty Builder >]};
            type B = MapBuilder<KeyType, ValueType>;
            let t = to.as_any_mut().downcast_mut::<B>().unwrap();
            t.append(false).expect("map append_null() error");
        }};
    }

    macro_rules! append_null_for_struct {
        ($fields:expr) => {{
            append_null_for_struct!(@make: $fields)
        }};
        (@make: $fields:expr) => {{
            type B = StructBuilder;
            let t = to.as_any_mut().downcast_mut::<B>().unwrap();
            for j in 0..$fields.len() {
                let field_builders = unsafe {
                     struct XNullBufferBuilder {
                        _bitmap_builder: Option<BooleanBufferBuilder>,
                        _len: usize,
                        _capacity: usize,
                    }
                    struct XStructBuilder {
                        _fields: Vec<Field>,
                        field_builders: Vec<Box<dyn ArrayBuilder>>,
                        _null_buffer_builder: XNullBufferBuilder,
                    }
                    let t: &mut XStructBuilder = std::mem::transmute(&mut (*t));
                    std::slice::from_raw_parts_mut(t.field_builders.as_mut_ptr(), t.field_builders.len())
                };
                builder_append_null(field_builders[j].as_mut(), $fields[j].data_type());
            }
            t.append_null();
        }};
    }

    match data_type {
        DataType::Null => {
            to.as_any_mut()
                .downcast_mut::<NullBuilder>()
                .unwrap()
                .append();
        }
        DataType::Boolean => append!(Boolean),
        DataType::Int8 => append!(Int8),
        DataType::Int16 => append!(Int16),
        DataType::Int32 => append!(Int32),
        DataType::Int64 => append!(Int64),
        DataType::UInt8 => append!(UInt8),
        DataType::UInt16 => append!(UInt16),
        DataType::UInt32 => append!(UInt32),
        DataType::UInt64 => append!(UInt64),
        DataType::Float32 => append!(Float32),
        DataType::Float64 => append!(Float64),
        DataType::Date32 => append!(Date32),
        DataType::Date64 => append!(Date64),
        DataType::Timestamp(TimeUnit::Second, _) => append!(TimestampSecond),
        DataType::Timestamp(TimeUnit::Millisecond, _) => append!(TimestampMillisecond),
        DataType::Timestamp(TimeUnit::Microsecond, _) => append!(TimestampMicrosecond),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => append!(TimestampNanosecond),
        DataType::Time32(TimeUnit::Second) => append!(Time32Second),
        DataType::Time32(TimeUnit::Millisecond) => append!(Time32Millisecond),
        DataType::Time64(TimeUnit::Microsecond) => append!(Time64Microsecond),
        DataType::Time64(TimeUnit::Nanosecond) => append!(Time64Nanosecond),
        DataType::Binary => append!(Binary),
        DataType::LargeBinary => append!(LargeBinary),
        DataType::Utf8 => append!(String),
        DataType::LargeUtf8 => append!(LargeString),
        DataType::Decimal128(_, _) => append!(ConfiguredDecimal128),
        DataType::Decimal256(_, _) => append!(ConfiguredDecimal256),
        DataType::List(field) => append_null_for_list!(field.data_type()),
        DataType::Map(field, _) => {
            if let DataType::Struct(fields) = field.data_type() {
                let key_type = fields.first().unwrap().data_type();
                let value_type = fields.last().unwrap().data_type();
                append_null_for_map!(key_type, value_type)
            } else {
                unimplemented!("map field not support {}", field)
            }
        }
        DataType::Struct(fields) => append_null_for_struct!(fields),
        dt => unimplemented!("data type not supported in builder_append_null: {:?}", dt),
    }
}

fn new_array_builder(dt: &DataType, batch_size: usize) -> Box<dyn ArrayBuilder> {
    macro_rules! make_dictionary_builder {
        ($key_type:expr, $value_type:expr) => {{
            match $key_type.as_ref() {
                DataType::Int8 => make_dictionary_builder!(@match_value: Int8, $value_type),
                DataType::Int16 => make_dictionary_builder!(@match_value: Int16, $value_type),
                DataType::Int32 => make_dictionary_builder!(@match_value: Int32, $value_type),
                DataType::Int64 => make_dictionary_builder!(@match_value: Int64, $value_type),
                DataType::UInt8 => make_dictionary_builder!(@match_value: UInt8, $value_type),
                DataType::UInt16 => make_dictionary_builder!(@match_value: UInt16, $value_type),
                DataType::UInt32 => make_dictionary_builder!(@match_value: UInt32, $value_type),
                DataType::UInt64 => make_dictionary_builder!(@match_value: UInt64, $value_type),
                _ => unimplemented!("unsupported dictionary key type: {:?}", $key_type),
            }
        }};
        (@match_value: $keyarrowty:ident, $value_type:expr) => {{
            match $value_type.as_ref() {
                DataType::Int8 => make_dictionary_builder!(@make: $keyarrowty, Int8),
                DataType::Int16 => make_dictionary_builder!(@make: $keyarrowty, Int16),
                DataType::Int32 => make_dictionary_builder!(@make: $keyarrowty, Int32),
                DataType::Int64 => make_dictionary_builder!(@make: $keyarrowty, Int64),
                DataType::UInt8 => make_dictionary_builder!(@make: $keyarrowty, UInt8),
                DataType::UInt16 => make_dictionary_builder!(@make: $keyarrowty, UInt16),
                DataType::UInt32 => make_dictionary_builder!(@make: $keyarrowty, UInt32),
                DataType::UInt64 => make_dictionary_builder!(@make: $keyarrowty, UInt64),
                DataType::Float32 => make_dictionary_builder!(@make: $keyarrowty, Float32),
                DataType::Float64 => make_dictionary_builder!(@make: $keyarrowty, Float64),
                DataType::Date32 => make_dictionary_builder!(@make: $keyarrowty, Date32),
                DataType::Date64 => make_dictionary_builder!(@make: $keyarrowty, Date64),
                DataType::Timestamp(TimeUnit::Second, _) => {
                    make_dictionary_builder!(@make: $keyarrowty, TimestampSecond)
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    make_dictionary_builder!(@make: $keyarrowty, TimestampMillisecond)
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    make_dictionary_builder!(@make: $keyarrowty, TimestampMicrosecond)
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    make_dictionary_builder!(@make: $keyarrowty, TimestampNanosecond)
                }
                DataType::Utf8 | DataType::LargeUtf8 => {
                    make_dictionary_builder!(@make_str: $keyarrowty)
                }
                _ => unimplemented!("dictionary value type not supported: {:?}", $value_type),
            }
        }};
        (@make: $keyarrowty:ident, $valuearrowty:ident) => {{
            type KeyType = paste! {[< $keyarrowty Type >]};
            type ValueType = paste! {[< $valuearrowty Type >]};
            Box::new(PrimitiveDictionaryBuilder::<KeyType, ValueType>::new())
        }};
        (@make_str: $keyarrowty:ident) => {{
            type KeyType = paste! {[< $keyarrowty Type >]};
            Box::new(StringDictionaryBuilder::<KeyType>::new())
        }};
    }

    macro_rules! make_list_builder {
            ($data_type:expr) => {{
                match $data_type {
                    DataType::Int8 => make_list_builder!(@make: Int8),
                    DataType::Int16 => make_list_builder!(@make: Int16),
                    DataType::Int32 => make_list_builder!(@make: Int32),
                    DataType::Int64 => make_list_builder!(@make: Int64),
                    DataType::UInt8 => make_list_builder!(@make: UInt8),
                    DataType::UInt16 => make_list_builder!(@make: UInt16),
                    DataType::UInt32 => make_list_builder!(@make: UInt32),
                    DataType::UInt64 => make_list_builder!(@make: UInt64),
                    DataType::Float32 => make_list_builder!(@make: Float32),
                    DataType::Float64 => make_list_builder!(@make: Float64),
                    DataType::Date32 => make_list_builder!(@make: Date32),
                    DataType::Date64 => make_list_builder!(@make: Date64),
                    DataType::Timestamp(TimeUnit::Second, _) => {
                        make_list_builder!(@make: TimestampSecond)
                    }
                    DataType::Timestamp(TimeUnit::Millisecond, _) => {
                        make_list_builder!(@make: TimestampMillisecond)
                    }
                    DataType::Timestamp(TimeUnit::Microsecond, _) => {
                        make_list_builder!(@make: TimestampMicrosecond)
                    }
                    DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                        make_list_builder!(@make: TimestampNanosecond)
                    }
                    DataType::Boolean => make_list_builder!(@make: Boolean),
                    DataType::Decimal128(prec, scale) => {
                        make_list_builder!(@make_decimal: Decimal128(prec, scale))
                    }
                    DataType::Decimal256(prec, scale) => {
                        make_list_builder!(@make_decimal: Decimal256(prec, scale))
                    }
                    DataType::Utf8 => make_list_builder!(@make: String),
                    DataType::LargeUtf8 => make_list_builder!(@make: LargeString),
                    DataType::Binary => make_list_builder!(@make: Binary),
                    DataType::LargeBinary => make_list_builder!(@make: LargeBinary),
                    _ => unimplemented!("unsupported list data type: {:?}", $data_type),
                }
            }};
            (@make: $arrowty:ident) => {{
                type TypeBuilder = paste! {[< $arrowty Builder >]};
                Box::new(ListBuilder::with_capacity(
                    TypeBuilder::new(),
                    batch_size,
                ))
            }};
            (@make_decimal: $arrowty:ident($prec:expr, $scale:expr)) => {{
                type TypeBuilder = paste! {[<Configured $arrowty Builder >]};
                Box::new(ListBuilder::with_capacity(
                    TypeBuilder::with_capacity(0, $prec, $scale),
                    batch_size,
                ))
            }};
        }

    macro_rules! make_map_builder {
        ($key_type:expr, $value_type:expr, $map_fields_name:expr) => {{
            match $key_type {
                DataType::Boolean => make_map_builder!(@match_value: Boolean, $value_type, $map_fields_name),
                DataType::Int8 => make_map_builder!(@match_value: Int8, $value_type, $map_fields_name),
                DataType::Int16 => make_map_builder!(@match_value: Int16, $value_type, $map_fields_name),
                DataType::Int32 => make_map_builder!(@match_value: Int32, $value_type, $map_fields_name),
                DataType::Int64 => make_map_builder!(@match_value: Int64, $value_type, $map_fields_name),
                DataType::UInt8 => make_map_builder!(@match_value: UInt8, $value_type, $map_fields_name),
                DataType::UInt16 => make_map_builder!(@match_value: UInt16, $value_type, $map_fields_name),
                DataType::UInt32 => make_map_builder!(@match_value: UInt32, $value_type, $map_fields_name),
                DataType::UInt64 => make_map_builder!(@match_value: UInt64, $value_type, $map_fields_name),
                DataType::Float32 => make_map_builder!(@match_value: Float32, $value_type, $map_fields_name),
                DataType::Float64 => make_map_builder!(@match_value: Float64, $value_type, $map_fields_name),
                DataType::Date32 => make_map_builder!(@match_value: Date32, $value_type, $map_fields_name),
                DataType::Date64 => make_map_builder!(@match_value: Date64, $value_type, $map_fields_name),
                DataType::Timestamp(TimeUnit::Second, _) => make_map_builder!(@match_value: TimestampSecond, $value_type, $map_fields_name),
                DataType::Timestamp(TimeUnit::Millisecond, _) => make_map_builder!(@match_value: TimestampMillisecond, $value_type, $map_fields_name),
                DataType::Timestamp(TimeUnit::Microsecond, _) => make_map_builder!(@match_value: TimestampMicrosecond, $value_type, $map_fields_name),
                DataType::Timestamp(TimeUnit::Nanosecond, _) => make_map_builder!(@match_value: TimestampNanosecond, $value_type, $map_fields_name),
                DataType::Utf8 => make_map_builder!(@match_value: String, $value_type, $map_fields_name),
                DataType::LargeUtf8 => make_map_builder!(@match_value: LargeString, $value_type, $map_fields_name),
                DataType::Binary => make_map_builder!(@match_value: Binary, $value_type, $map_fields_name),
                DataType::LargeBinary => make_map_builder!(@match_value: LargeBinary, $value_type, $map_fields_name),
                _ => unimplemented!("unsupported map key type: {:?}", $key_type),
            }
        }};
        (@match_value: $keyarrowty:ident, $value_type:expr, $map_fields_name:expr) => {{
            match $value_type {
                DataType::Boolean => make_map_builder!(@make: $keyarrowty, Boolean, $map_fields_name),
                DataType::Int8 => make_map_builder!(@make: $keyarrowty, Int8, $map_fields_name),
                DataType::Int16 => make_map_builder!(@make: $keyarrowty, Int16, $map_fields_name),
                DataType::Int32 => make_map_builder!(@make: $keyarrowty, Int32, $map_fields_name),
                DataType::Int64 => make_map_builder!(@make: $keyarrowty, Int64, $map_fields_name),
                DataType::UInt8 => make_map_builder!(@make: $keyarrowty, UInt8, $map_fields_name),
                DataType::UInt16 => make_map_builder!(@make: $keyarrowty, UInt16, $map_fields_name),
                DataType::UInt32 => make_map_builder!(@make: $keyarrowty, UInt32, $map_fields_name),
                DataType::UInt64 => make_map_builder!(@make: $keyarrowty, UInt64, $map_fields_name),
                DataType::Float32 => make_map_builder!(@make: $keyarrowty, Float32, $map_fields_name),
                DataType::Float64 => make_map_builder!(@make: $keyarrowty, Float64, $map_fields_name),
                DataType::Date32 => make_map_builder!(@make: $keyarrowty, Date32, $map_fields_name),
                DataType::Date64 => make_map_builder!(@make: $keyarrowty, Date64, $map_fields_name),
                DataType::Timestamp(TimeUnit::Second, _) => make_map_builder!(@make: $keyarrowty, TimestampSecond, $map_fields_name),
                DataType::Timestamp(TimeUnit::Millisecond, _) => make_map_builder!(@make: $keyarrowty, TimestampMillisecond, $map_fields_name),
                DataType::Timestamp(TimeUnit::Microsecond, _) => make_map_builder!(@make: $keyarrowty, TimestampMicrosecond, $map_fields_name),
                DataType::Timestamp(TimeUnit::Nanosecond, _) => make_map_builder!(@make: $keyarrowty, TimestampNanosecond, $map_fields_name),
                DataType::Utf8  => make_map_builder!(@make: $keyarrowty, String, $map_fields_name),
                DataType::LargeUtf8 => make_map_builder!(@make: $keyarrowty, LargeString, $map_fields_name),
                DataType::Binary => make_map_builder!(@make: $keyarrowty, Binary, $map_fields_name),
                DataType::LargeBinary => make_map_builder!(@make: $keyarrowty, LargeBinary, $map_fields_name),
                _ => unimplemented!("map value type not supported: {:?}", $value_type),
            }
        }};
        (@make: $keyarrowty:ident, $valuearrowty:ident, $map_fields_name:expr) => {{
            type KeyBuilder = paste! {[< $keyarrowty Builder >]};
            type ValueBuilder = paste! {[< $valuearrowty Builder >]};
            Box::new(MapBuilder::with_capacity(
                $map_fields_name,
                KeyBuilder::new(),
                ValueBuilder::new(),
                batch_size,
            ))
        }};

    }

    match dt {
        DataType::Null => Box::new(NullBuilder::new()),
        DataType::Decimal128(precision, scale) => Box::new(
            ConfiguredDecimal128Builder::with_capacity(0, *precision, *scale),
        ),
        DataType::Decimal256(precision, scale) => Box::new(
            ConfiguredDecimal256Builder::with_capacity(0, *precision, *scale),
        ),
        DataType::Dictionary(key_type, value_type) => {
            make_dictionary_builder!(key_type, value_type)
        }
        DataType::List(fields) => {
            make_list_builder!(fields.data_type().clone())
        }
        DataType::Map(field, _) => {
            if let Struct(fields) = field.data_type() {
                let map_fields_name = Some(MapFieldNames {
                    entry: "entries".to_string(),
                    key: "key".to_string(),
                    value: "value".to_string(),
                });
                let key_type = fields.first().unwrap().data_type().clone();
                let value_type = fields.last().unwrap().data_type().clone();
                make_map_builder!(key_type, value_type, map_fields_name)
            } else {
                unimplemented!("map field not support {}", field);
            }
        }
        dt => make_builder(dt, batch_size),
    }
}

pub struct NullBuilder {
    len: usize,
}
impl Default for NullBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl NullBuilder {
    pub fn new() -> Self {
        Self { len: 0 }
    }

    pub fn append(&mut self) {
        self.len += 1;
    }

    pub fn extend(&mut self, len: usize) {
        self.len += len;
    }
}

impl ArrayBuilder for NullBuilder {
    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn finish(&mut self) -> ArrayRef {
        self.finish_cloned()
    }

    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(NullArray::new(self.len))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

pub struct ConfiguredDecimalBuilder<T: DecimalType> {
    inner: PrimitiveBuilder<T>,
    precision: u8,
    scale: i8,
}

#[allow(unused)]
impl<T: DecimalType> ConfiguredDecimalBuilder<T> {
    pub fn with_capacity(capacity: usize, precision: u8, scale: i8) -> Self {
        Self {
            inner: PrimitiveBuilder::with_capacity(capacity),
            precision,
            scale,
        }
    }

    pub fn append_value(&mut self, v: T::Native) {
        self.inner.append_value(v)
    }

    pub fn append_option(&mut self, v: Option<T::Native>) {
        self.inner.append_option(v)
    }

    pub fn append_null(&mut self) {
        self.inner.append_null()
    }

    pub fn precision(&self) -> u8 {
        self.precision
    }

    pub fn scale(&self) -> i8 {
        self.scale
    }
}

impl<T: DecimalType> ArrayBuilder for ConfiguredDecimalBuilder<T> {
    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(
            self.inner
                .finish()
                .with_precision_and_scale(self.precision, self.scale)
                .unwrap(),
        )
    }

    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(
            self.inner
                .finish_cloned()
                .with_precision_and_scale(self.precision, self.scale)
                .unwrap(),
        )
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}
pub type ConfiguredDecimal128Builder = ConfiguredDecimalBuilder<Decimal128Type>;
pub type ConfiguredDecimal256Builder = ConfiguredDecimalBuilder<Decimal256Type>;

#[test]
fn test_struct_array_from_vec() {
    let strings: ArrayRef = Arc::new(StringArray::from(vec![
        Some("joe"),
        None,
        None,
        Some("mark"),
    ]));
    let ints: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)]));

    let arr = StructArray::try_from(vec![("f1", strings.clone()), ("f2", ints.clone())]).unwrap();

    eprintln!("ans is: {:#?}", arr.is_valid(1))
}
