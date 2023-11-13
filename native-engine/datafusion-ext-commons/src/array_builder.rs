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
use arrow::datatypes::*;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;

// NOTE:
// we suggest not using this mod because array_builders do not support
// complex types like list/struct/map.

pub fn has_array_builder_supported(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(TimeUnit::Second, _)
            | DataType::Timestamp(TimeUnit::Millisecond, _)
            | DataType::Timestamp(TimeUnit::Microsecond, _)
            | DataType::Timestamp(TimeUnit::Nanosecond, _)
            | DataType::Time32(TimeUnit::Second)
            | DataType::Time32(TimeUnit::Millisecond)
            | DataType::Time64(TimeUnit::Microsecond)
            | DataType::Time64(TimeUnit::Nanosecond)
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Decimal128(_, _)
    )
}

pub fn new_array_builders(schema: &SchemaRef, batch_size: usize) -> Vec<Box<dyn ArrayBuilder>> {
    schema
        .fields()
        .iter()
        .map(|field| make_builder(field.data_type(), batch_size))
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
    macro_rules! append {
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

    match data_type {
        DataType::Null => {
            let null_builder = builder.as_any_mut().downcast_mut::<NullBuilder>().unwrap();
            null_builder.append_nulls(indices.len());
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
        DataType::Decimal128(_, _) => append!(Decimal128),
        dt => unimplemented!("data type not supported in builder_extend: {:?}", dt),
    }
}
