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

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, IntervalUnit, Schema, TimeUnit};
use datafusion::{common::JoinSide, logical_expr::Operator, scalar::ScalarValue};
use datafusion_ext_plans::{agg::AggFunction, joins::join_utils::JoinType};

use crate::error::PlanSerDeError;

// include the generated protobuf source as a submodule
#[allow(clippy::all)]
pub mod protobuf {
    include!(concat!(env!("OUT_DIR"), "/plan.protobuf.rs"));
}

pub mod error;
pub mod from_proto;

pub(crate) fn proto_error<S: Into<String>>(message: S) -> PlanSerDeError {
    PlanSerDeError::General(message.into())
}

#[macro_export]
macro_rules! convert_required {
    ($PB:expr) => {{
        if let Some(field) = $PB.as_ref() {
            field.try_into()
        } else {
            Err(proto_error("Missing required field in protobuf"))
        }
    }};
}

#[macro_export]
macro_rules! into_required {
    ($PB:expr) => {{
        if let Some(field) = $PB.as_ref() {
            Ok(field.into())
        } else {
            Err(proto_error("Missing required field in protobuf"))
        }
    }};
}

#[macro_export]
macro_rules! convert_box_required {
    ($PB:expr) => {{
        if let Some(field) = $PB.as_ref() {
            field.as_ref().try_into()
        } else {
            Err(proto_error("Missing required field in protobuf"))
        }
    }};
}

pub fn from_proto_binary_op(op: &str) -> Result<Operator, PlanSerDeError> {
    match op {
        "And" => Ok(Operator::And),
        "Or" => Ok(Operator::Or),
        "Eq" => Ok(Operator::Eq),
        "NotEq" => Ok(Operator::NotEq),
        "LtEq" => Ok(Operator::LtEq),
        "Lt" => Ok(Operator::Lt),
        "Gt" => Ok(Operator::Gt),
        "GtEq" => Ok(Operator::GtEq),
        "Plus" => Ok(Operator::Plus),
        "Minus" => Ok(Operator::Minus),
        "Multiply" => Ok(Operator::Multiply),
        "Divide" => Ok(Operator::Divide),
        "Modulo" => Ok(Operator::Modulo),
        "IsDistinctFrom" => Ok(Operator::IsDistinctFrom),
        "IsNotDistinctFrom" => Ok(Operator::IsNotDistinctFrom),
        "BitwiseAnd" => Ok(Operator::BitwiseAnd),
        "BitwiseOr" => Ok(Operator::BitwiseOr),
        "BitwiseXor" => Ok(Operator::BitwiseXor),
        "BitwiseShiftLeft" => Ok(Operator::BitwiseShiftLeft),
        "BitwiseShiftRight" => Ok(Operator::BitwiseShiftRight),
        "RegexIMatch" => Ok(Operator::RegexIMatch),
        "RegexMatch" => Ok(Operator::RegexMatch),
        "RegexNotIMatch" => Ok(Operator::RegexNotIMatch),
        "RegexNotMatch" => Ok(Operator::RegexNotMatch),
        "StringConcat" => Ok(Operator::StringConcat),
        other => Err(proto_error(format!(
            "Unsupported binary operator '{:?}'",
            other
        ))),
    }
}

impl From<protobuf::JoinType> for JoinType {
    fn from(t: protobuf::JoinType) -> Self {
        match t {
            protobuf::JoinType::Inner => JoinType::Inner,
            protobuf::JoinType::Left => JoinType::Left,
            protobuf::JoinType::Right => JoinType::Right,
            protobuf::JoinType::Full => JoinType::Full,
            protobuf::JoinType::Semi => JoinType::LeftSemi,
            protobuf::JoinType::Anti => JoinType::LeftAnti,
            protobuf::JoinType::Existence => JoinType::Existence,
        }
    }
}

impl From<protobuf::JoinSide> for JoinSide {
    fn from(t: protobuf::JoinSide) -> Self {
        match t {
            protobuf::JoinSide::LeftSide => JoinSide::Left,
            protobuf::JoinSide::RightSide => JoinSide::Right,
        }
    }
}

impl From<protobuf::AggFunction> for AggFunction {
    fn from(agg_fun: protobuf::AggFunction) -> AggFunction {
        match agg_fun {
            protobuf::AggFunction::Min => AggFunction::Min,
            protobuf::AggFunction::Max => AggFunction::Max,
            protobuf::AggFunction::Sum => AggFunction::Sum,
            protobuf::AggFunction::Avg => AggFunction::Avg,
            protobuf::AggFunction::Count => AggFunction::Count,
            protobuf::AggFunction::CollectList => AggFunction::CollectList,
            protobuf::AggFunction::CollectSet => AggFunction::CollectSet,
            protobuf::AggFunction::First => AggFunction::First,
            protobuf::AggFunction::FirstIgnoresNull => AggFunction::FirstIgnoresNull,
            protobuf::AggFunction::BloomFilter => AggFunction::BloomFilter,
            protobuf::AggFunction::BrickhouseCollect => AggFunction::BrickhouseCollect,
            protobuf::AggFunction::BrickhouseCombineUnique => AggFunction::BrickhouseCombineUnique,
        }
    }
}

impl protobuf::TimeUnit {
    pub fn from_arrow_time_unit(val: &TimeUnit) -> Self {
        match val {
            TimeUnit::Second => protobuf::TimeUnit::Second,
            TimeUnit::Millisecond => protobuf::TimeUnit::Millisecond,
            TimeUnit::Microsecond => protobuf::TimeUnit::Microsecond,
            TimeUnit::Nanosecond => protobuf::TimeUnit::Nanosecond,
        }
    }
    pub fn try_from_to_arrow(time_unit_i32: i32) -> Result<TimeUnit, PlanSerDeError> {
        let pb_time_unit = protobuf::TimeUnit::try_from(time_unit_i32).expect("invalid TimeUnit");
        Ok(match pb_time_unit {
            protobuf::TimeUnit::Second => TimeUnit::Second,
            protobuf::TimeUnit::Millisecond => TimeUnit::Millisecond,
            protobuf::TimeUnit::Microsecond => TimeUnit::Microsecond,
            protobuf::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
        })
    }
}

impl protobuf::IntervalUnit {
    pub fn from_arrow_interval_unit(interval_unit: &IntervalUnit) -> Self {
        match interval_unit {
            IntervalUnit::YearMonth => protobuf::IntervalUnit::YearMonth,
            IntervalUnit::DayTime => protobuf::IntervalUnit::DayTime,
            IntervalUnit::MonthDayNano => protobuf::IntervalUnit::MonthDayNano,
        }
    }

    pub fn try_from_to_arrow(interval_unit_i32: i32) -> Result<IntervalUnit, PlanSerDeError> {
        let pb_interval_unit =
            protobuf::IntervalUnit::try_from(interval_unit_i32).expect("invalid IntervalUnit");
        Ok(match pb_interval_unit {
            protobuf::IntervalUnit::YearMonth => IntervalUnit::YearMonth,
            protobuf::IntervalUnit::DayTime => IntervalUnit::DayTime,
            protobuf::IntervalUnit::MonthDayNano => IntervalUnit::MonthDayNano,
        })
    }
}

impl TryInto<arrow::datatypes::DataType> for &protobuf::arrow_type::ArrowTypeEnum {
    type Error = PlanSerDeError;
    fn try_into(self) -> Result<arrow::datatypes::DataType, Self::Error> {
        use protobuf::arrow_type;
        Ok(match self {
            arrow_type::ArrowTypeEnum::None(_) => DataType::Null,
            arrow_type::ArrowTypeEnum::Bool(_) => DataType::Boolean,
            arrow_type::ArrowTypeEnum::Uint8(_) => DataType::UInt8,
            arrow_type::ArrowTypeEnum::Int8(_) => DataType::Int8,
            arrow_type::ArrowTypeEnum::Uint16(_) => DataType::UInt16,
            arrow_type::ArrowTypeEnum::Int16(_) => DataType::Int16,
            arrow_type::ArrowTypeEnum::Uint32(_) => DataType::UInt32,
            arrow_type::ArrowTypeEnum::Int32(_) => DataType::Int32,
            arrow_type::ArrowTypeEnum::Uint64(_) => DataType::UInt64,
            arrow_type::ArrowTypeEnum::Int64(_) => DataType::Int64,
            arrow_type::ArrowTypeEnum::Float16(_) => DataType::Float16,
            arrow_type::ArrowTypeEnum::Float32(_) => DataType::Float32,
            arrow_type::ArrowTypeEnum::Float64(_) => DataType::Float64,
            arrow_type::ArrowTypeEnum::Utf8(_) => DataType::Utf8,
            arrow_type::ArrowTypeEnum::LargeUtf8(_) => DataType::LargeUtf8,
            arrow_type::ArrowTypeEnum::Binary(_) => DataType::Binary,
            arrow_type::ArrowTypeEnum::FixedSizeBinary(size) => DataType::FixedSizeBinary(*size),
            arrow_type::ArrowTypeEnum::LargeBinary(_) => DataType::LargeBinary,
            arrow_type::ArrowTypeEnum::Date32(_) => DataType::Date32,
            arrow_type::ArrowTypeEnum::Date64(_) => DataType::Date64,
            arrow_type::ArrowTypeEnum::Duration(time_unit) => {
                DataType::Duration(protobuf::TimeUnit::try_from_to_arrow(*time_unit)?)
            }
            arrow_type::ArrowTypeEnum::Timestamp(protobuf::Timestamp {
                time_unit,
                timezone,
            }) => DataType::Timestamp(
                protobuf::TimeUnit::try_from_to_arrow(*time_unit)?,
                match timezone.len() {
                    0 => None,
                    _ => Some(timezone.to_owned().into()),
                },
            ),
            arrow_type::ArrowTypeEnum::Time32(time_unit) => {
                DataType::Time32(protobuf::TimeUnit::try_from_to_arrow(*time_unit)?)
            }
            arrow_type::ArrowTypeEnum::Time64(time_unit) => {
                DataType::Time64(protobuf::TimeUnit::try_from_to_arrow(*time_unit)?)
            }
            arrow_type::ArrowTypeEnum::Interval(interval_unit) => {
                DataType::Interval(protobuf::IntervalUnit::try_from_to_arrow(*interval_unit)?)
            }
            arrow_type::ArrowTypeEnum::Decimal(protobuf::Decimal { whole, fractional }) => {
                DataType::Decimal128(*whole as u8, *fractional as i8)
            }
            arrow_type::ArrowTypeEnum::List(list) => {
                let list_type: &protobuf::Field = list
                    .as_ref()
                    .field_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: List message missing required field 'field_type'"))?
                    .as_ref();
                DataType::List(Arc::new(list_type.try_into()?))
            }
            arrow_type::ArrowTypeEnum::LargeList(list) => {
                let list_type: &protobuf::Field = list
                    .as_ref()
                    .field_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: List message missing required field 'field_type'"))?
                    .as_ref();
                DataType::LargeList(Arc::new(list_type.try_into()?))
            }
            arrow_type::ArrowTypeEnum::FixedSizeList(list) => {
                let list_type: &protobuf::Field = list
                    .as_ref()
                    .field_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: List message missing required field 'field_type'"))?
                    .as_ref();
                let list_size = list.list_size;
                DataType::FixedSizeList(Arc::new(list_type.try_into()?), list_size)
            }
            arrow_type::ArrowTypeEnum::Struct(strct) => DataType::Struct(
                strct
                    .sub_field_types
                    .iter()
                    .map(|field| Ok(Arc::new(field.try_into()?)))
                    .collect::<Result<Fields, Self::Error>>()?,
            ),
            arrow_type::ArrowTypeEnum::Union(_union) => {
                // let union_mode = protobuf::UnionMode::try_from(union.union_mode)
                //     .ok_or_else(|| {
                //         proto_error(
                //             "Protobuf deserialization error: Unknown union mode type",
                //         )
                //     })?;
                // let union_mode = match union_mode {
                //     protobuf::UnionMode::Dense => UnionMode::Dense,
                //     protobuf::UnionMode::Sparse => UnionMode::Sparse,
                // };
                // let union_types = union
                //     .union_types
                //     .iter()
                //     .map(|field| field.try_into())
                //     .collect::<Result<Vec<_>, _>>()?;
                // DataType::Union(union_types, _, union_mode)
                unimplemented!()
            }
            arrow_type::ArrowTypeEnum::Map(map) => {
                let key_type: &protobuf::Field = map
                    .as_ref()
                    .key_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: Map message missing required field 'key_type'"))?
                    .as_ref();
                let value_type: &protobuf::Field = map
                    .as_ref()
                    .value_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: Map message missing required field 'value_type'"))?
                    .as_ref();

                let vec_field = vec![
                    Arc::new(key_type.try_into()?),
                    Arc::new(value_type.try_into()?),
                ];
                let fields = Arc::new(Field::new(
                    "entries",
                    DataType::Struct(vec_field.into()),
                    false,
                ));
                DataType::Map(fields, false)
            }
            arrow_type::ArrowTypeEnum::Dictionary(dict) => {
                let pb_key_datatype = dict
                    .as_ref()
                    .key
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: Dictionary message missing required field 'key'"))?;
                let pb_value_datatype = dict
                    .as_ref()
                    .value
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: Dictionary message missing required field 'key'"))?;
                let key_datatype: DataType = pb_key_datatype.as_ref().try_into()?;
                let value_datatype: DataType = pb_value_datatype.as_ref().try_into()?;
                DataType::Dictionary(Box::new(key_datatype), Box::new(value_datatype))
            }
        })
    }
}

#[allow(clippy::from_over_into)]
impl Into<arrow::datatypes::DataType> for protobuf::PrimitiveScalarType {
    fn into(self) -> arrow::datatypes::DataType {
        match self {
            protobuf::PrimitiveScalarType::Bool => DataType::Boolean,
            protobuf::PrimitiveScalarType::Uint8 => DataType::UInt8,
            protobuf::PrimitiveScalarType::Int8 => DataType::Int8,
            protobuf::PrimitiveScalarType::Uint16 => DataType::UInt16,
            protobuf::PrimitiveScalarType::Int16 => DataType::Int16,
            protobuf::PrimitiveScalarType::Uint32 => DataType::UInt32,
            protobuf::PrimitiveScalarType::Int32 => DataType::Int32,
            protobuf::PrimitiveScalarType::Uint64 => DataType::UInt64,
            protobuf::PrimitiveScalarType::Int64 => DataType::Int64,
            protobuf::PrimitiveScalarType::Float32 => DataType::Float32,
            protobuf::PrimitiveScalarType::Float64 => DataType::Float64,
            protobuf::PrimitiveScalarType::Utf8 => DataType::Utf8,
            protobuf::PrimitiveScalarType::LargeUtf8 => DataType::LargeUtf8,
            protobuf::PrimitiveScalarType::Date32 => DataType::Date32,
            protobuf::PrimitiveScalarType::Null => DataType::Null,
            protobuf::PrimitiveScalarType::Decimal128 => DataType::Decimal128(1, 0),
            protobuf::PrimitiveScalarType::Date64 => DataType::Date64,
            protobuf::PrimitiveScalarType::TimestampSecond => {
                DataType::Timestamp(TimeUnit::Second, None)
            }
            protobuf::PrimitiveScalarType::TimestampMillisecond => {
                DataType::Timestamp(TimeUnit::Millisecond, None)
            }
            protobuf::PrimitiveScalarType::TimestampMicrosecond => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            protobuf::PrimitiveScalarType::TimestampNanosecond => {
                DataType::Timestamp(TimeUnit::Nanosecond, None)
            }
            protobuf::PrimitiveScalarType::IntervalYearmonth => {
                DataType::Interval(IntervalUnit::YearMonth)
            }
            protobuf::PrimitiveScalarType::IntervalDaytime => {
                DataType::Interval(IntervalUnit::DayTime)
            }
        }
    }
}

impl TryInto<DataType> for &protobuf::ArrowType {
    type Error = PlanSerDeError;
    fn try_into(self) -> Result<DataType, Self::Error> {
        let pb_arrow_type = self.arrow_type_enum.as_ref().ok_or_else(|| {
            proto_error(
                "Protobuf deserialization error: ArrowType missing required field 'data_type'",
            )
        })?;
        pb_arrow_type.try_into()
    }
}

impl TryInto<DataType> for &Box<protobuf::List> {
    type Error = PlanSerDeError;
    fn try_into(self) -> Result<DataType, Self::Error> {
        let list_ref = self.as_ref();
        match &list_ref.field_type {
            Some(pb_field) => {
                let pb_field_ref = pb_field.as_ref();
                let arrow_field: Field = pb_field_ref.try_into()?;
                Ok(DataType::List(Arc::new(arrow_field)))
            }
            None => Err(proto_error(
                "List message missing required field 'field_type'",
            )),
        }
    }
}

impl TryInto<Field> for &protobuf::Field {
    type Error = PlanSerDeError;
    fn try_into(self) -> Result<Field, Self::Error> {
        let pb_datatype = self.arrow_type.as_ref().ok_or_else(|| {
            proto_error(
                "Protobuf deserialization error: Field message missing required field 'arrow_type'",
            )
        })?;

        Ok(Field::new(
            self.name.as_str(),
            pb_datatype.as_ref().try_into()?,
            self.nullable,
        ))
    }
}

impl TryInto<Schema> for &protobuf::Schema {
    type Error = PlanSerDeError;

    fn try_into(self) -> Result<Schema, PlanSerDeError> {
        let fields = self
            .columns
            .iter()
            .map(|c| {
                let pb_arrow_type_res = c
                    .arrow_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: Field message was missing required field 'arrow_type'"));
                let pb_arrow_type: &protobuf::ArrowType = match pb_arrow_type_res {
                    Ok(res) => res,
                    Err(e) => return Err(e),
                };
                Ok(Field::new(&c.name, pb_arrow_type.try_into()?, c.nullable))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Schema::new(fields))
    }
}

impl TryInto<datafusion::scalar::ScalarValue> for &protobuf::ScalarValue {
    type Error = PlanSerDeError;
    fn try_into(self) -> Result<datafusion::scalar::ScalarValue, Self::Error> {
        let value = self.value.as_ref().ok_or_else(|| {
            proto_error("Protobuf deserialization error: missing required field 'value'")
        })?;
        Ok(match value {
            protobuf::scalar_value::Value::BoolValue(v) => ScalarValue::Boolean(Some(*v)),
            protobuf::scalar_value::Value::Utf8Value(v) => ScalarValue::Utf8(Some(v.to_owned())),
            protobuf::scalar_value::Value::LargeUtf8Value(v) => {
                ScalarValue::LargeUtf8(Some(v.to_owned()))
            }
            protobuf::scalar_value::Value::Int8Value(v) => ScalarValue::Int8(Some(*v as i8)),
            protobuf::scalar_value::Value::Int16Value(v) => ScalarValue::Int16(Some(*v as i16)),
            protobuf::scalar_value::Value::Int32Value(v) => ScalarValue::Int32(Some(*v)),
            protobuf::scalar_value::Value::Int64Value(v) => ScalarValue::Int64(Some(*v)),
            protobuf::scalar_value::Value::Uint8Value(v) => ScalarValue::UInt8(Some(*v as u8)),
            protobuf::scalar_value::Value::Uint16Value(v) => ScalarValue::UInt16(Some(*v as u16)),
            protobuf::scalar_value::Value::Uint32Value(v) => ScalarValue::UInt32(Some(*v)),
            protobuf::scalar_value::Value::Uint64Value(v) => ScalarValue::UInt64(Some(*v)),
            protobuf::scalar_value::Value::Float32Value(v) => ScalarValue::Float32(Some(*v)),
            protobuf::scalar_value::Value::Float64Value(v) => ScalarValue::Float64(Some(*v)),
            protobuf::scalar_value::Value::Date32Value(v) => ScalarValue::Date32(Some(*v)),
            protobuf::scalar_value::Value::TimestampSecondValue(v) => {
                ScalarValue::TimestampSecond(Some(*v), None)
            }
            protobuf::scalar_value::Value::TimestampMillisecondValue(v) => {
                ScalarValue::TimestampMillisecond(Some(*v), None)
            }
            protobuf::scalar_value::Value::TimestampMicrosecondValue(v) => {
                ScalarValue::TimestampMicrosecond(Some(*v), None)
            }
            protobuf::scalar_value::Value::TimestampNanosecondValue(v) => {
                ScalarValue::TimestampNanosecond(Some(*v), None)
            }
            protobuf::scalar_value::Value::DecimalValue(v) => {
                let decimal = v.decimal.as_ref().unwrap();
                ScalarValue::Decimal128(
                    Some(v.long_value as i128),
                    decimal.whole as u8,
                    decimal.fractional as i8,
                )
            }
            protobuf::scalar_value::Value::ListValue(scalar_list) => {
                let protobuf::ScalarListValue {
                    values,
                    datatype: opt_scalar_type,
                } = &scalar_list;
                let pb_scalar_type = opt_scalar_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization err: ScalaListValue missing required field 'datatype'"))?;
                let typechecked_values: Vec<ScalarValue> = values
                    .iter()
                    .map(|val| val.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                let scalar_type: DataType = pb_scalar_type.try_into()?;
                ScalarValue::List(ScalarValue::new_list(&typechecked_values, &scalar_type))
            }
            protobuf::scalar_value::Value::NullValue(v) => {
                match v.datatype.as_ref().expect("missing scalar data type") {
                    protobuf::scalar_type::Datatype::Scalar(scalar) => {
                        let null_type_enum = protobuf::PrimitiveScalarType::try_from(*scalar)
                            .expect("invalid PrimitiveScalarType");
                        null_type_enum.try_into()?
                    }
                    protobuf::scalar_type::Datatype::List(list) => {
                        let pb_scalar_type = list
                            .element_type
                            .as_ref()
                            .expect("missing list element type");
                        let scalar_type: DataType = pb_scalar_type.as_ref().try_into()?;
                        ScalarValue::try_from(DataType::new_list(scalar_type, true))?
                    }
                }
            }
        })
    }
}

impl TryInto<DataType> for &protobuf::ScalarType {
    type Error = PlanSerDeError;
    fn try_into(self) -> Result<DataType, Self::Error> {
        let pb_scalartype = self.datatype.as_ref().expect("missing data type");
        pb_scalartype.try_into()
    }
}

impl TryInto<DataType> for &protobuf::scalar_type::Datatype {
    type Error = PlanSerDeError;
    fn try_into(self) -> Result<DataType, Self::Error> {
        use protobuf::scalar_type::Datatype;
        Ok(match self {
            Datatype::Scalar(scalar_type) => {
                let pb_scalar_enum = protobuf::PrimitiveScalarType::try_from(*scalar_type)
                    .expect("invalid PrimitiveScalarType");
                pb_scalar_enum.into()
            }
            Datatype::List(list_type) => {
                let element_scalar_type: DataType = list_type
                    .element_type
                    .as_ref()
                    .expect("missing element type")
                    .as_ref()
                    .try_into()?;
                DataType::new_list(element_scalar_type, true)
            }
        })
    }
}

impl TryInto<datafusion::scalar::ScalarValue> for &protobuf::scalar_value::Value {
    type Error = PlanSerDeError;
    fn try_into(self) -> Result<datafusion::scalar::ScalarValue, Self::Error> {
        use protobuf::PrimitiveScalarType;
        let scalar = match self {
            protobuf::scalar_value::Value::BoolValue(v) => ScalarValue::Boolean(Some(*v)),
            protobuf::scalar_value::Value::Utf8Value(v) => ScalarValue::Utf8(Some(v.to_owned())),
            protobuf::scalar_value::Value::LargeUtf8Value(v) => {
                ScalarValue::LargeUtf8(Some(v.to_owned()))
            }
            protobuf::scalar_value::Value::Int8Value(v) => ScalarValue::Int8(Some(*v as i8)),
            protobuf::scalar_value::Value::Int16Value(v) => ScalarValue::Int16(Some(*v as i16)),
            protobuf::scalar_value::Value::Int32Value(v) => ScalarValue::Int32(Some(*v)),
            protobuf::scalar_value::Value::Int64Value(v) => ScalarValue::Int64(Some(*v)),
            protobuf::scalar_value::Value::Uint8Value(v) => ScalarValue::UInt8(Some(*v as u8)),
            protobuf::scalar_value::Value::Uint16Value(v) => ScalarValue::UInt16(Some(*v as u16)),
            protobuf::scalar_value::Value::Uint32Value(v) => ScalarValue::UInt32(Some(*v)),
            protobuf::scalar_value::Value::Uint64Value(v) => ScalarValue::UInt64(Some(*v)),
            protobuf::scalar_value::Value::Float32Value(v) => ScalarValue::Float32(Some(*v)),
            protobuf::scalar_value::Value::Float64Value(v) => ScalarValue::Float64(Some(*v)),
            protobuf::scalar_value::Value::Date32Value(v) => ScalarValue::Date32(Some(*v)),
            protobuf::scalar_value::Value::TimestampSecondValue(v) => {
                ScalarValue::TimestampSecond(Some(*v), None)
            }
            protobuf::scalar_value::Value::TimestampMillisecondValue(v) => {
                ScalarValue::TimestampMillisecond(Some(*v), None)
            }
            protobuf::scalar_value::Value::TimestampMicrosecondValue(v) => {
                ScalarValue::TimestampMicrosecond(Some(*v), None)
            }
            protobuf::scalar_value::Value::TimestampNanosecondValue(v) => {
                ScalarValue::TimestampNanosecond(Some(*v), None)
            }
            protobuf::scalar_value::Value::ListValue(v) => v.try_into()?,
            protobuf::scalar_value::Value::NullValue(v) => {
                match v.datatype.as_ref().expect("missing null value type") {
                    protobuf::scalar_type::Datatype::Scalar(scalar) => {
                        PrimitiveScalarType::try_from(*scalar)
                            .expect("invalid PrimitiveScalarType")
                            .try_into()?
                    }
                    protobuf::scalar_type::Datatype::List(list) => {
                        let element_scalar_type: DataType = list
                            .element_type
                            .as_ref()
                            .expect("missing list element type")
                            .as_ref()
                            .try_into()?;
                        ScalarValue::try_from(DataType::new_list(element_scalar_type, true))?
                    }
                }
            }
            protobuf::scalar_value::Value::DecimalValue(v) => {
                let decimal = v.decimal.as_ref().unwrap();
                ScalarValue::Decimal128(
                    Some(v.long_value as i128),
                    decimal.whole as u8,
                    decimal.fractional as i8,
                )
            }
        };
        Ok(scalar)
    }
}

impl TryInto<ScalarValue> for &protobuf::ScalarListValue {
    type Error = PlanSerDeError;
    fn try_into(self) -> Result<ScalarValue, Self::Error> {
        let element_scalar_type: DataType = self
            .datatype
            .as_ref()
            .expect("missing list data type")
            .try_into()?;
        let values: Vec<ScalarValue> = self
            .values
            .iter()
            .map(|value| Ok(value.try_into()?))
            .collect::<Result<_, Self::Error>>()?;
        Ok(ScalarValue::List(ScalarValue::new_list(
            &values,
            &element_scalar_type,
        )))
    }
}

impl TryInto<DataType> for &protobuf::ScalarListType {
    type Error = PlanSerDeError;
    fn try_into(self) -> Result<DataType, Self::Error> {
        let element_scalar_type: DataType = self
            .element_type
            .as_ref()
            .expect("missing list element type")
            .as_ref()
            .try_into()?;
        Ok(DataType::new_list(element_scalar_type, true))
    }
}

impl TryInto<datafusion::scalar::ScalarValue> for protobuf::PrimitiveScalarType {
    type Error = PlanSerDeError;
    fn try_into(self) -> Result<datafusion::scalar::ScalarValue, Self::Error> {
        Ok(match self {
            // protobuf::PrimitiveScalarType::Null => {
            //     return Err(proto_error("Untyped null is an invalid scalar value"))
            // }
            protobuf::PrimitiveScalarType::Null => ScalarValue::Null,
            protobuf::PrimitiveScalarType::Bool => ScalarValue::Boolean(None),
            protobuf::PrimitiveScalarType::Uint8 => ScalarValue::UInt8(None),
            protobuf::PrimitiveScalarType::Int8 => ScalarValue::Int8(None),
            protobuf::PrimitiveScalarType::Uint16 => ScalarValue::UInt16(None),
            protobuf::PrimitiveScalarType::Int16 => ScalarValue::Int16(None),
            protobuf::PrimitiveScalarType::Uint32 => ScalarValue::UInt32(None),
            protobuf::PrimitiveScalarType::Int32 => ScalarValue::Int32(None),
            protobuf::PrimitiveScalarType::Uint64 => ScalarValue::UInt64(None),
            protobuf::PrimitiveScalarType::Int64 => ScalarValue::Int64(None),
            protobuf::PrimitiveScalarType::Float32 => ScalarValue::Float32(None),
            protobuf::PrimitiveScalarType::Float64 => ScalarValue::Float64(None),
            protobuf::PrimitiveScalarType::Utf8 => ScalarValue::Utf8(None),
            protobuf::PrimitiveScalarType::LargeUtf8 => ScalarValue::LargeUtf8(None),
            protobuf::PrimitiveScalarType::Date32 => ScalarValue::Date32(None),
            protobuf::PrimitiveScalarType::Decimal128 => ScalarValue::Decimal128(None, 1, 0),
            protobuf::PrimitiveScalarType::Date64 => ScalarValue::Date64(None),
            protobuf::PrimitiveScalarType::TimestampSecond => {
                ScalarValue::TimestampSecond(None, None)
            }
            protobuf::PrimitiveScalarType::TimestampMillisecond => {
                ScalarValue::TimestampMillisecond(None, None)
            }
            protobuf::PrimitiveScalarType::TimestampMicrosecond => {
                ScalarValue::TimestampMicrosecond(None, None)
            }
            protobuf::PrimitiveScalarType::TimestampNanosecond => {
                ScalarValue::TimestampNanosecond(None, None)
            }
            protobuf::PrimitiveScalarType::IntervalYearmonth => {
                ScalarValue::IntervalYearMonth(None)
            }
            protobuf::PrimitiveScalarType::IntervalDaytime => ScalarValue::IntervalDayTime(None),
        })
    }
}
