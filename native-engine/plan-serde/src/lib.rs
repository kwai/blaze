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

use crate::error::PlanSerDeError;
use datafusion::arrow::datatypes::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use datafusion::logical_plan::Operator;
use datafusion::physical_plan::join_utils::JoinSide;
use datafusion::prelude::JoinType;
use datafusion::scalar::ScalarValue;

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

pub(crate) fn from_proto_binary_op(op: &str) -> Result<Operator, PlanSerDeError> {
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
        "Like" => Ok(Operator::Like),
        "NotLike" => Ok(Operator::NotLike),
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
            protobuf::JoinType::Semi => JoinType::Semi,
            protobuf::JoinType::Anti => JoinType::Anti,
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

impl protobuf::TimeUnit {
    pub fn from_arrow_time_unit(val: &TimeUnit) -> Self {
        match val {
            TimeUnit::Second => protobuf::TimeUnit::Second,
            TimeUnit::Millisecond => protobuf::TimeUnit::TimeMillisecond,
            TimeUnit::Microsecond => protobuf::TimeUnit::Microsecond,
            TimeUnit::Nanosecond => protobuf::TimeUnit::Nanosecond,
        }
    }
    pub fn from_i32_to_arrow(time_unit_i32: i32) -> Result<TimeUnit, PlanSerDeError> {
        let pb_time_unit = protobuf::TimeUnit::from_i32(time_unit_i32);
        match pb_time_unit {
            Some(time_unit) => Ok(match time_unit {
                protobuf::TimeUnit::Second => TimeUnit::Second,
                protobuf::TimeUnit::TimeMillisecond => TimeUnit::Millisecond,
                protobuf::TimeUnit::Microsecond => TimeUnit::Microsecond,
                protobuf::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
            }),
            None => Err(proto_error(
                "Error converting i32 to TimeUnit: Passed invalid variant",
            )),
        }
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

    pub fn from_i32_to_arrow(
        interval_unit_i32: i32,
    ) -> Result<IntervalUnit, PlanSerDeError> {
        let pb_interval_unit = protobuf::IntervalUnit::from_i32(interval_unit_i32);
        match pb_interval_unit {
            Some(interval_unit) => Ok(match interval_unit {
                protobuf::IntervalUnit::YearMonth => IntervalUnit::YearMonth,
                protobuf::IntervalUnit::DayTime => IntervalUnit::DayTime,
                protobuf::IntervalUnit::MonthDayNano => IntervalUnit::MonthDayNano,
            }),
            None => Err(proto_error(
                "Error converting i32 to DateUnit: Passed invalid variant",
            )),
        }
    }
}

impl TryInto<datafusion::arrow::datatypes::DataType>
    for &protobuf::arrow_type::ArrowTypeEnum
{
    type Error = PlanSerDeError;
    fn try_into(self) -> Result<datafusion::arrow::datatypes::DataType, Self::Error> {
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
            arrow_type::ArrowTypeEnum::FixedSizeBinary(size) => {
                DataType::FixedSizeBinary(*size)
            }
            arrow_type::ArrowTypeEnum::LargeBinary(_) => DataType::LargeBinary,
            arrow_type::ArrowTypeEnum::Date32(_) => DataType::Date32,
            arrow_type::ArrowTypeEnum::Date64(_) => DataType::Date64,
            arrow_type::ArrowTypeEnum::Duration(time_unit) => {
                DataType::Duration(protobuf::TimeUnit::from_i32_to_arrow(*time_unit)?)
            }
            arrow_type::ArrowTypeEnum::Timestamp(protobuf::Timestamp {
                time_unit,
                timezone,
            }) => DataType::Timestamp(
                protobuf::TimeUnit::from_i32_to_arrow(*time_unit)?,
                match timezone.len() {
                    0 => None,
                    _ => Some(timezone.to_owned()),
                },
            ),
            arrow_type::ArrowTypeEnum::Time32(time_unit) => {
                DataType::Time32(protobuf::TimeUnit::from_i32_to_arrow(*time_unit)?)
            }
            arrow_type::ArrowTypeEnum::Time64(time_unit) => {
                DataType::Time64(protobuf::TimeUnit::from_i32_to_arrow(*time_unit)?)
            }
            arrow_type::ArrowTypeEnum::Interval(interval_unit) => DataType::Interval(
                protobuf::IntervalUnit::from_i32_to_arrow(*interval_unit)?,
            ),
            arrow_type::ArrowTypeEnum::Decimal(protobuf::Decimal {
                whole,
                fractional,
            }) => DataType::Decimal(*whole as usize, *fractional as usize),
            arrow_type::ArrowTypeEnum::List(list) => {
                let list_type: &protobuf::Field = list
                    .as_ref()
                    .field_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: List message missing required field 'field_type'"))?
                    .as_ref();
                DataType::List(Box::new(list_type.try_into()?))
            }
            arrow_type::ArrowTypeEnum::LargeList(list) => {
                let list_type: &protobuf::Field = list
                    .as_ref()
                    .field_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: List message missing required field 'field_type'"))?
                    .as_ref();
                DataType::LargeList(Box::new(list_type.try_into()?))
            }
            arrow_type::ArrowTypeEnum::FixedSizeList(list) => {
                let list_type: &protobuf::Field = list
                    .as_ref()
                    .field_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: List message missing required field 'field_type'"))?
                    .as_ref();
                let list_size = list.list_size;
                DataType::FixedSizeList(Box::new(list_type.try_into()?), list_size)
            }
            arrow_type::ArrowTypeEnum::Struct(strct) => DataType::Struct(
                strct
                    .sub_field_types
                    .iter()
                    .map(|field| field.try_into())
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            arrow_type::ArrowTypeEnum::Union(_union) => {
                // let union_mode = protobuf::UnionMode::from_i32(union.union_mode)
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
impl Into<datafusion::arrow::datatypes::DataType> for protobuf::PrimitiveScalarType {
    fn into(self) -> datafusion::arrow::datatypes::DataType {
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
            protobuf::PrimitiveScalarType::TimeMicrosecond => {
                DataType::Time64(TimeUnit::Microsecond)
            }
            protobuf::PrimitiveScalarType::TimeNanosecond => {
                DataType::Time64(TimeUnit::Nanosecond)
            }
            protobuf::PrimitiveScalarType::Null => DataType::Null,
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
                Ok(DataType::List(Box::new(arrow_field)))
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
            protobuf::scalar_value::Value::Utf8Value(v) => {
                ScalarValue::Utf8(Some(v.to_owned()))
            }
            protobuf::scalar_value::Value::LargeUtf8Value(v) => {
                ScalarValue::LargeUtf8(Some(v.to_owned()))
            }
            protobuf::scalar_value::Value::Int8Value(v) => {
                ScalarValue::Int8(Some(*v as i8))
            }
            protobuf::scalar_value::Value::Int16Value(v) => {
                ScalarValue::Int16(Some(*v as i16))
            }
            protobuf::scalar_value::Value::Int32Value(v) => ScalarValue::Int32(Some(*v)),
            protobuf::scalar_value::Value::Int64Value(v) => ScalarValue::Int64(Some(*v)),
            protobuf::scalar_value::Value::Uint8Value(v) => {
                ScalarValue::UInt8(Some(*v as u8))
            }
            protobuf::scalar_value::Value::Uint16Value(v) => {
                ScalarValue::UInt16(Some(*v as u16))
            }
            protobuf::scalar_value::Value::Uint32Value(v) => {
                ScalarValue::UInt32(Some(*v))
            }
            protobuf::scalar_value::Value::Uint64Value(v) => {
                ScalarValue::UInt64(Some(*v))
            }
            protobuf::scalar_value::Value::Float32Value(v) => {
                ScalarValue::Float32(Some(*v))
            }
            protobuf::scalar_value::Value::Float64Value(v) => {
                ScalarValue::Float64(Some(*v))
            }
            protobuf::scalar_value::Value::Date32Value(v) => {
                ScalarValue::Date32(Some(*v))
            }
            protobuf::scalar_value::Value::TimeMicrosecondValue(v) => {
                ScalarValue::TimestampMicrosecond(Some(*v), None)
            }
            protobuf::scalar_value::Value::TimeNanosecondValue(v) => {
                ScalarValue::TimestampNanosecond(Some(*v), None)
            }
            protobuf::scalar_value::Value::DecimalValue(v) => {
                let decimal = v.decimal.as_ref().unwrap();
                ScalarValue::Decimal128(
                    Some(v.long_value as i128),
                    decimal.whole as usize,
                    decimal.fractional as usize,
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
                let scalar_type = Box::new(scalar_type);
                ScalarValue::List(Some(typechecked_values), scalar_type)
            }
            protobuf::scalar_value::Value::NullListValue(v) => {
                let pb_datatype = v
                    .datatype
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: NullListValue message missing required field 'datatyp'"))?;
                let pb_datatype = Box::new(pb_datatype.try_into()?);
                ScalarValue::List(None, pb_datatype)
            }
            protobuf::scalar_value::Value::NullValue(v) => {
                let null_type_enum = protobuf::PrimitiveScalarType::from_i32(*v)
                    .ok_or_else(|| proto_error("Protobuf deserialization error found invalid enum variant for DatafusionScalar"))?;
                null_type_enum.try_into()?
            }
        })
    }
}

impl TryInto<DataType> for &protobuf::ScalarType {
    type Error = PlanSerDeError;
    fn try_into(self) -> Result<DataType, Self::Error> {
        let pb_scalartype = self.datatype.as_ref().ok_or_else(|| {
            proto_error("ScalarType message missing required field 'datatype'")
        })?;
        pb_scalartype.try_into()
    }
}

impl TryInto<DataType> for &protobuf::scalar_type::Datatype {
    type Error = PlanSerDeError;
    fn try_into(self) -> Result<DataType, Self::Error> {
        use protobuf::scalar_type::Datatype;
        Ok(match self {
            Datatype::Scalar(scalar_type) => {
                let pb_scalar_enum = protobuf::PrimitiveScalarType::from_i32(*scalar_type).ok_or_else(|| {
                    proto_error(format!(
                        "Protobuf deserialization error, scalar_type::Datatype missing was provided invalid enum variant: {}",
                        *scalar_type
                    ))
                })?;
                pb_scalar_enum.into()
            }
            Datatype::List(protobuf::ScalarListType {
                deepest_type,
                field_names,
            }) => {
                if field_names.is_empty() {
                    return Err(proto_error(
                        "Protobuf deserialization error: found no field names in ScalarListType message which requires at least one",
                    ));
                }
                let pb_scalar_type = protobuf::PrimitiveScalarType::from_i32(
                    *deepest_type,
                )
                .ok_or_else(|| {
                    proto_error(format!(
                        "Protobuf deserialization error: invalid i32 for scalar enum: {}",
                        *deepest_type
                    ))
                })?;
                //Because length is checked above it is safe to unwrap .last()
                let mut scalar_type = DataType::List(Box::new(Field::new(
                    field_names.last().unwrap().as_str(),
                    pb_scalar_type.into(),
                    true,
                )));
                //Iterate over field names in reverse order except for the last item in the vector
                for name in field_names.iter().rev().skip(1) {
                    let new_datatype = DataType::List(Box::new(Field::new(
                        name.as_str(),
                        scalar_type,
                        true,
                    )));
                    scalar_type = new_datatype;
                }
                scalar_type
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
            protobuf::scalar_value::Value::Utf8Value(v) => {
                ScalarValue::Utf8(Some(v.to_owned()))
            }
            protobuf::scalar_value::Value::LargeUtf8Value(v) => {
                ScalarValue::LargeUtf8(Some(v.to_owned()))
            }
            protobuf::scalar_value::Value::Int8Value(v) => {
                ScalarValue::Int8(Some(*v as i8))
            }
            protobuf::scalar_value::Value::Int16Value(v) => {
                ScalarValue::Int16(Some(*v as i16))
            }
            protobuf::scalar_value::Value::Int32Value(v) => ScalarValue::Int32(Some(*v)),
            protobuf::scalar_value::Value::Int64Value(v) => ScalarValue::Int64(Some(*v)),
            protobuf::scalar_value::Value::Uint8Value(v) => {
                ScalarValue::UInt8(Some(*v as u8))
            }
            protobuf::scalar_value::Value::Uint16Value(v) => {
                ScalarValue::UInt16(Some(*v as u16))
            }
            protobuf::scalar_value::Value::Uint32Value(v) => {
                ScalarValue::UInt32(Some(*v))
            }
            protobuf::scalar_value::Value::Uint64Value(v) => {
                ScalarValue::UInt64(Some(*v))
            }
            protobuf::scalar_value::Value::Float32Value(v) => {
                ScalarValue::Float32(Some(*v))
            }
            protobuf::scalar_value::Value::Float64Value(v) => {
                ScalarValue::Float64(Some(*v))
            }
            protobuf::scalar_value::Value::Date32Value(v) => {
                ScalarValue::Date32(Some(*v))
            }
            protobuf::scalar_value::Value::TimeMicrosecondValue(v) => {
                ScalarValue::TimestampMicrosecond(Some(*v), None)
            }
            protobuf::scalar_value::Value::TimeNanosecondValue(v) => {
                ScalarValue::TimestampNanosecond(Some(*v), None)
            }
            protobuf::scalar_value::Value::ListValue(v) => v.try_into()?,
            protobuf::scalar_value::Value::NullListValue(v) => {
                ScalarValue::List(None, Box::new(v.try_into()?))
            }
            protobuf::scalar_value::Value::NullValue(null_enum) => {
                PrimitiveScalarType::from_i32(*null_enum)
                    .ok_or_else(|| proto_error("Invalid scalar type"))?
                    .try_into()?
            }
            protobuf::scalar_value::Value::DecimalValue(v) => {
                let decimal = v.decimal.as_ref().unwrap();
                ScalarValue::Decimal128(
                    Some(v.long_value as i128),
                    decimal.whole as usize,
                    decimal.fractional as usize,
                )
            }
        };
        Ok(scalar)
    }
}

impl TryInto<datafusion::scalar::ScalarValue> for &protobuf::ScalarListValue {
    type Error = PlanSerDeError;
    fn try_into(self) -> Result<datafusion::scalar::ScalarValue, Self::Error> {
        use protobuf::scalar_type::Datatype;
        use protobuf::PrimitiveScalarType;
        let protobuf::ScalarListValue { datatype, values } = self;
        let pb_scalar_type = datatype
            .as_ref()
            .ok_or_else(|| proto_error("Protobuf deserialization error: ScalarListValue messsage missing required field 'datatype'"))?;
        let scalar_type = pb_scalar_type
            .datatype
            .as_ref()
            .ok_or_else(|| proto_error("Protobuf deserialization error: ScalarListValue.Datatype messsage missing required field 'datatype'"))?;
        let scalar_values = match scalar_type {
            Datatype::Scalar(scalar_type_i32) => {
                let leaf_scalar_type =
                    protobuf::PrimitiveScalarType::from_i32(*scalar_type_i32)
                        .ok_or_else(|| {
                            proto_error("Error converting i32 to basic scalar type")
                        })?;
                let typechecked_values: Vec<datafusion::scalar::ScalarValue> = values
                    .iter()
                    .map(|protobuf::ScalarValue { value: opt_value }| {
                        let value = opt_value.as_ref().ok_or_else(|| {
                            proto_error(
                                "Protobuf deserialization error: missing required field 'value'",
                            )
                        })?;
                        typechecked_scalar_value_conversion(value, leaf_scalar_type)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                datafusion::scalar::ScalarValue::List(
                    Some(typechecked_values),
                    Box::new(leaf_scalar_type.into()),
                )
            }
            Datatype::List(list_type) => {
                let protobuf::ScalarListType {
                    deepest_type,
                    field_names,
                } = &list_type;
                let leaf_type =
                    PrimitiveScalarType::from_i32(*deepest_type).ok_or_else(|| {
                        proto_error("Error converting i32 to basic scalar type")
                    })?;
                let depth = field_names.len();

                let typechecked_values: Vec<datafusion::scalar::ScalarValue> = if depth
                    == 0
                {
                    return Err(proto_error(
                        "Protobuf deserialization error, ScalarListType had no field names, requires at least one",
                    ));
                } else if depth == 1 {
                    values
                        .iter()
                        .map(|protobuf::ScalarValue { value: opt_value }| {
                            let value = opt_value
                                .as_ref()
                                .ok_or_else(|| proto_error("Protobuf deserialization error: missing required field 'value'"))?;
                            typechecked_scalar_value_conversion(value, leaf_type)
                        })
                        .collect::<Result<Vec<_>, _>>()?
                } else {
                    values
                        .iter()
                        .map(|protobuf::ScalarValue { value: opt_value }| {
                            let value = opt_value
                                .as_ref()
                                .ok_or_else(|| proto_error("Protobuf deserialization error: missing required field 'value'"))?;
                            value.try_into()
                        })
                        .collect::<Result<Vec<_>, _>>()?
                };
                datafusion::scalar::ScalarValue::List(
                    match typechecked_values.len() {
                        0 => None,
                        _ => Some(typechecked_values),
                    },
                    Box::new(list_type.try_into()?),
                )
            }
        };
        Ok(scalar_values)
    }
}

impl TryInto<DataType> for &protobuf::ScalarListType {
    type Error = PlanSerDeError;
    fn try_into(self) -> Result<DataType, Self::Error> {
        use protobuf::PrimitiveScalarType;
        let protobuf::ScalarListType {
            deepest_type,
            field_names,
        } = self;

        let depth = field_names.len();
        if depth == 0 {
            return Err(proto_error(
                "Protobuf deserialization error: Found a ScalarListType message with no field names, at least one is required",
            ));
        }

        let mut curr_type = DataType::List(Box::new(Field::new(
            //Since checked vector is not empty above this is safe to unwrap
            field_names.last().unwrap(),
            PrimitiveScalarType::from_i32(*deepest_type)
                .ok_or_else(|| {
                    proto_error("Could not convert to datafusion scalar type")
                })?
                .into(),
            true,
        )));
        //Iterates over field names in reverse order except for the last item in the vector
        for name in field_names.iter().rev().skip(1) {
            let temp_curr_type =
                DataType::List(Box::new(Field::new(name, curr_type, true)));
            curr_type = temp_curr_type;
        }
        Ok(curr_type)
    }
}

//Does not typecheck lists
fn typechecked_scalar_value_conversion(
    tested_type: &protobuf::scalar_value::Value,
    required_type: protobuf::PrimitiveScalarType,
) -> Result<datafusion::scalar::ScalarValue, PlanSerDeError> {
    use protobuf::scalar_value::Value;
    use protobuf::PrimitiveScalarType;
    Ok(match (tested_type, &required_type) {
        (Value::BoolValue(v), PrimitiveScalarType::Bool) => {
            ScalarValue::Boolean(Some(*v))
        }
        (Value::Int8Value(v), PrimitiveScalarType::Int8) => {
            ScalarValue::Int8(Some(*v as i8))
        }
        (Value::Int16Value(v), PrimitiveScalarType::Int16) => {
            ScalarValue::Int16(Some(*v as i16))
        }
        (Value::Int32Value(v), PrimitiveScalarType::Int32) => {
            ScalarValue::Int32(Some(*v))
        }
        (Value::Int64Value(v), PrimitiveScalarType::Int64) => {
            ScalarValue::Int64(Some(*v))
        }
        (Value::Uint8Value(v), PrimitiveScalarType::Uint8) => {
            ScalarValue::UInt8(Some(*v as u8))
        }
        (Value::Uint16Value(v), PrimitiveScalarType::Uint16) => {
            ScalarValue::UInt16(Some(*v as u16))
        }
        (Value::Uint32Value(v), PrimitiveScalarType::Uint32) => {
            ScalarValue::UInt32(Some(*v))
        }
        (Value::Uint64Value(v), PrimitiveScalarType::Uint64) => {
            ScalarValue::UInt64(Some(*v))
        }
        (Value::Float32Value(v), PrimitiveScalarType::Float32) => {
            ScalarValue::Float32(Some(*v))
        }
        (Value::Float64Value(v), PrimitiveScalarType::Float64) => {
            ScalarValue::Float64(Some(*v))
        }
        (Value::Date32Value(v), PrimitiveScalarType::Date32) => {
            ScalarValue::Date32(Some(*v))
        }
        (Value::TimeMicrosecondValue(v), PrimitiveScalarType::TimeMicrosecond) => {
            ScalarValue::TimestampMicrosecond(Some(*v), None)
        }
        (Value::TimeNanosecondValue(v), PrimitiveScalarType::TimeMicrosecond) => {
            ScalarValue::TimestampNanosecond(Some(*v), None)
        }
        (Value::Utf8Value(v), PrimitiveScalarType::Utf8) => {
            ScalarValue::Utf8(Some(v.to_owned()))
        }
        (Value::LargeUtf8Value(v), PrimitiveScalarType::LargeUtf8) => {
            ScalarValue::LargeUtf8(Some(v.to_owned()))
        }

        (Value::NullValue(i32_enum), required_scalar_type) => {
            if *i32_enum == *required_scalar_type as i32 {
                let pb_scalar_type = PrimitiveScalarType::from_i32(*i32_enum).ok_or_else(|| {
                    PlanSerDeError::General(format!(
                        "Invalid i32_enum={} when converting with PrimitiveScalarType::from_i32()",
                        *i32_enum
                    ))
                })?;
                let scalar_value: ScalarValue = match pb_scalar_type {
                    PrimitiveScalarType::Bool => ScalarValue::Boolean(None),
                    PrimitiveScalarType::Uint8 => ScalarValue::UInt8(None),
                    PrimitiveScalarType::Int8 => ScalarValue::Int8(None),
                    PrimitiveScalarType::Uint16 => ScalarValue::UInt16(None),
                    PrimitiveScalarType::Int16 => ScalarValue::Int16(None),
                    PrimitiveScalarType::Uint32 => ScalarValue::UInt32(None),
                    PrimitiveScalarType::Int32 => ScalarValue::Int32(None),
                    PrimitiveScalarType::Uint64 => ScalarValue::UInt64(None),
                    PrimitiveScalarType::Int64 => ScalarValue::Int64(None),
                    PrimitiveScalarType::Float32 => ScalarValue::Float32(None),
                    PrimitiveScalarType::Float64 => ScalarValue::Float64(None),
                    PrimitiveScalarType::Utf8 => ScalarValue::Utf8(None),
                    PrimitiveScalarType::LargeUtf8 => ScalarValue::LargeUtf8(None),
                    PrimitiveScalarType::Date32 => ScalarValue::Date32(None),
                    PrimitiveScalarType::TimeMicrosecond => {
                        ScalarValue::TimestampMicrosecond(None, None)
                    }
                    PrimitiveScalarType::TimeNanosecond => {
                        ScalarValue::TimestampNanosecond(None, None)
                    }
                    PrimitiveScalarType::Null => {
                        return Err(proto_error(
                            "Untyped scalar null is not a valid scalar value",
                        ))
                    }
                };
                scalar_value
            } else {
                return Err(proto_error("Could not convert to the proper type"));
            }
        }
        _ => return Err(proto_error("Could not convert to the proper type")),
    })
}

impl TryInto<datafusion::scalar::ScalarValue> for protobuf::PrimitiveScalarType {
    type Error = PlanSerDeError;
    fn try_into(self) -> Result<datafusion::scalar::ScalarValue, Self::Error> {
        Ok(match self {
            protobuf::PrimitiveScalarType::Null => {
                return Err(proto_error("Untyped null is an invalid scalar value"))
            }
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
            protobuf::PrimitiveScalarType::TimeMicrosecond => {
                ScalarValue::TimestampMicrosecond(None, None)
            }
            protobuf::PrimitiveScalarType::TimeNanosecond => {
                ScalarValue::TimestampNanosecond(None, None)
            }
        })
    }
}
