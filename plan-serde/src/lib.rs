// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::error::PlanSerDeError;
use crate::protobuf::scalar_type;
use datafusion::arrow::datatypes::{
    DataType, Field, IntervalUnit, Schema, SchemaRef, TimeUnit, UnionMode,
};
use datafusion::logical_plan::{JoinConstraint, Operator};
use datafusion::physical_plan::aggregates::AggregateFunction;
use datafusion::physical_plan::functions::BuiltinScalarFunction;
use datafusion::physical_plan::window_functions::BuiltInWindowFunction;
use datafusion::prelude::JoinType;
use datafusion::scalar::ScalarValue;

// include the generated protobuf source as a submodule
#[allow(clippy::all)]
pub mod protobuf {
    include!(concat!(env!("OUT_DIR"), "/plan.protobuf.rs"));
}

pub mod error;
pub mod execution_plans;
pub mod from_proto;
pub mod to_proto;

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

impl From<protobuf::AggregateFunction> for AggregateFunction {
    fn from(agg_fun: protobuf::AggregateFunction) -> AggregateFunction {
        match agg_fun {
            protobuf::AggregateFunction::Min => AggregateFunction::Min,
            protobuf::AggregateFunction::Max => AggregateFunction::Max,
            protobuf::AggregateFunction::Sum => AggregateFunction::Sum,
            protobuf::AggregateFunction::Avg => AggregateFunction::Avg,
            protobuf::AggregateFunction::Count => AggregateFunction::Count,
            protobuf::AggregateFunction::ApproxDistinct => AggregateFunction::ApproxDistinct,
            protobuf::AggregateFunction::ArrayAgg => AggregateFunction::ArrayAgg,
            protobuf::AggregateFunction::Variance => AggregateFunction::Variance,
            protobuf::AggregateFunction::VariancePop => AggregateFunction::VariancePop,
            protobuf::AggregateFunction::Covariance => AggregateFunction::Covariance,
            protobuf::AggregateFunction::CovariancePop => AggregateFunction::CovariancePop,
            protobuf::AggregateFunction::Stddev => AggregateFunction::Stddev,
            protobuf::AggregateFunction::StddevPop => AggregateFunction::StddevPop,
            protobuf::AggregateFunction::Correlation => AggregateFunction::Correlation,
        }
    }
}

impl From<protobuf::BuiltInWindowFunction> for BuiltInWindowFunction {
    fn from(built_in_function: protobuf::BuiltInWindowFunction) -> Self {
        match built_in_function {
            protobuf::BuiltInWindowFunction::RowNumber => BuiltInWindowFunction::RowNumber,
            protobuf::BuiltInWindowFunction::Rank => BuiltInWindowFunction::Rank,
            protobuf::BuiltInWindowFunction::PercentRank => BuiltInWindowFunction::PercentRank,
            protobuf::BuiltInWindowFunction::DenseRank => BuiltInWindowFunction::DenseRank,
            protobuf::BuiltInWindowFunction::Lag => BuiltInWindowFunction::Lag,
            protobuf::BuiltInWindowFunction::Lead => BuiltInWindowFunction::Lead,
            protobuf::BuiltInWindowFunction::FirstValue => BuiltInWindowFunction::FirstValue,
            protobuf::BuiltInWindowFunction::CumeDist => BuiltInWindowFunction::CumeDist,
            protobuf::BuiltInWindowFunction::Ntile => BuiltInWindowFunction::Ntile,
            protobuf::BuiltInWindowFunction::NthValue => BuiltInWindowFunction::NthValue,
            protobuf::BuiltInWindowFunction::LastValue => BuiltInWindowFunction::LastValue,
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

    pub fn from_i32_to_arrow(interval_unit_i32: i32) -> Result<IntervalUnit, PlanSerDeError> {
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

impl TryInto<datafusion::arrow::datatypes::DataType> for &protobuf::arrow_type::ArrowTypeEnum {
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
            arrow_type::ArrowTypeEnum::FixedSizeBinary(size) => DataType::FixedSizeBinary(*size),
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
            arrow_type::ArrowTypeEnum::Interval(interval_unit) => {
                DataType::Interval(protobuf::IntervalUnit::from_i32_to_arrow(*interval_unit)?)
            }
            arrow_type::ArrowTypeEnum::Decimal(protobuf::Decimal { whole, fractional }) => {
                DataType::Decimal(*whole as usize, *fractional as usize)
            }
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
            arrow_type::ArrowTypeEnum::Union(union) => {
                let union_mode =
                    protobuf::UnionMode::from_i32(union.union_mode).ok_or_else(|| {
                        proto_error("Protobuf deserialization error: Unknown union mode type")
                    })?;
                let union_mode = match union_mode {
                    protobuf::UnionMode::Dense => UnionMode::Dense,
                    protobuf::UnionMode::Sparse => UnionMode::Sparse,
                };
                let union_types = union
                    .union_types
                    .iter()
                    .map(|field| field.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                DataType::Union(union_types, union_mode)
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
            protobuf::PrimitiveScalarType::TimeNanosecond => DataType::Time64(TimeUnit::Nanosecond),
            protobuf::PrimitiveScalarType::Null => DataType::Null,
        }
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

impl From<JoinType> for protobuf::JoinType {
    fn from(t: JoinType) -> Self {
        match t {
            JoinType::Inner => protobuf::JoinType::Inner,
            JoinType::Left => protobuf::JoinType::Left,
            JoinType::Right => protobuf::JoinType::Right,
            JoinType::Full => protobuf::JoinType::Full,
            JoinType::Semi => protobuf::JoinType::Semi,
            JoinType::Anti => protobuf::JoinType::Anti,
        }
    }
}

impl From<protobuf::JoinConstraint> for JoinConstraint {
    fn from(t: protobuf::JoinConstraint) -> Self {
        match t {
            protobuf::JoinConstraint::On => JoinConstraint::On,
            protobuf::JoinConstraint::Using => JoinConstraint::Using,
        }
    }
}

impl From<JoinConstraint> for protobuf::JoinConstraint {
    fn from(t: JoinConstraint) -> Self {
        match t {
            JoinConstraint::On => protobuf::JoinConstraint::On,
            JoinConstraint::Using => protobuf::JoinConstraint::Using,
        }
    }
}

impl TryFrom<&DataType> for protobuf::ScalarType {
    type Error = PlanSerDeError;
    fn try_from(value: &DataType) -> Result<Self, Self::Error> {
        let datatype = protobuf::scalar_type::Datatype::try_from(value)?;
        Ok(protobuf::ScalarType {
            datatype: Some(datatype),
        })
    }
}

impl TryFrom<&DataType> for protobuf::scalar_type::Datatype {
    type Error = PlanSerDeError;
    fn try_from(val: &DataType) -> Result<Self, Self::Error> {
        use protobuf::PrimitiveScalarType;
        let scalar_value = match val {
            DataType::Boolean => scalar_type::Datatype::Scalar(PrimitiveScalarType::Bool as i32),
            DataType::Int8 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Int8 as i32),
            DataType::Int16 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Int16 as i32),
            DataType::Int32 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Int32 as i32),
            DataType::Int64 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Int64 as i32),
            DataType::UInt8 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Uint8 as i32),
            DataType::UInt16 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Uint16 as i32),
            DataType::UInt32 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Uint32 as i32),
            DataType::UInt64 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Uint64 as i32),
            DataType::Float32 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Float32 as i32),
            DataType::Float64 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Float64 as i32),
            DataType::Date32 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Date32 as i32),
            DataType::Time64(time_unit) => match time_unit {
                TimeUnit::Microsecond => scalar_type::Datatype::Scalar(PrimitiveScalarType::TimeMicrosecond as i32),
                TimeUnit::Nanosecond => scalar_type::Datatype::Scalar(PrimitiveScalarType::TimeNanosecond as i32),
                _ => {
                    return Err(proto_error(format!(
                        "Found invalid time unit for scalar value, only TimeUnit::Microsecond and TimeUnit::Nanosecond are valid time units: {:?}",
                        time_unit
                    )))
                }
            },
            DataType::Utf8 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Utf8 as i32),
            DataType::LargeUtf8 => scalar_type::Datatype::Scalar(PrimitiveScalarType::LargeUtf8 as i32),
            DataType::List(field_type) => {
                let mut field_names: Vec<String> = Vec::new();
                let mut curr_field = field_type.as_ref();
                field_names.push(curr_field.name().to_owned());
                //For each nested field check nested datatype, since datafusion scalars only support recursive lists with a leaf scalar type
                // any other compound types are errors.

                while let DataType::List(nested_field_type) = curr_field.data_type() {
                    curr_field = nested_field_type.as_ref();
                    field_names.push(curr_field.name().to_owned());
                    if !is_valid_scalar_type_no_list_check(curr_field.data_type()) {
                        return Err(proto_error(format!("{:?} is an invalid scalar type", curr_field)));
                    }
                }
                let deepest_datatype = curr_field.data_type();
                if !is_valid_scalar_type_no_list_check(deepest_datatype) {
                    return Err(proto_error(format!("The list nested type {:?} is an invalid scalar type", curr_field)));
                }
                let pb_deepest_type: PrimitiveScalarType = match deepest_datatype {
                    DataType::Boolean => PrimitiveScalarType::Bool,
                    DataType::Int8 => PrimitiveScalarType::Int8,
                    DataType::Int16 => PrimitiveScalarType::Int16,
                    DataType::Int32 => PrimitiveScalarType::Int32,
                    DataType::Int64 => PrimitiveScalarType::Int64,
                    DataType::UInt8 => PrimitiveScalarType::Uint8,
                    DataType::UInt16 => PrimitiveScalarType::Uint16,
                    DataType::UInt32 => PrimitiveScalarType::Uint32,
                    DataType::UInt64 => PrimitiveScalarType::Uint64,
                    DataType::Float32 => PrimitiveScalarType::Float32,
                    DataType::Float64 => PrimitiveScalarType::Float64,
                    DataType::Date32 => PrimitiveScalarType::Date32,
                    DataType::Time64(time_unit) => match time_unit {
                        TimeUnit::Microsecond => PrimitiveScalarType::TimeMicrosecond,
                        TimeUnit::Nanosecond => PrimitiveScalarType::TimeNanosecond,
                        _ => {
                            return Err(proto_error(format!(
                                "Found invalid time unit for scalar value, only TimeUnit::Microsecond and TimeUnit::Nanosecond are valid time units: {:?}",
                                time_unit
                            )))
                        }
                    },

                    DataType::Utf8 => PrimitiveScalarType::Utf8,
                    DataType::LargeUtf8 => PrimitiveScalarType::LargeUtf8,
                    _ => {
                        return Err(proto_error(format!(
                            "Error converting to Datatype to scalar type, {:?} is invalid as a datafusion scalar.",
                            val
                        )))
                    }
                };
                protobuf::scalar_type::Datatype::List(protobuf::ScalarListType {
                    field_names,
                    deepest_type: pb_deepest_type as i32,
                })
            }
            DataType::Null
            | DataType::Float16
            | DataType::Timestamp(_, _)
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::LargeBinary
            | DataType::FixedSizeList(_, _)
            | DataType::LargeList(_)
            | DataType::Struct(_)
            | DataType::Union(_, _)
            | DataType::Dictionary(_, _)
            | DataType::Map(_, _)
            | DataType::Decimal(_, _) => {
                return Err(proto_error(format!(
                    "Error converting to Datatype to scalar type, {:?} is invalid as a datafusion scalar.",
                    val
                )))
            }
        };
        Ok(scalar_value)
    }
}

//Does not check if list subtypes are valid
fn is_valid_scalar_type_no_list_check(datatype: &DataType) -> bool {
    match datatype {
        DataType::Boolean
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
        | DataType::LargeUtf8
        | DataType::Utf8
        | DataType::Date32 => true,
        DataType::Time64(time_unit) => {
            matches!(time_unit, TimeUnit::Microsecond | TimeUnit::Nanosecond)
        }

        DataType::List(_) => true,
        _ => false,
    }
}

impl TryFrom<&datafusion::scalar::ScalarValue> for protobuf::ScalarValue {
    type Error = PlanSerDeError;
    fn try_from(
        val: &datafusion::scalar::ScalarValue,
    ) -> Result<protobuf::ScalarValue, Self::Error> {
        use datafusion::scalar;
        use protobuf::scalar_value::Value;
        use protobuf::PrimitiveScalarType;
        let scalar_val = match val {
            scalar::ScalarValue::Boolean(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Bool, |s| Value::BoolValue(*s))
            }
            scalar::ScalarValue::Float32(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Float32, |s| {
                    Value::Float32Value(*s)
                })
            }
            scalar::ScalarValue::Float64(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Float64, |s| {
                    Value::Float64Value(*s)
                })
            }
            scalar::ScalarValue::Int8(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Int8, |s| {
                    Value::Int8Value(*s as i32)
                })
            }
            scalar::ScalarValue::Int16(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Int16, |s| {
                    Value::Int16Value(*s as i32)
                })
            }
            scalar::ScalarValue::Int32(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Int32, |s| Value::Int32Value(*s))
            }
            scalar::ScalarValue::Int64(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Int64, |s| Value::Int64Value(*s))
            }
            scalar::ScalarValue::UInt8(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Uint8, |s| {
                    Value::Uint8Value(*s as u32)
                })
            }
            scalar::ScalarValue::UInt16(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Uint16, |s| {
                    Value::Uint16Value(*s as u32)
                })
            }
            scalar::ScalarValue::UInt32(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Uint32, |s| Value::Uint32Value(*s))
            }
            scalar::ScalarValue::UInt64(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Uint64, |s| Value::Uint64Value(*s))
            }
            scalar::ScalarValue::Utf8(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Utf8, |s| {
                    Value::Utf8Value(s.to_owned())
                })
            }
            scalar::ScalarValue::LargeUtf8(val) => {
                create_proto_scalar(val, PrimitiveScalarType::LargeUtf8, |s| {
                    Value::LargeUtf8Value(s.to_owned())
                })
            }
            scalar::ScalarValue::List(value, datatype) => {
                println!("Current datatype of list: {:?}", datatype);
                match value {
                    Some(values) => {
                        if values.is_empty() {
                            protobuf::ScalarValue {
                                value: Some(protobuf::scalar_value::Value::ListValue(
                                    protobuf::ScalarListValue {
                                        datatype: Some(datatype.as_ref().try_into()?),
                                        values: Vec::new(),
                                    },
                                )),
                            }
                        } else {
                            let scalar_type = match datatype.as_ref() {
                                DataType::List(field) => field.as_ref().data_type(),
                                _ => todo!("Proper error handling"),
                            };
                            println!("Current scalar type for list: {:?}", scalar_type);
                            let type_checked_values: Vec<protobuf::ScalarValue> = values
                                .iter()
                                .map(|scalar| match (scalar, scalar_type) {
                                    (scalar::ScalarValue::List(_, list_type), DataType::List(field)) => {
                                        if let DataType::List(list_field) = list_type.as_ref() {
                                            let scalar_datatype = field.data_type();
                                            let list_datatype = list_field.data_type();
                                            if std::mem::discriminant(list_datatype) != std::mem::discriminant(scalar_datatype) {
                                                return Err(proto_error(format!(
                                                    "Protobuf serialization error: Lists with inconsistent typing {:?} and {:?} found within list",
                                                    list_datatype, scalar_datatype
                                                )));
                                            }
                                            scalar.try_into()
                                        } else {
                                            Err(proto_error(format!(
                                                "Protobuf serialization error, {:?} was inconsistent with designated type {:?}",
                                                scalar, datatype
                                            )))
                                        }
                                    }
                                    (scalar::ScalarValue::Boolean(_), DataType::Boolean) => scalar.try_into(),
                                    (scalar::ScalarValue::Float32(_), DataType::Float32) => scalar.try_into(),
                                    (scalar::ScalarValue::Float64(_), DataType::Float64) => scalar.try_into(),
                                    (scalar::ScalarValue::Int8(_), DataType::Int8) => scalar.try_into(),
                                    (scalar::ScalarValue::Int16(_), DataType::Int16) => scalar.try_into(),
                                    (scalar::ScalarValue::Int32(_), DataType::Int32) => scalar.try_into(),
                                    (scalar::ScalarValue::Int64(_), DataType::Int64) => scalar.try_into(),
                                    (scalar::ScalarValue::UInt8(_), DataType::UInt8) => scalar.try_into(),
                                    (scalar::ScalarValue::UInt16(_), DataType::UInt16) => scalar.try_into(),
                                    (scalar::ScalarValue::UInt32(_), DataType::UInt32) => scalar.try_into(),
                                    (scalar::ScalarValue::UInt64(_), DataType::UInt64) => scalar.try_into(),
                                    (scalar::ScalarValue::Utf8(_), DataType::Utf8) => scalar.try_into(),
                                    (scalar::ScalarValue::LargeUtf8(_), DataType::LargeUtf8) => scalar.try_into(),
                                    _ => Err(proto_error(format!(
                                        "Protobuf serialization error, {:?} was inconsistent with designated type {:?}",
                                        scalar, datatype
                                    ))),
                                })
                                .collect::<Result<Vec<_>, _>>()?;
                            protobuf::ScalarValue {
                                value: Some(protobuf::scalar_value::Value::ListValue(
                                    protobuf::ScalarListValue {
                                        datatype: Some(datatype.as_ref().try_into()?),
                                        values: type_checked_values,
                                    },
                                )),
                            }
                        }
                    }
                    None => protobuf::ScalarValue {
                        value: Some(protobuf::scalar_value::Value::NullListValue(
                            datatype.as_ref().try_into()?,
                        )),
                    },
                }
            }
            datafusion::scalar::ScalarValue::Date32(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Date32, |s| Value::Date32Value(*s))
            }
            datafusion::scalar::ScalarValue::TimestampMicrosecond(val, _) => {
                create_proto_scalar(val, PrimitiveScalarType::TimeMicrosecond, |s| {
                    Value::TimeMicrosecondValue(*s)
                })
            }
            datafusion::scalar::ScalarValue::TimestampNanosecond(val, _) => {
                create_proto_scalar(val, PrimitiveScalarType::TimeNanosecond, |s| {
                    Value::TimeNanosecondValue(*s)
                })
            }
            _ => {
                return Err(proto_error(format!(
                    "Error converting to Datatype to scalar type, {:?} is invalid as a datafusion scalar.",
                    val
                )))
            }
        };
        Ok(scalar_val)
    }
}

impl From<&Field> for protobuf::Field {
    fn from(field: &Field) -> Self {
        protobuf::Field {
            name: field.name().to_owned(),
            arrow_type: Some(Box::new(field.data_type().into())),
            nullable: field.is_nullable(),
            children: Vec::new(),
        }
    }
}

impl From<&DataType> for protobuf::ArrowType {
    fn from(val: &DataType) -> protobuf::ArrowType {
        protobuf::ArrowType {
            arrow_type_enum: Some(val.into()),
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

fn create_proto_scalar<I, T: FnOnce(&I) -> protobuf::scalar_value::Value>(
    v: &Option<I>,
    null_arrow_type: protobuf::PrimitiveScalarType,
    constructor: T,
) -> protobuf::ScalarValue {
    protobuf::ScalarValue {
        value: Some(v.as_ref().map(constructor).unwrap_or(
            protobuf::scalar_value::Value::NullValue(null_arrow_type as i32),
        )),
    }
}

#[allow(clippy::from_over_into)]
impl Into<protobuf::Schema> for &Schema {
    fn into(self) -> protobuf::Schema {
        protobuf::Schema {
            columns: self
                .fields()
                .iter()
                .map(protobuf::Field::from)
                .collect::<Vec<_>>(),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<protobuf::Schema> for SchemaRef {
    fn into(self) -> protobuf::Schema {
        protobuf::Schema {
            columns: self
                .fields()
                .iter()
                .map(protobuf::Field::from)
                .collect::<Vec<_>>(),
        }
    }
}

impl TryInto<protobuf::ScalarFunction> for &BuiltinScalarFunction {
    type Error = PlanSerDeError;
    fn try_into(self) -> Result<protobuf::ScalarFunction, Self::Error> {
        match self {
            BuiltinScalarFunction::Sqrt => Ok(protobuf::ScalarFunction::Sqrt),
            BuiltinScalarFunction::Sin => Ok(protobuf::ScalarFunction::Sin),
            BuiltinScalarFunction::Cos => Ok(protobuf::ScalarFunction::Cos),
            BuiltinScalarFunction::Tan => Ok(protobuf::ScalarFunction::Tan),
            BuiltinScalarFunction::Asin => Ok(protobuf::ScalarFunction::Asin),
            BuiltinScalarFunction::Acos => Ok(protobuf::ScalarFunction::Acos),
            BuiltinScalarFunction::Atan => Ok(protobuf::ScalarFunction::Atan),
            BuiltinScalarFunction::Exp => Ok(protobuf::ScalarFunction::Exp),
            BuiltinScalarFunction::Log => Ok(protobuf::ScalarFunction::Log),
            BuiltinScalarFunction::Ln => Ok(protobuf::ScalarFunction::Ln),
            BuiltinScalarFunction::Log10 => Ok(protobuf::ScalarFunction::Log10),
            BuiltinScalarFunction::Floor => Ok(protobuf::ScalarFunction::Floor),
            BuiltinScalarFunction::Ceil => Ok(protobuf::ScalarFunction::Ceil),
            BuiltinScalarFunction::Round => Ok(protobuf::ScalarFunction::Round),
            BuiltinScalarFunction::Trunc => Ok(protobuf::ScalarFunction::Trunc),
            BuiltinScalarFunction::Abs => Ok(protobuf::ScalarFunction::Abs),
            BuiltinScalarFunction::OctetLength => Ok(protobuf::ScalarFunction::Octetlength),
            BuiltinScalarFunction::Concat => Ok(protobuf::ScalarFunction::Concat),
            BuiltinScalarFunction::Lower => Ok(protobuf::ScalarFunction::Lower),
            BuiltinScalarFunction::Upper => Ok(protobuf::ScalarFunction::Upper),
            BuiltinScalarFunction::Trim => Ok(protobuf::ScalarFunction::Trim),
            BuiltinScalarFunction::Ltrim => Ok(protobuf::ScalarFunction::Ltrim),
            BuiltinScalarFunction::Rtrim => Ok(protobuf::ScalarFunction::Rtrim),
            BuiltinScalarFunction::ToTimestamp => Ok(protobuf::ScalarFunction::Totimestamp),
            BuiltinScalarFunction::Array => Ok(protobuf::ScalarFunction::Array),
            BuiltinScalarFunction::NullIf => Ok(protobuf::ScalarFunction::Nullif),
            BuiltinScalarFunction::DatePart => Ok(protobuf::ScalarFunction::Datepart),
            BuiltinScalarFunction::DateTrunc => Ok(protobuf::ScalarFunction::Datetrunc),
            BuiltinScalarFunction::MD5 => Ok(protobuf::ScalarFunction::Md5),
            BuiltinScalarFunction::SHA224 => Ok(protobuf::ScalarFunction::Sha224),
            BuiltinScalarFunction::SHA256 => Ok(protobuf::ScalarFunction::Sha256),
            BuiltinScalarFunction::SHA384 => Ok(protobuf::ScalarFunction::Sha384),
            BuiltinScalarFunction::SHA512 => Ok(protobuf::ScalarFunction::Sha512),
            BuiltinScalarFunction::Digest => Ok(protobuf::ScalarFunction::Digest),
            BuiltinScalarFunction::ToTimestampMillis => {
                Ok(protobuf::ScalarFunction::Totimestampmillis)
            }
            _ => Err(PlanSerDeError::General(format!(
                "logical_plan::to_proto() unsupported scalar function {:?}",
                self
            ))),
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
            protobuf::scalar_value::Value::TimeMicrosecondValue(v) => {
                ScalarValue::TimestampMicrosecond(Some(*v), None)
            }
            protobuf::scalar_value::Value::TimeNanosecondValue(v) => {
                ScalarValue::TimestampNanosecond(Some(*v), None)
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
                ScalarValue::List(Some(Box::new(typechecked_values)), scalar_type)
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
        let pb_scalartype = self
            .datatype
            .as_ref()
            .ok_or_else(|| proto_error("ScalarType message missing required field 'datatype'"))?;
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
                let pb_scalar_type = protobuf::PrimitiveScalarType::from_i32(*deepest_type)
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
                    let new_datatype =
                        DataType::List(Box::new(Field::new(name.as_str(), scalar_type, true)));
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
                let leaf_scalar_type = protobuf::PrimitiveScalarType::from_i32(*scalar_type_i32)
                    .ok_or_else(|| proto_error("Error converting i32 to basic scalar type"))?;
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
                    Some(Box::new(typechecked_values)),
                    Box::new(leaf_scalar_type.into()),
                )
            }
            Datatype::List(list_type) => {
                let protobuf::ScalarListType {
                    deepest_type,
                    field_names,
                } = &list_type;
                let leaf_type = PrimitiveScalarType::from_i32(*deepest_type)
                    .ok_or_else(|| proto_error("Error converting i32 to basic scalar type"))?;
                let depth = field_names.len();

                let typechecked_values: Vec<datafusion::scalar::ScalarValue> = if depth == 0 {
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
                        _ => Some(Box::new(typechecked_values)),
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
                .ok_or_else(|| proto_error("Could not convert to datafusion scalar type"))?
                .into(),
            true,
        )));
        //Iterates over field names in reverse order except for the last item in the vector
        for name in field_names.iter().rev().skip(1) {
            let temp_curr_type = DataType::List(Box::new(Field::new(name, curr_type, true)));
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
        (Value::BoolValue(v), PrimitiveScalarType::Bool) => ScalarValue::Boolean(Some(*v)),
        (Value::Int8Value(v), PrimitiveScalarType::Int8) => ScalarValue::Int8(Some(*v as i8)),
        (Value::Int16Value(v), PrimitiveScalarType::Int16) => ScalarValue::Int16(Some(*v as i16)),
        (Value::Int32Value(v), PrimitiveScalarType::Int32) => ScalarValue::Int32(Some(*v)),
        (Value::Int64Value(v), PrimitiveScalarType::Int64) => ScalarValue::Int64(Some(*v)),
        (Value::Uint8Value(v), PrimitiveScalarType::Uint8) => ScalarValue::UInt8(Some(*v as u8)),
        (Value::Uint16Value(v), PrimitiveScalarType::Uint16) => {
            ScalarValue::UInt16(Some(*v as u16))
        }
        (Value::Uint32Value(v), PrimitiveScalarType::Uint32) => ScalarValue::UInt32(Some(*v)),
        (Value::Uint64Value(v), PrimitiveScalarType::Uint64) => ScalarValue::UInt64(Some(*v)),
        (Value::Float32Value(v), PrimitiveScalarType::Float32) => ScalarValue::Float32(Some(*v)),
        (Value::Float64Value(v), PrimitiveScalarType::Float64) => ScalarValue::Float64(Some(*v)),
        (Value::Date32Value(v), PrimitiveScalarType::Date32) => ScalarValue::Date32(Some(*v)),
        (Value::TimeMicrosecondValue(v), PrimitiveScalarType::TimeMicrosecond) => {
            ScalarValue::TimestampMicrosecond(Some(*v), None)
        }
        (Value::TimeNanosecondValue(v), PrimitiveScalarType::TimeMicrosecond) => {
            ScalarValue::TimestampNanosecond(Some(*v), None)
        }
        (Value::Utf8Value(v), PrimitiveScalarType::Utf8) => ScalarValue::Utf8(Some(v.to_owned())),
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

fn byte_to_string(b: u8) -> Result<String, PlanSerDeError> {
    let b = &[b];
    let b = std::str::from_utf8(b)
        .map_err(|_| PlanSerDeError::General("Invalid CSV delimiter".to_owned()))?;
    Ok(b.to_owned())
}

fn str_to_byte(s: &str) -> Result<u8, PlanSerDeError> {
    if s.len() != 1 {
        return Err(PlanSerDeError::General("Invalid CSV delimiter".to_owned()));
    }
    Ok(s.as_bytes()[0])
}

#[cfg(test)]
mod roundtrip_tests {
    use std::{convert::TryInto, sync::Arc};

    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::{
        arrow::{
            compute::kernels::sort::SortOptions,
            datatypes::{DataType, Field, Schema},
        },
        logical_plan::{JoinType, Operator},
        physical_plan::{
            empty::EmptyExec,
            expressions::{binary, col, lit, InListExpr, NotExpr},
            expressions::{Avg, Column, PhysicalSortExpr},
            filter::FilterExec,
            hash_aggregate::{AggregateMode, HashAggregateExec},
            hash_join::{HashJoinExec, PartitionMode},
            limit::{GlobalLimitExec, LocalLimitExec},
            AggregateExpr, ExecutionPlan, Partitioning, PhysicalExpr,
        },
        scalar::ScalarValue,
    };

    use crate::error::Result;
    use crate::execution_plans::ShuffleWriterExec;
    use crate::protobuf;

    fn roundtrip_test(exec_plan: Arc<dyn ExecutionPlan>) -> Result<()> {
        let proto: protobuf::PhysicalPlanNode = exec_plan.clone().try_into()?;
        let result_exec_plan: Arc<dyn ExecutionPlan> = (&proto).try_into()?;
        assert_eq!(
            format!("{:?}", exec_plan),
            format!("{:?}", result_exec_plan)
        );
        Ok(())
    }

    #[test]
    fn roundtrip_empty() -> Result<()> {
        roundtrip_test(Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))))
    }

    #[test]
    fn roundtrip_local_limit() -> Result<()> {
        roundtrip_test(Arc::new(LocalLimitExec::new(
            Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))),
            25,
        )))
    }

    #[test]
    fn roundtrip_global_limit() -> Result<()> {
        roundtrip_test(Arc::new(GlobalLimitExec::new(
            Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))),
            25,
        )))
    }

    #[test]
    fn roundtrip_hash_join() -> Result<()> {
        let field_a = Field::new("col", DataType::Int64, false);
        let schema_left = Schema::new(vec![field_a.clone()]);
        let schema_right = Schema::new(vec![field_a]);
        let on = vec![(
            Column::new("col", schema_left.index_of("col")?),
            Column::new("col", schema_right.index_of("col")?),
        )];

        let schema_left = Arc::new(schema_left);
        let schema_right = Arc::new(schema_right);
        for join_type in &[
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::Anti,
            JoinType::Semi,
        ] {
            for partition_mode in &[PartitionMode::Partitioned, PartitionMode::CollectLeft] {
                roundtrip_test(Arc::new(HashJoinExec::try_new(
                    Arc::new(EmptyExec::new(false, schema_left.clone())),
                    Arc::new(EmptyExec::new(false, schema_right.clone())),
                    on.clone(),
                    join_type,
                    *partition_mode,
                    &false,
                )?))?;
            }
        }
        Ok(())
    }

    #[test]
    fn rountrip_hash_aggregate() -> Result<()> {
        let field_a = Field::new("a", DataType::Int64, false);
        let field_b = Field::new("b", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b]));

        let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
            vec![(col("a", &schema)?, "unused".to_string())];

        let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Avg::new(
            col("b", &schema)?,
            "AVG(b)".to_string(),
            DataType::Float64,
        ))];

        roundtrip_test(Arc::new(HashAggregateExec::try_new(
            AggregateMode::Final,
            groups.clone(),
            aggregates.clone(),
            Arc::new(EmptyExec::new(false, schema.clone())),
            schema,
        )?))
    }

    #[test]
    fn roundtrip_filter_with_not_and_in_list() -> Result<()> {
        let field_a = Field::new("a", DataType::Boolean, false);
        let field_b = Field::new("b", DataType::Int64, false);
        let field_c = Field::new("c", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b, field_c]));
        let not = Arc::new(NotExpr::new(col("a", &schema)?));
        let in_list = Arc::new(InListExpr::new(
            col("b", &schema)?,
            vec![
                lit(ScalarValue::Int64(Some(1))),
                lit(ScalarValue::Int64(Some(2))),
            ],
            false,
        ));
        let and = binary(not, Operator::And, in_list, &schema)?;
        roundtrip_test(Arc::new(FilterExec::try_new(
            and,
            Arc::new(EmptyExec::new(false, schema.clone())),
        )?))
    }

    #[test]
    fn roundtrip_sort() -> Result<()> {
        let field_a = Field::new("a", DataType::Boolean, false);
        let field_b = Field::new("b", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b]));
        let sort_exprs = vec![
            PhysicalSortExpr {
                expr: col("a", &schema)?,
                options: SortOptions {
                    descending: true,
                    nulls_first: false,
                },
            },
            PhysicalSortExpr {
                expr: col("b", &schema)?,
                options: SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            },
        ];
        roundtrip_test(Arc::new(SortExec::try_new(
            sort_exprs,
            Arc::new(EmptyExec::new(false, schema)),
        )?))
    }

    #[test]
    fn roundtrip_shuffle_writer() -> Result<()> {
        let field_a = Field::new("a", DataType::Int64, false);
        let field_b = Field::new("b", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b]));

        roundtrip_test(Arc::new(ShuffleWriterExec::try_new(
            "job123".to_string(),
            123,
            Arc::new(EmptyExec::new(false, schema)),
            "".to_string(),
            Some(Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 4)),
        )?))
    }
}
