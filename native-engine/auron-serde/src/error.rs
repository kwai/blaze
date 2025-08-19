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

//! Plan serde error types

use std::{
    error::Error,
    fmt::{Display, Formatter},
    io, result,
};

use arrow::error::ArrowError;
use datafusion::error::DataFusionError;

pub type Result<T> = result::Result<T, PlanSerDeError>;

#[derive(Debug)]
pub enum PlanSerDeError {
    NotImplemented(String),
    General(String),
    Internal(String),
    ArrowError(ArrowError),
    DataFusionError(DataFusionError),
    IoError(io::Error),
    MissingRequiredField(String),
    UnknownEnumVariant { name: String, value: i32 },
}

#[allow(clippy::from_over_into)]
impl<T> Into<Result<T>> for PlanSerDeError {
    fn into(self) -> Result<T> {
        Err(self)
    }
}

pub fn serde_error(message: &str) -> PlanSerDeError {
    PlanSerDeError::General(message.to_owned())
}

impl From<String> for PlanSerDeError {
    fn from(e: String) -> Self {
        PlanSerDeError::General(e)
    }
}

impl From<ArrowError> for PlanSerDeError {
    fn from(e: ArrowError) -> Self {
        PlanSerDeError::ArrowError(e)
    }
}

impl From<DataFusionError> for PlanSerDeError {
    fn from(e: DataFusionError) -> Self {
        PlanSerDeError::DataFusionError(e)
    }
}

impl From<io::Error> for PlanSerDeError {
    fn from(e: io::Error) -> Self {
        PlanSerDeError::IoError(e)
    }
}

impl Display for PlanSerDeError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            PlanSerDeError::NotImplemented(desc) => {
                write!(f, "Not implemented: {}", desc)
            }
            PlanSerDeError::General(desc) => write!(f, "General error: {}", desc),
            PlanSerDeError::ArrowError(desc) => write!(f, "Arrow error: {}", desc),
            PlanSerDeError::DataFusionError(desc) => {
                write!(f, "DataFusion error: {:?}", desc)
            }
            PlanSerDeError::IoError(desc) => write!(f, "IO error: {}", desc),
            PlanSerDeError::Internal(desc) => {
                write!(f, "Internal error: {}", desc)
            }
            Self::MissingRequiredField(name) => {
                write!(f, "Missing required field {}", name)
            }
            Self::UnknownEnumVariant { name, value } => {
                write!(f, "Unknown i32 value for {} enum: {}", name, value)
            }
        }
    }
}

impl Error for PlanSerDeError {}

impl PlanSerDeError {
    pub(crate) fn required(field: impl Into<String>) -> PlanSerDeError {
        PlanSerDeError::MissingRequiredField(field.into())
    }
}

/// An extension trait that adds the methods `optional` and `required` to any
/// Option containing a type implementing `TryInto<U, Error = Error>`
pub trait FromOptionalField<T> {
    /// Converts an optional protobuf field to an option of a different type
    ///
    /// Returns None if the option is None, otherwise calls [`FromField::field`]
    /// on the contained data, returning any error encountered
    fn optional(self) -> std::result::Result<Option<T>, PlanSerDeError>;

    /// Converts an optional protobuf field to a different type, returning an
    /// error if None
    ///
    /// Returns `Error::MissingRequiredField` if None, otherwise calls
    /// [`FromField::field`] on the contained data, returning any error
    /// encountered
    fn required(self, field: impl Into<String>) -> std::result::Result<T, PlanSerDeError>;
}

impl<T, U> FromOptionalField<U> for Option<T>
where
    T: TryInto<U, Error = PlanSerDeError>,
{
    fn optional(self) -> std::result::Result<Option<U>, PlanSerDeError> {
        self.map(|t| t.try_into()).transpose()
    }

    fn required(self, field: impl Into<String>) -> std::result::Result<U, PlanSerDeError> {
        match self {
            None => Err(PlanSerDeError::required(field)),
            Some(t) => t.try_into(),
        }
    }
}
