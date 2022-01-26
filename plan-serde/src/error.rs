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

//! Plan serde error types

use std::{
    error::Error,
    fmt::{Display, Formatter},
    io, result,
};

use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;

pub type Result<T> = result::Result<T, PlanSerDeError>;

/// Ballista error
#[derive(Debug)]
pub enum PlanSerDeError {
    NotImplemented(String),
    General(String),
    Internal(String),
    ArrowError(ArrowError),
    DataFusionError(DataFusionError),
    IoError(io::Error),
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
            PlanSerDeError::NotImplemented(ref desc) => {
                write!(f, "Not implemented: {}", desc)
            }
            PlanSerDeError::General(ref desc) => write!(f, "General error: {}", desc),
            PlanSerDeError::ArrowError(ref desc) => write!(f, "Arrow error: {}", desc),
            PlanSerDeError::DataFusionError(ref desc) => {
                write!(f, "DataFusion error: {:?}", desc)
            }
            PlanSerDeError::IoError(ref desc) => write!(f, "IO error: {}", desc),
            PlanSerDeError::Internal(desc) => {
                write!(f, "Internal Ballista error: {}", desc)
            }
        }
    }
}

impl Error for PlanSerDeError {}
