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

use datafusion::common::{DataFusionError, Result};
use datafusion_ext_commons::df_execution_err;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    LeftAnti,
    RightAnti,
    LeftSemi,
    RightSemi,
    Existence,
}

impl TryFrom<JoinType> for datafusion::prelude::JoinType {
    type Error = DataFusionError;

    fn try_from(value: JoinType) -> Result<Self> {
        match value {
            JoinType::Inner => Ok(datafusion::prelude::JoinType::Inner),
            JoinType::Left => Ok(datafusion::prelude::JoinType::Left),
            JoinType::Right => Ok(datafusion::prelude::JoinType::Right),
            JoinType::Full => Ok(datafusion::prelude::JoinType::Full),
            JoinType::LeftAnti => Ok(datafusion::prelude::JoinType::LeftAnti),
            JoinType::RightAnti => Ok(datafusion::prelude::JoinType::RightAnti),
            JoinType::LeftSemi => Ok(datafusion::prelude::JoinType::LeftSemi),
            JoinType::RightSemi => Ok(datafusion::prelude::JoinType::RightSemi),
            other => df_execution_err!("unsupported join type: {other:?}"),
        }
    }
}

impl TryFrom<datafusion::prelude::JoinType> for JoinType {
    type Error = DataFusionError;

    fn try_from(value: datafusion::prelude::JoinType) -> Result<Self> {
        match value {
            datafusion::prelude::JoinType::Inner => Ok(JoinType::Inner),
            datafusion::prelude::JoinType::Left => Ok(JoinType::Left),
            datafusion::prelude::JoinType::Right => Ok(JoinType::Right),
            datafusion::prelude::JoinType::Full => Ok(JoinType::Full),
            datafusion::prelude::JoinType::LeftAnti => Ok(JoinType::LeftAnti),
            datafusion::prelude::JoinType::RightAnti => Ok(JoinType::RightAnti),
            datafusion::prelude::JoinType::LeftSemi => Ok(JoinType::LeftSemi),
            datafusion::prelude::JoinType::RightSemi => Ok(JoinType::RightSemi),
            datafusion::prelude::JoinType::LeftMark => unreachable!(),
            datafusion::prelude::JoinType::RightMark => unreachable!(),
        }
    }
}
