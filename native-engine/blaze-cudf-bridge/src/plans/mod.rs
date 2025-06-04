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

use arrow::error::Result;

use crate::{ffi::*, tables::CudfTableStream, AsRawPtrAddr};

pub mod filter;
pub mod parquet_scan;
pub mod project;
pub mod split_table;
pub mod union;

pub type CudfPlanRef = Arc<dyn CudfPlan>;

pub trait CudfPlan: AsRawPtrAddr + Send + Sync + 'static {
    fn execute(&self) -> Result<CudfTableStream> {
        let raw_table_stream_result = unsafe { ffi_execute_plan(self.as_raw_ptr_addr()) };
        let raw_table_stream =
            raw_table_stream_result.into_result(format!("error execute plan"))?;
        Ok(CudfTableStream::from_raw_table_stream(raw_table_stream))
    }
}
