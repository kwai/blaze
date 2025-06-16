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

use crate::{ffi::*, plans::CudfPlan, AsRawPtrAddr};

pub struct CudfSplitTablePlan {
    plan: FFI_PlanPtr,
}

impl AsRawPtrAddr for CudfSplitTablePlan {
    fn as_raw_ptr_addr(&self) -> usize {
        self.plan.as_raw_ptr_addr()
    }
}

impl CudfPlan for CudfSplitTablePlan {}

impl CudfSplitTablePlan {
    pub fn new(input_plan: Arc<dyn CudfPlan>, batch_size: usize) -> Self {
        let plan = unsafe { ffi_new_split_table_plan(input_plan.as_raw_ptr_addr(), batch_size) };
        Self { plan }
    }
}
