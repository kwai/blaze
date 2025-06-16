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

use arrow::{
    datatypes::{DataType, SchemaRef},
    error::Result,
    ffi::FFI_ArrowSchema,
};

use crate::{exprs::CudfExprBuilder, ffi::*, plans::CudfPlan, AsRawPtrAddr};

pub struct CudfProjectPlan {
    plan: FFI_PlanPtr,
}

impl AsRawPtrAddr for CudfProjectPlan {
    fn as_raw_ptr_addr(&self) -> usize {
        self.plan.as_raw_ptr_addr()
    }
}

impl CudfPlan for CudfProjectPlan {}

impl CudfProjectPlan {
    pub fn try_new(
        input_plan: Arc<dyn CudfPlan>,
        exprs: &[&CudfExprBuilder],
        schema: SchemaRef,
    ) -> Result<Self> {
        let exprs_ptr_addrs = exprs
            .iter()
            .map(|expr| expr.as_raw_ptr_addr())
            .collect::<Vec<_>>();
        let (exprs_ptr, exprs_len) = slice_to_const_ptr_and_len(&exprs_ptr_addrs);

        let schema_type = DataType::Struct(schema.fields().clone());
        let mut ffi_schema = FFI_ArrowSchema::try_from(schema_type)?;

        let plan = unsafe {
            ffi_new_project_plan(
                input_plan.as_raw_ptr_addr(),
                exprs_ptr,
                exprs_len,
                &mut ffi_schema,
            )
        };
        Ok(Self { plan })
    }
}
