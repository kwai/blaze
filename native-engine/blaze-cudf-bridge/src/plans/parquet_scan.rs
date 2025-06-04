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

use arrow::{
    datatypes::{DataType, SchemaRef},
    error::Result,
    ffi::FFI_ArrowSchema,
};

use crate::{
    exprs::CudfExprBuilder,
    ffi::*,
    io::{CudfDataSource, IntoFFICudfDataSource},
    plans::CudfPlan,
    AsRawPtrAddr,
};

pub struct CudfParquetScanPlan {
    plan: FFI_PlanPtr,
}

impl AsRawPtrAddr for CudfParquetScanPlan {
    fn as_raw_ptr_addr(&self) -> usize {
        self.plan.as_raw_ptr_addr()
    }
}

impl CudfPlan for CudfParquetScanPlan {}

impl CudfParquetScanPlan {
    pub fn try_new(
        data_source: Box<dyn CudfDataSource>,
        predicate_filter_expr: &CudfExprBuilder,
        read_offset: usize,
        read_size: usize,
        schema: SchemaRef,
    ) -> Result<Self> {
        let schema_type = DataType::Struct(schema.fields().clone());
        let mut ffi_schema = FFI_ArrowSchema::try_from(schema_type)?;
        let plan = unsafe {
            ffi_new_parquet_scan_plan(
                &mut data_source.into_ffi(),
                predicate_filter_expr.as_raw_ptr_addr(),
                read_offset,
                read_size,
                &mut ffi_schema,
            )
        };
        Ok(Self { plan })
    }
}
