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

use std::{any::TypeId, ffi::CString, str::FromStr};

use num::ToPrimitive;

use crate::{ffi::*, AsRawPtrAddr};

pub type CudfExprOperator = FFI_AstOperator;

pub struct CudfExprBuilder {
    expr_builder: FFI_ExprBuilderPtr,
}

impl AsRawPtrAddr for CudfExprBuilder {
    fn as_raw_ptr_addr(&self) -> usize {
        self.expr_builder.as_raw_ptr_addr()
    }
}

impl CudfExprBuilder {
    pub fn new() -> Self {
        Self {
            expr_builder: unsafe { ffi_new_expr_builder() },
        }
    }

    pub fn add_literal(&self, scalar: &CudfScalar) -> usize {
        unsafe { ffi_expr_builder_add_literal(self.as_raw_ptr_addr(), scalar.as_raw_ptr_addr()) }
    }

    pub fn add_column_ref(&self, column_idx: usize) -> usize {
        unsafe { ffi_expr_builder_add_column_reference(self.as_raw_ptr_addr(), column_idx) }
    }

    pub fn add_column_name_ref(&self, name: &str) -> usize {
        let name_c_str = CString::from_str(name).unwrap();
        unsafe {
            ffi_expr_builder_add_column_name_reference(self.as_raw_ptr_addr(), name_c_str.as_ptr())
        }
    }

    pub fn add_unary(&self, arg1_expr_id: usize, op: CudfExprOperator) -> usize {
        unsafe { ffi_expr_builder_add_unary_expr(self.as_raw_ptr_addr(), arg1_expr_id, op) }
    }

    pub fn add_binary(
        &self,
        arg1_expr_id: usize,
        arg2_expr_id: usize,
        op: CudfExprOperator,
    ) -> usize {
        unsafe {
            ffi_expr_builder_add_binary_expr(self.as_raw_ptr_addr(), arg1_expr_id, arg2_expr_id, op)
        }
    }
}

pub struct CudfScalar {
    scalar: FFI_ScalarPtr,
}

impl AsRawPtrAddr for CudfScalar {
    fn as_raw_ptr_addr(&self) -> usize {
        self.scalar.as_raw_ptr_addr()
    }
}

impl CudfScalar {
    pub fn new_prim<T: Copy + ToPrimitive + 'static>(value: T) -> Self {
        let typeid = TypeId::of::<T>();
        let scalar = unsafe {
            match () {
                _ if typeid == TypeId::of::<i8>() => ffi_i8_scalar(value.to_i8().unwrap()),
                _ if typeid == TypeId::of::<i16>() => ffi_i16_scalar(value.to_i16().unwrap()),
                _ if typeid == TypeId::of::<i32>() => ffi_i32_scalar(value.to_i32().unwrap()),
                _ if typeid == TypeId::of::<i64>() => ffi_i64_scalar(value.to_i64().unwrap()),
                _ if typeid == TypeId::of::<u8>() => ffi_u8_scalar(value.to_u8().unwrap()),
                _ if typeid == TypeId::of::<u16>() => ffi_u16_scalar(value.to_u16().unwrap()),
                _ if typeid == TypeId::of::<u32>() => ffi_u32_scalar(value.to_u32().unwrap()),
                _ if typeid == TypeId::of::<u64>() => ffi_u64_scalar(value.to_u64().unwrap()),
                _ if typeid == TypeId::of::<f32>() => ffi_f32_scalar(value.to_f32().unwrap()),
                _ if typeid == TypeId::of::<f64>() => ffi_f64_scalar(value.to_f64().unwrap()),
                _ => unreachable!("unimplemented fixed withd scalar"),
            }
        };
        Self { scalar }
    }

    pub fn new_string(value: &str) -> Self {
        let c_str = CString::from_str(value).unwrap();
        let scalar = unsafe { ffi_string_scalar(c_str.as_ptr()) };
        Self { scalar }
    }

    pub fn new_bool(value: bool) -> Self {
        let scalar = unsafe { ffi_i8_scalar(if value { 1 } else { 0 }) };
        Self { scalar }
    }
}
