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

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use paste::paste;

use crate::io::CudfDataSource;

macro_rules! define_ffi_obj_wrapped_type {
    ($tyname:ident) => {
        #[repr(C, align(8))]
        #[allow(non_camel_case_types)]
        pub struct $tyname {
            raw_ptr_addr: usize,
        }

        impl $tyname {
            #[allow(unused)]
            pub fn as_raw_ptr_addr(&self) -> usize {
                return self.raw_ptr_addr;
            }
        }

        impl Drop for $tyname {
            fn drop(&mut self) {
                unsafe {
                    paste! {
                        extern "C" {
                            fn [<ffi_ $tyname _delete>] (raw_ptr_addr: usize) -> ();
                        }
                        [<ffi_ $tyname _delete>] (self.raw_ptr_addr);
                    }
                }
            }
        }
    };
}

macro_rules! define_ffi_result_type {
    ($tyname:ident, $t:ty) => {
        #[repr(C, align(4))]
        #[allow(non_camel_case_types)]
        pub struct $tyname {
            pub ok: bool,
            pub value: $t,
        }

        impl $tyname {
            #[allow(unused)]
            pub fn into_result(self, errmsg: String) -> arrow::error::Result<$t> {
                if !self.ok {
                    return Err(arrow::error::ArrowError::ComputeError(errmsg));
                }
                Ok(self.value)
            }
        }
    };
}

define_ffi_obj_wrapped_type!(FFI_PlanPtr);
define_ffi_obj_wrapped_type!(FFI_TableStreamPtr);
define_ffi_obj_wrapped_type!(FFI_TablePtr);
define_ffi_obj_wrapped_type!(FFI_ScalarPtr);
define_ffi_obj_wrapped_type!(FFI_ExprBuilderPtr);

define_ffi_result_type!(FFI_VoidResult, ());
define_ffi_result_type!(FFI_BoolResult, bool);
define_ffi_result_type!(FFI_TableStreamPtrResult, FFI_TableStreamPtr);
define_ffi_result_type!(FFI_TableResult, FFI_TablePtr);
define_ffi_result_type!(FFI_USizeResult, usize);

pub fn slice_to_const_ptr_and_len<T>(vec: &[T]) -> (*const T, usize) {
    let len = vec.len();
    let ptr = vec.as_ptr();
    (ptr, len)
}

pub fn slice_to_mut_ptr_and_len<T>(vec: &mut [T]) -> (*mut T, usize) {
    let len = vec.len();
    let ptr = vec.as_mut_ptr();
    (ptr, len)
}

#[allow(unused)]
extern "C" {
    pub fn ffi_new_parquet_scan_plan(
        data_source: *mut FFI_CudfDataSource,
        predicate_filter_expr_ptr_addr: usize,
        read_offset: usize,
        read_size: usize,
        schema: *mut FFI_ArrowSchema,
    ) -> FFI_PlanPtr;

    pub fn ffi_new_split_table_plan(input_plan_ptr_addr: usize, batch_size: usize) -> FFI_PlanPtr;

    pub fn ffi_new_union_plan(
        input_plan_ptr_addrs: *const usize,
        input_plan_len: usize,
        schema: *mut FFI_ArrowSchema,
    ) -> FFI_PlanPtr;

    pub fn ffi_new_project_plan(
        input_plan_ptr_addr: usize,
        exprs_ptr_addrs: *const usize,
        exprs_len: usize,
        schema: *mut FFI_ArrowSchema,
    ) -> FFI_PlanPtr;

    pub fn ffi_new_filter_plan(input_plan_ptr_addr: usize, expr_ptr_addr: usize) -> FFI_PlanPtr;

    pub fn ffi_execute_plan(plan_ptr_addr: usize) -> FFI_TableStreamPtrResult;

    pub fn ffi_table_stream_has_next(table_stream_ptr_addr: usize) -> FFI_BoolResult;

    pub fn ffi_table_stream_next(table_stream_ptr_addr: usize) -> FFI_TableResult;

    pub fn ffi_table_to_arrow(
        table_ptr_addr: usize,
        ffi_schema: *mut FFI_ArrowSchema,
        ffi_array: *mut FFI_ArrowArray,
    ) -> FFI_VoidResult;

    pub fn ffi_new_expr_builder() -> FFI_ExprBuilderPtr;

    pub fn ffi_i8_scalar(value: i8) -> FFI_ScalarPtr;
    pub fn ffi_i16_scalar(value: i16) -> FFI_ScalarPtr;
    pub fn ffi_i32_scalar(value: i32) -> FFI_ScalarPtr;
    pub fn ffi_i64_scalar(value: i64) -> FFI_ScalarPtr;
    pub fn ffi_u8_scalar(value: u8) -> FFI_ScalarPtr;
    pub fn ffi_u16_scalar(value: u16) -> FFI_ScalarPtr;
    pub fn ffi_u32_scalar(value: u32) -> FFI_ScalarPtr;
    pub fn ffi_u64_scalar(value: u64) -> FFI_ScalarPtr;
    pub fn ffi_f32_scalar(value: f32) -> FFI_ScalarPtr;
    pub fn ffi_f64_scalar(value: f64) -> FFI_ScalarPtr;
    pub fn ffi_string_scalar(value: *const std::ffi::c_char) -> FFI_ScalarPtr;

    pub fn ffi_expr_builder_add_literal(
        expr_builder_ptr_addr: usize,
        scalar_ptr_addr: usize,
    ) -> usize;
    pub fn ffi_expr_builder_add_column_reference(
        expr_builder_ptr_addr: usize,
        column_idx: usize,
    ) -> usize;
    pub fn ffi_expr_builder_add_column_name_reference(
        expr_builder_ptr_addr: usize,
        name: *const std::ffi::c_char,
    ) -> usize;
    pub fn ffi_expr_builder_add_unary_expr(
        expr_builder_ptr_addr: usize,
        arg1_expr_id: usize,
        op: FFI_AstOperator,
    ) -> usize;
    pub fn ffi_expr_builder_add_binary_expr(
        expr_builder_ptr_addr: usize,
        arg1_expr_id: usize,
        arg2_expr_id: usize,
        op: FFI_AstOperator,
    ) -> usize;
}

#[repr(i32)]
#[allow(non_camel_case_types)]
#[allow(unused)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FFI_AstOperator {
    // this enum is copied from <cudf/ast/expressions.hpp>
    // Binary operators
    ADD = 0,   // < operator +
    SUB,       // < operator -
    MUL,       // < operator *
    DIV,       // < operator / using common type of lhs and rhs
    TRUE_DIV,  // < operator / after promoting type to floating point
    FLOOR_DIV, // < operator / after promoting to 64 bit floating point and then
    // < flooring the result
    MOD,   // < operator %
    PYMOD, // < operator % using Python's sign rules for negatives
    POW,   // < lhs ^ rhs
    EQUAL, // < operator ==
    NULL_EQUAL, /* < operator == with Spark rules: NULL_EQUAL(null, null) is true,
            * NULL_EQUAL(null, */
    // < valid) is false, and
    // < NULL_EQUAL(valid, valid) == EQUAL(valid, valid)
    NOT_EQUAL,        // < operator !=
    LESS,             // < operator <
    GREATER,          // < operator >
    LESS_EQUAL,       // < operator <=
    GREATER_EQUAL,    // < operator >=
    BITWISE_AND,      // < operator &
    BITWISE_OR,       // < operator |
    BITWISE_XOR,      // < operator ^
    LOGICAL_AND,      // < operator &&
    NULL_LOGICAL_AND, // < operator && with Spark rules: NULL_LOGICAL_AND(null, null) is null,
    // < NULL_LOGICAL_AND(null, true) is
    // < null, NULL_LOGICAL_AND(null, false) is false, and NULL_LOGICAL_AND(valid,
    // < valid) == LOGICAL_AND(valid, valid)
    LOGICAL_OR,      // < operator ||
    NULL_LOGICAL_OR, // < operator || with Spark rules: NULL_LOGICAL_OR(null, null) is null,
    // < NULL_LOGICAL_OR(null, true) is true,
    // < NULL_LOGICAL_OR(null, false) is null, and NULL_LOGICAL_OR(valid, valid) ==
    // < LOGICAL_OR(valid, valid)
    // Unary operators
    IDENTITY,        // < Identity function
    IS_NULL,         // < Check if operand is null
    SIN,             // < Trigonometric sine
    COS,             // < Trigonometric cosine
    TAN,             // < Trigonometric tangent
    ARCSIN,          // < Trigonometric sine inverse
    ARCCOS,          // < Trigonometric cosine inverse
    ARCTAN,          // < Trigonometric tangent inverse
    SINH,            // < Hyperbolic sine
    COSH,            // < Hyperbolic cosine
    TANH,            // < Hyperbolic tangent
    ARCSINH,         // < Hyperbolic sine inverse
    ARCCOSH,         // < Hyperbolic cosine inverse
    ARCTANH,         // < Hyperbolic tangent inverse
    EXP,             // < Exponential (base e, Euler number)
    LOG,             // < Natural Logarithm (base e)
    SQRT,            // < Square-root (x^0.5)
    CBRT,            // < Cube-root (x^(1.0/3))
    CEIL,            // < Smallest integer value not less than arg
    FLOOR,           // < largest integer value not greater than arg
    ABS,             // < Absolute value
    RINT,            // < Rounds the floating-point argument arg to an integer value
    BIT_INVERT,      // < Bitwise Not (~)
    NOT,             // < Logical Not (!)
    CAST_TO_INT64,   // < Cast value to int64_t
    CAST_TO_UINT64,  // < Cast value to uint64_t
    CAST_TO_FLOAT64, // < Cast value to double
}

#[repr(C, align(8))]
#[allow(non_camel_case_types)]
pub struct FFI_CudfDataSource {
    pub dtor: extern "C" fn(this: *mut FFI_CudfDataSource) -> (),
    pub data_size: extern "C" fn(this: *const FFI_CudfDataSource) -> usize,
    pub read: extern "C" fn(
        this: *mut FFI_CudfDataSource,
        offset: usize,
        buf: *mut u8,
        buf_size: usize,
    ) -> FFI_USizeResult,

    pub raw: *mut Box<dyn CudfDataSource>,
}
