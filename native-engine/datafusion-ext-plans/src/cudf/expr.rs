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

use blaze_cudf_bridge::exprs::{CudfExprBuilder, CudfExprOperator, CudfScalar};
use datafusion::{
    error::Result,
    logical_expr::Operator,
    physical_expr::PhysicalExprRef,
    physical_plan::expressions::{
        BinaryExpr, Column, IsNotNullExpr, IsNullExpr, Literal, NegativeExpr,
    },
    scalar::ScalarValue,
};
use datafusion_ext_commons::{df_unimplemented_err, downcast_any};

pub fn convert_datafusion_expr_to_cudf(
    de: &PhysicalExprRef,
    use_column_name: bool,
) -> Result<CudfExprBuilder> {
    let root_expr_builder = CudfExprBuilder::new();
    let _ = convert_impl(&de, &root_expr_builder, use_column_name)?;
    Ok(root_expr_builder)
}

fn convert_impl(de: &PhysicalExprRef, b: &CudfExprBuilder, use_column_name: bool) -> Result<usize> {
    if let Ok(lit) = downcast_any!(de, Literal) {
        let cudf_scalar = match lit.value() {
            ScalarValue::Boolean(Some(v)) => CudfScalar::new_bool(*v),
            ScalarValue::Int8(Some(v)) => CudfScalar::new_prim(*v),
            ScalarValue::Int16(Some(v)) => CudfScalar::new_prim(*v),
            ScalarValue::Int32(Some(v)) => CudfScalar::new_prim(*v),
            ScalarValue::Int64(Some(v)) => CudfScalar::new_prim(*v),
            ScalarValue::UInt8(Some(v)) => CudfScalar::new_prim(*v),
            ScalarValue::UInt16(Some(v)) => CudfScalar::new_prim(*v),
            ScalarValue::UInt32(Some(v)) => CudfScalar::new_prim(*v),
            ScalarValue::UInt64(Some(v)) => CudfScalar::new_prim(*v),
            ScalarValue::Float32(Some(v)) => CudfScalar::new_prim(*v),
            ScalarValue::Float64(Some(v)) => CudfScalar::new_prim(*v),
            ScalarValue::Utf8(Some(v)) => CudfScalar::new_string(v),
            _ => return df_unimplemented_err!("cannot convert scalar to cudf: {lit:?}"),
        };
        return Ok(b.add_literal(&cudf_scalar));
    } else if let Ok(col) = downcast_any!(de, Column) {
        if use_column_name {
            return Ok(b.add_column_name_ref(col.name()));
        } else {
            return Ok(b.add_column_ref(col.index()));
        }
    } else if let Ok(neg) = downcast_any!(de, NegativeExpr) {
        let arg_expr_id = convert_impl(neg.arg(), b, use_column_name)?;
        return Ok(b.add_unary(arg_expr_id, CudfExprOperator::SUB));
    } else if let Ok(binary) = downcast_any!(de, BinaryExpr) {
        let left_expr_id = convert_impl(binary.left(), b, use_column_name)?;
        let right_expr_id = convert_impl(binary.right(), b, use_column_name)?;
        let op = convert_op(binary.op())?;
        return Ok(b.add_binary(left_expr_id, right_expr_id, op));
    } else if let Ok(is_null) = downcast_any!(de, IsNullExpr) {
        let arg_expr_id = convert_impl(is_null.arg(), b, use_column_name)?;
        return Ok(b.add_unary(arg_expr_id, CudfExprOperator::IS_NULL));
    } else if let Ok(is_not_null) = downcast_any!(de, IsNotNullExpr) {
        let arg_expr_id = convert_impl(is_not_null.arg(), b, use_column_name)?;
        let is_null_expr_id = b.add_unary(arg_expr_id, CudfExprOperator::IS_NULL);
        return Ok(b.add_unary(is_null_expr_id, CudfExprOperator::NOT));
    }
    df_unimplemented_err!("cannot convert expr to cudf: {de:?}")
}

fn convert_op(op: &Operator) -> Result<CudfExprOperator> {
    Ok(match op {
        Operator::Eq => CudfExprOperator::EQUAL,
        Operator::NotEq => CudfExprOperator::NOT_EQUAL,
        Operator::Lt => CudfExprOperator::LESS,
        Operator::LtEq => CudfExprOperator::LESS_EQUAL,
        Operator::Gt => CudfExprOperator::GREATER,
        Operator::GtEq => CudfExprOperator::GREATER_EQUAL,
        Operator::Plus => CudfExprOperator::ADD,
        Operator::Minus => CudfExprOperator::SUB,
        Operator::Multiply => CudfExprOperator::MUL,
        Operator::Divide => CudfExprOperator::DIV,
        Operator::Modulo => CudfExprOperator::MOD,
        Operator::And => CudfExprOperator::LOGICAL_AND,
        Operator::Or => CudfExprOperator::LOGICAL_OR,
        Operator::BitwiseAnd => CudfExprOperator::BITWISE_AND,
        Operator::BitwiseOr => CudfExprOperator::BITWISE_OR,
        Operator::BitwiseXor => CudfExprOperator::BITWISE_XOR,
        other => return df_unimplemented_err!("connot convert operator to cudf: {other:?}"),
    })
}
