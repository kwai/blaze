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

#include "expr.hpp"
#include <cudf/ast/expressions.hpp>
#include <cudf/scalar/scalar.hpp>
#include <memory>
#include <stdexcept>

size_t ExprBuilder::add_column_reference(size_t column_idx) {
    auto expr_id = tree.size();
    tree.emplace<cudf::ast::column_reference>(column_idx);
    return expr_id;
}

size_t ExprBuilder::add_column_name_reference(std::string const& name) {
    auto expr_id = tree.size();
    tree.emplace<cudf::ast::column_name_reference>(name);
    return expr_id;
}

size_t ExprBuilder::add_unary_operation(size_t arg1_expr_id, int op) {
    auto const& arg1 = tree.at(arg1_expr_id);
    auto expr_id = tree.size();
    tree.emplace<cudf::ast::operation>((cudf::ast::ast_operator)op, arg1);
    return expr_id;
}

size_t ExprBuilder::add_binary_operation(size_t arg1_expr_id, size_t arg2_expr_id, int op) {
    auto const& arg1 = tree.at(arg1_expr_id);
    auto const& arg2 = tree.at(arg2_expr_id);
    auto expr_id = tree.size();
    tree.emplace<cudf::ast::operation>((cudf::ast::ast_operator)op, arg1, arg2);
    return expr_id;
}

size_t ExprBuilder::add_literal(std::shared_ptr<cudf::scalar> scalar) {
    auto& scalar_value = *scalar.get();
    auto&& type_id = typeid(scalar_value);
    auto expr_id = tree.size();

#define handle_numeric_type(c_ty)                                                                  \
    if (type_id == typeid(cudf::numeric_scalar<c_ty>)) {                                           \
        tree.emplace<cudf::ast::literal>(dynamic_cast<cudf::numeric_scalar<c_ty>&>(scalar_value)); \
        scalar_pool.push_back((scalar));                                                           \
        return expr_id;                                                                            \
    }
    handle_numeric_type(int8_t);
    handle_numeric_type(int16_t);
    handle_numeric_type(int32_t);
    handle_numeric_type(int64_t);
    handle_numeric_type(uint8_t);
    handle_numeric_type(uint16_t);
    handle_numeric_type(uint32_t);
    handle_numeric_type(uint64_t);
    handle_numeric_type(float);
    handle_numeric_type(double);

    if (type_id == typeid(cudf::string_scalar)) {
        tree.emplace<cudf::ast::literal>(dynamic_cast<cudf::string_scalar&>(scalar_value));
        scalar_pool.push_back(scalar);
        return expr_id;
    }
    throw std::runtime_error(std::string("unsupported scalar type: ") + type_id.name());
}

cudf::ast::expression const& ExprBuilder::expr() const {
    return tree.back();
}
