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

#pragma once

#include <cudf/ast/expressions.hpp>
#include <cudf/scalar/scalar.hpp>
#include <memory>

class ExprBuilder {
    public:
    size_t add_unary_operation(size_t arg1_expr_id, int op);
    size_t add_binary_operation(size_t arg1_expr_id, size_t arg2_expr_id, int op);
    size_t add_column_reference(size_t column_idx);
    size_t add_column_name_reference(std::string const& name);
    size_t add_literal(std::shared_ptr<cudf::scalar> scalar);

    cudf::ast::expression const& expr() const;

    private:
    cudf::ast::tree tree;
    std::vector<std::shared_ptr<cudf::scalar>> scalar_pool;
};
