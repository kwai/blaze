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

#include <arrow/type.h>
#include <cudf/ast/expressions.hpp>
#include <cudf/io/datasource.hpp>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include "src/expr.hpp"
#include "src/plan/plan.hpp"

class ParquetScanPlan : public Plan {
    public:
    ParquetScanPlan(
        std::unique_ptr<cudf::io::datasource> datasource,
        std::shared_ptr<ExprBuilder> predicate_filter_expr,
        size_t read_offset,
        size_t read_size,
        std::shared_ptr<arrow::Schema> schema);

    void desc(std::ostream& oss) const;
    std::shared_ptr<arrow::Schema> schema() const;
    arrow::Result<std::unique_ptr<TableStream>> execute() const;

    private:
    std::unique_ptr<cudf::io::datasource> datasource;
    std::shared_ptr<ExprBuilder> predicate_filter_expr;
    size_t read_offset;
    size_t read_size;
    std::shared_ptr<arrow::Schema> output_schema;
    std::optional<std::reference_wrapper<cudf::ast::expression const>> bound_predicate_filter;
};
