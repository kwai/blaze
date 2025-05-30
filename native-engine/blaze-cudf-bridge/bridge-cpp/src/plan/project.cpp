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

#include "project.hpp"
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <cudf/column/column.hpp>
#include <cudf/table/table.hpp>
#include <cudf/transform.hpp>
#include <memory>
#include <ostream>
#include <regex>
#include <vector>
#include "src/expr.hpp"
#include "src/plan/plan.hpp"
#include "src/util.hpp"

class ProjectStream : public TableStream {
    public:
    ProjectStream(
        std::unique_ptr<TableStream> input,
        const std::vector<std::shared_ptr<ExprBuilder>>& exprs) :
        input(std::move(input)), exprs(exprs) {
    }

    public:
    arrow::Result<bool> has_next() {
        return input->has_next();
    }

    arrow::Result<OwnedTable> next() {
        auto next_input = ARROW_TRY_RESULT(input->next());
        auto computed_columns = std::vector<std::unique_ptr<cudf::column>>();
        computed_columns.reserve(exprs.size());

        for (const auto& expr : exprs) {
            auto column = cudf::compute_column(next_input.view(), expr->expr());
            computed_columns.push_back(std::move(column));
        }
        auto computed_table =
            std::unique_ptr<cudf::table>(new cudf::table(std::move(computed_columns)));
        return arrow::Result(std::move(computed_table));
    }

    private:
    std::unique_ptr<TableStream> input;
    std::vector<std::shared_ptr<ExprBuilder>> exprs;
};

ProjectPlan::ProjectPlan(
    std::shared_ptr<Plan> input,
    std::vector<std::shared_ptr<ExprBuilder>> exprs,
    std::shared_ptr<arrow::Schema> schema) :
    input(input), exprs(exprs), output_schema(schema) {
}

void ProjectPlan::desc(std::ostream& oss) const {
    oss << "CudfProject [";
    oss << std::regex_replace(output_schema->ToString(), std::regex("\n"), ", ");
    oss << "]";
}

std::shared_ptr<arrow::Schema> ProjectPlan::schema() const {
    return output_schema;
}

arrow::Result<std::unique_ptr<TableStream>> ProjectPlan::execute() const {
    auto input_stream = ARROW_TRY_RESULT(input->execute());
    return arrow::Result(std::make_unique<ProjectStream>(std::move(input_stream), exprs));
}
