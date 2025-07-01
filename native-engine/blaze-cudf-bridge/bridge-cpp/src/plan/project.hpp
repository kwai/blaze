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
#include <memory>
#include <vector>
#include "src/expr.hpp"
#include "src/plan/plan.hpp"

class ProjectPlan : public Plan {
    public:
    ProjectPlan(
        std::shared_ptr<Plan> input,
        std::vector<std::shared_ptr<ExprBuilder>> exprs,
        std::shared_ptr<arrow::Schema> schema);

    public:
    std::shared_ptr<arrow::Schema> schema() const;
    arrow::Result<std::unique_ptr<TableStream>> execute() const;
    void desc(std::ostream& oss) const;

    inline std::vector<std::shared_ptr<Plan>> children() const {
        return std::vector{input};
    }

    private:
    std::shared_ptr<Plan> input;
    std::vector<std::shared_ptr<ExprBuilder>> exprs;
    std::shared_ptr<arrow::Schema> output_schema;
};
