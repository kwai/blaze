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

#include "filter.hpp"
#include <arrow/result.h>
#include <cudf/stream_compaction.hpp>
#include <cudf/transform.hpp>
#include <memory>
#include <ostream>
#include "src/plan/plan.hpp"
#include "src/util.hpp"

class FilterStream : public TableStream {
    public:
    FilterStream(std::unique_ptr<TableStream> input, std::shared_ptr<ExprBuilder> expr) :
        input(std::move(input)), expr(expr) {
    }

    arrow::Result<bool> has_next() {
        return input->has_next();
    }

    arrow::Result<OwnedTable> next() {
        auto next_input = ARROW_TRY_RESULT(input->next());
        auto filtered_mask = cudf::compute_column(next_input.view(), expr->expr());
        auto filtered_table = cudf::apply_boolean_mask(next_input.view(), *filtered_mask);
        return arrow::Result(std::move(filtered_table));
    }

    private:
    std::unique_ptr<TableStream> input;
    std::shared_ptr<ExprBuilder> expr;
};

FilterPlan::FilterPlan(std::shared_ptr<Plan> input, std::shared_ptr<ExprBuilder> expr) :
    input(input), expr(expr) {
}

void FilterPlan::desc(std::ostream& oss) const {
    oss << "CudfFilter []";
}

std::shared_ptr<arrow::Schema> FilterPlan::schema() const {
    return input->schema();
}

arrow::Result<std::unique_ptr<TableStream>> FilterPlan::execute() const {
    auto input_stream = ARROW_TRY_RESULT(input->execute());
    return arrow::Result(std::make_unique<FilterStream>(std::move(input_stream), expr));
}
