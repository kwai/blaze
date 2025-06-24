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

#include "split_table.hpp"
#include <algorithm>
#include <cudf/copying.hpp>
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/types.hpp>
#include <deque>
#include <memory>
#include "src/plan/plan.hpp"
#include "src/plan/split_table.hpp"
#include "src/util.hpp"

class SplitTableStream : public TableStream {
    public:
    SplitTableStream(std::unique_ptr<TableStream> input_stream, size_t batch_size) :
        input_stream(std::move(input_stream)), batch_size(batch_size) {
    }

    arrow::Result<bool> has_next() {
        if (buffered_tables.empty()) {
            auto input_has_next = ARROW_TRY_RESULT(input_stream->has_next());
            if (input_has_next) {
                auto input_next = ARROW_TRY_RESULT(input_stream->next());
                auto input_num_rows = size_t(input_next.view().num_rows());

                if (input_num_rows == 0) {
                    return has_next();  // process next input table
                }
                auto target_num_splitted_tables = (input_num_rows + batch_size - 1) / batch_size;
                auto target_num_rows = (input_num_rows / target_num_splitted_tables) + 1;

                if (target_num_splitted_tables == 1) {
                    buffered_tables.push_back(std::move(input_next));
                } else {
                    auto split_indices = std::vector<cudf::size_type>();
                    auto start = size_t(0);
                    while (start < input_num_rows) {
                        auto split_start = start;
                        auto split_end = std::min(start + target_num_rows, input_num_rows);
                        split_indices.push_back(cudf::size_type(split_start));
                        split_indices.push_back(cudf::size_type(split_end));
                        start = split_end;
                    }
                    auto holder = std::make_shared<OwnedTable>(std::move(input_next));
                    auto sliced = cudf::slice(holder->view(), split_indices);
                    for (auto splitted: sliced) {
                        buffered_tables.emplace_back(holder, splitted);
                    }
                }
            }
        }
        return !buffered_tables.empty();
    }

    arrow::Result<OwnedTable> next() {
        auto front = std::move(buffered_tables.front());
        buffered_tables.pop_front();
        return front;
    }

    private:
    std::unique_ptr<TableStream> input_stream;
    size_t batch_size;
    std::deque<OwnedTable> buffered_tables;
};

SplitTablePlan::SplitTablePlan(std::shared_ptr<Plan> input, size_t batch_size) :
    input(input), batch_size(batch_size) {
}

std::shared_ptr<arrow::Schema> SplitTablePlan::schema() const {
    return input->schema();
}

arrow::Result<std::unique_ptr<TableStream>> SplitTablePlan::execute() const {
    auto input_stream = ARROW_TRY_RESULT(input->execute());
    return arrow::Result(std::make_unique<SplitTableStream>(std::move(input_stream), batch_size));
}

void SplitTablePlan::desc(std::ostream& oss) const {
    oss << "SplitTable []";
}
