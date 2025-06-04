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

#include <arrow/result.h>
#include <arrow/type.h>
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>
#include <memory>
#include <optional>
#include <ostream>
#include <sstream>
#include <stack>
#include <stdexcept>
#include <vector>

class OwnedTable {
    public:
    inline OwnedTable(std::unique_ptr<cudf::table> owned) :
        owned(std::make_optional(std::move(owned))) {
    }
    inline OwnedTable(std::shared_ptr<OwnedTable> holder, cudf::table_view referenced) :
        referenced(std::make_pair(holder, referenced)) {
    }
    inline OwnedTable(OwnedTable&& moving) :
        owned(std::move(moving.owned)), referenced(std::move(moving.referenced)) {
    }

    inline cudf::table_view view() const {
        if (owned.has_value()) {
            return owned.value()->view();
        }
        return referenced.value().second;
    }

    private:
    std::optional<std::unique_ptr<cudf::table>> owned;
    std::optional<std::pair<std::shared_ptr<OwnedTable>, cudf::table_view>> referenced;
};

class TableStream {
    public:
    TableStream() {
    }

    virtual ~TableStream() {
    }

    public:
    virtual arrow::Result<bool> has_next() = 0;
    virtual arrow::Result<OwnedTable> next() = 0;

    inline static std::unique_ptr<TableStream> empty_stream() {
        class EmptyTableStream : public TableStream {
            public:
            inline arrow::Result<bool> has_next() {
                return arrow::Result(false);
            }

            inline arrow::Result<OwnedTable> next() {
                throw std::runtime_error("unreachable: EmptyTableStream has no tables");
            }
        };
        return std::make_unique<EmptyTableStream>();
    }
};

class Plan {
    public:
    Plan() {
    }
    virtual ~Plan() {
    }

    public:
    virtual void desc(std::ostream& oss) const = 0;
    virtual std::shared_ptr<arrow::Schema> schema() const = 0;
    virtual arrow::Result<std::unique_ptr<TableStream>> execute() const = 0;

    virtual std::vector<std::shared_ptr<Plan>> children() const {
        return std::vector<std::shared_ptr<Plan>>{};
    }
};

inline static std::string desc_plan_tree(std::shared_ptr<Plan> plan) {
    struct plan_tree_node {
        std::shared_ptr<Plan> plan;
        int level;
        plan_tree_node(std::shared_ptr<Plan> plan, int level) : plan(plan), level(level) {
        }
    };
    auto dfs_stack = std::stack<plan_tree_node>();
    auto nodes = std::vector<plan_tree_node>{};

    dfs_stack.push(plan_tree_node(plan, 0));
    while (!dfs_stack.empty()) {
        auto node = dfs_stack.top();
        nodes.push_back(node);
        dfs_stack.pop();
        for (auto child : node.plan->children()) {
            dfs_stack.push(plan_tree_node(child, node.level + 1));
        }
    }

    std::ostringstream oss;
    for (auto node : nodes) {
        for (auto indent = 0; indent < 2 * node.level; indent++) {
            oss << " ";
        }
        node.plan->desc(oss);
        oss << std::endl;
    }
    return oss.str();
}
