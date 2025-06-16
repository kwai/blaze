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

#include "parquet_scan.hpp"
#include <arrow/result.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <spdlog/fmt/ranges.h>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <cstring>
#include <cudf/ast/expressions.hpp>
#include <cudf/column/column.hpp>
#include <cudf/io/datasource.hpp>
#include <cudf/io/parquet.hpp>
#include <cudf/io/parquet_metadata.hpp>
#include <cudf/io/types.hpp>
#include <cudf/types.hpp>
#include <memory>
#include <optional>
#include <regex>
#include <stdexcept>
#include <utility>
#include <vector>
#include "src/expr.hpp"
#include "src/plan/plan.hpp"

class ParquetScanTableStream : public TableStream {
    public:
    ParquetScanTableStream(std::shared_ptr<cudf::io::chunked_parquet_reader> reader) :
        reader(reader) {
        // according to cudf docs, first chunk must be existed
        auto first_table = reader->read_chunk().tbl;
        if (first_table->num_rows() > 0) {
            next_table = std::make_optional(std::move(first_table));
        }
    }

    arrow::Result<bool> has_next() {
        if (!next_table.has_value() && reader->has_next()) {
            next_table = std::make_optional(std::move(reader->read_chunk().tbl));
        }
        return arrow::Result(next_table.has_value());
    }

    arrow::Result<OwnedTable> next() {
        auto p = std::move(next_table).value();
        next_table = std::nullopt;
        return arrow::Result(std::move(p));
    }

    private:
    std::shared_ptr<cudf::io::chunked_parquet_reader> reader;
    std::optional<std::unique_ptr<cudf::table>> next_table;
};

ParquetScanPlan::ParquetScanPlan(
    std::unique_ptr<cudf::io::datasource> datasource,
    std::shared_ptr<ExprBuilder> predicate_filter_expr,
    size_t read_offset,
    size_t read_size,
    std::shared_ptr<arrow::Schema> schema) :
    datasource(std::move(datasource)),
    predicate_filter_expr(predicate_filter_expr),
    read_offset(read_offset),
    read_size(read_size),
    output_schema(schema) {
}

void ParquetScanPlan::desc(std::ostream& oss) const {
    oss << "CudfParquetScan [";
    oss << std::regex_replace(output_schema->ToString(), std::regex("\n"), ", ");
    oss << "]";
}

std::shared_ptr<arrow::Schema> ParquetScanPlan::schema() const {
    return output_schema;
}

inline static std::vector<cudf::io::reader_column_schema> build_reader_column_schema(
    arrow::Schema const& output_schema, cudf::io::parquet_metadata const& metadata) {
    auto column_schema = std::vector<cudf::io::reader_column_schema>();
    auto& fields = output_schema.fields();

    for (auto const& col : metadata.schema().root().children()) {
        auto field = std::find_if(fields.begin(), fields.end(), [&](auto f) {
            return strcasecmp(f->name().c_str(), col.name().c_str()) == 0;
        });
        if (field == fields.end()) {
            column_schema.emplace_back();
        } else if (field->get()->type()->id() == arrow::Type::STRING) {
            column_schema.emplace_back().set_convert_binary_to_strings(true);
        } else if (field->get()->type()->id() == arrow::Type::STRUCT) {
            throw std::runtime_error("complex type not implemented");
        } else {
            column_schema.emplace_back();
        }
    }
    return column_schema;
}

arrow::Result<std::unique_ptr<TableStream>> ParquetScanPlan::execute() const {
    auto datasource_total_size = datasource->size();
    auto source_info = cudf::io::source_info(&*datasource);
    auto metadata = cudf::io::read_parquet_metadata(source_info);

    // calculate row group ids to read
    auto read_row_group_ids = std::vector<cudf::size_type>();
    auto num_row_groups = metadata.num_rowgroups();
    auto file_total_byte_size = (uint64_t)0;
    for (auto i = 0; i < num_row_groups; i++) {
        auto rg_total_byte_size = metadata.rowgroup_metadata()[i].at("total_byte_size");
        file_total_byte_size += rg_total_byte_size;
    }
    auto start = (uint64_t)0;
    for (auto i = 0; i < num_row_groups; i++) {
        auto rg_total_byte_size = metadata.rowgroup_metadata()[i].at("total_byte_size");
        start += rg_total_byte_size;
        auto mapped_end = start * datasource_total_size / file_total_byte_size;
        if (mapped_end > read_offset && mapped_end <= read_offset + read_size) {
            read_row_group_ids.push_back(i);
        }
    }
    if (read_row_group_ids.empty()) {  // no row groups to read
        return TableStream::empty_stream();
    }
    spdlog::info("reading parquet row groups: {}", read_row_group_ids);
    spdlog::info("reading parquet fields: {}", output_schema->field_names());

    auto reader_column_schema = build_reader_column_schema(*this->output_schema, metadata);
    auto options_builder =
        cudf::io::parquet_reader_options_builder(source_info)
            .row_groups(std::vector<std::vector<cudf::size_type>>{std::move(read_row_group_ids)})
            .set_column_schema(reader_column_schema)
            .columns(output_schema->field_names())
            .allow_mismatched_pq_schemas(true);

    // set optional predicate filter
    auto const& pred = predicate_filter_expr->expr();
    if (typeid(pred) == typeid(cudf::ast::operation)) {
        options_builder.filter(pred);
    }
    auto options = options_builder.build();

    auto chunk_bytes_size = 16777216;
    auto reader = std::make_shared<cudf::io::chunked_parquet_reader>(chunk_bytes_size, options);
    return std::make_unique<ParquetScanTableStream>(reader);
}
