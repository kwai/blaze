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

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/result.h>
#include <arrow/type.h>
#include <spdlog/spdlog.h>
#include <cstdint>
#include <cudf/interop.hpp>
#include <cudf/io/datasource.hpp>
#include <cudf/scalar/scalar.hpp>
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/utilities/error.hpp>
#include <memory>
#include <stdexcept>
#include <vector>
#include "src/expr.hpp"
#include "src/plan/filter.hpp"
#include "src/plan/parquet_scan.hpp"
#include "src/plan/plan.hpp"
#include "src/plan/project.hpp"
#include "src/plan/split_table.hpp"
#include "src/plan/union.hpp"

template <typename T>
struct alignas(8) FFIObjWrapper {
    FFIObjWrapper(T inner) : inner(std::move(inner)) {
    }
    T inner;
};

template <typename T>
struct alignas(8) FFIResult {
    FFIResult(bool ok, T value) : ok(ok), value(std::move(value)) {
    }
    bool ok;
    T value;
};

template <>
struct alignas(8) FFIResult<void> {
    FFIResult(bool ok) : ok(ok) {
    }
    bool ok;
};

#define define_ffi_obj_wrapped_type(name, ty)       \
    using name = FFIObjWrapper<ty>*;                \
    extern "C" void ffi_##name##_delete(name ptr) { \
        delete ptr;                                 \
    }

define_ffi_obj_wrapped_type(FFI_PlanPtr, std::shared_ptr<Plan>);
define_ffi_obj_wrapped_type(FFI_TableStreamPtr, std::unique_ptr<TableStream>);
define_ffi_obj_wrapped_type(FFI_TablePtr, std::unique_ptr<OwnedTable>);
define_ffi_obj_wrapped_type(FFI_ScalarPtr, std::shared_ptr<cudf::scalar>);
define_ffi_obj_wrapped_type(FFI_ExprBuilderPtr, std::shared_ptr<ExprBuilder>);

#define catch_exceptions_and_return(ret)                                     \
    catch (cudf::logic_error err) {                                          \
        spdlog::error("cudf logic error({}): {}", __func__, err.what());     \
        return (ret);                                                        \
    }                                                                        \
    catch (cudf::data_type_error err) {                                      \
        spdlog::error("cudf data type error({}): {}", __func__, err.what()); \
        std::cerr << err.stacktrace();                                       \
        return (ret);                                                        \
    }                                                                        \
    catch (cudf::cuda_error err) {                                           \
        spdlog::error("cudf cuda error({}): {}", __func__, err.what());      \
        std::cerr << err.stacktrace();                                       \
        return (ret);                                                        \
    }                                                                        \
    catch (std::runtime_error err) {                                         \
        spdlog::error("runtime error({}): {}", __func__, err.what());        \
        return (ret);                                                        \
    }                                                                        \
    catch (std::exception err) {                                             \
        spdlog::error("exception({}): {}", __func__, err.what());            \
        return (ret);                                                        \
    }

struct alignas(8) FFI_CudfDataSource {
    void (*dtor)(FFI_CudfDataSource* self);
    size_t (*data_size)(FFI_CudfDataSource const* self);
    FFIResult<size_t> (*read)(
        FFI_CudfDataSource* self, size_t offset, uint8_t* buf, size_t buf_size);
    void* raw;

    std::unique_ptr<cudf::io::datasource> to_cudf_io_datasource() {
        struct Wrapper : public cudf::io::datasource {
            public:
            FFI_CudfDataSource inner;

            Wrapper(FFI_CudfDataSource wrapped) : inner(wrapped) {
            }

            ~Wrapper() {
                inner.dtor(&inner);
            }

            std::unique_ptr<datasource::buffer> host_read(size_t offset, size_t size) {
                auto buffer = std::vector<uint8_t>(size);
                auto total_read_size = 0;

                while (total_read_size < size) {
                    auto read_size = host_read(offset, size, &buffer[total_read_size]);
                    if (read_size == 0) {
                        throw std::runtime_error("got unexpected EOF while reading datasource");
                    }
                    total_read_size += read_size;
                }
                return owning_buffer<std::vector<uint8_t>>::create(std::move(buffer));
            }

            size_t host_read(size_t offset, size_t size, uint8_t* dst) {
                auto read_size_ret = inner.read(&inner, offset, dst, size);
                if (!read_size_ret.ok) {
                    throw std::runtime_error("error reading datasource");
                }
                return read_size_ret.value;
            }

            size_t size() const {
                return inner.data_size(&inner);
            }
        };
        return std::make_unique<Wrapper>(*this);
    }
};

extern "C" FFI_PlanPtr ffi_new_split_table_plan(
    FFI_PlanPtr input_plan,
    size_t batch_size) {
    try {
        auto plan = std::make_shared<SplitTablePlan>(input_plan->inner, batch_size);
        return new FFIObjWrapper((std::shared_ptr<Plan>&&)std::move(plan));
    }
    catch_exceptions_and_return(nullptr);
}

extern "C" FFI_PlanPtr ffi_new_union_plan(
    FFI_PlanPtr* input_plans_ptr,
    size_t input_plans_len,
    ArrowSchema* ffi_schema) {
    try {
        auto input_plans = std::vector<std::shared_ptr<Plan>>();
        for (auto i = 0; i < input_plans_len; i++) {
            input_plans.push_back(input_plans_ptr[i]->inner);
        }

        auto schema_ret = arrow::ImportSchema(ffi_schema);
        if (!schema_ret.ok()) {
            return nullptr;
        }
        auto schema = schema_ret.ValueUnsafe();

        auto plan = std::make_shared<UnionPlan>(std::move(input_plans), schema);
        return new FFIObjWrapper((std::shared_ptr<Plan>&&)std::move(plan));
    }
    catch_exceptions_and_return(nullptr);
}

extern "C" FFI_PlanPtr ffi_new_parquet_scan_plan(
    FFI_CudfDataSource* ffi_datasource,
    FFI_ExprBuilderPtr expr,
    size_t read_offset,
    size_t read_size,
    ArrowSchema* ffi_read_schema) {
    try {
        auto datasource = ffi_datasource->to_cudf_io_datasource();
        auto read_schema_ret = arrow::ImportSchema(ffi_read_schema);
        if (!read_schema_ret.ok()) {
            return nullptr;
        }
        auto read_schema = read_schema_ret.ValueUnsafe();

        auto plan = std::make_shared<ParquetScanPlan>(
            std::move(datasource), expr->inner, read_offset, read_size, read_schema);
        return new FFIObjWrapper((std::shared_ptr<Plan>&&)std::move(plan));
    }
    catch_exceptions_and_return(nullptr);
}

extern "C" FFI_PlanPtr ffi_new_project_plan(
    FFI_PlanPtr input_plan,
    FFI_ExprBuilderPtr* exprs_ptr,
    size_t exprs_len,
    ArrowSchema* ffi_schema) {
    try {
        auto schema_ret = arrow::ImportSchema(ffi_schema);
        if (!schema_ret.ok()) {
            return nullptr;
        }
        auto schema = schema_ret.MoveValueUnsafe();

        auto exprs = std::vector<std::shared_ptr<ExprBuilder>>();
        for (size_t i = 0; i < exprs_len; i++) {
            exprs.push_back(exprs_ptr[i]->inner);
        }
        auto plan = std::make_shared<ProjectPlan>(input_plan->inner, exprs, schema);
        return new FFIObjWrapper((std::shared_ptr<Plan>&&)std::move(plan));
    }
    catch_exceptions_and_return(nullptr);
}

extern "C" FFI_PlanPtr ffi_new_filter_plan(FFI_PlanPtr input_plan, FFI_ExprBuilderPtr expr) {
    try {
        auto plan = std::make_shared<FilterPlan>(input_plan->inner, expr->inner);
        return new FFIObjWrapper((std::shared_ptr<Plan>&&)std::move(plan));
    }
    catch_exceptions_and_return(nullptr);
}

extern "C" FFIResult<FFI_TableStreamPtr> ffi_execute_plan(FFI_PlanPtr plan) {
    try {
        spdlog::info("cudf executing plan:\n{}", desc_plan_tree(plan->inner));
        auto table_stream_ret = plan->inner->execute();
        if (!table_stream_ret.ok()) {
            return FFIResult(false, (FFI_TableStreamPtr) nullptr);
        }
        return FFIResult(true, new FFIObjWrapper(std::move(table_stream_ret.ValueUnsafe())));
    }
    catch_exceptions_and_return(FFIResult(false, (FFI_TableStreamPtr) nullptr));
}

extern "C" FFIResult<bool> ffi_table_stream_has_next(FFI_TableStreamPtr table_stream) {
    try {
        auto has_next_ret = table_stream->inner->has_next();
        if (!has_next_ret.ok()) {
            return FFIResult(false, false);
        }
        return FFIResult(true, has_next_ret.ValueUnsafe());
    }
    catch_exceptions_and_return(FFIResult(false, false));
}

extern "C" FFIResult<FFI_TablePtr> ffi_table_stream_next(FFI_TableStreamPtr table_stream) {
    try {
        auto next_ret = table_stream->inner->next();
        if (!next_ret.ok()) {
            return FFIResult(false, (FFI_TablePtr) nullptr);
        }
        return FFIResult(
            true, new FFIObjWrapper(std::make_unique<OwnedTable>(next_ret.MoveValueUnsafe())));
    }
    catch_exceptions_and_return(FFIResult(false, (FFI_TablePtr) nullptr));
}

extern "C" FFIResult<void> ffi_table_to_arrow(
    FFI_TablePtr table, ArrowSchema* ffi_schema, ArrowArray* ffi_array) {
    try {
        auto schema_ret = arrow::ImportType(ffi_schema);
        if (!schema_ret.ok()) {
            return FFIResult<void>(false);
        }
        auto schema = schema_ret.ValueUnsafe();

        auto device_table = cudf::to_arrow_host(table->inner->view());
        auto arrow_table_ret = arrow::ImportDeviceArray(device_table.get(), schema);
        if (!arrow_table_ret.ok()) {
            return FFIResult<void>(false);
        }
        auto arrow_table = arrow_table_ret.ValueUnsafe();

        if (!arrow::ExportArray(*arrow_table, ffi_array).ok()) {
            return FFIResult<void>(false);
        }
        return FFIResult<void>(true);
    }
    catch_exceptions_and_return(FFIResult<void>(false));
}

#define ffi_def_numeric_scalar(rust_ty, c_ty)                                             \
    extern "C" FFI_ScalarPtr ffi_##rust_ty##_scalar(c_ty value) {                         \
        try {                                                                             \
            auto scalar = std::make_shared<cudf::numeric_scalar<c_ty>>(value);            \
            return new FFIObjWrapper((std::shared_ptr<cudf::scalar>&&)std::move(scalar)); \
        }                                                                                 \
        catch_exceptions_and_return(nullptr);                                             \
    }

ffi_def_numeric_scalar(i8, int8_t);
ffi_def_numeric_scalar(i16, int16_t);
ffi_def_numeric_scalar(i32, int32_t);
ffi_def_numeric_scalar(i64, int64_t);
ffi_def_numeric_scalar(u8, uint8_t);
ffi_def_numeric_scalar(u16, uint16_t);
ffi_def_numeric_scalar(u32, uint32_t);
ffi_def_numeric_scalar(u64, uint64_t);
ffi_def_numeric_scalar(f32, float);
ffi_def_numeric_scalar(f64, double);

extern "C" FFI_ScalarPtr ffi_string_scalar(char const* value) {
    try {
        auto scalar = std::make_shared<cudf::string_scalar>(std::string(value));
        return new FFIObjWrapper((std::shared_ptr<cudf::scalar>&&)std::move(scalar));
    }
    catch_exceptions_and_return(nullptr);
}

extern "C" FFI_ExprBuilderPtr ffi_new_expr_builder() {
    try {
        return new FFIObjWrapper(std::make_shared<ExprBuilder>());
    }
    catch_exceptions_and_return(nullptr);
}

extern "C" size_t ffi_expr_builder_add_literal(
    FFI_ExprBuilderPtr expr_builder, FFI_ScalarPtr scalar_value) {
    try {
        return expr_builder->inner->add_literal(scalar_value->inner);
    }
    catch_exceptions_and_return(-1);
}

extern "C" size_t ffi_expr_builder_add_column_reference(
    FFI_ExprBuilderPtr expr_builder, size_t column_idx) {
    try {
        return expr_builder->inner->add_column_reference(column_idx);
    }
    catch_exceptions_and_return(-1);
}

extern "C" size_t ffi_expr_builder_add_column_name_reference(
    FFI_ExprBuilderPtr expr_builder, char const* name) {
    try {
        return expr_builder->inner->add_column_name_reference(name);
    }
    catch_exceptions_and_return(-1);
}

extern "C" size_t ffi_expr_builder_add_unary_expr(
    FFI_ExprBuilderPtr expr_builder, size_t arg1_expr_id, int32_t op) {
    try {
        return expr_builder->inner->add_unary_operation(arg1_expr_id, op);
    }
    catch_exceptions_and_return(-1);
}

extern "C" size_t ffi_expr_builder_add_binary_expr(
    FFI_ExprBuilderPtr expr_builder, size_t arg1_expr_id, size_t arg2_expr_id, int32_t op) {
    try {
        return expr_builder->inner->add_binary_operation(arg1_expr_id, arg2_expr_id, op);
    }
    catch_exceptions_and_return(-1);
}
