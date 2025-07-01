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

use std::sync::Arc;

use blaze_cudf_bridge::{
    io::CudfDataSource,
    plans::{
        filter::CudfFilterPlan, parquet_scan::CudfParquetScanPlan, project::CudfProjectPlan,
        union::CudfUnionPlan, CudfPlan, CudfPlanRef,
    },
};
use datafusion::{
    datasource::listing::FileRange,
    error::Result,
    logical_expr::Operator,
    physical_expr::PhysicalExprRef,
    physical_plan::{
        expressions::{lit, BinaryExpr, Column, Literal, SCAndExpr},
        metrics::Time,
        ExecutionPlan,
    },
};
use datafusion_ext_commons::{df_unimplemented_err, downcast_any};

use super::expr::convert_datafusion_expr_to_cudf;
use crate::{
    filter_exec::FilterExec, parquet_exec::ParquetExec, project_exec::ProjectExec,
    rename_columns_exec::RenameColumnsExec, scan::internal_file_reader::InternalFileReader,
};

pub fn convert_datafusion_plan_to_cudf(exec: &dyn ExecutionPlan) -> Result<CudfPlanRef> {
    if let Ok(exec) = downcast_any!(exec, ParquetExec) {
        return convert_parquet_scan(exec);
    }
    if let Ok(exec) = downcast_any!(exec, RenameColumnsExec) {
        return convert_rename_columns(exec);
    }
    if let Ok(exec) = downcast_any!(exec, ProjectExec) {
        return convert_project(exec);
    }
    if let Ok(exec) = downcast_any!(exec, FilterExec) {
        return convert_filter(exec);
    }
    df_unimplemented_err!("cudf not supported for plan: {exec:?}")
}

fn convert_parquet_scan(exec: &ParquetExec) -> Result<CudfPlanRef> {
    let io_time = Time::new();
    let config = exec.file_scan_config();
    let fs_provider = exec.get_fs_provider(&io_time)?;
    let mut inputs = vec![];

    for part_file in config.file_groups.iter().flatten() {
        assert!(
            part_file.partition_values.is_empty(),
            "XXX partition tables are not yet supported: {:?}",
            part_file.partition_values,
        );
        let range = part_file.range.clone().unwrap_or(FileRange {
            start: 0,
            end: i64::MAX,
        });
        let object_meta = part_file.object_meta.clone();
        inputs.push((object_meta, range));
    }

    struct InternalFileDataSource {
        file_reader: InternalFileReader,
        file_size: usize,
    }
    impl CudfDataSource for InternalFileDataSource {
        fn data_size(&self) -> usize {
            self.file_size
        }

        fn read(&mut self, offset: usize, buf: &mut [u8]) -> arrow::error::Result<usize> {
            let range = offset..offset + buf.len();
            self.file_reader.read_fully_into_buffer(range, buf)?;
            Ok(buf.len())
        }
    }

    let schema = exec.schema();

    // cudf only supports <column cmp literal> filters
    let predicate_exprs = split_expr_by_logical_ands(exec.predicate_expr().unwrap_or(lit(true)))
        .into_iter()
        .filter(|expr| {
            matches!(
            downcast_any!(&expr, BinaryExpr),
            Ok(expr) if
                downcast_any!(expr.left(), Column).is_ok() &&
                downcast_any!(expr.right(), Literal).is_ok())
        })
        .collect::<Vec<_>>();
    let predicate_expr = join_exprs_by_logical_ands(&predicate_exprs);
    let cudf_predicate_expr = convert_datafusion_expr_to_cudf(&predicate_expr, true)?;

    let mut scan_plans: Vec<Arc<dyn CudfPlan>> = vec![];
    for input in inputs {
        let data_source = Box::new(InternalFileDataSource {
            file_size: input.0.size,
            file_reader: InternalFileReader::try_new(fs_provider.clone(), input.0.clone())?,
        });
        let scan_plan = Arc::new(CudfParquetScanPlan::try_new(
            data_source,
            &cudf_predicate_expr,
            input.1.start as usize,
            input.1.end as usize - input.1.start as usize,
            schema.clone(),
        )?);
        scan_plans.push(scan_plan);
    }

    if scan_plans.len() == 1 {
        return Ok(scan_plans[0].clone());
    }
    Ok(Arc::new(CudfUnionPlan::try_new(&scan_plans, schema)?))
}

fn convert_rename_columns(exec: &RenameColumnsExec) -> Result<CudfPlanRef> {
    let output_schema = exec.schema();
    let input_schema = exec.children()[0].schema();

    if output_schema.fields().len() != input_schema.fields.len()
        || output_schema
            .fields()
            .iter()
            .zip(input_schema.fields())
            .all(|(f1, f2)| f1.name() == f2.name() && f1.is_nullable() == f2.is_nullable())
    {
        return df_unimplemented_err!(
            "cudf-bridge only supports RenameColumnsExec with same data types"
        );
    }

    let input = convert_datafusion_plan_to_cudf(exec.children()[0].as_ref())?;
    let cudf_project_exprs = (0..output_schema.fields().len())
        .map(|i| {
            let col: PhysicalExprRef = Arc::new(Column::new("", i));
            Ok(convert_datafusion_expr_to_cudf(&col, false)?)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(CudfProjectPlan::try_new(
        input,
        &cudf_project_exprs.iter().collect::<Vec<_>>(),
        output_schema,
    )?))
}

fn convert_project(exec: &ProjectExec) -> Result<CudfPlanRef> {
    let schema = exec.schema();
    let cudf_exprs = exec
        .named_exprs()
        .iter()
        .map(|(expr, _name)| convert_datafusion_expr_to_cudf(expr, false))
        .collect::<Result<Vec<_>>>()?;
    let cudf_input = convert_datafusion_plan_to_cudf(exec.children()[0].as_ref())?;
    Ok(Arc::new(CudfProjectPlan::try_new(
        cudf_input,
        &cudf_exprs.iter().collect::<Vec<_>>(),
        schema,
    )?))
}

fn convert_filter(exec: &FilterExec) -> Result<CudfPlanRef> {
    let predicate_expr = join_exprs_by_logical_ands(exec.predicates());
    let cudf_predicate_expr = convert_datafusion_expr_to_cudf(&predicate_expr, false)?;
    let cudf_input = convert_datafusion_plan_to_cudf(exec.children()[0].as_ref())?;
    Ok(Arc::new(CudfFilterPlan::try_new(
        cudf_input,
        &cudf_predicate_expr,
    )?))
}

fn split_expr_by_logical_ands(expr: PhysicalExprRef) -> Vec<PhysicalExprRef> {
    if let Ok(expr) = downcast_any!(&expr, SCAndExpr) {
        let splitted_left = split_expr_by_logical_ands(expr.left.clone());
        let splitted_right = split_expr_by_logical_ands(expr.right.clone());
        return [splitted_left, splitted_right].concat();
    }
    if let Ok(expr) = downcast_any!(&expr, BinaryExpr) {
        if expr.op() == &Operator::And {
            let splitted_left = split_expr_by_logical_ands(expr.left().clone());
            let splitted_right = split_expr_by_logical_ands(expr.right().clone());
            return [splitted_left, splitted_right].concat();
        }
    }
    vec![expr]
}

fn join_exprs_by_logical_ands(expr: &[PhysicalExprRef]) -> PhysicalExprRef {
    expr.iter()
        .cloned()
        .reduce(|pred1, pred2| Arc::new(BinaryExpr::new(pred1, Operator::And, pred2)))
        .unwrap_or(lit(true))
}
