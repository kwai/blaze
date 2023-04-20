// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! This module contains code to prune "containers" of row groups
//! based on statistics prior to execution. This can lead to
//! significant performance improvements by avoiding the need
//! to evaluate a plan on entire containers (e.g. an entire file)
//!
//! For example, DataFusion uses this code to prune (skip) row groups
//! while reading parquet files if it can be determined from the
//! predicate that nothing in the row group can match.
//!
//! This code can also be used by other systems to prune other
//! entities (e.g. entire files) if the statistics are known via some
//! other source (e.g. a catalog)
//!
//! Copied from datafusion v13.0.0, modified to support pruning
//! IsNotNull operator.

use std::{collections::HashSet, sync::Arc};

use arrow::record_batch::RecordBatchOptions;
use arrow::{
    array::{new_null_array, ArrayRef, BooleanArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{downcast_value, Column, DFSchema, ScalarValue};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::utils::expr_to_columns;
use datafusion::logical_expr::{
    binary_expr, cast, substring, try_cast, BinaryExpr, Cast, ColumnarValue, ExprSchemable, TryCast,
};
use datafusion::physical_expr::create_physical_expr;
use datafusion::prelude::{and, lit};
use datafusion::{
    error::{DataFusionError, Result},
    logical_expr::{Expr, Operator},
    physical_expr::PhysicalExpr,
};

/// Interface to pass statistics information to [`PruningPredicate`]
///
/// Returns statistics for containers / files of data in Arrays.
///
/// For example, for the following three files with a single column
/// ```text
/// file1: column a: min=5, max=10
/// file2: column a: No stats
/// file2: column a: min=20, max=30
/// ```
///
/// PruningStatistics should return:
///
/// ```text
/// min_values("a") -> Some([5, Null, 20])
/// max_values("a") -> Some([10, Null, 30])
/// min_values("X") -> None
/// ```
pub trait PruningStatistics {
    /// return the number of values (including nulls)
    fn num_rows(&self, column: &Column) -> Option<ArrayRef>;

    /// return the minimum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows
    fn min_values(&self, column: &Column) -> Option<ArrayRef>;

    /// return the maximum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows.
    fn max_values(&self, column: &Column) -> Option<ArrayRef>;

    /// return the number of containers (e.g. row groups) being
    /// pruned with these statistics
    fn num_containers(&self) -> usize;

    /// return the number of null values for the named column as an
    /// `Option<UInt64Array>`.
    ///
    /// Note: the returned array must contain `num_containers()` rows.
    fn null_counts(&self, column: &Column) -> Option<ArrayRef>;
}

/// Evaluates filter expressions on statistics in order to
/// prune data containers (e.g. parquet row group)
///
/// See [`PruningPredicate::try_new`] for more information.
#[derive(Debug, Clone)]
pub struct PruningPredicate {
    /// The input schema against which the predicate will be evaluated
    schema: SchemaRef,
    /// Actual pruning predicate (rewritten in terms of column min/max statistics)
    predicate_expr: Arc<dyn PhysicalExpr>,
    /// The statistics required to evaluate this predicate
    required_columns: RequiredStatColumns,
    /// Logical predicate from which this predicate expr is derived (required for serialization)
    logical_expr: Expr,
}

impl PruningPredicate {
    /// Try to create a new instance of [`PruningPredicate`]
    ///
    /// This will translate the provided `expr` filter expression into
    /// a *pruning predicate*.
    ///
    /// A pruning predicate is one that has been rewritten in terms of
    /// the min and max values of column references and that evaluates
    /// to FALSE if the filter predicate would evaluate FALSE *for
    /// every row* whose values fell within the min / max ranges (aka
    /// could be pruned).
    ///
    /// The pruning predicate evaluates to TRUE or NULL
    /// if the filter predicate *might* evaluate to TRUE for at least
    /// one row whose values fell within the min/max ranges (in other
    /// words they might pass the predicate)
    ///
    /// For example, the filter expression `(column / 2) = 4` becomes
    /// the pruning predicate
    /// `(column_min / 2) <= 4 && 4 <= (column_max / 2))`
    pub fn try_new(expr: Expr, schema: SchemaRef) -> Result<Self> {
        // build predicate expression once
        let mut required_columns = RequiredStatColumns::new();
        let logical_predicate_expr =
            build_predicate_expression(&expr, schema.as_ref(), &mut required_columns)?;
        let stat_fields = required_columns
            .iter()
            .map(|(_, _, f)| f.clone())
            .collect::<Vec<_>>();
        let stat_schema = Schema::new(stat_fields);
        let stat_dfschema = DFSchema::try_from(stat_schema.clone())?;

        // TODO allow these properties to be passed in
        let execution_props = ExecutionProps::new();
        let predicate_expr = create_physical_expr(
            &logical_predicate_expr,
            &stat_dfschema,
            &stat_schema,
            &execution_props,
        )?;
        Ok(Self {
            schema,
            predicate_expr,
            required_columns,
            logical_expr: expr,
        })
    }

    /// For each set of statistics, evaluates the pruning predicate
    /// and returns a `bool` with the following meaning for a
    /// all rows whose values match the statistics:
    ///
    /// `true`: There MAY be rows that match the predicate
    ///
    /// `false`: There are no rows that could match the predicate
    ///
    /// Note this function takes a slice of statistics as a parameter
    /// to amortize the cost of the evaluation of the predicate
    /// against a single record batch.
    ///
    /// Note: the predicate passed to `prune` should be simplified as
    /// much as possible (e.g. this pass doesn't handle some
    /// expressions like `b = false`, but it does handle the
    /// simplified version `b`. The predicates are simplified via the
    /// ConstantFolding optimizer pass
    pub fn prune<S: PruningStatistics>(&self, statistics: &S) -> Result<Vec<bool>> {
        // build a RecordBatch that contains the min/max values in the
        // appropriate statistics columns
        let statistics_batch = build_statistics_record_batch(statistics, &self.required_columns)?;

        // Evaluate the pruning predicate on that record batch.
        //
        // Use true when the result of evaluating a predicate
        // expression on a row group is null (aka `None`). Null can
        // arise when the statistics are unknown or some calculation
        // in the predicate means we don't know for sure if the row
        // group can be filtered out or not. To maintain correctness
        // the row group must be kept and thus `true` is returned.
        match self.predicate_expr.evaluate(&statistics_batch)? {
            ColumnarValue::Array(array) => {
                let predicate_array = downcast_value!(array, BooleanArray);

                Ok(predicate_array
                    .into_iter()
                    .map(|x| x.unwrap_or(true)) // None -> true per comments above
                    .collect::<Vec<_>>())
            }
            // result was a column
            ColumnarValue::Scalar(ScalarValue::Boolean(v)) => {
                let v = v.unwrap_or(true); // None -> true per comments above
                Ok(vec![v; statistics.num_containers()])
            }
            other => Err(DataFusionError::Internal(format!(
                "Unexpected result of pruning predicate evaluation. Expected Boolean array \
                     or scalar but got {:?}",
                other
            ))),
        }
    }

    /// Return a reference to the input schema
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns a reference to the logical expr used to construct this pruning predicate
    pub fn logical_expr(&self) -> &Expr {
        &self.logical_expr
    }

    /// Returns a reference to the predicate expr
    pub fn predicate_expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.predicate_expr
    }
}

/// Handles creating references to the min/max statistics
/// for columns as well as recording which statistics are needed
#[derive(Debug, Default, Clone)]
struct RequiredStatColumns {
    /// The statistics required to evaluate this predicate:
    /// * The unqualified column in the input schema
    /// * Statistics type (e.g. Min or Max or Null_Count)
    /// * The field the statistics value should be placed in for
    ///   pruning predicate evaluation
    columns: Vec<(Column, StatisticsType, Field)>,
}

impl RequiredStatColumns {
    fn new() -> Self {
        Self::default()
    }

    /// Returns an iterator over items in columns (see doc on
    /// `self.columns` for details)
    fn iter(&self) -> impl Iterator<Item = &(Column, StatisticsType, Field)> {
        self.columns.iter()
    }

    fn is_stat_column_missing(&self, column: &Column, statistics_type: StatisticsType) -> bool {
        !self
            .columns
            .iter()
            .any(|(c, t, _f)| c == column && t == &statistics_type)
    }

    /// Rewrites column_expr so that all appearances of column
    /// are replaced with a reference to either the min or max
    /// statistics column, while keeping track that a reference to the statistics
    /// column is required
    ///
    /// for example, an expression like `col("foo") > 5`, when called
    /// with Max would result in an expression like `col("foo_max") >
    /// 5` with the appropriate entry noted in self.columns
    fn stat_column_expr(
        &mut self,
        column: &Column,
        column_expr: &Expr,
        field: &Field,
        stat_type: StatisticsType,
        suffix: &str,
    ) -> Result<Expr> {
        let stat_column = Column {
            relation: column.relation.clone(),
            name: format!("{}_{}", column.flat_name(), suffix),
        };

        let stat_field = Field::new(
            stat_column.flat_name().as_str(),
            field.data_type().clone(),
            field.is_nullable(),
        );

        if self.is_stat_column_missing(column, stat_type) {
            // only add statistics column if not previously added
            self.columns.push((column.clone(), stat_type, stat_field));
        }
        rewrite_column_expr(column_expr.clone(), column, &stat_column)
    }

    fn num_rows_column_expr(
        &mut self,
        column: &Column,
        column_expr: &Expr,
        field: &Field,
    ) -> Result<Expr> {
        self.stat_column_expr(
            column,
            column_expr,
            field,
            StatisticsType::NumValues,
            "num_rows",
        )
    }

    /// rewrite col --> col_min
    fn min_column_expr(
        &mut self,
        column: &Column,
        column_expr: &Expr,
        field: &Field,
    ) -> Result<Expr> {
        self.stat_column_expr(column, column_expr, field, StatisticsType::Min, "min")
    }

    /// rewrite col --> col_max
    fn max_column_expr(
        &mut self,
        column: &Column,
        column_expr: &Expr,
        field: &Field,
    ) -> Result<Expr> {
        self.stat_column_expr(column, column_expr, field, StatisticsType::Max, "max")
    }

    /// rewrite col --> col_null_count
    fn null_count_column_expr(
        &mut self,
        column: &Column,
        column_expr: &Expr,
        field: &Field,
    ) -> Result<Expr> {
        self.stat_column_expr(
            column,
            column_expr,
            field,
            StatisticsType::NullCount,
            "null_count",
        )
    }
}

impl From<Vec<(Column, StatisticsType, Field)>> for RequiredStatColumns {
    fn from(columns: Vec<(Column, StatisticsType, Field)>) -> Self {
        Self { columns }
    }
}

/// Build a RecordBatch from a list of statistics, creating arrays,
/// with one row for each PruningStatistics and columns specified in
/// in the required_columns parameter.
///
/// For example, if the requested columns are
/// ```text
/// ("s1", Min, Field:s1_min)
/// ("s2", Max, field:s2_max)
///```
///
/// And the input statistics had
/// ```text
/// S1(Min: 5, Max: 10)
/// S2(Min: 99, Max: 1000)
/// S3(Min: 1, Max: 2)
/// ```
///
/// Then this function would build a record batch with 2 columns and
/// one row s1_min and s2_max as follows (s3 is not requested):
///
/// ```text
/// s1_min | s2_max
/// -------+--------
///   5    | 1000
/// ```
fn build_statistics_record_batch<S: PruningStatistics>(
    statistics: &S,
    required_columns: &RequiredStatColumns,
) -> Result<RecordBatch> {
    let mut fields = Vec::<Field>::new();
    let mut arrays = Vec::<ArrayRef>::new();
    // For each needed statistics column:
    for (column, statistics_type, stat_field) in required_columns.iter() {
        let data_type = stat_field.data_type();

        let num_containers = statistics.num_containers();

        let array = match statistics_type {
            StatisticsType::NumValues => statistics.num_rows(column),
            StatisticsType::Min => statistics.min_values(column),
            StatisticsType::Max => statistics.max_values(column),
            StatisticsType::NullCount => statistics.null_counts(column),
        };
        let array = array.unwrap_or_else(|| new_null_array(data_type, num_containers));

        if num_containers != array.len() {
            return Err(DataFusionError::Internal(format!(
                "mismatched statistics length. Expected {}, got {}",
                num_containers,
                array.len()
            )));
        }

        // cast statistics array to required data type (e.g. parquet
        // provides timestamp statistics as "Int64")
        let array = arrow::compute::cast(&array, data_type)?;

        fields.push(stat_field.clone());
        arrays.push(array);
    }

    let schema = Arc::new(Schema::new(fields));
    // provide the count in case there were no needed statistics
    let mut options = RecordBatchOptions::default();
    options.row_count = Some(statistics.num_containers());

    RecordBatch::try_new_with_options(schema, arrays, &options).map_err(|err| {
        DataFusionError::Plan(format!("Can not create statistics record batch: {}", err))
    })
}

struct PruningExpressionBuilder<'a> {
    column: Column,
    column_expr: Expr,
    op: Operator,
    scalar_expr: Expr,
    field: &'a Field,
    required_columns: &'a mut RequiredStatColumns,
}

impl<'a> PruningExpressionBuilder<'a> {
    fn try_new(
        left: &'a Expr,
        right: &'a Expr,
        op: Operator,
        schema: &'a Schema,
        required_columns: &'a mut RequiredStatColumns,
    ) -> Result<Self> {
        // find column name; input could be a more complicated expression
        let mut left_columns = HashSet::<Column>::new();
        expr_to_columns(left, &mut left_columns)?;
        let mut right_columns = HashSet::<Column>::new();
        expr_to_columns(right, &mut right_columns)?;
        let (column_expr, scalar_expr, columns, correct_operator) =
            match (left_columns.len(), right_columns.len()) {
                (1, 0) => (left, right, left_columns, op),
                (0, 1) => (right, left, right_columns, reverse_operator(op)),
                _ => {
                    // if more than one column used in expression - not supported
                    return Err(DataFusionError::Plan(
                        "Multi-column expressions are not currently supported".to_string(),
                    ));
                }
            };

        let df_schema = DFSchema::try_from(schema.clone())?;
        let (column_expr, correct_operator, scalar_expr) =
            rewrite_expr_to_prunable(column_expr, correct_operator, scalar_expr, df_schema)?;
        let column = columns.iter().next().unwrap().clone();
        let field = match schema.column_with_name(&column.flat_name()) {
            Some((_, f)) => f,
            _ => {
                return Err(DataFusionError::Plan(
                    "Field not found in schema".to_string(),
                ));
            }
        };

        Ok(Self {
            column,
            column_expr,
            op: correct_operator,
            scalar_expr,
            field,
            required_columns,
        })
    }

    fn op(&self) -> Operator {
        self.op
    }

    fn scalar_expr(&self) -> &Expr {
        &self.scalar_expr
    }

    fn min_column_expr(&mut self) -> Result<Expr> {
        self.required_columns
            .min_column_expr(&self.column, &self.column_expr, self.field)
    }

    fn max_column_expr(&mut self) -> Result<Expr> {
        self.required_columns
            .max_column_expr(&self.column, &self.column_expr, self.field)
    }
}

/// This function is designed to rewrite the column_expr to
/// ensure the column_expr is monotonically increasing.
///
/// For example,
/// 1. `col > 10`
/// 2. `-col > 10` should be rewritten to `col < -10`
/// 3. `!col = true` would be rewritten to `col = !true`
/// 4. `abs(a - 10) > 0` not supported
/// 5. `cast(can_prunable_expr) > 10`
/// 6. `try_cast(can_prunable_expr) > 10`
///
/// More rewrite rules are still in progress.
fn rewrite_expr_to_prunable(
    column_expr: &Expr,
    op: Operator,
    scalar_expr: &Expr,
    schema: DFSchema,
) -> Result<(Expr, Operator, Expr)> {
    if !is_compare_op(op) {
        return Err(DataFusionError::Plan(
            "rewrite_expr_to_prunable only support compare expression".to_string(),
        ));
    }

    match column_expr {
        // `col op lit()`
        Expr::Column(_) => Ok((column_expr.clone(), op, scalar_expr.clone())),
        // `cast(col) op lit()`
        Expr::Cast(Cast { expr, data_type }) => {
            let from_type = expr.get_type(&schema)?;
            verify_support_type_for_prune(&from_type, data_type)?;
            let (left, op, right) = rewrite_expr_to_prunable(expr, op, scalar_expr, schema)?;
            Ok((cast(left, data_type.clone()), op, right))
        }
        // `try_cast(col) op lit()`
        Expr::TryCast(TryCast { expr, data_type }) => {
            let from_type = expr.get_type(&schema)?;
            verify_support_type_for_prune(&from_type, data_type)?;
            let (left, op, right) = rewrite_expr_to_prunable(expr, op, scalar_expr, schema)?;
            Ok((try_cast(left, data_type.clone()), op, right))
        }
        // `-col > lit()`  --> `col < -lit()`
        Expr::Negative(c) => {
            let (left, op, right) = rewrite_expr_to_prunable(c, op, scalar_expr, schema)?;
            Ok((left, reverse_operator(op), Expr::Negative(Box::new(right))))
        }
        // `!col = true` --> `col = !true`
        Expr::Not(c) => {
            if op != Operator::Eq && op != Operator::NotEq {
                return Err(DataFusionError::Plan(
                    "Not with operator other than Eq / NotEq is not supported".to_string(),
                ));
            }
            return match c.as_ref() {
                Expr::Column(_) => Ok((
                    c.as_ref().clone(),
                    reverse_operator(op),
                    Expr::Not(Box::new(scalar_expr.clone())),
                )),
                _ => Err(DataFusionError::Plan(format!(
                    "Not with complex expression {:?} is not supported",
                    column_expr
                ))),
            };
        }

        _ => Err(DataFusionError::Plan(format!(
            "column expression {:?} is not supported",
            column_expr
        ))),
    }
}

fn is_compare_op(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
    )
}

// The pruning logic is based on the comparing the min/max bounds.
// Must make sure the two type has order.
// For example, casts from string to numbers is not correct.
// Because the "13" is less than "3" with UTF8 comparison order.
fn verify_support_type_for_prune(from_type: &DataType, to_type: &DataType) -> Result<()> {
    // TODO: support other data type for prunable cast or try cast
    if matches!(
        from_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Decimal128(_, _)
    ) && matches!(
        to_type,
        DataType::Int8 | DataType::Int32 | DataType::Int64 | DataType::Decimal128(_, _)
    ) {
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!(
            "Try Cast/Cast with from type {} to type {} is not supported",
            from_type, to_type
        )))
    }
}

/// replaces a column with an old name with a new name in an expression
fn rewrite_column_expr(e: Expr, column_old: &Column, column_new: &Column) -> Result<Expr> {
    e.transform(&|expr| {
        Ok(match expr {
            Expr::Column(c) if c == *column_old => {
                Transformed::Yes(Expr::Column(column_new.clone()))
            }
            _ => Transformed::No(expr),
        })
    })
}

fn reverse_operator(op: Operator) -> Operator {
    match op {
        Operator::Lt => Operator::Gt,
        Operator::Gt => Operator::Lt,
        Operator::LtEq => Operator::GtEq,
        Operator::GtEq => Operator::LtEq,
        _ => op,
    }
}

/// Given a column reference to `column`, returns a pruning
/// expression in terms of the min and max that will evaluate to true
/// if the column may contain values, and false if definitely does not
/// contain values
fn build_single_column_expr(
    column: &Column,
    schema: &Schema,
    required_columns: &mut RequiredStatColumns,
    is_not: bool, // if true, treat as !col
) -> Option<Expr> {
    let field = schema.field_with_name(&column.name).ok()?;

    if matches!(field.data_type(), &DataType::Boolean) {
        let col_ref = Expr::Column(column.clone());

        let min = required_columns
            .min_column_expr(column, &col_ref, field)
            .ok()?;
        let max = required_columns
            .max_column_expr(column, &col_ref, field)
            .ok()?;

        // remember -- we want an expression that is:
        // TRUE: if there may be rows that match
        // FALSE: if there are no rows that match
        if is_not {
            // The only way we know a column couldn't match is if both the min and max are true
            // !(min && max)
            Some(!(min.and(max)))
        } else {
            // the only way we know a column couldn't match is if both the min and max are false
            // !(!min && !max) --> min || max
            Some(min.or(max))
        }
    } else {
        None
    }
}

/// Given an expression reference to `expr`, if `expr` is a column expression,
/// returns a pruning expression in terms of IsNull that will evaluate to true
/// if the column may contain null, and false if definitely does not
/// contain null.
fn build_is_null_column_expr(
    expr: &Expr,
    schema: &Schema,
    required_columns: &mut RequiredStatColumns,
) -> Option<Expr> {
    match expr {
        Expr::Column(ref col) => {
            if !schema
                .fields
                .iter()
                .filter(|e| e.data_type().is_nested())
                .filter(|e| e.name() == &col.name)
                .collect::<Vec<_>>()
                .is_empty()
            {
                return None;
            }
            let field = schema.field_with_name(&col.name).ok()?;

            let null_count_field = &Field::new(field.name(), DataType::UInt64, true);
            required_columns
                .null_count_column_expr(col, expr, null_count_field)
                .map(|null_count_column_expr| {
                    // IsNull(column) => null_count is null or null_count > 0
                    let is_null = null_count_column_expr.clone().is_null();
                    is_null.or(null_count_column_expr.gt(lit::<u64>(0)))
                })
                .ok()
        }
        _ => None,
    }
}

fn build_is_not_null_column_expr(
    expr: &Expr,
    schema: &Schema,
    required_columns: &mut RequiredStatColumns,
) -> Option<Expr> {
    match expr {
        Expr::Column(ref col) => {
            if !schema
                .fields
                .iter()
                .filter(|e| e.data_type().is_nested())
                .filter(|e| e.name() == &col.name)
                .collect::<Vec<_>>()
                .is_empty()
            {
                return None;
            }

            let field = schema.field_with_name(&col.name).ok()?;

            let null_count_field = &Field::new(field.name(), DataType::UInt64, true);
            let num_rows_field = null_count_field;

            let null_count_column_expr =
                required_columns.null_count_column_expr(col, expr, null_count_field);
            let num_rows_column_expr =
                required_columns.num_rows_column_expr(col, expr, num_rows_field);

            // IsNotNull(column) => null_count is null or null_count != num_rows
            match (null_count_column_expr, num_rows_column_expr) {
                (Ok(a), Ok(b)) => Some({
                    let a_is_null = a.clone().is_null();
                    a_is_null.or(a.not_eq(b))
                }),
                _ => None,
            }
        }
        _ => None,
    }
}

fn build_starts_with_predicate_expression_expr(
    column: &Column,
    prefix: &str,
    schema: &Schema,
    required_columns: &mut RequiredStatColumns,
) -> Result<Expr> {
    let column_expr = Expr::Column(column.clone());
    let field = schema.field_with_name(&column.name)?;
    let min = required_columns.min_column_expr(column, &column_expr, field)?;
    let max = required_columns.max_column_expr(column, &column_expr, field)?;

    let min_prefix = substring(
        min,
        Expr::Literal(ScalarValue::Int64(Some(1))),
        Expr::Literal(ScalarValue::Int64(Some(prefix.len() as i64 + 1))),
    );
    let max_prefix = substring(
        max,
        Expr::Literal(ScalarValue::Int64(Some(1))),
        Expr::Literal(ScalarValue::Int64(Some(prefix.len() as i64 + 1))),
    );
    Ok(and(
        min_prefix.lt_eq(Expr::Literal(prefix.into())),
        max_prefix.gt_eq(Expr::Literal(prefix.into())),
    ))
}

/// Translate logical filter expression into pruning predicate
/// expression that will evaluate to FALSE if it can be determined no
/// rows between the min/max values could pass the predicates.
///
/// Returns the pruning predicate as an [`Expr`]
fn build_predicate_expression(
    expr: &Expr,
    schema: &Schema,
    required_columns: &mut RequiredStatColumns,
) -> Result<Expr> {
    // Returned for unsupported expressions. Such expressions are
    // converted to TRUE. This can still be useful when multiple
    // conditions are joined using AND such as: column > 10 AND TRUE
    let unhandled = lit(true);

    // predicate expression can only be a binary expression
    let (left, op, right) = match expr {
        Expr::ScalarUDF { fun, args } => {
            if fun.name == "string_starts_with" {
                let column = match args.get(0) {
                    Some(Expr::Column(column)) => column,
                    _ => {
                        return Err(DataFusionError::NotImplemented(
                            "pruning unsupported".to_string(),
                        ))
                    }
                };
                let prefix = match args.get(1) {
                    Some(Expr::Literal(ScalarValue::Utf8(Some(prefix)))) => prefix,
                    _ => {
                        return Err(DataFusionError::NotImplemented(
                            "pruning unsupported".to_string(),
                        ))
                    }
                };
                return build_starts_with_predicate_expression_expr(
                    column,
                    prefix,
                    schema,
                    required_columns,
                );
            }
            return Err(DataFusionError::NotImplemented(
                "UDF unsupported for pruning".to_string(),
            ));
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => (left, *op, right),
        Expr::IsNull(expr) => {
            let expr =
                build_is_null_column_expr(expr, schema, required_columns).unwrap_or(unhandled);
            return Ok(expr);
        }
        Expr::IsNotNull(expr) => {
            let expr =
                build_is_not_null_column_expr(expr, schema, required_columns).unwrap_or(unhandled);
            return Ok(expr);
        }
        Expr::Column(col) => {
            let expr =
                build_single_column_expr(col, schema, required_columns, false).unwrap_or(unhandled);
            return Ok(expr);
        }
        // match !col (don't do so recursively)
        Expr::Not(input) => {
            if let Expr::Column(col) = input.as_ref() {
                let expr = build_single_column_expr(col, schema, required_columns, true)
                    .unwrap_or(unhandled);
                return Ok(expr);
            } else {
                return Ok(unhandled);
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } if !list.is_empty() && list.len() < 20 => {
            let eq_fun = if *negated { Expr::not_eq } else { Expr::eq };
            let re_fun = if *negated { Expr::and } else { Expr::or };
            let change_expr = list
                .iter()
                .map(|e| eq_fun(*expr.clone(), e.clone()))
                .reduce(re_fun)
                .unwrap();
            return build_predicate_expression(&change_expr, schema, required_columns);
        }
        _ => {
            return Ok(unhandled);
        }
    };

    if op == Operator::And || op == Operator::Or {
        let left_expr = build_predicate_expression(left, schema, required_columns)?;
        let right_expr = build_predicate_expression(right, schema, required_columns)?;
        return Ok(binary_expr(left_expr, op, right_expr));
    }

    let expr_builder = PruningExpressionBuilder::try_new(left, right, op, schema, required_columns);
    let mut expr_builder = match expr_builder {
        Ok(builder) => builder,
        // allow partial failure in predicate expression generation
        // this can still produce a useful predicate when multiple conditions are joined using AND
        Err(_) => {
            return Ok(unhandled);
        }
    };

    let statistics_expr = build_statistics_expr(&mut expr_builder).unwrap_or(unhandled);
    Ok(statistics_expr)
}

fn build_statistics_expr(expr_builder: &mut PruningExpressionBuilder) -> Result<Expr> {
    let statistics_expr = match expr_builder.op() {
        Operator::NotEq => {
            // column != literal => (min, max) = literal =>
            // !(min != literal && max != literal) ==>
            // min != literal || literal != max
            let min_column_expr = expr_builder.min_column_expr()?;
            let max_column_expr = expr_builder.max_column_expr()?;
            min_column_expr
                .not_eq(expr_builder.scalar_expr().clone())
                .or(expr_builder.scalar_expr().clone().not_eq(max_column_expr))
        }
        Operator::Eq => {
            // column = literal => (min, max) = literal => min <= literal && literal <= max
            // (column / 2) = 4 => (column_min / 2) <= 4 && 4 <= (column_max / 2)
            let min_column_expr = expr_builder.min_column_expr()?;
            let max_column_expr = expr_builder.max_column_expr()?;
            min_column_expr
                .lt_eq(expr_builder.scalar_expr().clone())
                .and(expr_builder.scalar_expr().clone().lt_eq(max_column_expr))
        }
        Operator::Gt => {
            // column > literal => (min, max) > literal => max > literal
            expr_builder
                .max_column_expr()?
                .gt(expr_builder.scalar_expr().clone())
        }
        Operator::GtEq => {
            // column >= literal => (min, max) >= literal => max >= literal
            expr_builder
                .max_column_expr()?
                .gt_eq(expr_builder.scalar_expr().clone())
        }
        Operator::Lt => {
            // column < literal => (min, max) < literal => min < literal
            expr_builder
                .min_column_expr()?
                .lt(expr_builder.scalar_expr().clone())
        }
        Operator::LtEq => {
            // column <= literal => (min, max) <= literal => min <= literal
            expr_builder
                .min_column_expr()?
                .lt_eq(expr_builder.scalar_expr().clone())
        }
        // other expressions are not supported
        _ => {
            return Err(DataFusionError::Plan(
                "expressions other than (neq, eq, gt, gteq, lt, lteq) are not supported"
                    .to_string(),
            ))
        }
    };
    Ok(statistics_expr)
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum StatisticsType {
    NumValues,
    Min,
    Max,
    NullCount,
}
