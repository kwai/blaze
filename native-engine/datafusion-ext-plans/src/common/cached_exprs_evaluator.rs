// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    any::Any,
    cell::RefCell,
    collections::{HashMap, HashSet},
    fmt::{Debug, Display, Formatter},
    hash::{Hash, Hasher},
    rc::Rc,
    sync::Arc,
};

use arrow::{
    array::{Array, ArrayRef, BooleanArray},
    compute::{filter, filter_record_batch, prep_null_mask_filter},
    datatypes::{DataType, Schema, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use datafusion::{
    common::{
        Result, ScalarValue,
        cast::as_boolean_array,
        tree_node::{Transformed, TreeNode},
    },
    logical_expr::Operator,
    physical_expr::{
        PhysicalExpr, PhysicalExprRef,
        expressions::{BinaryExpr, CaseExpr, Column, Literal, NoOp, SCAndExpr, SCOrExpr},
    },
    physical_expr_common::{physical_expr::DynEq, utils::scatter},
    physical_plan::ColumnarValue,
};
use datafusion_ext_commons::{arrow::cast::cast, downcast_any, uda::UserDefinedArray};
use itertools::Itertools;
use parking_lot::Mutex;

pub struct CachedExprsEvaluator {
    transformed_projection_exprs: Vec<PhysicalExprRef>,
    transformed_pruned_filter_exprs: Vec<(PhysicalExprRef, Vec<usize>)>,
    output_schema: SchemaRef,
    cache: Cache,
}

impl CachedExprsEvaluator {
    pub fn try_new(
        filter_exprs: Vec<PhysicalExprRef>,
        projection_exprs: Vec<PhysicalExprRef>,
        output_schema: SchemaRef,
    ) -> Result<Self> {
        let (transformed_exprs, cache) =
            transform_to_cached_exprs(&[filter_exprs.clone(), projection_exprs.clone()].concat())?;
        let (transformed_filter_exprs, transformed_projection_exprs) =
            transformed_exprs.split_at(filter_exprs.len());

        let transformed_pruned_filter_exprs = transformed_filter_exprs
            .into_iter()
            .map(|expr| prune_expr_cols(expr))
            .collect();
        let transformed_projection_exprs = transformed_projection_exprs.to_vec();

        Ok(Self {
            transformed_projection_exprs,
            transformed_pruned_filter_exprs,
            output_schema,
            cache,
        })
    }

    pub fn filter(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        self.cache.with(|_| self.filter_impl(batch))
    }

    pub fn filter_project(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        self.cache.with(|_| self.filter_project_impl(batch))
    }

    fn filter_impl(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        // filter
        let mut current_filtered = FilterStat::AllRetained;
        for (filter_expr, proj) in &self.transformed_pruned_filter_exprs {
            // save previous selected, used for scattering
            let previous_selected = if let FilterStat::Some(array) = &current_filtered {
                Some(array.clone())
            } else {
                None
            };

            // execute current filtering
            current_filtered = filter_one_pred(batch, filter_expr, proj, current_filtered)?;
            if let FilterStat::AllFiltered = &current_filtered {
                return Ok(RecordBatch::new_empty(batch.schema()));
            }
            if let FilterStat::Some(selected) = &current_filtered {
                self.cache.update_all(|value| {
                    if let Some(ColumnarValue::Array(array)) = &value {
                        return Ok(Some(ColumnarValue::Array({
                            // also apply filter on cached arrays
                            if let Some(uda) = array.as_any().downcast_ref::<UserDefinedArray>() {
                                if let Some(previous_selected) = &previous_selected {
                                    Arc::new(uda.scatter(previous_selected)?.filter(selected)?)
                                } else {
                                    Arc::new(uda.filter(selected)?)
                                }
                            } else {
                                if let Some(previous_selected) = &previous_selected {
                                    filter(&scatter(previous_selected, array)?, selected)?
                                } else {
                                    filter(&array, selected)?
                                }
                            }
                        })));
                    }
                    Ok(value)
                })?;
            }
        }
        let batch = match current_filtered {
            FilterStat::AllFiltered => RecordBatch::new_empty(batch.schema()),
            FilterStat::AllRetained => batch.clone(),
            FilterStat::Some(selected) => filter_record_batch(batch, &selected)?,
        };
        Ok(batch)
    }

    fn filter_project_impl(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        // execute filters, cache are retained for later projection
        let filtered_batch = self.filter_impl(batch)?;
        if filtered_batch.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(self.output_schema.clone()));
        }

        // project
        let output_cols = self
            .transformed_projection_exprs
            .iter()
            .zip(self.output_schema.fields())
            .map(|(expr, field)| {
                let col = expr
                    .evaluate(&filtered_batch)?
                    .into_array(filtered_batch.num_rows())?;
                if col.data_type() != field.data_type() {
                    return cast(col.as_ref(), field.data_type());
                }
                Ok(col)
            })
            .collect::<Result<Vec<ArrayRef>>>()?;
        Ok(RecordBatch::try_new_with_options(
            self.output_schema.clone(),
            output_cols,
            &RecordBatchOptions::new().with_row_count(Some(filtered_batch.num_rows())),
        )?)
    }
}

fn transform_to_cached_exprs(exprs: &[PhysicalExprRef]) -> Result<(Vec<PhysicalExprRef>, Cache)> {
    // count all children exprs
    fn count(expr: &PhysicalExprRef, expr_counts: &mut HashMap<ExprKey, usize>) {
        expr_counts
            .entry(ExprKey(expr.clone()))
            .and_modify(|count| *count += 1)
            .or_insert(1);
        expr.children()
            .iter()
            .for_each(|child| count(&child, expr_counts));
    }
    let mut expr_counts = HashMap::new();
    for expr in exprs {
        count(&expr, &mut expr_counts);
    }

    // find all duplicated exprs (which count is larger than its parent)
    fn collect_dups(
        expr: &PhysicalExprRef,
        parent_count: usize,
        expr_counts: &HashMap<ExprKey, usize>,
        dups: &mut HashSet<ExprKey>,
    ) {
        // ignore trivial leaf exprs
        if matches!(downcast_any!(expr, NoOp), Ok(_))
            || matches!(downcast_any!(expr, Column), Ok(_))
            || matches!(downcast_any!(expr, Literal), Ok(_))
        {
            return;
        }

        // insert exprs with occurrences more than its parent
        let expr_key = ExprKey(expr.clone());
        let current_count = expr_counts.get(&expr_key).cloned().unwrap_or(0);
        if current_count > parent_count {
            dups.insert(expr_key);
        }

        // traverse children, excluding exprs with short circuiting evaluation
        if matches!(downcast_any!(expr, CaseExpr), Ok(_))
            || matches!(downcast_any!(expr, SCAndExpr), Ok(_))
            || matches!(downcast_any!(expr, SCOrExpr), Ok(_))
            || matches!(downcast_any!(expr, BinaryExpr), Ok(e) if e.op() == &Operator::And)
            || matches!(downcast_any!(expr, BinaryExpr), Ok(e) if e.op() == &Operator::Or)
        {
            // short circuiting expression - only first child can be cached
            // first `when` expr can also be cached
            collect_dups(&expr.children()[0], current_count, expr_counts, dups);
            if let Ok(case_expr) = downcast_any!(expr, CaseExpr) {
                if case_expr.expr().is_some() {
                    let children = case_expr.children();
                    if children.len() >= 2 {
                        // cache first `when` expr
                        collect_dups(&expr.children()[1], current_count, expr_counts, dups);
                    }
                }
            }
        } else {
            expr.children().iter().for_each(|child| {
                collect_dups(child, current_count, expr_counts, dups);
            });
        }
    }
    let mut dups = HashSet::new();
    for expr in exprs {
        collect_dups(&expr, 1, &expr_counts, &mut dups);
    }

    // generate cached expr ids
    let cached_expr_ids: HashMap<ExprKey, usize> = dups
        .into_iter()
        .enumerate()
        .map(|(id, expr)| (expr, id))
        .collect();

    // transform all exprs with CachedExpr using dup_exprs
    fn transform(
        expr: PhysicalExprRef,
        cached_expr_ids: &HashMap<ExprKey, usize>,
        cache: &Cache,
    ) -> Result<PhysicalExprRef> {
        // ignore trivial leaf exprs
        if matches!(downcast_any!(expr, NoOp), Ok(_))
            || matches!(downcast_any!(expr, Column), Ok(_))
            || matches!(downcast_any!(expr, Literal), Ok(_))
        {
            return Ok(expr);
        }

        // get cache id if exists of current expr
        let expr_key = ExprKey(expr.clone());
        let current_cache_id = cached_expr_ids.get(&expr_key).cloned();

        // transform children
        let transformed_expr = if matches!(downcast_any!(expr, CaseExpr), Ok(_))
            || matches!(downcast_any!(expr, SCAndExpr), Ok(_))
            || matches!(downcast_any!(expr, SCOrExpr), Ok(_))
            || matches!(downcast_any!(expr, BinaryExpr), Ok(e) if e.op() == &Operator::And)
            || matches!(downcast_any!(expr, BinaryExpr), Ok(e) if e.op() == &Operator::Or)
        {
            // short circuiting expression - only first child can be cached
            // first `when` expr can also be cached
            let mut children = expr
                .children()
                .iter()
                .map(|&child| child.clone())
                .collect::<Vec<_>>();
            children[0] = transform(children[0].clone(), cached_expr_ids, cache)?;

            if let Some(case_expr) = expr.as_any().downcast_ref::<CaseExpr>() {
                if children.len() >= 2 && case_expr.expr().is_some() {
                    // cache first `when` expr
                    children[1] = transform(children[1].clone(), cached_expr_ids, cache)?;
                }
            }
            expr.clone().with_new_children(children)?
        } else {
            expr.clone().with_new_children(
                expr.children()
                    .into_iter()
                    .map(|child| transform(child.clone(), cached_expr_ids, cache))
                    .collect::<Result<_>>()?,
            )?
        };

        Ok(if let Some(cache_id) = current_cache_id {
            Arc::new(CachedExpr {
                cache: cache.clone(),
                id: cache_id,
                orig_expr: transformed_expr,
            })
        } else {
            transformed_expr
        })
    }

    let cache = Cache::new(cached_expr_ids.len());
    let transformed_exprs = exprs
        .iter()
        .map(|expr| Ok(transform(expr.clone(), &cached_expr_ids, &cache)?))
        .collect::<Result<_>>()?;
    Ok((transformed_exprs, cache))
}

/// A physical expr wrapper to use in HashSet/HashMap
#[derive(Clone, Debug, Hash)]
struct ExprKey(PhysicalExprRef);

impl PartialEq for ExprKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ref().eq(other.0.as_ref())
    }
}

impl Eq for ExprKey {}

/// A physical expr wrapper which supports caching when evaluated more than once
#[derive(Clone)]
struct CachedExpr {
    cache: Cache,
    id: usize,
    orig_expr: PhysicalExprRef,
}

impl Display for CachedExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Debug for CachedExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cached(")?;
        std::fmt::Debug::fmt(&self.orig_expr, f)?;
        write!(f, ")")?;
        Ok(())
    }
}

impl PartialEq for CachedExpr {
    fn eq(&self, other: &Self) -> bool {
        other.id == self.id
    }
}

impl DynEq for CachedExpr {
    fn dyn_eq(&self, other: &dyn Any) -> bool {
        other
            .downcast_ref::<Self>()
            .map(|other| other.eq(self))
            .unwrap_or(false)
    }
}

impl Hash for CachedExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PhysicalExpr for CachedExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.orig_expr.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.orig_expr.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        self.cache.get(self.id, || self.orig_expr.evaluate(batch))
    }

    fn children(&self) -> Vec<&PhysicalExprRef> {
        self.orig_expr.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalExprRef>,
    ) -> Result<PhysicalExprRef> {
        Ok(Arc::new(Self {
            cache: self.cache.clone(),
            id: self.id,
            orig_expr: self.orig_expr.clone().with_new_children(children)?,
        }))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "fmt_sql not used")
    }
}

/// A struct holding all evaluated values of cachable expressions
#[derive(Clone)]
struct Cache {
    values: Arc<Mutex<Vec<Option<ColumnarValue>>>>,
}

impl Cache {
    fn new(len: usize) -> Self {
        Self {
            values: Arc::new(Mutex::new(vec![None; len])),
        }
    }

    fn with<T>(&self, func: impl Fn(&Self) -> Result<T>) -> Result<T> {
        self.reset(); // reset before using cache
        let result = func(&self);
        self.reset(); // reset after using cache (to release holding arrays)
        result
    }

    fn get(
        &self,
        id: usize,
        evaluate_on_vacant: impl Fn() -> Result<ColumnarValue>,
    ) -> Result<ColumnarValue> {
        if let Some(cached) = &self.values.lock()[id] {
            return Ok(cached.clone());
        }
        let cached = evaluate_on_vacant()?;
        self.values.lock()[id] = Some(cached.clone());
        Ok(cached)
    }

    fn update_all(
        &self,
        on_update: impl Fn(Option<ColumnarValue>) -> Result<Option<ColumnarValue>>,
    ) -> Result<()> {
        let current_values = self.values.lock().clone();
        let updated_values = current_values
            .into_iter()
            .map(|value| on_update(value))
            .collect::<Result<_>>()?;
        *self.values.lock() = updated_values;
        Ok(())
    }

    fn reset(&self) {
        self.values.lock().fill(None);
    }
}

/// A enum that represents filter result
pub enum FilterStat {
    AllRetained,
    AllFiltered,
    Some(BooleanArray),
}

/// Get pruned expr with minimal set of input columns
fn prune_expr_cols(expr: &PhysicalExprRef) -> (PhysicalExprRef, Vec<usize>) {
    let used_cols: Rc<RefCell<HashMap<usize, usize>>> = Rc::new(RefCell::default());

    let transformed = expr
        .clone()
        .transform(&|expr: PhysicalExprRef| {
            if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                let used_cols = used_cols.clone();
                let mut used_cols_ref = used_cols.borrow_mut();
                let new_idx = used_cols_ref.len();

                let mapped_idx = *used_cols_ref.entry(col.index()).or_insert(new_idx);
                let mapped_col: PhysicalExprRef = Arc::new(Column::new(col.name(), mapped_idx));
                Ok(Transformed::yes(mapped_col))
            } else {
                Ok(Transformed::yes(expr))
            }
        })
        .unwrap()
        .data;

    let mapped_cols: Vec<usize> = used_cols
        .take()
        .into_iter()
        .sorted_by_key(|(_orig_idx, mapped_idx)| *mapped_idx)
        .map(|(orig_idx, _mapped_idx)| orig_idx)
        .collect();
    (transformed, mapped_cols)
}

/// Execute one filter predicate expr on a record batch with existed FilterStat
fn filter_one_pred(
    batch: &RecordBatch,
    pruned_pred_expr: &PhysicalExprRef,
    pruned_projection: &[usize],
    current_filtered: FilterStat,
) -> Result<FilterStat> {
    let current_selected: Option<BooleanArray> = match &current_filtered {
        FilterStat::AllRetained => None,
        FilterStat::AllFiltered => return Ok(FilterStat::AllFiltered),
        FilterStat::Some(bools) => Some(bools.clone()),
    };

    let pruned_batch = batch.project(pruned_projection)?;
    let pred_ret = match &current_selected {
        Some(selected) => pruned_pred_expr.evaluate_selection(&pruned_batch, selected)?,
        None => pruned_pred_expr.evaluate(&pruned_batch)?,
    };

    match pred_ret {
        ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => Ok(current_filtered),
        ColumnarValue::Scalar(_) => Ok(FilterStat::AllFiltered),
        ColumnarValue::Array(new_selected) => {
            let mut new_selected = as_boolean_array(&new_selected)?.clone();
            if new_selected.null_count() > 0 {
                new_selected = prep_null_mask_filter(&new_selected);
            }
            Ok(FilterStat::Some(new_selected))
        }
    }
}
