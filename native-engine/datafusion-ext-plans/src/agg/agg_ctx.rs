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

use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};

use arrow::{
    array::{Array, ArrayRef, BinaryArray, RecordBatchOptions},
    datatypes::{DataType, Field, Fields, Schema, SchemaRef},
    record_batch::RecordBatch,
    row::{RowConverter, Rows, SortField},
};
use blaze_jni_bridge::{
    conf,
    conf::{BooleanConf, DoubleConf, IntConf},
};
use datafusion::{
    common::{cast::as_binary_array, Result},
    physical_expr::PhysicalExprRef,
};
use parking_lot::Mutex;

use crate::{
    agg::{
        acc::AccTable,
        agg::{Agg, IdxSelection},
        AggExecMode, AggExpr, AggMode, GroupingExpr, AGG_BUF_COLUMN_NAME,
    },
    common::{
        cached_exprs_evaluator::CachedExprsEvaluator,
        execution_context::{ExecutionContext, WrappedRecordBatchSender},
    },
};

pub struct AggContext {
    pub exec_mode: AggExecMode,
    pub need_partial_update: bool,
    pub need_partial_merge: bool,
    pub need_final_merge: bool,
    pub need_partial_update_aggs: Vec<(usize, Arc<dyn Agg>)>,
    pub need_partial_merge_aggs: Vec<(usize, Arc<dyn Agg>)>,

    pub output_schema: SchemaRef,
    pub grouping_row_converter: Arc<Mutex<RowConverter>>,
    pub groupings: Vec<GroupingExpr>,
    pub aggs: Vec<AggExpr>,
    pub supports_partial_skipping: bool,
    pub partial_skipping_ratio: f64,
    pub partial_skipping_min_rows: usize,
    pub partial_skipping_skip_spill: bool,
    pub is_expand_agg: bool,
    pub agg_expr_evaluator: CachedExprsEvaluator,
}

impl Debug for AggContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[groupings={:?}, aggs={:?}]", self.groupings, self.aggs,)
    }
}

impl AggContext {
    pub fn try_new(
        exec_mode: AggExecMode,
        input_schema: SchemaRef,
        groupings: Vec<GroupingExpr>,
        aggs: Vec<AggExpr>,
        supports_partial_skipping: bool,
        is_expand_agg: bool,
    ) -> Result<Self> {
        let grouping_schema = Arc::new(Schema::new(
            groupings
                .iter()
                .map(|grouping: &GroupingExpr| {
                    Ok(Field::new(
                        grouping.field_name.as_str(),
                        grouping.expr.data_type(&input_schema)?,
                        grouping.expr.nullable(&input_schema)?,
                    ))
                })
                .collect::<Result<Fields>>()?,
        ));
        let grouping_row_converter = Arc::new(Mutex::new(RowConverter::new(
            grouping_schema
                .fields()
                .iter()
                .map(|field| SortField::new(field.data_type().clone()))
                .collect(),
        )?));

        // final aggregates may not exist along with partial/partial-merge
        let need_partial_update = aggs.iter().any(|agg| agg.mode == AggMode::Partial);
        let need_partial_merge = aggs.iter().any(|agg| agg.mode != AggMode::Partial);
        let need_final_merge = aggs.iter().any(|agg| agg.mode == AggMode::Final);
        assert!(!(need_final_merge && aggs.iter().any(|agg| agg.mode != AggMode::Final)));

        let need_partial_update_aggs: Vec<(usize, Arc<dyn Agg>)> = aggs
            .iter()
            .enumerate()
            .filter(|(_idx, agg)| agg.mode.is_partial())
            .map(|(idx, agg)| (idx, agg.agg.clone()))
            .collect();
        let need_partial_merge_aggs: Vec<(usize, Arc<dyn Agg>)> = aggs
            .iter()
            .enumerate()
            .filter(|(_idx, agg)| !agg.mode.is_partial())
            .map(|(idx, agg)| (idx, agg.agg.clone()))
            .collect();

        let mut agg_fields = vec![];
        if need_final_merge {
            for agg in &aggs {
                agg_fields.push(Field::new(
                    &agg.field_name,
                    agg.agg.data_type().clone(),
                    agg.agg.nullable(),
                ));
            }
        } else {
            agg_fields.push(Field::new(AGG_BUF_COLUMN_NAME, DataType::Binary, false));
        }
        let agg_schema = Arc::new(Schema::new(agg_fields));
        let output_schema = Arc::new(Schema::new(
            [
                grouping_schema.fields().to_vec(),
                agg_schema.fields().to_vec(),
            ]
            .concat(),
        ));

        let agg_exprs_flatten: Vec<PhysicalExprRef> = aggs
            .iter()
            .filter(|agg| agg.mode.is_partial())
            .flat_map(|agg| agg.agg.exprs())
            .collect();
        let agg_expr_evaluator_output_schema = Arc::new(Schema::new(
            agg_exprs_flatten
                .iter()
                .map(|e| {
                    Ok(Field::new(
                        "",
                        e.data_type(&input_schema)?,
                        e.nullable(&input_schema)?,
                    ))
                })
                .collect::<Result<Fields>>()?,
        ));
        let agg_expr_evaluator = CachedExprsEvaluator::try_new(
            vec![],
            agg_exprs_flatten,
            agg_expr_evaluator_output_schema,
        )?;

        let (partial_skipping_ratio, partial_skipping_min_rows, partial_skipping_skip_spill) =
            if supports_partial_skipping {
                (
                    conf::PARTIAL_AGG_SKIPPING_RATIO.value().unwrap_or(0.999),
                    conf::PARTIAL_AGG_SKIPPING_MIN_ROWS.value().unwrap_or(20000) as usize,
                    conf::PARTIAL_AGG_SKIPPING_SKIP_SPILL
                        .value()
                        .unwrap_or(false),
                )
            } else {
                Default::default()
            };

        Ok(Self {
            exec_mode,
            need_partial_update,
            need_partial_merge,
            need_final_merge,
            need_partial_update_aggs,
            need_partial_merge_aggs,
            output_schema,
            grouping_row_converter,
            groupings,
            aggs,
            agg_expr_evaluator,
            supports_partial_skipping,
            partial_skipping_ratio,
            partial_skipping_min_rows,
            partial_skipping_skip_spill,
            is_expand_agg,
        })
    }

    pub fn create_acc_table(&self, num_rows: usize) -> AccTable {
        AccTable::new(
            self.aggs
                .iter()
                .map(|agg| agg.agg.create_acc_column(num_rows))
                .collect(),
            num_rows,
        )
    }

    pub fn create_grouping_rows(&self, input_batch: &RecordBatch) -> Result<Rows> {
        let grouping_arrays: Vec<ArrayRef> = self
            .groupings
            .iter()
            .map(|grouping| grouping.expr.evaluate(&input_batch))
            .map(|r| r.and_then(|columnar| columnar.into_array(input_batch.num_rows())))
            .collect::<Result<_>>()
            .map_err(|err| err.context("agg: evaluating grouping arrays error"))?;
        Ok(self
            .grouping_row_converter
            .lock()
            .convert_columns(&grouping_arrays)?)
    }

    pub fn update_batch_to_acc_table(
        &self,
        batch: &RecordBatch,
        acc_table: &mut AccTable,
        acc_idx: IdxSelection,
    ) -> Result<()> {
        // partial update
        if self.need_partial_update {
            let agg_exprs_batch = self.agg_expr_evaluator.filter_project(&batch)?;
            let mut input_arrays = Vec::with_capacity(self.aggs.len());
            let mut offset = 0;
            for agg in &self.aggs {
                if agg.mode.is_partial() {
                    let num_agg_exprs = agg.agg.exprs().len();
                    let prepared = agg.agg.prepare_partial_args(
                        &agg_exprs_batch.columns()[offset..][..num_agg_exprs],
                    )?;
                    input_arrays.push(prepared);
                    offset += num_agg_exprs;
                } else {
                    input_arrays.push(vec![]);
                }
            }

            self.partial_update(
                acc_table,
                acc_idx,
                &input_arrays,
                IdxSelection::Range(0, batch.num_rows()),
            )?;
        }

        // partial merge
        if self.need_partial_merge {
            let mut merging_acc_table = self.create_acc_table(0);

            if self.need_partial_merge {
                let partial_merged_array = as_binary_array(batch.columns().last().unwrap())?;
                let array = partial_merged_array
                    .iter()
                    .map(|bytes| bytes.unwrap())
                    .collect::<Vec<_>>();
                let mut offsets = vec![0; partial_merged_array.len()];

                for (agg_idx, _agg) in &self.need_partial_merge_aggs {
                    let acc_col = &mut merging_acc_table.cols_mut()[*agg_idx];
                    acc_col.unfreeze_from_rows(&array, &mut offsets)?;
                }
            }

            self.partial_merge(
                acc_table,
                acc_idx,
                &mut merging_acc_table,
                IdxSelection::Range(0, batch.num_rows()),
            )?;
        }
        Ok(())
    }

    pub fn build_agg_columns(
        &self,
        acc_table: &mut AccTable,
        idx: IdxSelection,
    ) -> Result<Vec<ArrayRef>> {
        if self.need_final_merge {
            // output final merged value
            let mut agg_columns = vec![];
            for (agg, acc_col) in self.aggs.iter().zip(acc_table.cols_mut()) {
                let values = agg.agg.final_merge(acc_col, idx)?;
                agg_columns.push(values);
            }
            Ok(agg_columns)
        } else {
            // output acc as a binary column
            let freezed = self.freeze_acc_table(acc_table, idx)?;
            Ok(vec![Arc::new(BinaryArray::from_iter_values(freezed))])
        }
    }

    pub fn convert_records_to_batch(
        &self,
        keys: &[impl AsRef<[u8]>],
        acc_table: &mut AccTable,
        acc_idx: IdxSelection,
    ) -> Result<RecordBatch> {
        let grouping_row_converter = self.grouping_row_converter.lock();
        let grouping_row_parser = grouping_row_converter.parser();
        let grouping_columns = grouping_row_converter.convert_rows(
            keys.iter()
                .map(|key| grouping_row_parser.parse(key.as_ref())),
        )?;
        let agg_columns = self.build_agg_columns(acc_table, acc_idx)?;

        // at least one column exists
        Ok(RecordBatch::try_new(
            self.output_schema.clone(),
            [grouping_columns, agg_columns].concat(),
        )?)
    }

    pub fn partial_update(
        &self,
        acc_table: &mut AccTable,
        acc_idx: IdxSelection,
        input_arrays: &[Vec<ArrayRef>],
        input_idx: IdxSelection,
    ) -> Result<()> {
        if self.need_partial_update {
            for (agg_idx, agg) in &self.need_partial_update_aggs {
                let acc_col = &mut acc_table.cols_mut()[*agg_idx];
                agg.partial_update(acc_col, acc_idx, &input_arrays[*agg_idx], input_idx)?;
            }
        }
        Ok(())
    }

    pub fn partial_merge(
        &self,
        acc_table: &mut AccTable,
        acc_idx: IdxSelection,
        merging_acc_table: &mut AccTable,
        merging_acc_idx: IdxSelection,
    ) -> Result<()> {
        if self.need_partial_merge {
            for (agg_idx, agg) in &self.need_partial_merge_aggs {
                let acc_col = &mut acc_table.cols_mut()[*agg_idx];
                let merging_acc_col = &mut merging_acc_table.cols_mut()[*agg_idx];
                agg.partial_merge(acc_col, acc_idx, merging_acc_col, merging_acc_idx)?;
            }
        }
        Ok(())
    }

    pub fn freeze_acc_table(
        &self,
        acc_table: &AccTable,
        acc_idx: IdxSelection,
    ) -> Result<Vec<Vec<u8>>> {
        let mut vec = vec![vec![]; acc_idx.len()];
        for acc_col in acc_table.cols() {
            acc_col.freeze_to_rows(acc_idx, &mut vec)?;
        }
        Ok(vec)
    }

    pub fn unfreeze_acc_table(&self, acc_table: &mut AccTable, data: &[&[u8]]) -> Result<()> {
        let mut offsets = vec![0; data.len()];
        for acc_col in acc_table.cols_mut() {
            acc_col.unfreeze_from_rows(data, &mut offsets)?;
        }
        Ok(())
    }
    pub async fn process_partial_skipped(
        &self,
        batch: RecordBatch,
        exec_ctx: Arc<ExecutionContext>,
        sender: Arc<WrappedRecordBatchSender>,
    ) -> Result<()> {
        let batch_num_rows = batch.num_rows();
        let mut acc_table = self.create_acc_table(batch_num_rows);
        self.update_batch_to_acc_table(
            &batch,
            &mut acc_table,
            IdxSelection::Range(0, batch_num_rows),
        )?;

        // create output batch
        let grouping_columns = self
            .groupings
            .iter()
            .map(|grouping| grouping.expr.evaluate(&batch))
            .map(|r| r.and_then(|columnar| columnar.into_array(batch_num_rows)))
            .collect::<Result<Vec<ArrayRef>>>()?;
        let agg_columns =
            self.build_agg_columns(&mut acc_table, IdxSelection::Range(0, batch_num_rows))?;
        let output_batch = RecordBatch::try_new_with_options(
            self.output_schema.clone(),
            [grouping_columns, agg_columns].concat(),
            &RecordBatchOptions::new().with_row_count(Some(batch_num_rows)),
        )?;

        exec_ctx
            .baseline_metrics()
            .record_output(output_batch.num_rows());
        sender.send(output_batch).await;
        return Ok(());
    }
}
