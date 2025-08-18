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
    fmt::{Debug, Formatter},
    io::Cursor,
    sync::Arc,
};

use arrow::{
    array::{ArrayRef, BinaryArray, RecordBatchOptions},
    datatypes::{DataType, Field, Fields, Schema, SchemaRef},
    record_batch::RecordBatch,
    row::{RowConverter, Rows, SortField},
};
use auron_jni_bridge::{
    conf,
    conf::{BooleanConf, DoubleConf, IntConf},
};
use datafusion::{
    common::{Result, cast::as_binary_array},
    physical_expr::PhysicalExprRef,
};
use datafusion_ext_commons::{downcast_any, suggested_batch_mem_size};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;

use crate::{
    agg::{
        AGG_BUF_COLUMN_NAME, AggExecMode, AggExpr, AggMode, GroupingExpr,
        acc::AccTable,
        agg::{Agg, IdxSelection},
        agg_hash_map::AggHashMapKey,
        spark_udaf_wrapper::{AccUDAFBufferRowsColumn, SparkUDAFMemTracker, SparkUDAFWrapper},
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
    pub num_spill_buckets: OnceCell<usize>,
    pub udaf_mem_tracker: OnceCell<SparkUDAFMemTracker>,
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
            num_spill_buckets: Default::default(),
            udaf_mem_tracker: Default::default(),
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
        self.update_batch_slice_to_acc_table(batch, 0, batch.num_rows(), acc_table, acc_idx)
    }

    pub fn update_batch_slice_to_acc_table(
        &self,
        batch: &RecordBatch,
        batch_start_idx: usize,
        batch_end_idx: usize,
        acc_table: &mut AccTable,
        acc_idx: IdxSelection,
    ) -> Result<()> {
        // NOTE:
        // arrow-ffi with sliced batch is buggy in older arrow-java, so we use unsliced
        // batch with explicit offsets

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
            let batch_selection = IdxSelection::Range(batch_start_idx, batch_end_idx);
            self.partial_update(acc_table, acc_idx, &input_arrays, batch_selection)?;
        }

        // partial merge
        if self.need_partial_merge {
            let mut merging_acc_table = self.create_acc_table(0);

            if self.need_partial_merge {
                let partial_merged_array = as_binary_array(batch.columns().last().unwrap())?;
                let array = partial_merged_array
                    .iter()
                    .skip(batch_start_idx)
                    .take(batch_end_idx - batch_start_idx)
                    .map(|bytes| bytes.unwrap())
                    .collect::<Vec<_>>();
                let mut cursors = array
                    .iter()
                    .map(|bytes| Cursor::new(bytes.as_bytes()))
                    .collect::<Vec<_>>();

                for (agg_idx, _agg) in &self.need_partial_merge_aggs {
                    let acc_col = &mut merging_acc_table.cols_mut()[*agg_idx];
                    acc_col.unfreeze_from_rows(&mut cursors)?;
                }
            }
            let batch_selection = IdxSelection::Range(0, batch_end_idx - batch_start_idx);
            self.partial_merge(acc_table, acc_idx, &mut merging_acc_table, batch_selection)?;
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
            let udaf_indices_cache = OnceCell::new();
            let mut agg_columns = vec![];
            for (agg, acc_col) in self.aggs.iter().zip(acc_table.cols_mut()) {
                let values = if let Ok(udaf_agg) = downcast_any!(agg.agg, SparkUDAFWrapper) {
                    udaf_agg.final_merge_with_indices_cache(acc_col, idx, &udaf_indices_cache)?
                } else {
                    agg.agg.final_merge(acc_col, idx)?
                };
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
            let udaf_indices_cache = OnceCell::new();
            for (agg_idx, agg) in &self.need_partial_update_aggs {
                let acc_col = &mut acc_table.cols_mut()[*agg_idx];
                // use indices cached version for UDAFs
                if let Ok(udaf_agg) = downcast_any!(agg, SparkUDAFWrapper) {
                    udaf_agg.partial_update_with_indices_cache(
                        acc_col,
                        acc_idx,
                        &input_arrays[*agg_idx],
                        input_idx,
                        &udaf_indices_cache,
                    )?;
                } else {
                    agg.partial_update(acc_col, acc_idx, &input_arrays[*agg_idx], input_idx)?;
                }
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
            let udaf_indices_cache = OnceCell::new();
            for (agg_idx, agg) in &self.need_partial_merge_aggs {
                let acc_col = &mut acc_table.cols_mut()[*agg_idx];
                let merging_acc_col = &mut merging_acc_table.cols_mut()[*agg_idx];

                // use indices cached version for UDAFs
                if let Ok(udaf_agg) = downcast_any!(agg, SparkUDAFWrapper) {
                    udaf_agg.partial_merge_with_indices_cache(
                        acc_col,
                        acc_idx,
                        merging_acc_col,
                        merging_acc_idx,
                        &udaf_indices_cache,
                    )?;
                } else {
                    agg.partial_merge(acc_col, acc_idx, merging_acc_col, merging_acc_idx)?;
                }
            }
        }
        Ok(())
    }

    pub fn freeze_acc_table(
        &self,
        acc_table: &AccTable,
        acc_idx: IdxSelection,
    ) -> Result<Vec<Vec<u8>>> {
        let udaf_indices_cache = OnceCell::new();
        let mut vec = vec![vec![]; acc_idx.len()];
        for acc_col in acc_table.cols() {
            if let Ok(udaf_acc_col) = downcast_any!(acc_col, AccUDAFBufferRowsColumn) {
                udaf_acc_col.freeze_to_rows_with_indices_cache(
                    acc_idx,
                    &mut vec,
                    &udaf_indices_cache,
                )?;
            } else {
                acc_col.freeze_to_rows(acc_idx, &mut vec)?;
            }
        }
        Ok(vec)
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

    pub fn num_spill_buckets(&self, mem_size: usize) -> usize {
        *self
            .num_spill_buckets
            .get_or_init(|| (mem_size / suggested_batch_mem_size() / 2).max(16))
    }

    pub fn get_udaf_mem_tracker(&self) -> Option<&SparkUDAFMemTracker> {
        self.udaf_mem_tracker.get()
    }

    pub fn get_or_try_init_udaf_mem_tracker(&self) -> Result<&SparkUDAFMemTracker> {
        self.udaf_mem_tracker
            .get_or_try_init(|| SparkUDAFMemTracker::try_new())
    }
}
