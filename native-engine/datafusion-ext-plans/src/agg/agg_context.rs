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
    array::{Array, ArrayRef, BinaryArray, BinaryBuilder},
    datatypes::{DataType, Field, Fields, Schema, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
    row::{RowConverter, Rows, SortField},
};
use blaze_jni_bridge::{
    conf,
    conf::{DoubleConf, IntConf},
};
use datafusion::{
    common::{cast::as_binary_array, Result},
    physical_expr::PhysicalExprRef,
};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;

use crate::{
    agg::{
        agg_buf::{create_agg_buf_from_initial_value, AccumInitialValue, AggBuf},
        Agg, AggExecMode, AggExpr, AggMode, GroupingExpr, AGG_BUF_COLUMN_NAME,
    },
    common::cached_exprs_evaluator::CachedExprsEvaluator,
};

pub struct AggContext {
    pub exec_mode: AggExecMode,
    pub need_partial_update: bool,
    pub need_partial_merge: bool,
    pub need_final_merge: bool,
    pub need_partial_update_aggs: Vec<(usize, Arc<dyn Agg>)>,
    pub need_partial_merge_aggs: Vec<(usize, Arc<dyn Agg>)>,

    pub grouping_schema: SchemaRef,
    pub agg_schema: SchemaRef,
    pub output_schema: SchemaRef,
    pub grouping_row_converter: Arc<Mutex<RowConverter>>,
    pub groupings: Vec<GroupingExpr>,
    pub aggs: Vec<AggExpr>,
    pub initial_agg_buf: AggBuf,
    pub initial_input_agg_buf: AggBuf,
    pub initial_input_buffer_offset: usize,
    pub supports_partial_skipping: bool,
    pub partial_skipping_ratio: f64,
    pub partial_skipping_min_rows: usize,
    pub agg_expr_evaluator: CachedExprsEvaluator,

    // agg buf addr offsets/lens of every aggs
    pub agg_buf_addrs: Box<[u64]>,
    pub agg_buf_addr_offsets: Box<[usize]>,
    pub agg_buf_addr_counts: Box<[usize]>,
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
        initial_input_buffer_offset: usize,
        supports_partial_skipping: bool,
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

        let initial_accums: Box<[AccumInitialValue]> = aggs
            .iter()
            .flat_map(|agg: &AggExpr| agg.agg.accums_initial())
            .cloned()
            .collect();
        let (initial_agg_buf, agg_buf_addrs) = create_agg_buf_from_initial_value(&initial_accums)?;

        // in distinct aggregrations, partial and partial-merge may happen at the same
        // time, i.e:
        //
        //  Agg [groupings=[], aggs=[
        //      AggExpr { field_name: "#747", mode: PartialMerge, agg: Count(...) },
        //      AggExpr { field_name: "#748", mode: Partial, agg: Count(Column { name:
        // "#640", index: 0 }) }  ]]
        //  Agg [groupings=[GroupingExpr { field_name: "#640", ...], aggs=[
        //      AggExpr { field_name: "#747", mode: PartialMerge, agg: Count(...) }
        //  ]]
        //
        // in this situation, the processing agg_buf has more fields than input. so we
        // need to maintain a standalone agg_buf for the input.
        // the addrs is not used because the extra fields are always in the last. the
        // processing addrs can be reused.
        let initial_input_accums: Box<[AccumInitialValue]> = need_partial_merge_aggs
            .iter()
            .flat_map(|(_, agg)| agg.accums_initial())
            .cloned()
            .collect();
        let (initial_input_agg_buf, _input_agg_buf_addrs) =
            create_agg_buf_from_initial_value(&initial_input_accums)?;

        let mut agg_buf_addr_offsets = Vec::with_capacity(aggs.len());
        let mut agg_buf_addr_counts = Vec::with_capacity(aggs.len());
        let mut offset = 0;
        for agg in &aggs {
            let len = agg.agg.accums_initial().len();
            agg_buf_addr_offsets.push(offset);
            agg_buf_addr_counts.push(len);
            offset += len;
        }

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
            input_schema.clone(),
            agg_expr_evaluator_output_schema,
        )?;

        let (partial_skipping_ratio, partial_skipping_min_rows) = if supports_partial_skipping {
            (
                conf::PARTIAL_AGG_SKIPPING_RATIO.value()?,
                conf::PARTIAL_AGG_SKIPPING_MIN_ROWS.value()? as usize,
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
            grouping_schema,
            grouping_row_converter,
            agg_schema,
            groupings,
            aggs,
            initial_agg_buf,
            initial_input_agg_buf,
            agg_buf_addrs,
            agg_expr_evaluator,
            initial_input_buffer_offset,
            supports_partial_skipping,
            partial_skipping_ratio,
            partial_skipping_min_rows,
            agg_buf_addr_offsets: agg_buf_addr_offsets.into(),
            agg_buf_addr_counts: agg_buf_addr_counts.into(),
        })
    }

    pub fn create_grouping_rows(&self, input_batch: &RecordBatch) -> Result<Rows> {
        let grouping_arrays: Vec<ArrayRef> = self
            .groupings
            .iter()
            .map(|grouping| grouping.expr.evaluate(&input_batch))
            .map(|r| r.map(|columnar| columnar.into_array(input_batch.num_rows())))
            .collect::<Result<_>>()
            .map_err(|err| err.context("agg: evaluating grouping arrays error"))?;
        Ok(self
            .grouping_row_converter
            .lock()
            .convert_columns(&grouping_arrays)?)
    }

    pub fn create_input_arrays(&self, input_batch: &RecordBatch) -> Result<Vec<Vec<ArrayRef>>> {
        if !self.need_partial_update {
            return Ok(vec![]);
        }
        let agg_exprs_batch = self.agg_expr_evaluator.filter_project(input_batch)?;

        let mut input_arrays = Vec::with_capacity(self.aggs.len());
        let mut offset = 0;
        for agg in &self.aggs {
            if agg.mode.is_partial() {
                let num_agg_exprs = agg.agg.exprs().len();
                let prepared = agg
                    .agg
                    .prepare_partial_args(&agg_exprs_batch.columns()[offset..][..num_agg_exprs])?;
                input_arrays.push(prepared);
                offset += num_agg_exprs;
            } else {
                input_arrays.push(vec![]);
            }
        }
        Ok(input_arrays)
    }

    pub fn get_input_agg_buf_array<'a>(
        &self,
        input_batch: &'a RecordBatch,
    ) -> Result<&'a BinaryArray> {
        if self.need_partial_merge {
            as_binary_array(input_batch.columns().last().unwrap())
        } else {
            static EMPTY_BINARY_ARRAY: OnceCell<BinaryArray> = OnceCell::new();
            Ok(EMPTY_BINARY_ARRAY.get_or_init(|| BinaryArray::from_iter_values([[]; 0])))
        }
    }

    pub fn build_agg_columns(
        &self,
        mut records: Vec<(impl AsRef<[u8]>, AggBuf)>,
    ) -> Result<Vec<ArrayRef>> {
        let mut agg_columns = vec![];

        if self.need_final_merge {
            // output final merged value
            let mut agg_bufs: Vec<AggBuf> =
                records.into_iter().map(|(_, agg_buf)| agg_buf).collect();
            for (idx, agg) in self.aggs.iter().enumerate() {
                let addrs = &self.agg_buf_addrs[self.agg_buf_addr_offsets[idx]..];
                let values = agg.agg.final_batch_merge(&mut agg_bufs, addrs)?;
                agg_columns.push(values);
            }
        } else {
            // output agg_buf as a binary column
            let mut binary_array = BinaryBuilder::with_capacity(records.len(), 0);
            for (_, agg_buf) in &mut records {
                let agg_buf_bytes = agg_buf.save_to_bytes()?;
                binary_array.append_value(agg_buf_bytes);
            }
            agg_columns.push(Arc::new(binary_array.finish()));
        }
        Ok(agg_columns)
    }

    pub fn convert_records_to_batch(
        &self,
        records: Vec<(impl AsRef<[u8]>, AggBuf)>,
    ) -> Result<RecordBatch> {
        let row_count = records.len();
        let grouping_row_converter = self.grouping_row_converter.lock();
        let grouping_row_parser = grouping_row_converter.parser();
        let grouping_columns = grouping_row_converter.convert_rows(
            records
                .iter()
                .map(|(key, _)| grouping_row_parser.parse(key.as_ref())),
        )?;
        let agg_columns = self.build_agg_columns(records)?;

        Ok(RecordBatch::try_new_with_options(
            self.output_schema.clone(),
            [grouping_columns, agg_columns].concat(),
            &RecordBatchOptions::new().with_row_count(Some(row_count)),
        )?)
    }

    pub fn agg_addrs(&self, agg_idx: usize) -> &[u64] {
        let addr_offset = self.agg_buf_addr_offsets[agg_idx];
        &self.agg_buf_addrs[addr_offset..]
    }

    pub fn partial_update_input(
        &self,
        agg_buf: &mut AggBuf,
        input_arrays: &[Vec<ArrayRef>],
        row_idx: usize,
    ) -> Result<()> {
        if self.need_partial_update {
            for (idx, agg) in &self.need_partial_update_aggs {
                agg.partial_update(agg_buf, self.agg_addrs(*idx), &input_arrays[*idx], row_idx)?;
            }
        }
        Ok(())
    }

    pub fn partial_batch_update_input(
        &self,
        agg_bufs: &mut [AggBuf],
        input_arrays: &[Vec<ArrayRef>],
    ) -> Result<usize> {
        let mut mem_diff = 0;
        if self.need_partial_update {
            for (idx, agg) in &self.need_partial_update_aggs {
                mem_diff +=
                    agg.partial_batch_update(agg_bufs, self.agg_addrs(*idx), &input_arrays[*idx])?;
            }
        }
        Ok(mem_diff)
    }

    pub fn partial_update_input_all(
        &self,
        agg_buf: &mut AggBuf,
        input_arrays: &[Vec<ArrayRef>],
    ) -> Result<()> {
        if self.need_partial_update {
            for (idx, agg) in &self.need_partial_update_aggs {
                agg.partial_update_all(agg_buf, self.agg_addrs(*idx), &input_arrays[*idx])?;
            }
        }
        Ok(())
    }

    pub fn partial_merge_input(
        &self,
        agg_buf: &mut AggBuf,
        agg_buf_array: &BinaryArray,
        row_idx: usize,
    ) -> Result<()> {
        if self.need_partial_merge {
            let mut input_agg_buf = self.initial_input_agg_buf.clone();
            input_agg_buf.load_from_bytes(agg_buf_array.value(row_idx))?;
            for (idx, agg) in &self.need_partial_merge_aggs {
                agg.partial_merge(agg_buf, &mut input_agg_buf, self.agg_addrs(*idx))?;
            }
        }
        Ok(())
    }

    pub fn partial_batch_merge_input(
        &self,
        agg_bufs: &mut [AggBuf],
        agg_buf_array: &BinaryArray,
    ) -> Result<usize> {
        let mut mem_diff = 0;
        if self.need_partial_merge {
            let mut input_agg_bufs = agg_buf_array
                .iter()
                .map(|value| {
                    let mut input_agg_buf = self.initial_input_agg_buf.clone();
                    input_agg_buf.load_from_bytes(value.unwrap())?;
                    Ok(input_agg_buf)
                })
                .collect::<Result<Vec<_>>>()?;
            for (idx, agg) in &self.need_partial_merge_aggs {
                mem_diff +=
                    agg.partial_batch_merge(agg_bufs, &mut input_agg_bufs, self.agg_addrs(*idx))?;
            }
        }
        Ok(mem_diff)
    }

    pub fn partial_merge_input_all(
        &self,
        agg_buf: &mut AggBuf,
        agg_buf_array: &BinaryArray,
    ) -> Result<()> {
        if self.need_partial_merge {
            let mut input_agg_buf = self.initial_input_agg_buf.clone();
            for row_idx in 0..agg_buf_array.len() {
                input_agg_buf.load_from_bytes(agg_buf_array.value(row_idx))?;
                for (idx, agg) in &self.need_partial_merge_aggs {
                    agg.partial_merge(agg_buf, &mut input_agg_buf, self.agg_addrs(*idx))?;
                }
            }
        }
        Ok(())
    }
}
