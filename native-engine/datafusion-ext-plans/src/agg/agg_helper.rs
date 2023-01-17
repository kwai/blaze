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

use crate::agg::{AggExecMode, AggExpr, AggMode, GroupingExpr};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use arrow::row::RowConverter;
use datafusion::common::{Result, ScalarValue};
use datafusion::physical_expr::PhysicalExpr;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use smallvec::SmallVec;

pub type AggRecord = (Box<[u8]>, Box<[ScalarValue]>);

pub struct AggContext {
    pub exec_mode: AggExecMode,
    pub grouping_schema: SchemaRef,
    pub agg_schema: SchemaRef,
    pub output_schema: SchemaRef,
    pub groupings: Vec<GroupingExpr>,
    pub aggs: Vec<AggExpr>,
    pub initial_accums: Box<[ScalarValue]>,
    pub initial_accums_mem_size: usize,
    pub initial_input_buffer_offset: usize,

    // accum offsets/lens of every aggs
    pub accums_offsets: SmallVec<[usize; 64]>,
    pub accums_lens: SmallVec<[usize; 64]>,

    // indices to accums with dynamic size (like strings)
    pub dyn_size_accums_idxes: SmallVec<[usize; 64]>,
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
                .collect::<Result<_>>()?,
        ));

        let mut agg_fields = vec![];
        for agg in &aggs {
            if agg.mode.is_partial() || agg.mode.is_partial_merge() {
                agg_fields.extend(agg.agg.accum_fields().iter().map(|field| {
                    field.clone().with_name(partial_output_field_name(
                        &agg.field_name,
                        field.name(),
                    ))
                }));
            } else {
                agg_fields.push(Field::new(
                    &agg.field_name,
                    agg.agg.data_type().clone(),
                    agg.agg.nullable(),
                ));
            }
        }
        let agg_schema = Arc::new(Schema::new(agg_fields));
        let output_schema = Arc::new(Schema::new(
            [
                grouping_schema.fields().clone(),
                agg_schema.fields().clone(),
            ]
            .concat(),
        ));

        let initial_accums: Box<[ScalarValue]> = aggs
            .iter()
            .flat_map(|agg: &AggExpr| agg.agg.accums_initial())
            .map(|acc| acc.clone())
            .collect();
        let initial_accums_mem_size = initial_accums
            .iter()
            .map(|acc| acc.size())
            .sum::<usize>();
        let dyn_size_accums_idxes = initial_accums
            .iter()
            .enumerate()
            .filter(|(_, acc)| acc.get_datatype() == DataType::Utf8)
            .map(|(i, _)| i)
            .collect();

        let mut accums_offsets = SmallVec::with_capacity(aggs.len());
        let mut accums_lens = SmallVec::with_capacity(aggs.len());
        let mut offset = 0;
        for agg in &aggs {
            let len = agg.agg.accum_fields().len();
            accums_offsets.push(offset);
            accums_lens.push(len);
            offset += len;
        }

        Ok(Self {
            exec_mode,
            output_schema,
            grouping_schema,
            agg_schema,
            groupings,
            aggs,
            initial_accums,
            initial_accums_mem_size,
            initial_input_buffer_offset,
            accums_offsets,
            accums_lens,
            dyn_size_accums_idxes,
        })
    }

    pub fn create_initial_accums(&self) -> Box<[ScalarValue]> {
        self.initial_accums.clone()
    }

    pub fn get_accums_mem_size(&self, accums: &[ScalarValue]) -> usize {
        let dyn_size = self.dyn_size_accums_idxes
            .iter()
            .map(|&i| accums[i].size())
            .sum::<usize>();
        self.initial_accums_mem_size + dyn_size
    }

    pub fn create_input_arrays(
        &self,
        input_batch: &RecordBatch,
    ) -> Result<Vec<Vec<ArrayRef>>> {
        let mut input_arrays = Vec::with_capacity(self.aggs.len());
        let mut cached_partial_args: Vec<(Arc<dyn PhysicalExpr>, ArrayRef)> = vec![];

        for (idx, agg) in self.aggs.iter().enumerate() {
            let mut values = vec![];

            if agg.mode.is_partial() {
                // find all distincted partial input args. distinction is used
                // to avoid duplicated evaluating the input arrays.
                let mut cur_values = vec![];
                for expr in &agg.agg.exprs() {
                    let value = cached_partial_args
                        .iter()
                        .find(|(cached_expr, _)| expr.eq(cached_expr))
                        .map(|(_, values)| Ok(values.clone()))
                        .unwrap_or_else(|| {
                            expr.evaluate(input_batch)
                                .map(|r| r.into_array(input_batch.num_rows()))
                        })?;
                    cached_partial_args.push((expr.clone(), value.clone()));
                    cur_values.push(value);
                }
                values.extend(agg.agg.prepare_partial_args(&cur_values)?);

            } else {
                // find accum arrays by buffer offset
                values.extend(
                    input_batch.columns()
                        .iter()
                        .skip(self.initial_input_buffer_offset + self.accums_offsets[idx])
                        .take(self.accums_lens[idx])
                        .cloned(),
                );
            }
            input_arrays.push(values);
        }
        Ok(input_arrays)
    }

    pub fn build_agg_columns(&self, records: &mut [AggRecord]) -> Result<Vec<ArrayRef>> {
        let num_records = records.len();
        let num_agg_columns = self.aggs
            .iter()
            .enumerate()
            .map(|(idx, agg)| match agg.mode {
                AggMode::Partial | AggMode::PartialMerge => self.accums_lens[idx],
                AggMode::Final => 1,
            })
            .sum::<usize>();

        let mut agg_columns = Vec::with_capacity(num_agg_columns);
        let mut values = vec![];
        for (idx, agg) in self.aggs.iter().enumerate() {
            if agg.mode.is_partial() || agg.mode.is_partial_merge() {
                for i in 0..self.accums_lens[idx] {
                    values.reserve(num_records);
                    for record in records.iter_mut() {
                        let value = std::mem::replace(
                            &mut record.1[self.accums_offsets[idx] + i],
                            ScalarValue::Null,
                        );
                        values.push(value);
                    }
                    let values = std::mem::take(&mut values);
                    agg_columns.push(ScalarValue::iter_to_array(values)?);
                }
            } else {
                values.reserve(num_records);
                for record in records.iter_mut() {
                    let value = agg.agg
                        .final_merge(&mut record.1[self.accums_offsets[idx]..])?;
                    values.push(value);
                }
                let values = std::mem::take(&mut values);
                agg_columns.push(ScalarValue::iter_to_array(values)?);
            }
        }
        Ok(agg_columns)
    }

    pub fn convert_records_to_batch(
        &self,
        grouping_row_converter: &mut RowConverter,
        records: &mut [AggRecord],
    ) -> Result<RecordBatch> {
        let row_count = records.len();
        let grouping_row_parser = grouping_row_converter.parser();
        let grouping_columns = grouping_row_converter.convert_rows(
            records
                .iter()
                .map(|(row, _)| grouping_row_parser.parse(row)),
        )?;
        let agg_columns = self.build_agg_columns(records)?;

        Ok(RecordBatch::try_new_with_options(
            self.output_schema.clone(),
            [grouping_columns, agg_columns].concat(),
            &RecordBatchOptions::new().with_row_count(Some(row_count)),
        )?)
    }

    pub fn partial_update_or_merge_one_row(
        &self,
        accums: &mut [ScalarValue],
        input_arrays: &[Vec<ArrayRef>],
        row_idx: usize,
    ) -> Result<()> {

        for (idx, agg) in self.aggs.iter().enumerate() {
            let accums = &mut accums[self.accums_offsets[idx]..];
            if self.aggs[idx].mode.is_partial() {
                agg.agg.partial_update(accums, &input_arrays[idx], row_idx)?;
            } else {
                agg.agg.partial_merge(accums, &input_arrays[idx], row_idx)?;
            }
        }
        Ok(())
    }

    pub fn partial_update_or_merge_all(
        &self,
        accums: &mut [ScalarValue],
        input_arrays: &[Vec<ArrayRef>],
    ) -> Result<()> {

        for (idx, agg) in self.aggs.iter().enumerate() {
            let accums = &mut accums[self.accums_offsets[idx]..];
            if self.aggs[idx].mode.is_partial() {
                agg.agg.partial_update_all(accums, &input_arrays[idx])?;
            } else {
                agg.agg.partial_merge_all(accums, &input_arrays[idx])?;
            }
        }
        Ok(())
    }
}

fn partial_output_field_name(agg_field_name: &str, accum_filed_name: &str) -> String {
    format!("{}[{}]", agg_field_name, accum_filed_name)
}
