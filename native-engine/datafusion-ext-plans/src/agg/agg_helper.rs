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

use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use arrow::row::RowConverter;
use datafusion::common::Result;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_ext_commons::array_builder::new_array_builders;
use crate::agg::{AggAccumRef, AggExpr, GroupingExpr};

pub type AggRecord = (Box<[u8]>, Box<[AggAccumRef]>);

pub struct AggContext {
    pub grouping_schema: SchemaRef,
    pub agg_schema: SchemaRef,
    pub output_schema: SchemaRef,
    pub groupings: Vec<GroupingExpr>,
    pub aggs: Vec<AggExpr>,
    pub initial_input_buffer_offset: usize,
}

impl Debug for AggContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[groupings={:?}, aggs={:?}]",
               self.groupings,
               self.aggs,
        )
    }
}

impl AggContext {
    pub fn try_new(
        input_schema: SchemaRef,
        groupings: Vec<GroupingExpr>,
        aggs: Vec<AggExpr>,
        initial_input_buffer_offset: usize,
    ) -> Result<Self> {
        let grouping_schema = Arc::new(Schema::new(groupings
            .iter()
            .map(|grouping: &GroupingExpr| Ok(Field::new(
                grouping.field_name.as_str(),
                grouping.expr.data_type(&input_schema)?,
                grouping.expr.nullable(&input_schema)?,
            )))
            .collect::<Result<_>>()?));

        let mut agg_fields = vec![];
        for agg in &aggs {
            if agg.mode.is_partial() || agg.mode.is_partial_merge() {
                agg_fields.extend(
                    agg.agg.accum_fields().iter().map(|field| field
                        .clone()
                        .with_name(partial_output_field_name(
                            &agg.field_name,
                            field.name(),
                        ))
                    ));
            } else {
                agg_fields.push(Field::new(
                    &agg.field_name,
                    agg.agg.data_type().clone(),
                    agg.agg.nullable()
                ));
            }
        }
        let agg_schema = Arc::new(Schema::new(agg_fields));

        let output_schema = Arc::new(Schema::new([
            grouping_schema.fields().clone(),
            agg_schema.fields().clone(),
        ].concat()));

        Ok(Self {
            output_schema,
            grouping_schema,
            agg_schema,
            groupings,
            aggs,
            initial_input_buffer_offset,
        })
    }

    pub fn create_children_input_arrays(
        &self,
        input_batch: &RecordBatch,
    ) -> Result<Vec<Vec<ArrayRef>>> {

        let mut input_arrays = Vec::with_capacity(self.aggs.len());
        let mut cached_partial_args: Vec<(Arc<dyn PhysicalExpr>, ArrayRef)> = vec![];
        let mut buffer_offset = self.initial_input_buffer_offset;

        for agg in &self.aggs {
            let mut values = vec![];

            if agg.mode.is_partial() {
                // find all distincted partial input args. distinction is used
                // to avoid duplicated evaluating the input arrays.
                for expr in &agg.agg.exprs() {
                    let value = cached_partial_args
                        .iter()
                        .find(|(cached_expr, _)| expr.eq(cached_expr))
                        .map(|(_, values)| Ok(values.clone()))
                        .unwrap_or_else(|| expr
                            .evaluate(input_batch)
                            .map(|r| r.into_array(input_batch.num_rows()))
                        )?;
                    cached_partial_args.push((expr.clone(), value.clone()));
                    values.push(value);
                }
            } else {
                // find accum arrays by buffer offset
                let num_accum_fields = agg.agg.accum_fields().len();
                values.extend(
                    input_batch.columns()[buffer_offset..].iter()
                        .take(num_accum_fields)
                        .map(|array| array.clone()));
                buffer_offset += num_accum_fields;
            }
            input_arrays.push(values);
        }
        Ok(input_arrays)
    }

    pub fn build_agg_columns(
        &self,
        records: &[AggRecord],
    ) -> Result<Vec<ArrayRef>> {

        let mut agg_builders = new_array_builders(
            &self.agg_schema,
            records.len(),
        );
        for (_, accums) in records {
            let mut col_offset = 0;
            for (idx, accum) in accums.iter().enumerate() {
                let agg = &self.aggs[idx];

                if agg.mode.is_partial() || agg.mode.is_partial_merge() {
                    let col_len = agg.agg.accum_fields().len();
                    accum.save(&mut agg_builders[col_offset..][..col_len])?;
                    col_offset += col_len;
                } else {
                    accum.save_final(&mut agg_builders[col_offset])?;
                    col_offset += 1;
                }
            }
        }
        Ok(agg_builders.iter_mut().map(|array| array.finish()).collect())
    }

    pub fn convert_records_to_batch(
        &self,
        grouping_row_converter: &mut RowConverter,
        records: &[AggRecord],
    ) -> Result<RecordBatch> {

        let row_count = records.len();
        let grouping_row_parser = grouping_row_converter.parser();
        let grouping_columns = grouping_row_converter.convert_rows(records
            .iter()
            .map(|(row, _)| grouping_row_parser.parse(row)))?;
        let agg_columns = self.build_agg_columns(records)?;

        Ok(RecordBatch::try_new_with_options(
            self.output_schema.clone(),
            [grouping_columns, agg_columns].concat(),
            &RecordBatchOptions::new().with_row_count(Some(row_count)),
        )?)
    }
}

fn partial_output_field_name(
    agg_field_name: &str,
    accum_filed_name: &str,
) -> String {
    format!("{}[{}]", agg_field_name, accum_filed_name)
}