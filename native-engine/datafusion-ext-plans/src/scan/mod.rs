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

use std::{fmt::Debug, sync::Arc};

use arrow::{
    array::{new_null_array, Array, ArrayRef, AsArray, ListArray, RecordBatch, RecordBatchOptions},
    datatypes::{DataType, Schema, SchemaRef},
};
use datafusion::{
    common::Result,
    datasource::schema_adapter::{SchemaAdapter, SchemaAdapterFactory, SchemaMapper},
};
use datafusion_ext_commons::df_execution_err;

pub mod internal_file_reader;

#[derive(Debug)]
pub struct BlazeSchemaAdapterFactory;

impl SchemaAdapterFactory for BlazeSchemaAdapterFactory {
    fn create(&self, schema: SchemaRef) -> Box<dyn SchemaAdapter> {
        Box::new(BlazeSchemaAdapter::new(schema))
    }
}

pub struct BlazeSchemaAdapter {
    table_schema: SchemaRef,
}

impl BlazeSchemaAdapter {
    pub fn new(table_schema: SchemaRef) -> Self {
        Self { table_schema }
    }
}

impl SchemaAdapter for BlazeSchemaAdapter {
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        let field = self.table_schema.field(index);

        // use case insensitive matching
        file_schema
            .fields
            .iter()
            .position(|f| f.name().eq_ignore_ascii_case(field.name()))
    }

    fn map_schema(&self, file_schema: &Schema) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        let mut projection = Vec::with_capacity(file_schema.fields().len());
        let mut field_mappings = vec![None; self.table_schema.fields().len()];

        for (file_idx, file_field) in file_schema.fields.iter().enumerate() {
            if let Some((table_idx, _table_field)) =
                self.table_schema.fields().find(file_field.name())
            {
                field_mappings[table_idx] = Some(projection.len());
                projection.push(file_idx);
            }
        }

        Ok((
            Arc::new(BlazeSchemaMapping {
                table_schema: self.table_schema.clone(),
                field_mappings,
            }),
            projection,
        ))
    }
}

#[derive(Debug)]
pub struct BlazeSchemaMapping {
    table_schema: SchemaRef,
    field_mappings: Vec<Option<usize>>,
}

impl SchemaMapper for BlazeSchemaMapping {
    fn map_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let batch_rows = batch.num_rows();
        let batch_cols = batch.columns().to_vec();

        let cols = self
            .table_schema
            .fields()
            .iter()
            .zip(&self.field_mappings)
            .map(|(field, file_idx)| match file_idx {
                Some(batch_idx) => {
                    schema_adapter_cast_column(&batch_cols[*batch_idx], field.data_type())
                }
                None => Ok(new_null_array(field.data_type(), batch_rows)),
            })
            .collect::<Result<Vec<_>>>()?;

        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        let schema = self.table_schema.clone();
        let record_batch = RecordBatch::try_new_with_options(schema, cols, &options)?;
        Ok(record_batch)
    }

    fn map_partial_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let batch_cols = batch.columns().to_vec();
        let schema = batch.schema();

        let mut cols = vec![];
        let mut fields = vec![];
        for (i, f) in schema.fields().iter().enumerate() {
            let table_field = self.table_schema.field_with_name(f.name());
            if let Ok(tf) = table_field {
                cols.push(schema_adapter_cast_column(&batch_cols[i], tf.data_type())?);
                fields.push(tf.clone());
            }
        }

        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        let schema = Arc::new(Schema::new(fields));
        let record_batch = RecordBatch::try_new_with_options(schema, cols, &options)?;
        Ok(record_batch)
    }
}

fn schema_adapter_cast_column(col: &ArrayRef, data_type: &DataType) -> Result<ArrayRef> {
    macro_rules! handle_decimal {
        ($s:ident, $t:ident, $tnative:ty, $prec:expr, $scale:expr) => {{
            use arrow::{array::*, datatypes::*};
            type DecimalBuilder = paste::paste! {[<$t Builder>]};
            type IntType = paste::paste! {[<$s Type>]};

            let col = col.as_primitive::<IntType>();
            let mut decimal_builder = DecimalBuilder::new();
            for i in 0..col.len() {
                if col.is_valid(i) {
                    decimal_builder.append_value(col.value(i) as $tnative);
                } else {
                    decimal_builder.append_null();
                }
            }
            Ok(Arc::new(
                decimal_builder
                    .finish()
                    .with_precision_and_scale($prec, $scale)?,
            ))
        }};
    }
    match data_type {
        DataType::Decimal128(prec, scale) => match col.data_type() {
            DataType::Int8 => handle_decimal!(Int8, Decimal128, i128, *prec, *scale),
            DataType::Int16 => handle_decimal!(Int16, Decimal128, i128, *prec, *scale),
            DataType::Int32 => handle_decimal!(Int32, Decimal128, i128, *prec, *scale),
            DataType::Int64 => handle_decimal!(Int64, Decimal128, i128, *prec, *scale),
            DataType::Decimal128(p, s) if p == prec && s == scale => Ok(col.clone()),
            _ => df_execution_err!(
                "schema_adapter_cast_column unsupported type: {:?} => {:?}",
                col.data_type(),
                data_type,
            ),
        },
        DataType::List(to_field) => match col.data_type() {
            DataType::List(_from_field) => {
                log::warn!("XXX col.dt: {:?}", col.data_type());
                log::warn!("XXX to_field: {to_field:?}");
                log::warn!("XXX from_field: {_from_field:?}");
                let col = col.as_list::<i32>();
                let from_inner = col.values();
                log::warn!("XXX from_inner.dt: {:?}", from_inner.data_type());
                let to_inner = schema_adapter_cast_column(from_inner, to_field.data_type())?;
                Ok(Arc::new(ListArray::try_new(
                    to_field.clone(),
                    col.offsets().clone(),
                    to_inner,
                    col.nulls().cloned(),
                )?))
            }
            _ => df_execution_err!(
                "schema_adapter_cast_column unsupported type: {:?} => {:?}",
                col.data_type(),
                data_type,
            ),
        },
        _ => datafusion_ext_commons::arrow::cast::cast_scan_input_array(col.as_ref(), data_type),
    }
}
