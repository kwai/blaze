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

use std::{fmt::Debug, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, AsArray, ListArray},
    datatypes::{DataType, Schema, SchemaRef},
};
use arrow_schema::Field;
use datafusion::{
    common::Result,
    datasource::schema_adapter::{
        SchemaAdapter, SchemaAdapterFactory, SchemaMapper, SchemaMapping,
    },
};
use datafusion_ext_commons::df_execution_err;

pub mod internal_file_reader;

#[derive(Debug)]
pub struct AuronSchemaAdapterFactory;

impl SchemaAdapterFactory for AuronSchemaAdapterFactory {
    fn create(
        &self,
        projected_table_schema: SchemaRef,
        _table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        Box::new(AuronSchemaAdapter::new(projected_table_schema))
    }
}

pub struct AuronSchemaAdapter {
    table_schema: SchemaRef,
}

impl AuronSchemaAdapter {
    pub fn new(table_schema: SchemaRef) -> Self {
        Self { table_schema }
    }
}

impl SchemaAdapter for AuronSchemaAdapter {
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        let field = self.table_schema.field(index);

        // use case insensitive matching
        file_schema
            .fields()
            .iter()
            .position(|f| f.name().eq_ignore_ascii_case(field.name()))
    }

    fn map_schema(&self, file_schema: &Schema) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        let mut projection = Vec::with_capacity(file_schema.fields().len());
        let mut field_mappings = vec![None; self.table_schema.fields().len()];

        for (file_idx, file_field) in file_schema.fields.iter().enumerate() {
            if let Some(table_idx) = self
                .table_schema
                .fields()
                .iter()
                .position(|f| f.name().eq_ignore_ascii_case(file_field.name()))
            {
                field_mappings[table_idx] = Some(projection.len());
                projection.push(file_idx);
            }
        }

        let schema_mapper: Arc<dyn SchemaMapper> = Arc::new(SchemaMapping::new(
            self.table_schema.clone(),
            field_mappings,
            Arc::new(|col, field| schema_adapter_cast_column(col, field)),
        ));
        Ok((schema_mapper, projection))
    }
}

pub fn create_auron_schema_mapper(
    table_schema: &SchemaRef,
    field_mappings: &[Option<usize>],
) -> Arc<dyn SchemaMapper> {
    Arc::new(SchemaMapping::new(
        table_schema.clone(),
        field_mappings.to_vec(),
        Arc::new(|col, field| schema_adapter_cast_column(col, field)),
    ))
}

fn schema_adapter_cast_column(col: &ArrayRef, field: &Field) -> Result<ArrayRef> {
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

    let col_dt = col.data_type();
    let cast_dt = field.data_type();

    match cast_dt {
        DataType::Decimal128(prec, scale) => match col_dt {
            DataType::Int8 => handle_decimal!(Int8, Decimal128, i128, *prec, *scale),
            DataType::Int16 => handle_decimal!(Int16, Decimal128, i128, *prec, *scale),
            DataType::Int32 => handle_decimal!(Int32, Decimal128, i128, *prec, *scale),
            DataType::Int64 => handle_decimal!(Int64, Decimal128, i128, *prec, *scale),
            DataType::Decimal128(..) => {
                datafusion_ext_commons::arrow::cast::cast_scan_input_array(col.as_ref(), cast_dt)
            }
            _ => df_execution_err!(
                "schema_adapter_cast_column unsupported type: {col_dt:?} => {cast_dt:?}",
            ),
        },
        DataType::List(to_field) => match col_dt {
            DataType::List(_from_field) => {
                let col = col.as_list::<i32>();
                let from_inner = col.values();
                let to_inner = schema_adapter_cast_column(from_inner, &to_field)?;
                Ok(Arc::new(ListArray::try_new(
                    to_field.clone(),
                    col.offsets().clone(),
                    to_inner,
                    col.nulls().cloned(),
                )?))
            }
            _ => df_execution_err!(
                "schema_adapter_cast_column unsupported type: {col_dt:?} => {cast_dt:?}",
            ),
        },
        _ => datafusion_ext_commons::arrow::cast::cast_scan_input_array(col.as_ref(), cast_dt),
    }
}
