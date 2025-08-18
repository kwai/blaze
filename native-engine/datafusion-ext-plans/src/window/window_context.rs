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

use std::sync::{Arc, Mutex as SyncMutex};

use arrow::{
    datatypes::{Field, FieldRef, Fields, Schema, SchemaRef},
    record_batch::RecordBatch,
    row::{RowConverter, Rows, SortField},
};
use datafusion::{
    common::Result,
    physical_expr::{PhysicalExprRef, PhysicalSortExpr},
};

use crate::window::WindowExpr;

#[derive(Debug)]
pub struct WindowContext {
    pub window_exprs: Vec<WindowExpr>,
    pub partition_spec: Vec<PhysicalExprRef>,
    pub order_spec: Vec<PhysicalSortExpr>,
    pub group_limit: Option<usize>,
    pub output_window_cols: bool,

    pub input_schema: SchemaRef,
    pub output_schema: SchemaRef,
    pub partition_schema: SchemaRef,
    pub order_schema: SchemaRef,

    pub partition_row_converter: Arc<SyncMutex<RowConverter>>,
    pub order_row_converter: Arc<SyncMutex<RowConverter>>,
}

impl WindowContext {
    pub fn try_new(
        input_schema: SchemaRef,
        window_exprs: Vec<WindowExpr>,
        partition_spec: Vec<PhysicalExprRef>,
        order_spec: Vec<PhysicalSortExpr>,
        group_limit: Option<usize>,
        output_window_cols: bool,
    ) -> Result<Self> {
        let output_schema = Arc::new(Schema::new(
            vec![
                input_schema.fields().to_vec(),
                if output_window_cols {
                    window_exprs
                        .iter()
                        .map(|expr: &WindowExpr| expr.field.clone())
                        .collect::<Vec<FieldRef>>()
                } else {
                    vec![]
                },
            ]
            .concat(),
        ));

        let partition_schema = Arc::new(Schema::new(
            partition_spec
                .iter()
                .map(|expr: &PhysicalExprRef| {
                    Ok(Field::new(
                        "__partition_col__",
                        expr.data_type(&input_schema)?,
                        expr.nullable(&input_schema)?,
                    ))
                })
                .collect::<Result<Fields>>()?,
        ));

        let order_schema = Arc::new(Schema::new(
            order_spec
                .iter()
                .map(|expr: &PhysicalSortExpr| {
                    Ok(Field::new(
                        "__order_col__",
                        expr.expr.data_type(&input_schema)?,
                        expr.expr.nullable(&input_schema)?,
                    ))
                })
                .collect::<Result<Fields>>()?,
        ));

        let partition_row_converter = Arc::new(SyncMutex::new(RowConverter::new(
            partition_schema
                .fields()
                .iter()
                .map(|field: &FieldRef| SortField::new(field.data_type().clone()))
                .collect::<_>(),
        )?));
        let order_row_converter = Arc::new(SyncMutex::new(RowConverter::new(
            order_schema
                .fields()
                .iter()
                .zip(&order_spec)
                .map(|(field, order)| {
                    SortField::new_with_options(field.data_type().clone(), order.options)
                })
                .collect(),
        )?));

        Ok(Self {
            window_exprs,
            partition_spec,
            order_spec,
            group_limit,
            output_window_cols,

            input_schema,
            output_schema,
            partition_schema,
            order_schema,

            partition_row_converter,
            order_row_converter,
        })
    }

    pub fn has_partition(&self) -> bool {
        !self.partition_schema.fields().is_empty()
    }

    pub fn get_partition_rows(&self, batch: &RecordBatch) -> Result<Rows> {
        Ok(self
            .partition_row_converter
            .lock()
            .unwrap()
            .convert_columns(
                &self
                    .partition_spec
                    .iter()
                    .map(|expr: &PhysicalExprRef| {
                        expr.evaluate(batch)
                            .and_then(|v| v.into_array(batch.num_rows()))
                    })
                    .collect::<Result<Vec<_>>>()?,
            )?)
    }

    pub fn get_order_rows(&self, batch: &RecordBatch) -> Result<Rows> {
        Ok(self.order_row_converter.lock().unwrap().convert_columns(
            &self
                .order_spec
                .iter()
                .map(|expr: &PhysicalSortExpr| {
                    expr.expr
                        .evaluate(batch)
                        .and_then(|v| v.into_array(batch.num_rows()))
                })
                .collect::<Result<Vec<_>>>()?,
        )?)
    }
}
