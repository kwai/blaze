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

use std::{any::Any, sync::Arc};

use arrow::{
    array::{Array, ArrayRef},
    record_batch::RecordBatch,
};
use datafusion::{common::Result, physical_expr::PhysicalExprRef};
use datafusion_ext_commons::downcast_any;
use datafusion_ext_functions::spark_get_json_object::{
    spark_get_parsed_json_simple_field, spark_parse_json,
};

use crate::generate::{GenerateState, GeneratedRows, Generator};

#[derive(Debug)]
pub struct JsonTuple {
    child: PhysicalExprRef,
    json_paths: Vec<String>,
}

impl JsonTuple {
    pub fn new(child: PhysicalExprRef, json_paths: Vec<String>) -> Self {
        Self { child, json_paths }
    }
}

impl Generator for JsonTuple {
    fn exprs(&self) -> Vec<PhysicalExprRef> {
        vec![self.child.clone()]
    }

    fn with_new_exprs(&self, exprs: Vec<PhysicalExprRef>) -> Result<Arc<dyn Generator>> {
        Ok(Arc::new(Self::new(
            exprs[0].clone(),
            self.json_paths.clone(),
        )))
    }

    fn eval_start(&self, batch: &RecordBatch) -> Result<Box<dyn GenerateState>> {
        let num_rows = batch.num_rows();
        let json_values = spark_parse_json(&[self.child.evaluate(batch)?])?;
        let parsed_json_array = json_values.into_array(num_rows)?;
        Ok(Box::new(JsonTupleGenerateState {
            parsed_json_array,
            cur_row_id: 0,
        }))
    }

    fn eval_loop(&self, state: &mut Box<dyn GenerateState>) -> Result<Option<GeneratedRows>> {
        let state = downcast_any!(state, mut JsonTupleGenerateState)?;
        if state.cur_row_id >= state.parsed_json_array.len() {
            return Ok(None);
        }

        let evaluated: Vec<ArrayRef> = self
            .json_paths
            .iter()
            .map(|json_path| {
                spark_get_parsed_json_simple_field(&state.parsed_json_array, json_path)
            })
            .collect::<Result<_>>()?;

        let generated = GeneratedRows {
            row_ids: (state.cur_row_id..state.parsed_json_array.len())
                .map(|i| i as i32)
                .collect(),
            cols: evaluated,
        };
        state.cur_row_id = state.parsed_json_array.len();
        Ok(Some(generated))
    }
}

struct JsonTupleGenerateState {
    parsed_json_array: ArrayRef,
    cur_row_id: usize,
}

impl GenerateState for JsonTupleGenerateState {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cur_row_id(&self) -> usize {
        self.cur_row_id
    }
}
