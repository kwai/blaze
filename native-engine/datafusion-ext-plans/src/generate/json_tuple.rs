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

use std::sync::Arc;

use arrow::{array::ArrayRef, record_batch::RecordBatch};
use datafusion::{common::Result, physical_expr::PhysicalExpr};
use datafusion_ext_functions::spark_get_json_object::{
    spark_get_parsed_json_simple_field, spark_parse_json,
};

use crate::generate::{GeneratedRows, Generator};

#[derive(Debug)]
pub struct JsonTuple {
    child: Arc<dyn PhysicalExpr>,
    json_paths: Vec<String>,
}

impl JsonTuple {
    pub fn new(child: Arc<dyn PhysicalExpr>, json_paths: Vec<String>) -> Self {
        Self { child, json_paths }
    }
}

impl Generator for JsonTuple {
    fn exprs(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.child.clone()]
    }

    fn with_new_exprs(&self, exprs: Vec<Arc<dyn PhysicalExpr>>) -> Result<Arc<dyn Generator>> {
        Ok(Arc::new(Self::new(
            exprs[0].clone(),
            self.json_paths.clone(),
        )))
    }

    fn eval(&self, batch: &RecordBatch) -> Result<GeneratedRows> {
        let num_rows = batch.num_rows();
        let json_values = spark_parse_json(&[self.child.evaluate(batch)?])?;
        let parsed_json_array = json_values.into_array(num_rows)?;
        let evaluated: Vec<ArrayRef> = self
            .json_paths
            .iter()
            .map(|json_path| spark_get_parsed_json_simple_field(&parsed_json_array, json_path))
            .collect::<Result<_>>()?;

        Ok(GeneratedRows {
            orig_row_ids: (0..num_rows as i32).collect(),
            cols: evaluated,
        })
    }
}
