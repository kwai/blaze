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

use crate::generate::Generator;
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::physical_expr::PhysicalExpr;
use std::sync::Arc;

#[derive(Debug)]
pub struct ExplodeArray {
    child: Arc<dyn PhysicalExpr>,
    position: bool,
}

impl ExplodeArray {
    pub fn new(child: Arc<dyn PhysicalExpr>, position: bool) -> Self {
        Self { child, position }
    }
}

impl Generator for ExplodeArray {
    fn exprs(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.child.clone()]
    }

    fn with_new_exprs(&self, exprs: Vec<Arc<dyn PhysicalExpr>>) -> Result<Arc<dyn Generator>> {
        Ok(Arc::new(Self {
            child: exprs[0].clone(),
            position: self.position,
        }))
    }

    fn eval(&self, batch: &RecordBatch) -> Result<Vec<(u32, Vec<ArrayRef>)>> {
        let mut outputs = vec![];
        let input_array = self.child.evaluate(batch)?.into_array(batch.num_rows());
        let list = as_list_array(&input_array);

        for (row_id, array) in list.iter().enumerate() {
            if let Some(array) = array && !array.is_empty() {
                let arrays =
                    if self.position {
                        let pos_array = Arc::new(Int32Array::from_iter(0..array.len() as i32));
                        vec![pos_array, array.clone()]
                    } else {
                        vec![array.clone()]
                    };
                outputs.push((row_id as u32, arrays));
            }
        }
        Ok(outputs)
    }
}

#[derive(Debug)]
pub struct ExplodeMap {
    child: Arc<dyn PhysicalExpr>,
    position: bool,
}

impl ExplodeMap {
    pub fn new(child: Arc<dyn PhysicalExpr>, position: bool) -> Self {
        Self { child, position }
    }
}

impl Generator for ExplodeMap {
    fn exprs(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.child.clone()]
    }

    fn with_new_exprs(&self, exprs: Vec<Arc<dyn PhysicalExpr>>) -> Result<Arc<dyn Generator>> {
        Ok(Arc::new(Self {
            child: exprs[0].clone(),
            position: self.position,
        }))
    }

    fn eval(&self, batch: &RecordBatch) -> Result<Vec<(u32, Vec<Arc<dyn Array>>)>> {
        let mut outputs = vec![];
        let input_array = self.child.evaluate(batch)?.into_array(batch.num_rows());
        let map = as_map_array(&input_array);

        for row_id in 0..map.len() {
            if map.is_valid(row_id) && !map.value(row_id).is_empty() {
                let entries = as_struct_array(&map.value(row_id)).clone();
                let arrays = if self.position {
                    let pos_array = Arc::new(Int32Array::from_iter(0..entries.len() as i32));
                    vec![
                        pos_array,
                        entries.column(0).clone(),
                        entries.column(1).clone(),
                    ]
                } else {
                    vec![entries.column(0).clone(), entries.column(1).clone()]
                };
                outputs.push((row_id as u32, arrays));
            }
        }
        Ok(outputs)
    }
}
