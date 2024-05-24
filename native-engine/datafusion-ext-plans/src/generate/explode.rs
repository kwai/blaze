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

use arrow::{array::*, record_batch::RecordBatch};
use datafusion::{common::Result, physical_expr::PhysicalExpr};
use itertools::Itertools;

use crate::generate::{GeneratedRows, Generator};

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

    fn eval(&self, batch: &RecordBatch) -> Result<GeneratedRows> {
        let input_array = self.child.evaluate(batch)?.into_array(batch.num_rows())?;
        let list = as_list_array(&input_array);
        let value_offsets = list.value_offsets();
        let mut orig_row_id_builder = Int32Builder::new();
        let mut pos_builder = Int32Builder::new();

        // build row_id and pos arrays
        for (orig_row_id, (&start, &end)) in value_offsets.iter().tuple_windows().enumerate() {
            if list.is_valid(orig_row_id) && start < end {
                for i in start..end {
                    orig_row_id_builder.append_value(orig_row_id as i32);
                    pos_builder.append_value((i - start) as i32);
                }
            }
        }

        let orig_row_ids = orig_row_id_builder.finish();
        let values = list
            .values()
            .slice(value_offsets[0] as usize, orig_row_ids.len());
        let cols = if self.position {
            vec![Arc::new(pos_builder.finish()), values]
        } else {
            vec![values]
        };
        Ok(GeneratedRows { orig_row_ids, cols })
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

    fn eval(&self, batch: &RecordBatch) -> Result<GeneratedRows> {
        let input_array = self.child.evaluate(batch)?.into_array(batch.num_rows())?;
        let map = as_map_array(&input_array);
        let value_offsets = map.value_offsets();
        let mut orig_row_id_builder = Int32Builder::new();
        let mut pos_builder = Int32Builder::new();

        // build row_id and pos arrays
        for (orig_row_id, (&start, &end)) in value_offsets.iter().tuple_windows().enumerate() {
            if map.is_valid(orig_row_id) && start < end {
                for i in start..end {
                    orig_row_id_builder.append_value(orig_row_id as i32);
                    pos_builder.append_value((i - start) as i32);
                }
            }
        }

        let orig_row_ids = orig_row_id_builder.finish();
        let keys = map
            .keys()
            .slice(value_offsets[0] as usize, orig_row_ids.len());
        let values = map
            .values()
            .slice(value_offsets[0] as usize, orig_row_ids.len());
        let cols = if self.position {
            vec![Arc::new(pos_builder.finish()), keys, values]
        } else {
            vec![keys, values]
        };
        Ok(GeneratedRows { orig_row_ids, cols })
    }
}
