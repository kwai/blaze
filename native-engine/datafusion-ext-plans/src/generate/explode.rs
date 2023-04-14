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
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use datafusion::common::{Result, ScalarValue};
use datafusion::physical_expr::PhysicalExpr;
use crate::generate::Generator;

#[derive(Debug)]
pub struct ExplodeArray {
    child: Arc<dyn PhysicalExpr>,
    position: bool,
}

impl ExplodeArray {
    pub fn new(child: Arc<dyn PhysicalExpr>, position: bool) -> Self {
        Self {
            child,
            position,
        }
    }
}

impl Generator for ExplodeArray {
    fn exprs(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.child.clone()]
    }

    fn with_new_exprs(
        &self,
        exprs: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn Generator>> {
        Ok(Arc::new(Self {
            child: exprs[0].clone(),
            position: self.position,
        }))
    }

    fn eval(&self, batch: &RecordBatch) -> Result<(Vec<ArrayRef>, Vec<u32>)> {

        let mut row_mapping = Vec::with_capacity(batch.num_rows());
        let mut arrays = Vec::with_capacity(batch.num_rows());
        let mut positions = if self.position {
            Some(Vec::with_capacity(batch.num_rows()))
        } else {
            None
        };

        let input_array = self.child.evaluate(batch)?.into_array(batch.num_rows());
        let list = as_list_array(&input_array);
        for (row_id, array) in list.iter().enumerate() {
            if let Some(array) = array {
                if let Some(positions) = &mut positions {
                    for i in 0..array.len() {
                        row_mapping.push(row_id as u32);
                        positions.push(i as i32);
                    }
                } else {
                    for _ in 0..array.len() {
                        row_mapping.push(row_id as u32);
                    }
                }
                arrays.push(array);
            }
        }

        let output_array = if !arrays.is_empty() {
            arrow::compute::concat(&arrays
                .iter()
                .map(|array| array.as_ref())
                .collect::<Vec<_>>())?
        } else {
            ScalarValue::try_from(list.value_type())?.to_array_of_size(0)
        };
        let output_positions_array = positions.map(|positions| {
            let array: ArrayRef = Arc::new(Int32Array::from(positions));
            array
        });

        Ok((
            [output_positions_array, Some(output_array)]
                .into_iter()
                .flatten()
                .collect(),
            row_mapping,
        ))
    }
}

#[derive(Debug)]
pub struct ExplodeMap {
    child: Arc<dyn PhysicalExpr>,
    position: bool,
}

impl ExplodeMap {
    pub fn new(child: Arc<dyn PhysicalExpr>, position: bool) -> Self {
        Self {
            child,
            position,
        }
    }
}

impl Generator for ExplodeMap {
    fn exprs(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.child.clone()]
    }

    fn with_new_exprs(
        &self,
        exprs: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn Generator>> {
        Ok(Arc::new(Self {
            child: exprs[0].clone(),
            position: self.position,
        }))
    }

    fn eval(&self, batch: &RecordBatch) -> Result<(Vec<ArrayRef>, Vec<u32>)> {
        let mut row_mapping = Vec::with_capacity(batch.num_rows());
        let mut k_arrays = Vec::with_capacity(batch.num_rows());
        let mut v_arrays = Vec::with_capacity(batch.num_rows());
        let mut positions = if self.position {
            Some(Vec::with_capacity(batch.num_rows()))
        } else {
            None
        };

        let input_array = self.child.evaluate(batch)?.into_array(batch.num_rows());
        let map = as_map_array(&input_array);

        for row_id in 0..map.len() {
            if map.is_valid(row_id) {
                let kv_struct = as_struct_array(&map.value(row_id)).clone();
                if let Some(positions) = &mut positions {
                    for i in 0..kv_struct.len() {
                        row_mapping.push(row_id as u32);
                        positions.push(i as i32);
                    }
                } else {
                    for _ in 0..kv_struct.len() {
                        row_mapping.push(row_id as u32);
                    }
                }
                k_arrays.push(kv_struct.column(0).clone());
                v_arrays.push(kv_struct.column(1).clone());
            }
        }

        let output_k_array = arrow::compute::concat(&k_arrays
            .iter()
            .map(|array| array.as_ref())
            .collect::<Vec<_>>())?;
        let output_v_array = arrow::compute::concat(&v_arrays
            .iter()
            .map(|array| array.as_ref())
            .collect::<Vec<_>>())?;
        let output_positions_array = positions.map(|positions| {
            let array: ArrayRef = Arc::new(Int32Array::from(positions));
            array
        });

        Ok((
            [output_positions_array, Some(output_k_array), Some(output_v_array)]
                .into_iter()
                .flatten()
                .collect(),
            row_mapping,
        ))
    }
}
