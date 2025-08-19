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

use arrow::{array::*, record_batch::RecordBatch};
use datafusion::{common::Result, physical_expr::PhysicalExprRef};
use datafusion_ext_commons::{
    arrow::coalesce::coalesce_arrays_unchecked, batch_size, downcast_any,
};

use crate::generate::{GenerateState, GeneratedRows, Generator};

#[derive(Debug)]
pub struct ExplodeArray {
    child: PhysicalExprRef,
    position: bool,
}

impl ExplodeArray {
    pub fn new(child: PhysicalExprRef, position: bool) -> Self {
        Self { child, position }
    }
}

impl Generator for ExplodeArray {
    fn exprs(&self) -> Vec<PhysicalExprRef> {
        vec![self.child.clone()]
    }

    fn with_new_exprs(&self, exprs: Vec<PhysicalExprRef>) -> Result<Arc<dyn Generator>> {
        Ok(Arc::new(Self {
            child: exprs[0].clone(),
            position: self.position,
        }))
    }

    fn eval_start(&self, batch: &RecordBatch) -> Result<Box<dyn GenerateState>> {
        let input_array = self.child.evaluate(batch)?.into_array(batch.num_rows())?;
        Ok(Box::new(ExplodeArrayGenerateState {
            input_array: input_array.as_list().clone(),
            cur_row_id: 0,
        }))
    }

    fn eval_loop(&self, state: &mut Box<dyn GenerateState>) -> Result<Option<GeneratedRows>> {
        let state = downcast_any!(state, mut ExplodeArrayGenerateState)?;
        let batch_size = batch_size();

        let mut row_idx = state.cur_row_id;
        let mut row_ids = vec![];
        let mut pos_ids = vec![];
        let mut sub_lists = vec![];

        while row_idx < state.input_array.len() && row_ids.len() < batch_size {
            if state.input_array.is_valid(row_idx) {
                let sub_list = state.input_array.value(row_idx);
                row_ids.resize(row_ids.len() + sub_list.len(), row_idx as i32);
                pos_ids.extend(0..sub_list.len() as i32);
                sub_lists.push(sub_list);
            }
            row_idx += 1;
        }
        state.cur_row_id = row_idx;

        let values = coalesce_arrays_unchecked(&state.input_array.value_type(), &sub_lists);
        let cols = if self.position {
            vec![Arc::new(Int32Array::from(pos_ids)), values]
        } else {
            vec![values]
        };

        if row_ids.is_empty() {
            return Ok(None);
        }
        Ok(Some(GeneratedRows {
            row_ids: Int32Array::from(row_ids),
            cols,
        }))
    }
}

struct ExplodeArrayGenerateState {
    pub input_array: ListArray,
    pub cur_row_id: usize,
}

impl GenerateState for ExplodeArrayGenerateState {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cur_row_id(&self) -> usize {
        self.cur_row_id
    }
}

#[derive(Debug)]
pub struct ExplodeMap {
    child: PhysicalExprRef,
    position: bool,
}

impl ExplodeMap {
    pub fn new(child: PhysicalExprRef, position: bool) -> Self {
        Self { child, position }
    }
}

impl Generator for ExplodeMap {
    fn exprs(&self) -> Vec<PhysicalExprRef> {
        vec![self.child.clone()]
    }

    fn with_new_exprs(&self, exprs: Vec<PhysicalExprRef>) -> Result<Arc<dyn Generator>> {
        Ok(Arc::new(Self {
            child: exprs[0].clone(),
            position: self.position,
        }))
    }

    fn eval_start(&self, batch: &RecordBatch) -> Result<Box<dyn GenerateState>> {
        let input_array = self.child.evaluate(batch)?.into_array(batch.num_rows())?;
        Ok(Box::new(ExplodeMapGenerateState {
            input_array: input_array.as_map().clone(),
            cur_row_id: 0,
        }))
    }

    fn eval_loop(&self, state: &mut Box<dyn GenerateState>) -> Result<Option<GeneratedRows>> {
        let state = downcast_any!(state, mut ExplodeMapGenerateState)?;
        let batch_size = batch_size();

        let mut row_idx = state.cur_row_id;
        let mut row_ids = vec![];
        let mut pos_ids = vec![];
        let mut sub_key_lists = vec![];
        let mut sub_val_lists = vec![];

        while row_idx < state.input_array.len() && row_ids.len() < batch_size {
            if state.input_array.is_valid(row_idx) {
                let sub_struct = state.input_array.value(row_idx);
                let sub_key_list = sub_struct.column(0);
                let sub_val_list = sub_struct.column(1);
                row_ids.resize(row_ids.len() + sub_key_list.len(), row_idx as i32);
                pos_ids.extend(0..sub_key_list.len() as i32);
                sub_key_lists.push(sub_key_list.clone());
                sub_val_lists.push(sub_val_list.clone());
            }
            row_idx += 1;
        }
        state.cur_row_id = row_idx;

        let keys = coalesce_arrays_unchecked(&state.input_array.key_type(), &sub_key_lists);
        let vals = coalesce_arrays_unchecked(&state.input_array.value_type(), &sub_val_lists);
        let cols = if self.position {
            vec![Arc::new(Int32Array::from(pos_ids)), keys, vals]
        } else {
            vec![keys, vals]
        };

        if row_ids.is_empty() {
            return Ok(None);
        }
        Ok(Some(GeneratedRows {
            row_ids: Int32Array::from(row_ids),
            cols,
        }))
    }
}

struct ExplodeMapGenerateState {
    pub input_array: MapArray,
    pub cur_row_id: usize,
}

impl GenerateState for ExplodeMapGenerateState {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cur_row_id(&self) -> usize {
        self.cur_row_id
    }
}
