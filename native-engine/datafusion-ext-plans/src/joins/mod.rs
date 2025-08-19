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

use std::sync::Arc;

use arrow::{
    array::ArrayRef,
    compute::SortOptions,
    datatypes::{DataType, SchemaRef},
};
use datafusion::{common::Result, physical_expr::PhysicalExprRef};
use stream_cursor::StreamCursor;

use crate::joins::join_utils::JoinType;

pub mod join_utils;

// join implementations
pub mod bhj;
pub mod join_hash_map;
pub mod smj;
pub mod stream_cursor;
mod test;

#[derive(Debug, Clone)]
pub struct JoinParams {
    pub join_type: JoinType,
    pub left_schema: SchemaRef,
    pub right_schema: SchemaRef,
    pub output_schema: SchemaRef,
    pub left_keys: Vec<PhysicalExprRef>,
    pub right_keys: Vec<PhysicalExprRef>,
    pub key_data_types: Vec<DataType>,
    pub sort_options: Vec<SortOptions>,
    pub projection: JoinProjection,
    pub batch_size: usize,
}

#[derive(Debug, Clone)]
pub struct JoinProjection {
    pub schema: SchemaRef,
    pub left: Vec<usize>,
    pub right: Vec<usize>,
}

impl JoinProjection {
    pub fn try_new(
        join_type: JoinType,
        schema: &SchemaRef,
        left_schema: &SchemaRef,
        right_schema: &SchemaRef,
        projection: &[usize],
    ) -> Result<Self> {
        let projected_schema = Arc::new(schema.project(projection)?);
        let mut left = vec![];
        let mut right = vec![];

        match join_type {
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                for &i in projection {
                    if i < left_schema.fields().len() {
                        left.push(i);
                    } else if i - left_schema.fields().len() < right_schema.fields().len() {
                        right.push(i - left_schema.fields().len());
                    }
                }
            }
            JoinType::LeftAnti | JoinType::LeftSemi => {
                left = projection.to_vec();
            }
            JoinType::RightAnti | JoinType::RightSemi => {
                right = projection.to_vec();
            }
            JoinType::Existence => {
                for &i in projection {
                    if i < left_schema.fields().len() {
                        left.push(i);
                    }
                }
            }
        }
        Ok(Self {
            schema: projected_schema,
            left,
            right,
        })
    }

    pub fn project_left(&self, cols: &[ArrayRef]) -> Vec<ArrayRef> {
        self.left.iter().map(|&i| cols[i].clone()).collect()
    }

    pub fn project_right(&self, cols: &[ArrayRef]) -> Vec<ArrayRef> {
        self.right.iter().map(|&i| cols[i].clone()).collect()
    }
}

pub type Idx = (usize, usize);
pub type StreamCursors = (StreamCursor, StreamCursor);
pub type StreamCursorsWithKeyRows = (StreamCursor, StreamCursor);
