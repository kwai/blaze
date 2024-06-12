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

use arrow::{
    compute::SortOptions,
    datatypes::{DataType, SchemaRef},
};
use datafusion::physical_expr::PhysicalExprRef;

use crate::joins::{join_utils::JoinType, stream_cursor::StreamCursor};

pub mod join_hash_map;
pub mod join_utils;
pub mod stream_cursor;

// join implementations
pub mod bhj;
pub mod smj;
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
    pub batch_size: usize,
}

pub type Idx = (usize, usize);
pub type StreamCursors = (StreamCursor, StreamCursor);
