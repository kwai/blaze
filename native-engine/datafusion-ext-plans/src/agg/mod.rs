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

pub mod acc;
pub mod agg;
pub mod agg_ctx;
pub mod agg_hash_map;
pub mod agg_table;
pub mod avg;
pub mod bloom_filter;
pub mod brickhouse;
pub mod collect;
pub mod count;
pub mod first;
pub mod first_ignores_null;
pub mod maxmin;
pub mod spark_udaf_wrapper;
pub mod sum;

use std::{fmt::Debug, sync::Arc};

use agg::Agg;
use datafusion::physical_expr::PhysicalExprRef;

pub const AGG_BUF_COLUMN_NAME: &str = "#9223372036854775807";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggExecMode {
    HashAgg,
    SortAgg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggMode {
    Partial,
    PartialMerge,
    Final,
}
impl AggMode {
    pub fn is_partial(&self) -> bool {
        matches!(self, AggMode::Partial)
    }

    pub fn is_partial_merge(&self) -> bool {
        matches!(self, AggMode::PartialMerge)
    }

    pub fn is_final(&self) -> bool {
        matches!(self, AggMode::Final)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunction {
    Count,
    Sum,
    Avg,
    Max,
    Min,
    First,
    FirstIgnoresNull,
    CollectList,
    CollectSet,
    BloomFilter,
    BrickhouseCollect,
    BrickhouseCombineUnique,
    Udaf,
}

#[derive(Debug, Clone)]
pub struct GroupingExpr {
    pub field_name: String,
    pub expr: PhysicalExprRef,
}

#[derive(Debug, Clone)]
pub struct AggExpr {
    pub field_name: String,
    pub mode: AggMode,
    pub agg: Arc<dyn Agg>,
}
