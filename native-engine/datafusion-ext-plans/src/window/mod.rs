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

use arrow::{array::ArrayRef, datatypes::FieldRef, record_batch::RecordBatch};
use datafusion::{common::Result, physical_expr::PhysicalExpr};

use crate::{
    agg::{create_agg, AggFunction},
    window::{
        processors::{
            agg_processor::AggProcessor, rank_processor::RankProcessor,
            row_number_processor::RowNumberProcessor,
        },
        window_context::WindowContext,
    },
};

pub mod processors;
pub mod window_context;

#[derive(Debug, Clone, Copy)]
pub enum WindowFunction {
    RankLike(WindowRankType),
    Agg(AggFunction),
}

#[derive(Debug, Clone, Copy)]
pub enum WindowRankType {
    RowNumber,
    Rank,
    DenseRank,
}

pub trait WindowFunctionProcessor: Send + Sync {
    fn process_batch(&mut self, context: &WindowContext, batch: &RecordBatch) -> Result<ArrayRef>;
    fn process_batch_without_partitions(
        &mut self,
        context: &WindowContext,
        batch: &RecordBatch,
    ) -> Result<ArrayRef>;
}

#[derive(Debug, Clone)]
pub struct WindowExpr {
    field: FieldRef,
    func: WindowFunction,
    children: Vec<Arc<dyn PhysicalExpr>>,
}

impl WindowExpr {
    pub fn new(
        func: WindowFunction,
        children: Vec<Arc<dyn PhysicalExpr>>,
        field: FieldRef,
    ) -> Self {
        Self {
            field,
            func,
            children,
        }
    }

    pub fn create_processor(
        &self,
        context: &Arc<WindowContext>,
    ) -> Result<Box<dyn WindowFunctionProcessor>> {
        match self.func {
            WindowFunction::RankLike(WindowRankType::RowNumber) => {
                Ok(Box::new(RowNumberProcessor::new()))
            }
            WindowFunction::RankLike(WindowRankType::Rank) => {
                Ok(Box::new(RankProcessor::new(false)))
            }
            WindowFunction::RankLike(WindowRankType::DenseRank) => {
                Ok(Box::new(RankProcessor::new(true)))
            }
            WindowFunction::Agg(agg_func) => {
                let agg = create_agg(agg_func, &self.children, &context.input_schema)?;
                Ok(Box::new(AggProcessor::try_new(agg)?))
            }
        }
    }
}
