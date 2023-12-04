// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow::record_batch::RecordBatch;
use datafusion::{
    common::{
        tree_node::{Transformed, TreeNode},
        Result,
    },
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::{expressions::Column, utils::collect_columns, PhysicalExprRef},
    physical_plan::{stream::RecordBatchStreamAdapter, ExecutionPlan},
};
use futures::StreamExt;
use itertools::Itertools;

pub trait ExecuteWithColumnPruning {
    fn execute_projected(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
        projection: &[usize],
    ) -> Result<SendableRecordBatchStream>;
}

impl ExecuteWithColumnPruning for dyn ExecutionPlan {
    fn execute_projected(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
        projection: &[usize],
    ) -> Result<SendableRecordBatchStream> {
        let projection = projection.to_vec();
        let schema = Arc::new(self.schema().project(&projection)?);
        let stream =
            self.execute(partition, context)?
                .map(move |batch_result: Result<RecordBatch>| {
                    batch_result.and_then(|batch| Ok(batch.project(&projection)?))
                });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

pub fn prune_columns(exprs: &[PhysicalExprRef]) -> Result<(Vec<PhysicalExprRef>, Vec<usize>)> {
    let required_columns: Vec<Column> = exprs
        .iter()
        .flat_map(|expr| collect_columns(expr).into_iter())
        .collect::<HashSet<Column>>()
        .into_iter()
        .sorted_unstable_by_key(|column| column.index())
        .collect();
    let required_columns_mapping: HashMap<usize, usize> = required_columns
        .iter()
        .enumerate()
        .map(|(to, from)| (from.index(), to))
        .collect();
    let mapped_exprs: Vec<PhysicalExprRef> = exprs
        .iter()
        .map(|expr| {
            expr.clone().transform_down(&|node: PhysicalExprRef| {
                Ok(Transformed::Yes(
                    if let Some(column) = node.as_any().downcast_ref::<Column>() {
                        let mapped_idx = required_columns_mapping[&column.index()];
                        Arc::new(Column::new(column.name(), mapped_idx))
                    } else {
                        node
                    },
                ))
            })
        })
        .collect::<Result<_>>()?;

    Ok((
        mapped_exprs,
        required_columns.into_iter().map(|c| c.index()).collect(),
    ))
}
