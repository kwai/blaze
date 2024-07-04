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

use std::{
    any::Any,
    fmt::{Debug, Formatter},
    sync::Arc,
    task::Poll,
    time::Duration,
};

use arrow::datatypes::SchemaRef;
use datafusion::{
    common::{JoinSide, Result},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::{Partitioning, PhysicalSortExpr},
    physical_plan::{
        joins::utils::JoinOn,
        metrics::{MetricValue, MetricsSet},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan,
    },
};
use datafusion_ext_commons::downcast_any;
use futures::{stream::poll_fn, StreamExt};

use crate::{
    broadcast_join_build_hash_map_exec::BroadcastJoinBuildHashMapExec,
    broadcast_join_exec::BroadcastJoinExec, joins::join_utils::JoinType,
};

#[derive(Debug)]
pub struct HashJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    wrapped_bhj: Arc<dyn ExecutionPlan>,
}

impl HashJoinExec {
    pub fn try_new(
        schema: SchemaRef,
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
        build_side: JoinSide,
    ) -> Result<Self> {
        let wrapped_build_map: Arc<dyn ExecutionPlan> = Arc::new(match build_side {
            JoinSide::Left => BroadcastJoinBuildHashMapExec::new(
                left.clone(),
                on.iter().map(|(l, _)| (l.clone())).collect(),
            ),
            JoinSide::Right => BroadcastJoinBuildHashMapExec::new(
                right.clone(),
                on.iter().map(|(_, r)| (r.clone())).collect(),
            ),
        });
        let wrapped_bhj: Arc<dyn ExecutionPlan> = Arc::new(BroadcastJoinExec::try_new(
            schema,
            match build_side {
                JoinSide::Left => wrapped_build_map.clone(),
                JoinSide::Right => left.clone(),
            },
            match build_side {
                JoinSide::Left => right.clone(),
                JoinSide::Right => wrapped_build_map.clone(),
            },
            on,
            join_type,
            build_side,
            None,
        )?);
        Ok(Self {
            left,
            right,
            wrapped_bhj,
        })
    }
}

impl DisplayAs for HashJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "HashJoin: wrapped ")?;
        self.wrapped_bhj.fmt_as(t, f)?;
        Ok(())
    }
}

impl ExecutionPlan for HashJoinExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.wrapped_bhj.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.wrapped_bhj.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.wrapped_bhj.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let wrapped_bhj = downcast_any!(self.wrapped_bhj, BroadcastJoinExec)?;
        Ok(Arc::new(Self::try_new(
            self.schema(),
            children[0].clone(),
            children[1].clone(),
            wrapped_bhj.on().clone(),
            wrapped_bhj.join_type(),
            wrapped_bhj.broadcast_side(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let wrapped_bhj = self.wrapped_bhj.clone();
        let stream = Box::pin(self.wrapped_bhj.execute(partition, context)?.chain(poll_fn(
            move |_| {
                // add build map time to join time
                let bhj = downcast_any!(&wrapped_bhj, BroadcastJoinExec).unwrap();
                let build_map = match bhj.broadcast_side() {
                    JoinSide::Left => &bhj.children()[0],
                    JoinSide::Right => &bhj.children()[1],
                };
                let build_map_time = build_map
                    .metrics()
                    .unwrap()
                    .elapsed_compute()
                    .unwrap_or_default();
                for metric in bhj.metrics().unwrap().iter() {
                    if let MetricValue::ElapsedCompute(time) = metric.value() {
                        time.add_duration(Duration::from_nanos(build_map_time as u64))
                    }
                }
                Poll::Ready(None)
            },
        )));
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.wrapped_bhj.metrics()
    }
}
