use std::{
    any::Any,
    fmt::{Debug, Formatter},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion::{
    common::Result,
    execution::context::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet},
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
        SendableRecordBatchStream, Statistics,
    },
};
use futures::{Stream, StreamExt};

#[derive(Debug)]
pub struct LimitExec {
    input: Arc<dyn ExecutionPlan>,
    limit: u64,
    pub metrics: ExecutionPlanMetricsSet,
}

impl LimitExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, limit: u64) -> Self {
        Self {
            input,
            limit,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for LimitExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "LimitExec(limit={})", self.limit)
    }
}

impl ExecutionPlan for LimitExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children[0].clone(), self.limit)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        Ok(Box::pin(LimitStream {
            input_stream,
            limit: self.limit,
            cur: 0,
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
        }))
    }

    fn statistics(&self) -> Result<Statistics> {
        todo!()
    }
}

struct LimitStream {
    input_stream: SendableRecordBatchStream,
    limit: u64,
    cur: u64,
    baseline_metrics: BaselineMetrics,
}

impl RecordBatchStream for LimitStream {
    fn schema(&self) -> SchemaRef {
        self.input_stream.schema()
    }
}

impl Stream for LimitStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let rest = self.limit.saturating_sub(self.cur);
        if rest == 0 {
            return Poll::Ready(None);
        }

        match self.input_stream.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(Some(Ok(batch))) => {
                let batch = if batch.num_rows() <= rest as usize {
                    self.cur += batch.num_rows() as u64;
                    batch
                } else {
                    self.cur += rest;
                    batch.slice(0, rest as usize)
                };
                self.baseline_metrics
                    .record_poll(Poll::Ready(Some(Ok(batch))))
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::{
        assert_batches_eq,
        common::Result,
        physical_plan::{common, memory::MemoryExec, ExecutionPlan},
        prelude::SessionContext,
    };

    use crate::{limit_exec::LimitExec, memmgr::MemManager};

    fn build_table_i32(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Int32, false),
            Field::new(b.0, DataType::Int32, false),
            Field::new(c.0, DataType::Int32, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(a.1.clone())),
                Arc::new(Int32Array::from(b.1.clone())),
                Arc::new(Int32Array::from(c.1.clone())),
            ],
        )
        .unwrap()
    }

    fn build_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a, b, c);
        let schema = batch.schema();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    #[tokio::test]
    async fn test_limit_exec() -> Result<()> {
        MemManager::init(10000);
        let input = build_table(
            ("a", &vec![9, 8, 7, 6, 5, 4, 3, 2, 1, 0]),
            ("b", &vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            ("c", &vec![5, 6, 7, 8, 9, 0, 1, 2, 3, 4]),
        );
        let limit_exec = LimitExec::new(input, 2_u64);
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let output = limit_exec.execute(0, task_ctx).unwrap();
        let batches = common::collect(output).await?;

        let expected = vec![
            "+---+---+---+",
            "| a | b | c |",
            "+---+---+---+",
            "| 9 | 0 | 5 |",
            "| 8 | 1 | 6 |",
            "+---+---+---+",
        ];
        assert_batches_eq!(expected, &batches);
        Ok(())
    }
}
