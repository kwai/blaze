use std::{
    any::Any,
    fmt::{Debug, Formatter},
    sync::Arc,
};

use arrow::datatypes::SchemaRef;
use datafusion::{
    common::Result,
    execution::context::TaskContext,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream, Statistics,
        execution_plan::{Boundedness, EmissionType},
        metrics::ExecutionPlanMetricsSet,
    },
};
use futures::StreamExt;
use once_cell::sync::OnceCell;

use crate::common::execution_context::ExecutionContext;

#[derive(Debug)]
pub struct LimitExec {
    input: Arc<dyn ExecutionPlan>,
    limit: u64,
    pub metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}

impl LimitExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, limit: u64) -> Self {
        Self {
            input,
            limit,
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
        }
    }
}

impl DisplayAs for LimitExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "LimitExec(limit={})", self.limit)
    }
}

impl ExecutionPlan for LimitExec {
    fn name(&self) -> &str {
        "LimitExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        self.props.get_or_init(|| {
            PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                self.input.output_partitioning().clone(),
                EmissionType::Both,
                Boundedness::Bounded,
            )
        })
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
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
        let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);
        let input = exec_ctx.execute_with_input_stats(&self.input)?;
        execute_limit(input, self.limit, exec_ctx)
    }

    fn statistics(&self) -> Result<Statistics> {
        Statistics::with_fetch(
            self.input.statistics()?,
            self.schema(),
            Some(self.limit as usize),
            0,
            1,
        )
    }
}

fn execute_limit(
    mut input: SendableRecordBatchStream,
    limit: u64,
    exec_ctx: Arc<ExecutionContext>,
) -> Result<SendableRecordBatchStream> {
    Ok(exec_ctx
        .clone()
        .output_with_sender("Limit", move |sender| async move {
            let mut remaining = limit;
            while remaining > 0
                && let Some(mut batch) = input.next().await.transpose()?
            {
                if remaining < batch.num_rows() as u64 {
                    batch = batch.slice(0, remaining as usize);
                    remaining = 0;
                } else {
                    remaining -= batch.num_rows() as u64;
                }
                exec_ctx.baseline_metrics().record_output(batch.num_rows());
                sender.send(batch).await;
            }
            Ok(())
        }))
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
        common::{Result, stats::Precision},
        physical_plan::{ExecutionPlan, common, test::TestMemoryExec},
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
        Arc::new(TestMemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
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
        let row_count = limit_exec.statistics()?.num_rows;

        let expected = vec![
            "+---+---+---+",
            "| a | b | c |",
            "+---+---+---+",
            "| 9 | 0 | 5 |",
            "| 8 | 1 | 6 |",
            "+---+---+---+",
        ];
        assert_batches_eq!(expected, &batches);
        assert_eq!(row_count, Precision::Exact(2));
        Ok(())
    }
}
