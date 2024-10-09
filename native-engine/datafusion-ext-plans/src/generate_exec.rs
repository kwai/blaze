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
};

use arrow::{
    array::{new_null_array, Array, ArrayRef, Int32Array, Int32Builder},
    datatypes::{Field, Schema, SchemaRef},
    error::ArrowError,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use datafusion::{
    common::{Result, Statistics},
    execution::context::TaskContext,
    physical_expr::{expressions::Column, EquivalenceProperties, PhysicalExpr},
    physical_plan::{
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, ExecutionPlanProperties,
        PlanProperties, SendableRecordBatchStream,
    },
};
use datafusion_ext_commons::{batch_size, cast::cast, streams::coalesce_stream::CoalesceInput};
use futures::{stream::once, StreamExt, TryFutureExt, TryStreamExt};
use num::integer::Roots;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;

use crate::{
    common::{
        batch_statisitcs::{stat_input, InputBatchStatistics},
        output::TaskOutputter,
        timer_helper::TimerHelper,
    },
    generate::Generator,
};

#[derive(Debug)]
pub struct GenerateExec {
    generator: Arc<dyn Generator>,
    required_child_output_cols: Vec<Column>,
    generator_output_schema: SchemaRef,
    outer: bool,
    input: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}

impl GenerateExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        generator: Arc<dyn Generator>,
        required_child_output_cols: Vec<Column>,
        generator_output_schema: SchemaRef,
        outer: bool,
    ) -> Result<Self> {
        let input_schema = input.schema();
        let required_child_output_schema = Arc::new(Schema::new(
            required_child_output_cols
                .iter()
                .map(|column| {
                    let name = column.name();
                    let data_type = column.data_type(&input_schema)?;
                    let nullable = column.nullable(&input_schema)?;
                    Ok(Field::new(name, data_type, nullable))
                })
                .collect::<Result<Vec<_>>>()?,
        ));
        let output_schema = Arc::new(Schema::new(
            [
                required_child_output_schema.fields().to_vec(),
                generator_output_schema.fields().to_vec(),
            ]
            .concat(),
        ));
        Ok(Self {
            generator,
            required_child_output_cols,
            generator_output_schema,
            outer,
            input,
            output_schema,
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
        })
    }

    /// only for testing
    pub fn with_outer(&self, outer: bool) -> Self {
        Self::try_new(
            self.input.clone(),
            self.generator.clone(),
            self.required_child_output_cols.clone(),
            self.generator_output_schema.clone(),
            outer,
        )
        .unwrap()
    }
}

impl DisplayAs for GenerateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Generate")
    }
}

impl ExecutionPlan for GenerateExec {
    fn name(&self) -> &str {
        "GenerateExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        self.props.get_or_init(|| {
            PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                self.input.output_partitioning().clone(),
                ExecutionMode::Bounded,
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
        Ok(Arc::new(Self::try_new(
            children[0].clone(),
            self.generator.clone(),
            self.required_child_output_cols.clone(),
            self.generator_output_schema.clone(),
            self.outer,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let output_schema = self.output_schema.clone();
        let generator = self.generator.clone();
        let outer = self.outer;
        let child_output_cols = self.required_child_output_cols.clone();
        let metrics = BaselineMetrics::new(&self.metrics, partition);
        let input = stat_input(
            InputBatchStatistics::from_metrics_set_and_blaze_conf(&self.metrics, partition)?,
            self.input.execute(partition, context.clone())?,
        )?;
        let output_stream = Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            once(
                execute_generate(
                    input,
                    context.clone(),
                    output_schema,
                    generator,
                    outer,
                    child_output_cols,
                    metrics,
                )
                .map_err(ArrowError::from),
            )
            .try_flatten(),
        ));

        let metrics = BaselineMetrics::new(&self.metrics, partition);
        let output_coalesced = context.coalesce_with_default_batch_size(output_stream, &metrics)?;
        Ok(output_coalesced)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        todo!()
    }
}

async fn execute_generate(
    mut input_stream: SendableRecordBatchStream,
    context: Arc<TaskContext>,
    output_schema: SchemaRef,
    generator: Arc<dyn Generator>,
    outer: bool,
    child_output_cols: Vec<Column>,
    metrics: BaselineMetrics,
) -> Result<SendableRecordBatchStream> {
    let batch_size = batch_size();
    let input_schema = input_stream.schema();

    context.output_with_sender(
        "Generate",
        output_schema.clone(),
        move |sender| async move {
            sender.exclude_time(metrics.elapsed_compute());

            let _timer = metrics.elapsed_compute().timer();
            let last_child_outputs: Arc<Mutex<Option<Vec<ArrayRef>>>> = Arc::default();
            while let Some(batch) =
                metrics
                    .elapsed_compute()
                    .exclude_timer_async(async {
                        input_stream.next().await.transpose().map_err(|err| {
                            err.context("generate: polling batches from input error")
                        })
                    })
                    .await?
            {
                // evaluate child output
                let child_outputs = child_output_cols
                    .iter()
                    .map(|column| {
                        column
                            .evaluate(&batch)
                            .and_then(|r| r.into_array(batch.num_rows()))
                    })
                    .collect::<Result<Vec<_>>>()
                    .map_err(|err| err.context("generate: evaluating child output arrays error"))?;

                // cache last child outputs for UDTF termination
                last_child_outputs.lock().replace(child_outputs.clone());

                // split batch into smaller slice to avoid too much memory usage
                let slice_step = batch_size.sqrt().max(1);
                for slice_start in (0..batch.num_rows()).step_by(slice_step) {
                    let slice_len = slice_step.min(batch.num_rows().saturating_sub(slice_start));
                    let slice = batch.slice(slice_start, slice_len);

                    // evaluate generated output
                    let generated_outputs = generator
                        .eval(&slice)
                        .map_err(|err| err.context("generate: evaluating generator error"))?;
                    let capacity = generated_outputs.orig_row_ids.len();
                    let mut child_output_row_ids = Int32Builder::with_capacity(capacity);
                    let mut generated_ids = Int32Builder::with_capacity(capacity);
                    let mut cur_row_id = 0;

                    // build ids for joining
                    for (i, &row_id) in generated_outputs.orig_row_ids.values().iter().enumerate() {
                        while cur_row_id < row_id {
                            if outer {
                                child_output_row_ids.append_value(cur_row_id);
                                generated_ids.append_null();
                            }
                            cur_row_id += 1;
                        }
                        child_output_row_ids.append_value(row_id);
                        generated_ids.append_value(i as i32);
                        cur_row_id = row_id + 1;
                    }
                    while cur_row_id < slice.num_rows() as i32 {
                        if outer {
                            child_output_row_ids.append_value(cur_row_id);
                            generated_ids.append_null();
                        }
                        cur_row_id += 1;
                    }

                    let child_output_row_ids = arrow::compute::kernels::numeric::add(
                        &child_output_row_ids.finish(),
                        &Int32Array::new_scalar(slice_start as i32),
                    )?;
                    let generated_ids = generated_ids.finish();

                    let mut start = 0;
                    while start < child_output_row_ids.len() {
                        let end = (start + batch_size).min(child_output_row_ids.len());

                        let child_output_row_ids = child_output_row_ids.slice(start, end - start);
                        let generated_ids = generated_ids.slice(start, end - start);

                        let child_outputs = child_outputs
                            .iter()
                            .map(|col| Ok(arrow::compute::take(col, &child_output_row_ids, None)?))
                            .collect::<Result<Vec<_>>>()?;
                        let generated_outputs = generated_outputs
                            .cols
                            .iter()
                            .map(|col| Ok(arrow::compute::take(col, &generated_ids, None)?))
                            .collect::<Result<Vec<_>>>()?;

                        let num_rows = generated_ids.len();
                        let outputs: Vec<ArrayRef> = child_outputs
                            .iter()
                            .chain(&generated_outputs)
                            .zip(output_schema.fields())
                            .map(|(array, field)| {
                                if array.data_type() != field.data_type() {
                                    return cast(&array, field.data_type());
                                }
                                Ok(array.clone())
                            })
                            .collect::<Result<_>>()?;
                        let output_batch = RecordBatch::try_new_with_options(
                            output_schema.clone(),
                            outputs,
                            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
                        )?;
                        start = end;

                        metrics.record_output(output_batch.num_rows());
                        sender.send(Ok(output_batch)).await;
                    }
                }
            }

            // execute generator.terminate(), joining with last child output row
            // keep it the same as hive does
            let last_child_outputs = match last_child_outputs.lock().take() {
                Some(arrays) => arrays,
                None => child_output_cols
                    .iter()
                    .map(|col| Ok(new_null_array(&col.data_type(&input_schema)?, 1)))
                    .collect::<Result<_>>()?,
            };

            let last_row_id = last_child_outputs
                .iter()
                .filter(|col| !col.is_empty())
                .map(|col| col.len() - 1)
                .max()
                .unwrap_or(0);

            if let Some(generated_outputs) = generator.terminate(last_row_id as i32)? {
                let child_outputs = last_child_outputs
                    .iter()
                    .map(|c| {
                        Ok(arrow::compute::take(
                            c,
                            &generated_outputs.orig_row_ids,
                            None,
                        )?)
                    })
                    .collect::<Result<Vec<_>>>()?;

                let num_rows = generated_outputs.orig_row_ids.len();
                let outputs: Vec<ArrayRef> = child_outputs
                    .iter()
                    .chain(&generated_outputs.cols)
                    .zip(output_schema.fields())
                    .map(|(array, field)| {
                        if array.data_type() != field.data_type() {
                            return cast(&array, field.data_type());
                        }
                        Ok(array.clone())
                    })
                    .collect::<Result<_>>()?;
                let output_batch = RecordBatch::try_new_with_options(
                    output_schema.clone(),
                    outputs,
                    &RecordBatchOptions::new().with_row_count(Some(num_rows)),
                )?;
                metrics.record_output(output_batch.num_rows());
                sender.send(Ok(output_batch)).await;
            }
            Ok(())
        },
    )
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{array::*, datatypes::*, record_batch::RecordBatch};
    use datafusion::{
        assert_batches_eq,
        common::Result,
        physical_expr::expressions::Column,
        physical_plan::{common, memory::MemoryExec, ExecutionPlan},
        prelude::SessionContext,
    };

    use crate::{
        generate::{create_generator, GenerateFunc},
        generate_exec::GenerateExec,
    };

    #[tokio::test]
    async fn test_explode() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let col_a: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3), None]));
        let col_b: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(400), Some(500), None]),
            Some(vec![Some(600), Some(700), Some(800)]),
            Some(vec![]),
            None,
        ]));
        let col_c: ArrayRef = Arc::new(MapArray::new_from_strings(
            ["A", "B", "C", "D", "E", "F"].into_iter(),
            &Int32Array::from(vec![Some(10), Some(20), Some(30), Some(40), Some(50), None]),
            &[0, 2, 4, 6, 6],
        )?);

        let input_batch = RecordBatch::try_from_iter_with_nullable(vec![
            ("a", col_a, true),
            ("b", col_b, true),
            ("c", col_c, true),
        ])?;
        let input = vec![
            "+---+-----------------+----------------+",
            "| a | b               | c              |",
            "+---+-----------------+----------------+",
            "| 1 | [400, 500, ]    | {A: 10, B: 20} |",
            "| 2 | [600, 700, 800] | {C: 30, D: 40} |",
            "| 3 | []              | {E: 50, F: }   |",
            "|   |                 | {}             |",
            "+---+-----------------+----------------+",
        ];
        assert_batches_eq!(input, &[input_batch.clone()]);

        let input = Arc::new(MemoryExec::try_new(
            &[vec![input_batch.clone()]],
            input_batch.schema(),
            None,
        )?);

        // explode list
        let generator = create_generator(
            &input.schema(),
            GenerateFunc::Explode,
            vec![Arc::new(Column::new("b", 1))],
        )?;
        let generate = Arc::new(GenerateExec::try_new(
            input.clone(),
            generator,
            vec![Column::new("a", 0), Column::new("c", 2)],
            Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, true)])),
            false,
        )?);

        let output = generate.execute(0, task_ctx.clone())?;
        let batches = common::collect(output).await?;
        let expected = vec![
            "+---+----------------+-----+",
            "| a | c              | b   |",
            "+---+----------------+-----+",
            "| 1 | {A: 10, B: 20} | 400 |",
            "| 1 | {A: 10, B: 20} | 500 |",
            "| 1 | {A: 10, B: 20} |     |",
            "| 2 | {C: 30, D: 40} | 600 |",
            "| 2 | {C: 30, D: 40} | 700 |",
            "| 2 | {C: 30, D: 40} | 800 |",
            "+---+----------------+-----+",
        ];
        assert_batches_eq!(expected, &batches);

        // explode list (outer)
        let generate = generate.with_outer(true);
        let output = generate.execute(0, task_ctx.clone())?;
        let batches = common::collect(output).await?;
        let expected = vec![
            "+---+----------------+-----+",
            "| a | c              | b   |",
            "+---+----------------+-----+",
            "| 1 | {A: 10, B: 20} | 400 |",
            "| 1 | {A: 10, B: 20} | 500 |",
            "| 1 | {A: 10, B: 20} |     |",
            "| 2 | {C: 30, D: 40} | 600 |",
            "| 2 | {C: 30, D: 40} | 700 |",
            "| 2 | {C: 30, D: 40} | 800 |",
            "| 3 | {E: 50, F: }   |     |",
            "|   | {}             |     |",
            "+---+----------------+-----+",
        ];
        assert_batches_eq!(expected, &batches);

        // explode map
        let generator = create_generator(
            &input.schema(),
            GenerateFunc::Explode,
            vec![Arc::new(Column::new("c", 2))],
        )?;
        let generate = Arc::new(GenerateExec::try_new(
            input.clone(),
            generator,
            vec![Column::new("a", 0), Column::new("b", 1)],
            Arc::new(Schema::new(vec![
                Field::new("ck", DataType::Utf8, true),
                Field::new("cv", DataType::Int32, true),
            ])),
            false,
        )?);

        let output = generate.execute(0, task_ctx.clone())?;
        let batches = common::collect(output).await?;
        let expected = vec![
            "+---+-----------------+----+----+",
            "| a | b               | ck | cv |",
            "+---+-----------------+----+----+",
            "| 1 | [400, 500, ]    | A  | 10 |",
            "| 1 | [400, 500, ]    | B  | 20 |",
            "| 2 | [600, 700, 800] | C  | 30 |",
            "| 2 | [600, 700, 800] | D  | 40 |",
            "| 3 | []              | E  | 50 |",
            "| 3 | []              | F  |    |",
            "+---+-----------------+----+----+",
        ];
        assert_batches_eq!(expected, &batches);

        // explode map (outer)
        let generate = generate.with_outer(true);
        let output = generate.execute(0, task_ctx.clone())?;
        let batches = common::collect(output).await?;
        let expected = vec![
            "+---+-----------------+----+----+",
            "| a | b               | ck | cv |",
            "+---+-----------------+----+----+",
            "| 1 | [400, 500, ]    | A  | 10 |",
            "| 1 | [400, 500, ]    | B  | 20 |",
            "| 2 | [600, 700, 800] | C  | 30 |",
            "| 2 | [600, 700, 800] | D  | 40 |",
            "| 3 | []              | E  | 50 |",
            "| 3 | []              | F  |    |",
            "|   |                 |    |    |",
            "+---+-----------------+----+----+",
        ];
        assert_batches_eq!(expected, &batches);

        // pos_explode map (outer)
        let generator = create_generator(
            &input.schema(),
            GenerateFunc::PosExplode,
            vec![Arc::new(Column::new("c", 2))],
        )?;
        let generate = Arc::new(GenerateExec::try_new(
            input.clone(),
            generator,
            vec![Column::new("a", 0), Column::new("b", 1)],
            Arc::new(Schema::new(vec![
                Field::new("cpos", DataType::Int32, true),
                Field::new("ck", DataType::Utf8, true),
                Field::new("cv", DataType::Int32, true),
            ])),
            true,
        )?);
        let output = generate.execute(0, task_ctx.clone())?;
        let batches = common::collect(output).await?;
        let expected = vec![
            "+---+-----------------+------+----+----+",
            "| a | b               | cpos | ck | cv |",
            "+---+-----------------+------+----+----+",
            "| 1 | [400, 500, ]    | 0    | A  | 10 |",
            "| 1 | [400, 500, ]    | 1    | B  | 20 |",
            "| 2 | [600, 700, 800] | 0    | C  | 30 |",
            "| 2 | [600, 700, 800] | 1    | D  | 40 |",
            "| 3 | []              | 0    | E  | 50 |",
            "| 3 | []              | 1    | F  |    |",
            "|   |                 |      |    |    |",
            "+---+-----------------+------+----+----+",
        ];
        assert_batches_eq!(expected, &batches);
        Ok(())
    }
}
