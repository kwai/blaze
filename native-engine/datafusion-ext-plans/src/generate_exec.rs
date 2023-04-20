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

use crate::common::output_with_sender;
use crate::generate::Generator;
use arrow::array::{Array, UInt32Array};
use arrow::compute::TakeOptions;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::execution::context::TaskContext;
use datafusion::from_slice::FromSlice;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use futures::stream::once;
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[derive(Debug)]
pub struct GenerateExec {
    generator: Arc<dyn Generator>,
    required_child_output_cols: Vec<Column>,
    generator_output_schema: SchemaRef,
    outer: bool,
    input: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
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

impl ExecutionPlan for GenerateExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
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
        let batch_size = context.session_config().batch_size();
        let output_schema = self.output_schema.clone();
        let generator = self.generator.clone();
        let outer = self.outer;
        let child_output_cols = self.required_child_output_cols.clone();
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        let input_stream = self.input.execute(partition, context)?;
        let output_stream = execute_generate(
            input_stream,
            batch_size,
            output_schema,
            generator,
            outer,
            child_output_cols,
            metrics,
        )
        .map_err(|e| ArrowError::ExternalError(Box::new(e)));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            once(output_stream).try_flatten(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Generate")
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}

async fn execute_generate(
    mut input_stream: SendableRecordBatchStream,
    batch_size: usize,
    output_schema: SchemaRef,
    generator: Arc<dyn Generator>,
    outer: bool,
    child_output_cols: Vec<Column>,
    metrics: BaselineMetrics,
) -> Result<SendableRecordBatchStream> {
    output_with_sender(
        "Generate",
        output_schema.clone(),
        move |sender| async move {
            while let Some(batch) = input_stream
                .next()
                .await
                .transpose()
                .map_err(|err| err.context("generate: polling batches from input error"))?
            {
                let mut timer = metrics.elapsed_compute().timer();

                // evaluate child output
                let child_output_arrays = child_output_cols
                    .iter()
                    .map(|column| {
                        column
                            .evaluate(&batch)
                            .map(|r| r.into_array(batch.num_rows()))
                    })
                    .collect::<Result<Vec<_>>>()
                    .map_err(|err| err.context("generate: evaluating child output arrays error"))?;

                // evaluate generated output
                let (generated_arrays, child_output_row_ids) = generator
                    .eval(&batch)
                    .map_err(|err| err.context("generate: evaluating generator error"))?;

                let (generated_arrays, child_output_row_ids) = if !outer {
                    (generated_arrays, child_output_row_ids)
                } else {
                    let mut outer_child_output_row_ids = vec![];
                    let mut outer_generated_row_ids = vec![];
                    let mut next_child_output_row_id = 0;
                    for (generated_row_id, &child_output_row_id) in
                        child_output_row_ids.iter().enumerate()
                    {
                        while next_child_output_row_id < child_output_row_id {
                            outer_child_output_row_ids.push(next_child_output_row_id);
                            outer_generated_row_ids.push(None); // null
                            next_child_output_row_id += 1;
                        }
                        outer_child_output_row_ids.push(child_output_row_id);
                        outer_generated_row_ids.push(Some(generated_row_id as u32));
                        next_child_output_row_id = child_output_row_id + 1;
                    }
                    while next_child_output_row_id < batch.num_rows() as u32 {
                        outer_child_output_row_ids.push(next_child_output_row_id);
                        outer_generated_row_ids.push(None); // null
                        next_child_output_row_id += 1;
                    }
                    let outer_generated_row_ids = UInt32Array::from(outer_generated_row_ids);
                    let outer_generated_arrays = generated_arrays
                        .iter()
                        .map(|array| {
                            Ok(arrow::compute::take(
                                &array,
                                &outer_generated_row_ids,
                                Some(TakeOptions {
                                    check_bounds: false,
                                }),
                            )?)
                        })
                        .collect::<Result<Vec<_>>>()?;

                    (outer_generated_arrays, outer_child_output_row_ids)
                };

                // output batches
                let mut generated_offset = 0;
                for row_ids in child_output_row_ids.chunks(batch_size) {
                    let sub_batch_size = row_ids.len();
                    let sub_generated_arrays = generated_arrays
                        .iter()
                        .map(|array| array.slice(generated_offset, sub_batch_size))
                        .collect::<Vec<_>>();
                    let sub_child_output_arrays = child_output_arrays
                        .iter()
                        .map(|array| {
                            Ok(arrow::compute::take(
                                array,
                                &UInt32Array::from_slice(row_ids),
                                Some(TakeOptions {
                                    check_bounds: false,
                                }),
                            )?)
                        })
                        .collect::<Result<Vec<_>>>()?;

                    let output_batch = RecordBatch::try_new(
                        output_schema.clone(),
                        [sub_child_output_arrays, sub_generated_arrays].concat(),
                    )?;
                    metrics.record_output(output_batch.num_rows());
                    timer.stop();
                    sender
                        .send(Ok(output_batch))
                        .await
                        .map_err(|err| DataFusionError::Execution(format!("{:?}", err)))?;
                    timer.restart();
                    generated_offset += sub_batch_size;
                }
            }
            Ok(())
        },
    )
}

#[cfg(test)]
mod test {
    use crate::generate::{create_generator, GenerateFunc};
    use crate::generate_exec::GenerateExec;
    use arrow::array::*;
    use arrow::datatypes::*;
    use arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_eq;
    use datafusion::common::Result;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::{common, ExecutionPlan};
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

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
