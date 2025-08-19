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

use std::{
    any::Any,
    fmt::{Debug, Formatter},
    sync::Arc,
};

use arrow::{
    array::{Array, ArrayBuilder, ArrayRef, Int32Builder, new_null_array},
    datatypes::{Field, Schema, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use arrow_schema::DataType;
use datafusion::{
    common::{Result, Statistics},
    execution::context::TaskContext,
    physical_expr::{EquivalenceProperties, PhysicalExpr, expressions::Column},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
    },
};
use datafusion_ext_commons::arrow::{cast::cast, selection::take_cols};
use futures::StreamExt;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;

use crate::{
    common::{execution_context::ExecutionContext, timer_helper::TimerHelper},
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
        let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);
        let generator = self.generator.clone();
        let generator_output_schema = self.generator_output_schema.clone();
        let outer = self.outer;
        let child_output_cols = self.required_child_output_cols.clone();
        let input = exec_ctx.execute_with_input_stats(&self.input)?;
        let output = execute_generate(
            input,
            exec_ctx.clone(),
            generator,
            generator_output_schema,
            outer,
            child_output_cols,
        )?;
        Ok(exec_ctx.coalesce_with_default_batch_size(output))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        todo!()
    }
}

fn execute_generate(
    mut input_stream: SendableRecordBatchStream,
    exec_ctx: Arc<ExecutionContext>,
    generator: Arc<dyn Generator>,
    generator_output_schema: SchemaRef,
    outer: bool,
    child_output_cols: Vec<Column>,
) -> Result<SendableRecordBatchStream> {
    let input_schema = input_stream.schema();
    let child_output_dts: Vec<DataType> = child_output_cols
        .iter()
        .map(|col| Ok(col.data_type(&input_schema)?))
        .collect::<Result<Vec<_>>>()?;

    Ok(exec_ctx
        .clone()
        .output_with_sender("Generate", move |sender| async move {
            sender.exclude_time(exec_ctx.baseline_metrics().elapsed_compute());
            let _timer = exec_ctx.baseline_metrics().elapsed_compute().timer();
            let last_child_outputs: Arc<Mutex<Option<Vec<ArrayRef>>>> = Arc::default();

            while let Some(batch) = exec_ctx
                .baseline_metrics()
                .elapsed_compute()
                .exclude_timer_async(input_stream.next())
                .await
                .transpose()?
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
                last_child_outputs.lock().replace(child_outputs.clone());

                let mut generate_state = generator.eval_start(&batch)?;
                let mut cur_row_id = 0;

                while cur_row_id < batch.num_rows() {
                    let mut child_output_row_ids = Int32Builder::new();
                    let mut generated_ids = Int32Builder::new();
                    let end_row_id;

                    macro_rules! build_ids_for_outer_generate {
                        ($from_row_id:expr, $to_row_id:expr) => {{
                            if outer {
                                for i in $from_row_id..$to_row_id {
                                    child_output_row_ids.append_value(i as i32);
                                    generated_ids.append_null();
                                }
                            }
                            $from_row_id = $to_row_id;
                            let _ = $from_row_id; // suppress unused warning
                        }};
                    }

                    // generate one output batch
                    let generated_outputs = generator.eval_loop(&mut generate_state)?;

                    // build ids for joining
                    if let Some(generated_outputs) = &generated_outputs {
                        for i in 0..generated_outputs.row_ids.len() {
                            let row_id = generated_outputs.row_ids.value(i) as usize;
                            build_ids_for_outer_generate!(cur_row_id, row_id);
                            child_output_row_ids.append_value(row_id as i32);
                            generated_ids.append_value(i as i32);
                            cur_row_id = row_id + 1;
                        }
                        end_row_id = generate_state.cur_row_id();
                    } else {
                        end_row_id = batch.num_rows();
                    }
                    build_ids_for_outer_generate!(cur_row_id, end_row_id);

                    // build output cols
                    let num_rows = generated_ids.len();
                    if num_rows > 0 {
                        let child_output_row_ids = child_output_row_ids.finish();
                        let generated_ids = generated_ids.finish();
                        let child_outputs = take_cols(&child_outputs, child_output_row_ids)?;
                        let generated_outputs = match generated_outputs {
                            Some(generated_outputs) => {
                                take_cols(&generated_outputs.cols, generated_ids)?
                            }
                            None => generator_output_schema
                                .fields()
                                .iter()
                                .map(|f| new_null_array(f.data_type(), generated_ids.len()))
                                .collect(),
                        };
                        let output_cols = [child_outputs, generated_outputs].concat();
                        let outputs: Vec<ArrayRef> = output_cols
                            .iter()
                            .zip(exec_ctx.output_schema().fields())
                            .map(|(array, field)| {
                                if array.data_type() != field.data_type() {
                                    return cast(&array, field.data_type());
                                }
                                Ok(array.clone())
                            })
                            .collect::<Result<_>>()?;

                        // output
                        let output_batch = RecordBatch::try_new_with_options(
                            exec_ctx.output_schema(),
                            outputs,
                            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
                        )?;
                        exec_ctx
                            .baseline_metrics()
                            .record_output(output_batch.num_rows());
                        sender.send(output_batch).await;
                    }
                }
            }

            // execute generator.terminate()
            while let Some(generated_outputs) = generator.terminate_loop()? {
                let last_child_outputs = last_child_outputs.lock().take();
                let num_rows = generated_outputs.row_ids.len();

                let child_output_row_ids = generated_outputs.row_ids;
                let child_outputs = last_child_outputs
                    .unwrap_or(
                        child_output_dts
                            .iter()
                            .map(|dt| new_null_array(dt, 1))
                            .collect(),
                    )
                    .iter()
                    .map(|c| Ok(arrow::compute::take(c, &child_output_row_ids, None)?))
                    .collect::<Result<Vec<_>>>()?;
                let output_cols = [child_outputs, generated_outputs.cols].concat();
                let outputs: Vec<ArrayRef> = output_cols
                    .iter()
                    .zip(exec_ctx.output_schema().fields())
                    .map(|(array, field)| {
                        if array.data_type() != field.data_type() {
                            return cast(&array, field.data_type());
                        }
                        Ok(array.clone())
                    })
                    .collect::<Result<_>>()?;

                let output_batch = RecordBatch::try_new_with_options(
                    exec_ctx.output_schema(),
                    outputs,
                    &RecordBatchOptions::new().with_row_count(Some(num_rows)),
                )?;
                exec_ctx
                    .baseline_metrics()
                    .record_output(output_batch.num_rows());
                sender.send(output_batch).await;
            }
            Ok(())
        }))
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{array::*, datatypes::*, record_batch::RecordBatch};
    use datafusion::{
        assert_batches_eq,
        common::Result,
        physical_expr::expressions::Column,
        physical_plan::{ExecutionPlan, common, test::TestMemoryExec},
        prelude::SessionContext,
    };

    use crate::{
        generate::{GenerateFunc, create_generator},
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

        let input = Arc::new(TestMemoryExec::try_new(
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
