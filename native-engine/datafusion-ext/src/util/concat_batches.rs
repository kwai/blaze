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

use datafusion::arrow::compute::concat;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::{ArrowError, Result};
use datafusion::arrow::record_batch::RecordBatch;
use itertools::izip;

// Concatenates batches together into a single record batch.
// this is an improved implementation that can handle schema with different
// nullables
pub fn concat_batches(
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }
    if let Some((i, _)) = batches
        .iter()
        .enumerate()
        .find(|&(_, batch)| !schema_equals_ignoring_nullables(&batch.schema(), schema))
    {
        return Err(ArrowError::InvalidArgumentError(format!(
            "batches[{}] schema is different with argument schema.",
            i
        )));
    }
    let field_num = schema.fields().len();
    let mut arrays = Vec::with_capacity(field_num);
    for i in 0..field_num {
        let array = concat(
            &batches
                .iter()
                .map(|batch| batch.column(i).as_ref())
                .collect::<Vec<_>>(),
        )?;
        arrays.push(array);
    }
    RecordBatch::try_new(schema.clone(), arrays)
}

fn schema_equals_ignoring_nullables(
    schema1: &SchemaRef,
    schema2: &SchemaRef,
) -> bool {
    let fields1 = schema1.fields();
    let fields2 = schema2.fields();

    if fields1.len() != fields2.len() {
        return false;
    }
    izip!(fields1, fields2).all(|(field1, field2)| {
        // TODO: improve data_type comparison once complex type is supported
        field1.name() == field2.name() && field1.data_type() == field2.data_type()
    })
}