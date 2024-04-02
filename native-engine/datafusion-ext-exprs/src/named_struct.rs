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
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow::{
    array::Array,
    datatypes::{Field, Fields, SchemaRef},
    record_batch::RecordBatchOptions,
};
use datafusion::{
    arrow::{
        array::StructArray,
        datatypes::{DataType, Schema},
        record_batch::RecordBatch,
    },
    common::Result,
    logical_expr::ColumnarValue,
    physical_expr::{physical_exprs_bag_equal, PhysicalExpr},
};
use datafusion_ext_commons::{df_execution_err, io::name_batch};

use crate::down_cast_any_ref;

/// expression to get a field of from NameStruct.
#[derive(Debug, Hash)]
pub struct NamedStructExpr {
    values: Vec<Arc<dyn PhysicalExpr>>,
    return_type: DataType,
    return_schema: SchemaRef,
}

impl NamedStructExpr {
    pub fn try_new(values: Vec<Arc<dyn PhysicalExpr>>, return_type: DataType) -> Result<Self> {
        let return_schema = match &return_type {
            DataType::Struct(fields) => Arc::new(Schema::new(fields.clone())),
            other => {
                df_execution_err!("NamedStruct expects returning struct type, but got {other}")?
            }
        };
        Ok(Self {
            values,
            return_type,
            return_schema,
        })
    }
}

impl std::fmt::Display for NamedStructExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "NamedStruct")
    }
}

impl PartialEq<dyn Any> for NamedStructExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                physical_exprs_bag_equal(&self.values, &x.values)
                    && self.return_type == x.return_type
            })
            .unwrap_or(false)
    }
}

impl PhysicalExpr for NamedStructExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let input_arrays = self
            .values
            .iter()
            .map(|expr| {
                expr.evaluate(batch)
                    .and_then(|r| r.into_array(batch.num_rows()))
            })
            .collect::<Result<Vec<_>>>()?;
        let input_empty_fields = input_arrays
            .iter()
            .map(|array| Field::new("", array.data_type().clone(), array.null_count() > 0))
            .collect::<Fields>();
        let input_batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::new(input_empty_fields)),
            input_arrays,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )?;

        let named_batch = name_batch(input_batch, &self.return_schema)?;
        let named_struct = Arc::new(StructArray::from(named_batch));
        Ok(ColumnarValue::Array(named_struct))
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.values.clone()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self::try_new(
            children.clone(),
            self.return_type.clone(),
        )?))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{array::*, datatypes::*, record_batch::RecordBatch};
    use datafusion::{
        assert_batches_eq,
        physical_plan::{expressions::Column, PhysicalExpr},
    };

    use crate::named_struct::NamedStructExpr;

    #[test]
    fn test_list() -> Result<(), Box<dyn std::error::Error>> {
        let array: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(100), Some(101), Some(102)]),
            Some(vec![Some(200), Some(201)]),
            None,
            Some(vec![Some(300)]),
            Some(vec![Some(400), Some(401), None, Some(403)]),
        ]));
        let input_batch = RecordBatch::try_from_iter_with_nullable(vec![("cccccc1", array, true)])?;

        let named_struct = Arc::new(NamedStructExpr::try_new(
            vec![
                Arc::new(Column::new("cccccc1", 0)),
                Arc::new(Column::new("cccccc1", 0)),
            ],
            DataType::Struct(Fields::from(vec![
                Field::new(
                    "field1",
                    DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                    true,
                ),
                Field::new(
                    "field2",
                    DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                    true,
                ),
            ])),
        )?);
        let output_array = named_struct.evaluate(&input_batch)?.into_array(0)?;
        let output_batch =
            RecordBatch::try_from_iter_with_nullable(vec![("cccccc1", output_array, true)])?;

        let expected = vec![
            "+--------------------------------------------------------+",
            "| cccccc1                                                |",
            "+--------------------------------------------------------+",
            "| {field1: [100, 101, 102], field2: [100, 101, 102]}     |",
            "| {field1: [200, 201], field2: [200, 201]}               |",
            "| {field1: , field2: }                                   |",
            "| {field1: [300], field2: [300]}                         |",
            "| {field1: [400, 401, , 403], field2: [400, 401, , 403]} |",
            "+--------------------------------------------------------+",
        ];
        assert_batches_eq!(expected, &[output_batch]);
        Ok(())
    }
}
