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

use crate::down_cast_any_ref;
use blaze_commons::{
    jni_call, jni_new_direct_byte_buffer, jni_new_global_ref, jni_new_object,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::error::Result;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::utils::expr_list_eq_any_order;
use datafusion::physical_plan::PhysicalExpr;
use datafusion_ext_commons::ipc::{read_one_batch, write_one_batch};
use jni::objects::{GlobalRef, JObject};
use once_cell::sync::OnceCell;
use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::io::Cursor;
use std::sync::Arc;

#[derive(Clone)]
pub struct SparkExpressionWrapperExpr {
    pub serialized: Vec<u8>,
    pub return_type: DataType,
    pub return_nullable: bool,
    pub params: Vec<Arc<dyn PhysicalExpr>>,
    pub input_schema: SchemaRef,
    pub params_schema: SchemaRef,
    jcontext: OnceCell<GlobalRef>,
}

impl PartialEq<dyn Any> for SparkExpressionWrapperExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                expr_list_eq_any_order(&self.params, &x.params)
                    && self.serialized == x.serialized
                    && self.return_type == x.return_type
                    && self.return_nullable == x.return_nullable
                    && self.input_schema == x.input_schema
                    && self.params_schema == x.params_schema
                    && self.jcontext.get().is_none()
                    && x.jcontext.get().is_none()
            })
            .unwrap_or(false)
    }
}

impl SparkExpressionWrapperExpr {
    pub fn try_new(
        serialized: Vec<u8>,
        return_type: DataType,
        return_nullable: bool,
        params: Vec<Arc<dyn PhysicalExpr>>,
        input_schema: SchemaRef,
    ) -> Result<Self> {
        let mut param_fields = Vec::with_capacity(params.len());
        for param in &params {
            param_fields.push(Field::new(
                &format!("_c{}", param_fields.len()),
                param.data_type(&input_schema)?,
                param.nullable(&input_schema)?,
            ));
        }
        let params_schema = Arc::new(Schema::new(param_fields));

        Ok(Self {
            serialized,
            return_type,
            return_nullable,
            params,
            input_schema,
            params_schema,
            jcontext: OnceCell::new(),
        })
    }
}

impl Display for SparkExpressionWrapperExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SparkExpressionWrapper")
    }
}

impl Debug for SparkExpressionWrapperExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SparkExpressionWrapper")
    }
}

impl PhysicalExpr for SparkExpressionWrapperExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(self.return_nullable)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let jcontext = self.jcontext.get_or_try_init(|| {
            let jcontext = jni_new_object!(
                SparkExpressionWrapperContext,
                jni_new_direct_byte_buffer!({
                    // safety - byte buffer is not mutated in jvm side
                    std::slice::from_raw_parts_mut(
                        self.serialized.as_ptr() as *mut u8,
                        self.serialized.len(),
                    )
                })?
            )?;
            Result::Ok(jni_new_global_ref!(jcontext)?)
        })?;

        let mut batch_buffer = vec![];
        let mut param_arrays = Vec::with_capacity(self.params.len());
        let mut input_contains_array = false;
        for param in &self.params {
            match param.evaluate(batch)? {
                ColumnarValue::Array(array) => {
                    param_arrays.push(array.clone());
                    input_contains_array = true;
                }
                ColumnarValue::Scalar(scalar) => {
                    param_arrays.push(scalar.to_array_of_size(batch.num_rows()));
                }
            }
        }
        let input_batch = RecordBatch::try_new(self.params_schema.clone(), param_arrays)?;
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "_c0",
            self.return_type.clone(),
            self.return_nullable,
        )]));

        if input_batch.num_rows() == 0 {
            let empty_array = RecordBatch::new_empty(output_schema).column(0).clone();
            return Ok(ColumnarValue::Array(empty_array));
        }

        write_one_batch(&input_batch, &mut Cursor::new(&mut batch_buffer), false)?;
        let batch_byte_buffer = jni_new_direct_byte_buffer!(&mut batch_buffer[..])?;
        let output_channel = jni_call!(
            SparkExpressionWrapperContext(jcontext.as_obj()).eval(batch_byte_buffer) -> JObject
        )?;

        let mut reader =
            datafusion_ext_commons::streams::ipc_stream::ReadableByteChannelReader(
                jni_new_global_ref!(output_channel)?,
            );
        let output_batch = match read_one_batch(&mut reader, output_schema.clone(), false)? {
            Some(batch) => batch,
            None => RecordBatch::new_empty(output_schema.clone()),
        };

        if input_contains_array {
            Ok(ColumnarValue::Array(output_batch.column(0).clone()))
        } else {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                output_batch.column(0),
                0,
            )?))
        }
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.params.clone()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self::try_new(
            self.serialized.clone(),
            self.return_type.clone(),
            self.return_nullable,
            children,
            self.input_schema.clone(),
        )?))
    }
}
