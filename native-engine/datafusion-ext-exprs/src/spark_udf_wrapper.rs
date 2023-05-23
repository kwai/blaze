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
use arrow::array::{as_struct_array, make_array, Array, ArrayRef, StructArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use blaze_commons::{
    is_task_running, jni_call, jni_new_direct_byte_buffer, jni_new_global_ref, jni_new_object,
};
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::utils::expr_list_eq_any_order;
use datafusion::physical_plan::PhysicalExpr;

use jni::objects::GlobalRef;
use once_cell::sync::OnceCell;

use std::any::Any;
use std::fmt::{Debug, Display, Formatter};

use arrow::ffi::{ArrowArray, ArrowArrayRef, FFI_ArrowArray, FFI_ArrowSchema};
use std::sync::Arc;

pub struct SparkUDFWrapperExpr {
    pub serialized: Vec<u8>,
    pub return_type: DataType,
    pub return_nullable: bool,
    pub params: Vec<Arc<dyn PhysicalExpr>>,
    pub input_schema: SchemaRef,
    pub import_schema: SchemaRef,
    pub params_schema: OnceCell<SchemaRef>,
    jcontext: OnceCell<GlobalRef>,
}

impl PartialEq<dyn Any> for SparkUDFWrapperExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                expr_list_eq_any_order(&self.params, &x.params)
                    && self.serialized == x.serialized
                    && self.return_type == x.return_type
                    && self.return_nullable == x.return_nullable
                    && self.input_schema == x.input_schema
            })
            .unwrap_or(false)
    }
}

impl SparkUDFWrapperExpr {
    pub fn try_new(
        serialized: Vec<u8>,
        return_type: DataType,
        return_nullable: bool,
        params: Vec<Arc<dyn PhysicalExpr>>,
        input_schema: SchemaRef,
    ) -> Result<Self> {
        Ok(Self {
            serialized,
            return_type: return_type.clone(),
            return_nullable,
            params,
            input_schema,
            import_schema: Arc::new(Schema::new(vec![Field::new("", return_type, true)])),
            params_schema: OnceCell::new(),
            jcontext: OnceCell::new(),
        })
    }
}

impl Display for SparkUDFWrapperExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SparkUDFWrapper")
    }
}

impl Debug for SparkUDFWrapperExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SparkUDFWrapper")
    }
}

impl PhysicalExpr for SparkUDFWrapperExpr {
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
        if !is_task_running() {
            return Err(DataFusionError::Execution(
                "SparkUDFWrapper: is_task_running=false".to_string(),
            ));
        }

        // init params schema
        let params_schema = self.params_schema.get_or_try_init(|| -> Result<SchemaRef> {
            let mut param_fields = Vec::with_capacity(self.params.len());
            for param in &self.params {
                param_fields.push(Field::new(
                    "",
                    param.data_type(&self.input_schema)?,
                    param.nullable(&self.input_schema)?,
                ));
            }
            Ok(Arc::new(Schema::new(param_fields)))
        })?;

        // init jvm side context
        let jcontext = self.jcontext.get_or_try_init(|| {
            let serialized_buf = jni_new_direct_byte_buffer!(&self.serialized)?;
            let jcontext_local = jni_new_object!(SparkUDFWrapperContext(serialized_buf.as_obj(),))?;
            jni_new_global_ref!(jcontext_local.as_obj())
        })?;

        // evalute params
        let num_rows = batch.num_rows();
        let params: Vec<ArrayRef> = self
            .params
            .iter()
            .map(|param| param.evaluate(batch).map(|r| r.into_array(num_rows)))
            .collect::<Result<_>>()?;
        let params_batch = RecordBatch::try_new_with_options(
            params_schema.clone(),
            params,
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )?;
        let params_struct_array = Arc::new(StructArray::from(params_batch));

        // evalute via context
        let mut export_ffi_array = FFI_ArrowArray::new(&params_struct_array.to_data());
        let mut import_ffi_array = FFI_ArrowArray::empty();
        jni_call!(SparkUDFWrapperContext(jcontext.as_obj()).eval(
            &mut export_ffi_array as *mut FFI_ArrowArray as i64,
            &mut import_ffi_array as *mut FFI_ArrowArray as i64,
        ) -> ())?;

        // import output from context
        let import_ffi_schema = FFI_ArrowSchema::try_from(self.import_schema.as_ref())?;
        let import_struct_array =
            make_array(ArrowArray::new(import_ffi_array, import_ffi_schema).to_data()?);
        let import_array = as_struct_array(&import_struct_array).column(0).clone();
        Ok(ColumnarValue::Array(import_array))
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
