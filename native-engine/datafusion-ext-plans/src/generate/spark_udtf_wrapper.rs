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
    array::{
        Array, ArrayRef, AsArray, Int32Array, RecordBatch, RecordBatchOptions, StructArray,
        make_array,
    },
    datatypes::{DataType, Field, Schema, SchemaRef},
    ffi::{FFI_ArrowArray, from_ffi_and_data_type},
};
use auron_jni_bridge::{
    is_task_running, jni_call, jni_new_direct_byte_buffer, jni_new_global_ref, jni_new_object,
};
use datafusion::{common::Result, physical_expr::PhysicalExprRef};
use datafusion_ext_commons::{arrow::cast::cast, df_execution_err, downcast_any};
use jni::objects::GlobalRef;
use once_cell::sync::OnceCell;

use crate::generate::{GenerateState, GeneratedRows, Generator};

pub struct SparkUDTFWrapper {
    serialized: Vec<u8>,
    return_schema: SchemaRef,
    children: Vec<PhysicalExprRef>,
    import_schema: SchemaRef,
    params_schema: OnceCell<SchemaRef>,
    jcontext: OnceCell<GlobalRef>,
}

impl SparkUDTFWrapper {
    pub fn try_new(
        serialized: Vec<u8>,
        return_schema: SchemaRef,
        children: Vec<PhysicalExprRef>,
    ) -> Result<Self> {
        let import_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "element",
                DataType::Struct(return_schema.fields().clone()),
                false,
            ),
        ]));
        Ok(Self {
            serialized,
            return_schema,
            children,
            import_schema,
            params_schema: OnceCell::new(),
            jcontext: OnceCell::new(),
        })
    }

    fn jcontext(&self) -> Result<GlobalRef> {
        self.jcontext
            .get_or_try_init(|| {
                let serialized_buf = jni_new_direct_byte_buffer!(&self.serialized)?;
                let jcontext_local =
                    jni_new_object!(SparkUDTFWrapperContext(serialized_buf.as_obj()))?;
                jni_new_global_ref!(jcontext_local.as_obj())
            })
            .cloned()
    }
}

impl Debug for SparkUDTFWrapper {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SparkUDTFWrapper")
    }
}

impl Generator for SparkUDTFWrapper {
    fn exprs(&self) -> Vec<PhysicalExprRef> {
        self.children.clone()
    }

    fn with_new_exprs(&self, exprs: Vec<PhysicalExprRef>) -> Result<Arc<dyn Generator>> {
        Ok(Arc::new(Self::try_new(
            self.serialized.clone(),
            self.return_schema.clone(),
            exprs,
        )?))
    }

    fn eval_start(&self, batch: &RecordBatch) -> Result<Box<dyn GenerateState>> {
        if !is_task_running() {
            df_execution_err!("SparkUDTFWrapper: is_task_running=false")?;
        }
        let batch_schema = batch.schema();

        // init params schema
        let params_schema = self
            .params_schema
            .get_or_try_init(|| -> Result<SchemaRef> {
                let mut param_fields = Vec::with_capacity(self.children.len());
                for param in &self.children {
                    param_fields.push(Field::new(
                        "",
                        param.data_type(batch_schema.as_ref())?,
                        param.nullable(batch_schema.as_ref())?,
                    ));
                }
                Ok(Arc::new(Schema::new(param_fields)))
            })?;

        // evaluate params
        let num_rows = batch.num_rows();
        let params: Vec<ArrayRef> = self
            .children
            .iter()
            .zip(params_schema.fields())
            .map(|(param, field)| {
                let param_array = param.evaluate(batch).and_then(|r| r.into_array(num_rows))?;
                cast(&param_array, field.data_type())
            })
            .collect::<Result<_>>()?;
        let params_batch = RecordBatch::try_new_with_options(
            params_schema.clone(),
            params,
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )?;

        // invoke UDAFWrapper.evalStart()
        let struct_array = StructArray::from(params_batch);
        let mut export_ffi_array = FFI_ArrowArray::new(&struct_array.to_data());
        let jcontext = self.jcontext()?;
        jni_call!(SparkUDTFWrapperContext(jcontext.as_obj())
            .evalStart(&mut export_ffi_array as *mut FFI_ArrowArray as i64) -> ())?;
        Ok(Box::new(UDTFGenerateState { cur_row_id: 0 }))
    }

    fn eval_loop(&self, state: &mut Box<dyn GenerateState>) -> Result<Option<GeneratedRows>> {
        let state = downcast_any!(state, mut UDTFGenerateState)?;

        // invoke UDAFWrapper.evalLoop()
        let mut import_ffi_array = FFI_ArrowArray::empty();
        let jcontext = self.jcontext()?;
        state.cur_row_id = jni_call!(SparkUDTFWrapperContext(jcontext.as_obj())
            .evalLoop(&mut import_ffi_array as *mut FFI_ArrowArray as i64) -> i32)?
            as usize;

        // safety: use arrow-ffi
        let result_array = unsafe {
            from_ffi_and_data_type(
                import_ffi_array,
                DataType::Struct(self.import_schema.fields().clone()),
            )?
        };
        if result_array.is_empty() {
            return Ok(None);
        }
        let result_struct = make_array(result_array).as_struct().clone();
        let result_row_ids: &Int32Array = result_struct.column(0).as_primitive();
        let result_elements: &[ArrayRef] = result_struct.column(1).as_struct().columns();
        Ok(Some(GeneratedRows {
            row_ids: result_row_ids.clone(),
            cols: result_elements.to_vec(),
        }))
    }

    fn terminate_loop(&self) -> Result<Option<GeneratedRows>> {
        // invoke UDAFWrapper.evalLoop()
        let mut import_ffi_array = FFI_ArrowArray::empty();
        let jcontext = self.jcontext()?;
        jni_call!(SparkUDTFWrapperContext(jcontext.as_obj())
            .terminateLoop(&mut import_ffi_array as *mut FFI_ArrowArray as i64) -> ())?;

        // safety: use arrow-ffi
        let result_array = unsafe {
            from_ffi_and_data_type(
                import_ffi_array,
                DataType::Struct(self.import_schema.fields().clone()),
            )?
        };
        if result_array.is_empty() {
            return Ok(None);
        }
        let result_struct = make_array(result_array).as_struct().clone();
        let result_row_ids: &Int32Array = result_struct.column(0).as_primitive();
        let result_elements: &[ArrayRef] = result_struct.column(1).as_struct().columns();
        Ok(Some(GeneratedRows {
            row_ids: result_row_ids.clone(),
            cols: result_elements.to_vec(),
        }))
    }
}

struct UDTFGenerateState {
    pub cur_row_id: usize,
}

impl GenerateState for UDTFGenerateState {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cur_row_id(&self) -> usize {
        self.cur_row_id
    }
}
