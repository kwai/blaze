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
    fmt::{Debug, Formatter},
    sync::Arc,
};

use arrow::{
    array::{make_array, ArrayRef, AsArray, Int32Array, RecordBatch, RecordBatchOptions},
    datatypes::{DataType, Field, Schema, SchemaRef},
    ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema},
};
use blaze_jni_bridge::{
    is_task_running, jni_call, jni_new_direct_byte_buffer, jni_new_global_ref, jni_new_object,
};
use datafusion::{common::Result, physical_expr::PhysicalExpr};
use datafusion_ext_commons::{cast::cast, df_execution_err, ffi_helper::batch_to_ffi};
use jni::objects::GlobalRef;
use once_cell::sync::OnceCell;

use crate::generate::{GeneratedRows, Generator};

pub struct SparkUDTFWrapper {
    serialized: Vec<u8>,
    return_schema: SchemaRef,
    children: Vec<Arc<dyn PhysicalExpr>>,
    import_schema: SchemaRef,
    params_schema: OnceCell<SchemaRef>,
    jcontext: OnceCell<GlobalRef>,
}

impl SparkUDTFWrapper {
    pub fn try_new(
        serialized: Vec<u8>,
        return_schema: SchemaRef,
        children: Vec<Arc<dyn PhysicalExpr>>,
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
    fn exprs(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.children.clone()
    }

    fn with_new_exprs(&self, exprs: Vec<Arc<dyn PhysicalExpr>>) -> Result<Arc<dyn Generator>> {
        Ok(Arc::new(Self::try_new(
            self.serialized.clone(),
            self.return_schema.clone(),
            exprs,
        )?))
    }

    fn eval(&self, batch: &RecordBatch) -> Result<GeneratedRows> {
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

        // invoke UDF through JNI
        let result_array = invoke_udtf(self.jcontext()?, params_batch, self.import_schema.clone())?;
        let result_struct = result_array.as_struct();
        let result_row_ids: &Int32Array = result_struct.column(0).as_primitive();
        let result_elements: &[ArrayRef] = result_struct.column(1).as_struct().columns();
        Ok(GeneratedRows {
            orig_row_ids: result_row_ids.clone(),
            cols: result_elements.to_vec(),
        })
    }
}

fn invoke_udtf(
    jcontext: GlobalRef,
    params_batch: RecordBatch,
    result_schema: SchemaRef,
) -> Result<ArrayRef> {
    // evalute via context
    let mut export_ffi_array = batch_to_ffi(params_batch);
    let mut import_ffi_array = FFI_ArrowArray::empty();
    jni_call!(SparkUDTFWrapperContext(jcontext.as_obj()).eval(
        &mut export_ffi_array as *mut FFI_ArrowArray as i64,
        &mut import_ffi_array as *mut FFI_ArrowArray as i64,
    ) -> ())?;

    // import output from context
    let import_ffi_schema = FFI_ArrowSchema::try_from(result_schema.as_ref())?;
    let import_array = make_array(unsafe { from_ffi(import_ffi_array, &import_ffi_schema)? });
    Ok(import_array)
}
