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

use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use std::sync::mpsc::Sender;
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::DataFusionError;
use blaze_commons::{is_task_running, jni_new_direct_byte_buffer, jni_new_global_ref, jni_new_object};
use datafusion::error::Result;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::utils::expr_list_eq_any_order;
use datafusion::physical_plan::PhysicalExpr;
use datafusion_ext_commons::ffi::MpscBatchReader;
use jni::objects::GlobalRef;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use crate::down_cast_any_ref;

pub struct SparkUDFWrapperExpr {
    pub serialized: Vec<u8>,
    pub return_type: DataType,
    pub return_nullable: bool,
    pub params: Vec<Arc<dyn PhysicalExpr>>,
    pub input_schema: SchemaRef,
    pub params_schema: SchemaRef,
    context: OnceCell<WrapperContext>,
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
                    && self.params_schema == x.params_schema
                    && self.context.get().is_none()
                    && x.context.get().is_none()
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
        let mut param_fields = Vec::with_capacity(params.len());
        for param in &params {
            param_fields.push(Field::new(
                "",
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
            context: OnceCell::new(),
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
                format!("SparkUDFWrapper: is_task_running=false")
            ));
        }

        let context = self.context.get_or_try_init(|| {
            WrapperContext::try_new(&self.serialized, &self.params_schema)
        })?;

        // evaluate params
        let num_rows = batch.num_rows();
        let params: Vec<ArrayRef> = self.params
            .iter()
            .map(|param| param.evaluate(batch).map(|r| r.into_array(num_rows)))
            .collect::<Result<_>>()?;
        let params_batch = RecordBatch::try_new_with_options(
            self.params_schema.clone(),
            params,
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )?;

        // send batch to context
        context
            .export_tx
            .send(Some(Ok(params_batch.clone())))
            .or_else(|err| Err(DataFusionError::Execution(
                format!("error sending batch: {}", err)
            )))?;

        // receive batch from context
        let return_array = context
            .import_reader
            .lock()
            .next()
            .transpose()?
            .ok_or_else(|| {
                DataFusionError::Execution(format!("error receiving batch"))
            })?;
        Ok(ColumnarValue::Array(return_array.column(0).clone()))
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

struct WrapperContext {
    _jcontext: GlobalRef,
    _export_stream: Box<FFI_ArrowArrayStream>,
    _import_stream: Box<FFI_ArrowArrayStream>,
    export_tx: Sender<Option<Result<RecordBatch>>>,
    import_reader: Arc<Mutex<ArrowArrayStreamReader>>,
}
unsafe impl Send for WrapperContext {}
unsafe impl Sync for WrapperContext {}

impl WrapperContext {
    fn try_new(
        serialized_expr_bytes: &[u8],
        export_schema: &SchemaRef,
    ) -> Result<Self> {

        // create channel and ffi-reader for exporting
        let (export_tx, export_rx) = std::sync::mpsc::channel();
        let ffi_reader = Box::new(MpscBatchReader {
            schema: export_schema.clone(),
            receiver: export_rx,
        });

        // create ffi-streams
        let export_stream = Box::new(FFI_ArrowArrayStream::new(ffi_reader));
        let mut import_stream = Box::new(FFI_ArrowArrayStream::empty());

        // create jcontext
        let serialized_buf = jni_new_direct_byte_buffer!(serialized_expr_bytes)?;
        let jcontext_local = jni_new_object!(SparkUDFWrapperContext(
            serialized_buf.as_obj(),
            &*export_stream as *const FFI_ArrowArrayStream as i64,
            &*import_stream as *const FFI_ArrowArrayStream as i64
        ))?;
        let jcontext = jni_new_global_ref!(jcontext_local.as_obj())?;

        // create import reader
        let import_reader = unsafe {
            Arc::new(Mutex::new(ArrowArrayStreamReader::from_raw(
                &mut *import_stream as *mut FFI_ArrowArrayStream
            )?))
        };

        Ok(Self {
            _jcontext: jcontext,
            _export_stream: export_stream,
            _import_stream: import_stream,
            export_tx,
            import_reader,
        })
    }
}