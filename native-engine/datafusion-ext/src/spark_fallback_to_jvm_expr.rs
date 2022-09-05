use crate::util::ipc::{read_one_batch, write_one_batch};
use crate::{jni_call, jni_new_direct_byte_buffer, jni_new_global_ref, jni_new_object};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::error::Result;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::PhysicalExpr;
use jni::objects::{GlobalRef, JObject};
use once_cell::sync::OnceCell;
use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::io::Cursor;
use std::sync::Arc;

#[derive(Clone)]
pub struct SparkFallbackToJvmExpr {
    pub serialized: Vec<u8>,
    pub return_type: DataType,
    pub return_nullable: bool,
    pub params: Vec<Arc<dyn PhysicalExpr>>,
    pub input_schema: SchemaRef,
    pub params_schema: SchemaRef,
    jcontext: OnceCell<GlobalRef>,
}

impl SparkFallbackToJvmExpr {
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

impl Display for SparkFallbackToJvmExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SparkFallbackToJvmExpr")
    }
}

impl Debug for SparkFallbackToJvmExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SparkFallbackToJvmExpr")
    }
}

impl PhysicalExpr for SparkFallbackToJvmExpr {
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
                SparkFallbackToJvmExprContext,
                jni_new_direct_byte_buffer!(unsafe {
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
        write_one_batch(&input_batch, &mut Cursor::new(&mut batch_buffer), false)?;

        let batch_byte_buffer = jni_new_direct_byte_buffer!(
            &mut batch_buffer[8..] // skip length header
        )?;
        let output_channel = jni_call!(
            SparkFallbackToJvmExprContext(jcontext.as_obj()).eval(batch_byte_buffer) -> JObject
        )?;

        let mut reader = crate::ipc_reader_exec::ReadableByteChannelReader(
            jni_new_global_ref!(output_channel)?,
        );

        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "_c0",
            self.return_type.clone(),
            self.return_nullable,
        )]));
        let output_batch = read_one_batch(&mut reader, output_schema, false, false)?;
        if input_contains_array {
            Ok(ColumnarValue::Array(output_batch.column(0).clone()))
        } else {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                output_batch.column(0),
                0,
            )?))
        }
    }
}
