use datafusion::arrow::array::{Array, ArrayRef, StructArray};
use datafusion::arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion::common::Result;
use datafusion::common::ScalarValue;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{ColumnarValue};
use std::fmt::{Debug, Formatter};
use std::{any::Any, sync::Arc};
use datafusion::arrow::datatypes::Field;
use datafusion::physical_expr::PhysicalExpr;

/// expression to get a field of from NameStruct.
#[derive(Debug)]
pub struct NamedStruct {
    names: Vec<String>,
    values: Vec<Arc<dyn PhysicalExpr>>,
    return_type: DataType,
}

impl NamedStruct {
    pub fn new(
        names: Vec<String>,
        values: Vec<Arc<dyn PhysicalExpr>>,
        return_type: DataType,
    ) -> Self {
        Self {
            names,
            values,
            return_type,
        }
    }

    pub fn names(&self) -> &Vec<String> {
        &self.names
    }

    pub fn values(&self) -> &Vec<Arc<dyn PhysicalExpr>> {
        &self.values
    }

    pub fn return_type(&self) -> &DataType {
        &self.return_type
    }
}

impl std::fmt::Display for NamedStruct {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "NamedStruct")
    }
}

impl PhysicalExpr for NamedStruct {
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
        fn array_struct(
            return_type: &DataType,
            _names: &Vec<String>,
            args: &[ArrayRef],
        ) -> Result<ColumnarValue> {
            if args.is_empty() {
                return Err(DataFusionError::Internal(
                    "NamedStruct requires at least one argument".to_string(),
                ));
            }

            let mut field_stored = Vec::new();
            if let DataType::Struct(fields) = return_type {
                for i in fields.iter() {
                    field_stored.push(i);
                }
            }

            let vec: Vec<(Field, ArrayRef)> = args
                .iter()
                .enumerate()
                .map(|(i, arg)| -> Result<(Field, ArrayRef)> {
                    let field_store = field_stored[i].clone();
                    match arg.data_type() {
                        DataType::Utf8
                        | DataType::LargeUtf8
                        | DataType::Boolean
                        | DataType::Float32
                        | DataType::Float64
                        | DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64 => Ok((field_store, arg.clone(),
                        )),
                        data_type => Err(DataFusionError::NotImplemented(format!(
                            "NamedStruct is not implemented for type '{:?}'.",
                            data_type
                        ))),
                    }
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(ColumnarValue::Array(Arc::new(StructArray::from(vec))))
        }

        let mut args = Vec::new();
        for value in self.values() {
            args.push(value.evaluate(batch)?);
        }
        let arrays: Vec<ArrayRef> = args
            .iter()
            .map(|x| match x {
                ColumnarValue::Array(array) => array.clone(),
                ColumnarValue::Scalar(scalar) => {
                    scalar.to_array_of_size(batch.num_rows()).clone()
                }
            })
            .collect();

        array_struct(
            &self.return_type.clone(),
            &self.names.clone(),
            arrays.as_slice(),
        )
    }
}
