use datafusion::arrow::array::{Array, FixedSizeListArray};
use datafusion::arrow::compute::concat;
use datafusion::arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion::common::DataFusionError;
use datafusion::common::Result;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::ColumnarValue;
use std::convert::TryInto;
use std::fmt::Debug;
use std::{any::Any, sync::Arc};
use datafusion::arrow::datatypes::Field;
use datafusion::physical_expr::PhysicalExpr;

/// expression to get a field of a struct array.
#[derive(Debug)]
pub struct FixedSizeListGetIndexedFieldExpr {
    arg: Arc<dyn PhysicalExpr>,
    key: ScalarValue,
}

impl FixedSizeListGetIndexedFieldExpr {
    pub fn new(arg: Arc<dyn PhysicalExpr>, key: ScalarValue) -> Self {
        Self { arg, key }
    }

    pub fn key(&self) -> &ScalarValue {
        &self.key
    }

    pub fn arg(&self) -> &Arc<dyn PhysicalExpr> {
        &self.arg
    }
}

impl std::fmt::Display for FixedSizeListGetIndexedFieldExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "({}).[{}]", self.arg, self.key)
    }
}

impl PhysicalExpr for FixedSizeListGetIndexedFieldExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        let data_type = self.arg.data_type(input_schema)?;
        get_data_type_field(&data_type, &self.key).map(|f| f.data_type().clone())
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        let data_type = self.arg.data_type(input_schema)?;
        get_data_type_field(&data_type, &self.key).map(|f| f.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let array = self.arg.evaluate(batch)?.into_array(1);
        match (array.data_type(), &self.key) {
            (DataType::FixedSizeList(_, _), _) if self.key.is_null() => {
                let scalar_null: ScalarValue = array.data_type().try_into()?;
                Ok(ColumnarValue::Scalar(scalar_null))
            }
            (DataType::FixedSizeList(_, l), ScalarValue::Int64(Some(i))) => {
                let as_list_array =
                    array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();

                if *i < 1 || *i >= *l as i64 || as_list_array.is_empty() {
                    let scalar_null: ScalarValue = array.data_type().try_into()?;
                    return Ok(ColumnarValue::Scalar(scalar_null))
                }

                let sliced_array: Vec<Arc<dyn Array>> = (0..as_list_array.len())
                    .filter_map(|idx| if as_list_array.is_valid(idx) {
                        Some(as_list_array.value(idx).slice(*i as usize - 1, 1))
                    } else {
                        None
                    })
                    .collect();

                // concat requires input of at least one array
                if sliced_array.is_empty() {
                    let scalar_null: ScalarValue = array.data_type().try_into()?;
                    Ok(ColumnarValue::Scalar(scalar_null))
                } else {
                    let vec = sliced_array.iter().map(|a| a.as_ref()).collect::<Vec<&dyn Array>>();
                    let iter = concat(vec.as_slice()).unwrap();

                    Ok(ColumnarValue::Array(iter))
                }
            }
            (dt, key) => Err(DataFusionError::Execution(format!("get indexed field (FixedSizeList) is only possible on lists with int64 indexes. Tried {:?} with {:?} index", dt, key))),
        }
    }
}

fn get_data_type_field(data_type: &DataType, key: &ScalarValue) -> Result<Field> {
    match (data_type, key) {
        (DataType::FixedSizeList(lt, _), ScalarValue::Int64(Some(i))) => {
            Ok(Field::new(&i.to_string(), lt.data_type().clone(), true))
        }
        _ => Err(DataFusionError::Plan(
            "The expression to get an indexed field is only valid for `FixedSizeList` types"
                .to_string(),
        )),
    }
}
