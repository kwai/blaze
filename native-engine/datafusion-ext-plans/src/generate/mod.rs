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

mod explode;
mod json_tuple;
mod spark_udtf_wrapper;

use std::{fmt::Debug, sync::Arc};

use arrow::{
    array::{ArrayRef, Int32Array},
    datatypes::{DataType, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::{
    common::{Result, ScalarValue},
    physical_expr::expressions::Literal,
    physical_plan::PhysicalExpr,
};
use datafusion_ext_commons::{df_execution_err, df_unimplemented_err, downcast_any};

use crate::generate::{
    explode::{ExplodeArray, ExplodeMap},
    json_tuple::JsonTuple,
    spark_udtf_wrapper::SparkUDTFWrapper,
};

pub trait Generator: Debug + Send + Sync {
    fn exprs(&self) -> Vec<Arc<dyn PhysicalExpr>>;

    fn with_new_exprs(&self, exprs: Vec<Arc<dyn PhysicalExpr>>) -> Result<Arc<dyn Generator>>;

    fn eval(&self, batch: &RecordBatch) -> Result<GeneratedRows>;
}

#[derive(Clone)]
pub struct GeneratedRows {
    pub orig_row_ids: Int32Array,
    pub cols: Vec<ArrayRef>,
}

#[derive(Debug, Clone, Copy)]
pub enum GenerateFunc {
    Explode,
    PosExplode,
    JsonTuple,
    UDTF,
}

pub fn create_generator(
    input_schema: &SchemaRef,
    func: GenerateFunc,
    children: Vec<Arc<dyn PhysicalExpr>>,
) -> Result<Arc<dyn Generator>> {
    match func {
        GenerateFunc::Explode => match children[0].data_type(input_schema)? {
            DataType::List(..) => Ok(Arc::new(ExplodeArray::new(children[0].clone(), false))),
            DataType::Map(..) => Ok(Arc::new(ExplodeMap::new(children[0].clone(), false))),
            other => df_unimplemented_err!("unsupported explode type: {other}"),
        },
        GenerateFunc::PosExplode => match children[0].data_type(input_schema)? {
            DataType::List(..) => Ok(Arc::new(ExplodeArray::new(children[0].clone(), true))),
            DataType::Map(..) => Ok(Arc::new(ExplodeMap::new(children[0].clone(), true))),
            other => df_unimplemented_err!("unsupported pos_explode type: {other}"),
        },
        GenerateFunc::JsonTuple => Ok(Arc::new(JsonTuple::new(
            children[0].clone(),
            children[1..]
                .iter()
                .map(|child| {
                    if let ScalarValue::Utf8(Some(s)) = downcast_any!(child, Literal)?.value() {
                        Ok(s.clone())
                    } else {
                        df_execution_err!("json_tuple() accepts only literal string params")
                    }
                })
                .collect::<Result<_>>()?,
        ))),
        GenerateFunc::UDTF => {
            unreachable!("UDTF should be handled in create_generator")
        }
    }
}

pub fn create_udtf_generator(
    serialized: Vec<u8>,
    return_schema: SchemaRef,
    children: Vec<Arc<dyn PhysicalExpr>>,
) -> Result<Arc<dyn Generator>> {
    Ok(Arc::new(SparkUDTFWrapper::try_new(
        serialized,
        return_schema,
        children,
    )?))
}
