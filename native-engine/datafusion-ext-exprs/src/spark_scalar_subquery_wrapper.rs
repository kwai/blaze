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
    fmt::{Debug, Display, Formatter},
    hash::{Hash, Hasher},
    io::Write,
    sync::Arc,
};

use arrow::{
    datatypes::{DataType, Schema},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use datafusion::{
    common::{Result, ScalarValue},
    logical_expr::ColumnarValue,
    physical_expr::PhysicalExprRef,
    physical_expr_common::physical_expr::DynEq,
    physical_plan::PhysicalExpr,
};
use once_cell::sync::OnceCell;

use crate::spark_udf_wrapper::SparkUDFWrapperExpr;

pub struct SparkScalarSubqueryWrapperExpr {
    pub serialized: Vec<u8>,
    pub return_type: DataType,
    pub return_nullable: bool,
    pub cached_value: OnceCell<ColumnarValue>,
}

impl SparkScalarSubqueryWrapperExpr {
    pub fn try_new(
        serialized: Vec<u8>,
        return_type: DataType,
        return_nullable: bool,
    ) -> Result<Self> {
        Ok(Self {
            serialized,
            return_type,
            return_nullable,
            cached_value: OnceCell::new(),
        })
    }
}

impl Display for SparkScalarSubqueryWrapperExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Debug for SparkScalarSubqueryWrapperExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ScalarSubquery")
    }
}

impl Hash for SparkScalarSubqueryWrapperExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.serialized.hash(state);
    }
}

impl PartialEq for SparkScalarSubqueryWrapperExpr {
    fn eq(&self, other: &Self) -> bool {
        other.serialized == self.serialized
            && other.return_type == self.return_type
            && other.return_nullable == self.return_nullable
    }
}

impl DynEq for SparkScalarSubqueryWrapperExpr {
    fn dyn_eq(&self, other: &dyn Any) -> bool {
        other
            .downcast_ref::<Self>()
            .map(|other| other.eq(self))
            .unwrap_or(false)
    }
}

impl PhysicalExpr for SparkScalarSubqueryWrapperExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _: &Schema) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn nullable(&self, _: &Schema) -> Result<bool> {
        Ok(self.return_nullable)
    }

    fn evaluate(&self, _: &RecordBatch) -> Result<ColumnarValue> {
        let result = self.cached_value.get_or_try_init(|| {
            let expr = SparkUDFWrapperExpr::try_new(
                self.serialized.clone(),
                self.return_type.clone(),
                self.return_nullable,
                vec![],
                format!("Subquery"),
            )?;
            let stub_batch = RecordBatch::try_new_with_options(
                Arc::new(Schema::empty()),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(1)),
            )?;
            let result = expr.evaluate(&stub_batch)?.into_array(1)?;
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        });
        result.cloned()
    }

    fn children(&self) -> Vec<&PhysicalExprRef> {
        vec![]
    }

    fn with_new_children(self: Arc<Self>, _: Vec<PhysicalExprRef>) -> Result<PhysicalExprRef> {
        Ok(self.clone())
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "fmt_sql not used")
    }
}
