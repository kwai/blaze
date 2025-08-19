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
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering::SeqCst},
    },
};

use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Schema},
};
use datafusion::{
    common::Result,
    logical_expr::ColumnarValue,
    physical_expr::{PhysicalExpr, PhysicalExprRef},
};

#[derive(Default)]
pub struct RowNumExpr {
    cur: AtomicI64,
}

impl Display for RowNumExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RowNum")
    }
}

impl Debug for RowNumExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RowNum")
    }
}

impl PartialEq for RowNumExpr {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for RowNumExpr {}

impl Hash for RowNumExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        0.hash(state)
    }
}

impl PhysicalExpr for RowNumExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let num_rows = batch.num_rows();
        let cur = self.cur.fetch_add(num_rows as i64, SeqCst);
        let array: Int64Array = (cur..cur + num_rows as i64).into_iter().collect();
        Ok(ColumnarValue::Array(Arc::new(array)))
    }

    fn children(&self) -> Vec<&PhysicalExprRef> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<PhysicalExprRef>,
    ) -> Result<PhysicalExprRef> {
        Ok(Arc::new(Self::default()))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "fmt_sql not used")
    }
}
