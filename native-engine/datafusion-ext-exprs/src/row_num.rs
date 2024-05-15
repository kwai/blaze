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
    any::Any,
    fmt::{Debug, Display, Formatter},
    hash::Hasher,
    sync::{
        atomic::{AtomicI64, Ordering::SeqCst},
        Arc,
    },
};

use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Schema},
};
use datafusion::{common::Result, logical_expr::ColumnarValue, physical_expr::PhysicalExpr};

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

impl PartialEq<dyn Any> for RowNumExpr {
    fn eq(&self, _other: &dyn Any) -> bool {
        true
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

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self::default()))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        state.write("RowNum".as_bytes())
    }
}
