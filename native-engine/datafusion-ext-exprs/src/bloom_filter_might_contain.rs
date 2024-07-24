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
    sync::Arc,
};

use arrow::{
    array::{AsArray, BooleanArray, RecordBatch},
    datatypes::{DataType, Int64Type, Schema},
};
use datafusion::{
    common::{Result, ScalarValue},
    logical_expr::ColumnarValue,
    physical_expr::PhysicalExpr,
};
use datafusion_ext_commons::{
    cast::cast, df_execution_err, df_unimplemented_err, spark_bloom_filter::SparkBloomFilter,
};
use once_cell::sync::OnceCell;

pub struct BloomFilterMightContainExpr {
    bloom_filter_expr: Arc<dyn PhysicalExpr>,
    value_expr: Arc<dyn PhysicalExpr>,
    bloom_filter: OnceCell<SparkBloomFilter>,
}

impl BloomFilterMightContainExpr {
    pub fn new(
        bloom_filter_expr: Arc<dyn PhysicalExpr>,
        value_expr: Arc<dyn PhysicalExpr>,
    ) -> Self {
        Self {
            bloom_filter_expr,
            value_expr,
            bloom_filter: OnceCell::new(),
        }
    }
}

impl Display for BloomFilterMightContainExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Debug for BloomFilterMightContainExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("BloomFilterMightContainExpr")
            .field(&self.value_expr)
            .finish()
    }
}

impl PartialEq<dyn Any> for BloomFilterMightContainExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        if let Some(other) = other.downcast_ref::<Self>() {
            self.bloom_filter_expr.eq(&other.bloom_filter_expr)
                && self.value_expr.eq(&other.value_expr)
        } else {
            false
        }
    }
}

impl PhysicalExpr for BloomFilterMightContainExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        // init bloom filter
        let bloom_filter = self.bloom_filter.get_or_try_init(|| {
            match self.bloom_filter_expr.evaluate(batch)? {
                ColumnarValue::Scalar(ScalarValue::Binary(Some(v))) => {
                    Ok(SparkBloomFilter::read_from(v.as_slice())?)
                }
                _ => {
                    df_execution_err!("bloom_filter_arg must be valid binary scalar value")
                }
            }
        })?;

        // process with bloom filter
        let values = self.value_expr.evaluate(&batch)?.into_array(1)?;
        let might_contain = match values.data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                let values = cast(&values, &DataType::Int64)?;
                BooleanArray::from_unary(values.as_primitive::<Int64Type>(), |v| {
                    bloom_filter.might_contain_long(v)
                })
            }
            DataType::Utf8 => BooleanArray::from_unary(values.as_string::<i32>(), |v| {
                bloom_filter.might_contain_binary(v.as_bytes())
            }),
            DataType::Binary => BooleanArray::from_unary(values.as_binary::<i32>(), |v| {
                bloom_filter.might_contain_binary(v)
            }),
            other => return df_unimplemented_err!("unsupported data type: {:?}", other),
        };
        Ok(ColumnarValue::Array(Arc::new(might_contain)))
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.bloom_filter_expr.clone(), self.value_expr.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self::new(
            children[0].clone(),
            children[1].clone(),
        )))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        state.write(b"BloomFilterMightContainExpr");
        self.bloom_filter_expr.dyn_hash(state);
        self.value_expr.dyn_hash(state);
    }
}
