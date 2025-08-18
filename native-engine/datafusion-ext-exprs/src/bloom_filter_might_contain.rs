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
    collections::HashMap,
    fmt::{Debug, Display, Formatter},
    hash::{Hash, Hasher},
    io::Cursor,
    sync::{Arc, Weak},
};

use arrow::{
    array::{Array, AsArray, BooleanArray, RecordBatch},
    datatypes::{DataType, Int64Type, Schema},
};
use datafusion::{
    common::{Result, ScalarValue},
    logical_expr::ColumnarValue,
    physical_expr::{PhysicalExpr, PhysicalExprRef},
    physical_expr_common::physical_expr::DynEq,
};
use datafusion_ext_commons::{
    arrow::cast::cast, df_execution_err, df_unimplemented_err, spark_bloom_filter::SparkBloomFilter,
};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;

pub struct BloomFilterMightContainExpr {
    uuid: String,
    bloom_filter_expr: PhysicalExprRef,
    value_expr: PhysicalExprRef,
    bloom_filter: OnceCell<Arc<Option<SparkBloomFilter>>>,
}

impl BloomFilterMightContainExpr {
    pub fn new(
        uuid: String,
        bloom_filter_expr: PhysicalExprRef,
        value_expr: PhysicalExprRef,
    ) -> Self {
        Self {
            uuid,
            bloom_filter_expr,
            value_expr,
            bloom_filter: OnceCell::new(),
        }
    }
}

impl Drop for BloomFilterMightContainExpr {
    fn drop(&mut self) {
        drop(self.bloom_filter.take());
        clear_cached_bloom_filter();
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

impl PartialEq for BloomFilterMightContainExpr {
    fn eq(&self, other: &Self) -> bool {
        self.bloom_filter_expr.eq(&other.bloom_filter_expr) && self.value_expr.eq(&other.value_expr)
    }
}

impl DynEq for BloomFilterMightContainExpr {
    fn dyn_eq(&self, other: &dyn Any) -> bool {
        other
            .downcast_ref::<Self>()
            .map(|other| other.eq(self))
            .unwrap_or(false)
    }
}

impl Hash for BloomFilterMightContainExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(b"BloomFilterMightContainExpr");
        self.bloom_filter_expr.dyn_hash(state);
        self.value_expr.dyn_hash(state);
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
            get_cached_bloom_filter(&self.uuid, || {
                match self.bloom_filter_expr.evaluate(batch)? {
                    ColumnarValue::Scalar(ScalarValue::Binary(Some(v))) => Ok(Some(
                        SparkBloomFilter::read_from(&mut Cursor::new(v.as_slice()))?,
                    )),
                    ColumnarValue::Scalar(ScalarValue::Binary(None)) => Ok(None),
                    _ => {
                        df_execution_err!("bloom_filter_arg must be valid binary scalar value")
                    }
                }
            })
        })?;

        // always return false if bloom filter is null
        if bloom_filter.is_none() {
            return Ok(ColumnarValue::Scalar(ScalarValue::from(false)));
        }
        let bloom_filter = bloom_filter.as_ref().as_ref().unwrap();

        // process with bloom filter
        let value = self.value_expr.evaluate(batch)?;
        let value_is_scalar = matches!(value, ColumnarValue::Scalar(_));
        let values = value.into_array(1)?;
        let might_contain = match values.data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                let values = cast(&values, &DataType::Int64)?;
                bloom_filter.might_contain_longs(values.as_primitive::<Int64Type>().values())
            }
            DataType::Utf8 => BooleanArray::from_unary(values.as_string::<i32>(), |v| {
                bloom_filter.might_contain_binary(v.as_bytes())
            }),
            DataType::Binary => BooleanArray::from_unary(values.as_binary::<i32>(), |v| {
                bloom_filter.might_contain_binary(v)
            }),
            other => return df_unimplemented_err!("unsupported data type: {:?}", other),
        };

        Ok(if value_is_scalar {
            ColumnarValue::Scalar(ScalarValue::from(
                might_contain.is_valid(0).then_some(might_contain.value(0)),
            ))
        } else {
            ColumnarValue::Array(Arc::new(might_contain))
        })
    }

    fn children(&self) -> Vec<&PhysicalExprRef> {
        vec![&self.bloom_filter_expr, &self.value_expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalExprRef>,
    ) -> Result<PhysicalExprRef> {
        Ok(Arc::new(Self::new(
            self.uuid.clone(),
            children[0].clone(),
            children[1].clone(),
        )))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "fmt_sql not used")
    }
}

type Slot = Arc<Mutex<Weak<Option<SparkBloomFilter>>>>;
static CACHED_BLOOM_FILTER: OnceCell<Arc<Mutex<HashMap<String, Slot>>>> = OnceCell::new();

fn get_cached_bloom_filter(
    uuid: &str,
    init: impl FnOnce() -> Result<Option<SparkBloomFilter>>,
) -> Result<Arc<Option<SparkBloomFilter>>> {
    // remove expire keys and insert new key
    let slot = {
        let cached_bloom_filter = CACHED_BLOOM_FILTER.get_or_init(|| Arc::default());
        let mut cached_bloom_filter = cached_bloom_filter.lock();
        cached_bloom_filter
            .entry(uuid.to_string())
            .or_default()
            .clone()
    };

    let mut slot = slot.lock();
    if let Some(cached) = slot.upgrade() {
        Ok(cached)
    } else {
        let new = Arc::new(init()?);
        *slot = Arc::downgrade(&new);
        Ok(new)
    }
}

fn clear_cached_bloom_filter() {
    let cached_bloom_filter = CACHED_BLOOM_FILTER.get_or_init(|| Arc::default());
    let mut cached_bloom_filter = cached_bloom_filter.lock();
    cached_bloom_filter.retain(|_, v| Arc::strong_count(v) > 0);
}
