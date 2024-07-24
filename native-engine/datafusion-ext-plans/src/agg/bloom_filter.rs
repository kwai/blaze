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
    fmt::{Debug, Formatter},
    sync::{atomic::AtomicUsize, Arc},
};

use arrow::{
    array::{ArrayRef, AsArray},
    datatypes::{DataType, Int64Type},
};
use datafusion::{
    common::{Result, ScalarValue},
    physical_expr::PhysicalExpr,
};
use datafusion_ext_commons::{
    cast::cast, df_unimplemented_err, downcast_any, spark_bloom_filter::SparkBloomFilter,
};

use crate::agg::{
    acc::{AccumInitialValue, AccumStateRow, AccumStateValAddr, RefAccumStateRow},
    Agg, WithAggBufAddrs, WithMemTracking,
};

pub struct AggBloomFilter {
    child: Arc<dyn PhysicalExpr>,
    child_data_type: DataType,
    estimated_num_items: usize,
    num_bits: usize,
    accums_initial: Vec<AccumInitialValue>,
    accum_state_val_addr: AccumStateValAddr,
    mem_used_tracker: AtomicUsize,
}

impl WithAggBufAddrs for AggBloomFilter {
    fn set_accum_state_val_addrs(&mut self, accum_state_val_addrs: &[AccumStateValAddr]) {
        self.accum_state_val_addr = accum_state_val_addrs[0];
    }
}

impl WithMemTracking for AggBloomFilter {
    fn mem_used_tracker(&self) -> &AtomicUsize {
        &self.mem_used_tracker
    }
}

impl AggBloomFilter {
    pub fn new(
        child: Arc<dyn PhysicalExpr>,
        child_data_type: DataType,
        estimated_num_items: usize,
        num_bits: usize,
    ) -> Self {
        Self {
            child,
            child_data_type,
            estimated_num_items,
            num_bits,
            accums_initial: vec![AccumInitialValue::BloomFilter {
                estimated_num_items,
                num_bits,
            }],
            accum_state_val_addr: AccumStateValAddr::default(),
            mem_used_tracker: AtomicUsize::new(0),
        }
    }
}

impl Debug for AggBloomFilter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AggBloomFilter({:?}, estimated_num_items={}, num_bits={})",
            self.child, self.estimated_num_items, self.num_bits,
        )
    }
}

impl Agg for AggBloomFilter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn exprs(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.child.clone()]
    }

    fn data_type(&self) -> &DataType {
        &DataType::Binary
    }

    fn nullable(&self) -> bool {
        true
    }

    fn accums_initial(&self) -> &[AccumInitialValue] {
        &self.accums_initial
    }

    fn with_new_exprs(&self, exprs: Vec<Arc<dyn PhysicalExpr>>) -> Result<Arc<dyn Agg>> {
        Ok(Arc::new(Self::new(
            exprs[0].clone(),
            self.child_data_type.clone(),
            self.estimated_num_items,
            self.num_bits,
        )))
    }

    fn increase_acc_mem_used(&self, acc: &mut RefAccumStateRow) {
        self.add_mem_used(
            acc.dyn_value(self.accum_state_val_addr)
                .as_ref()
                .unwrap()
                .mem_size(),
        );
    }

    fn partial_update(
        &self,
        _acc: &mut RefAccumStateRow,
        _values: &[ArrayRef],
        _row_idx: usize,
    ) -> Result<()> {
        df_unimplemented_err!("AggBloomFilter::partial_update is not implemented")
    }

    fn partial_update_all(&self, acc: &mut RefAccumStateRow, values: &[ArrayRef]) -> Result<()> {
        let bloom_filter = match acc.dyn_value_mut(self.accum_state_val_addr) {
            Some(v) => downcast_any!(v, mut SparkBloomFilter)?,
            v @ None => {
                *v = Some(Box::new(SparkBloomFilter::new_with_expected_num_items(
                    self.estimated_num_items,
                    self.num_bits,
                )));
                downcast_any!(v.as_mut().unwrap(), mut SparkBloomFilter)?
            }
        };

        match &self.child_data_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                let long_values = cast(&values[0], &DataType::Int64)?;
                for long_value in long_values.as_primitive::<Int64Type>().iter().flatten() {
                    bloom_filter.put_long(long_value);
                }
            }
            DataType::Utf8 => {
                for string_value in values[0].as_string::<i32>().iter().flatten() {
                    bloom_filter.put_binary(string_value.as_bytes());
                }
            }
            DataType::Binary => {
                for binary_value in values[0].as_binary::<i32>().iter().flatten() {
                    bloom_filter.put_binary(binary_value);
                }
            }
            other => {
                df_unimplemented_err!("AggBloomFilter is not implemented for data type {other}")?;
            }
        }
        Ok(())
    }

    fn partial_merge(
        &self,
        acc: &mut RefAccumStateRow,
        merging_acc: &mut RefAccumStateRow,
    ) -> Result<()> {
        if let Some(merging_value) = merging_acc.dyn_value_mut(self.accum_state_val_addr) {
            let w = acc.dyn_value_mut(self.accum_state_val_addr);
            match w {
                None => {
                    let merging_bloom_filter = downcast_any!(merging_value, mut SparkBloomFilter)?;
                    *w = Some(Box::new(std::mem::take(merging_bloom_filter)));
                }
                Some(w) => {
                    let bloom_filter = downcast_any!(w, mut SparkBloomFilter)?;
                    let merging_bloom_filter = downcast_any!(merging_value, mut SparkBloomFilter)?;
                    self.sub_mem_used(merging_bloom_filter.mem_size());
                    bloom_filter.put_all(&merging_bloom_filter);
                }
            }
        }
        Ok(())
    }

    fn final_merge(&self, acc: &mut RefAccumStateRow) -> Result<ScalarValue> {
        if let Some(value) = acc.dyn_value_mut(self.accum_state_val_addr) {
            let bloom_filter = std::mem::take(downcast_any!(value, mut SparkBloomFilter)?);
            self.sub_mem_used(bloom_filter.mem_size());
            let mut buf = vec![];
            bloom_filter.write_to(&mut buf)?;
            Ok(ScalarValue::Binary(Some(buf)))
        } else {
            Ok(ScalarValue::Binary(None))
        }
    }

    fn final_batch_merge(&self, accs: &mut [RefAccumStateRow]) -> Result<ArrayRef> {
        let scalars = accs
            .iter_mut()
            .map(|acc| self.final_merge(acc))
            .collect::<Result<Vec<_>>>()?;
        Ok(ScalarValue::iter_to_array(scalars)?)
    }
}
