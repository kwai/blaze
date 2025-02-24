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
    io::Cursor,
    sync::Arc,
};

use arrow::{
    array::{ArrayRef, AsArray, BinaryBuilder},
    datatypes::{DataType, Int64Type},
};
use byteorder::{ReadBytesExt, WriteBytesExt};
use datafusion::{common::Result, physical_expr::PhysicalExpr};
use datafusion_ext_commons::{
    arrow::cast::cast, df_unimplemented_err, downcast_any, spark_bloom_filter::SparkBloomFilter,
};

use crate::{
    agg::{
        acc::{AccColumn, AccColumnRef},
        agg::IdxSelection,
        Agg,
    },
    idx_for, idx_for_zipped,
    memmgr::spill::{SpillCompressedReader, SpillCompressedWriter},
};

pub struct AggBloomFilter {
    child: Arc<dyn PhysicalExpr>,
    child_data_type: DataType,
    estimated_num_items: usize,
    num_bits: usize,
}

impl AggBloomFilter {
    pub fn new(
        child: Arc<dyn PhysicalExpr>,
        child_data_type: DataType,
        estimated_num_items: usize,
        num_bits: usize,
    ) -> Self {
        assert!(num_bits.is_power_of_two());
        Self {
            child,
            child_data_type,
            estimated_num_items,
            num_bits,
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

    fn with_new_exprs(&self, exprs: Vec<Arc<dyn PhysicalExpr>>) -> Result<Arc<dyn Agg>> {
        Ok(Arc::new(Self::new(
            exprs[0].clone(),
            self.child_data_type.clone(),
            self.estimated_num_items,
            self.num_bits,
        )))
    }

    fn create_acc_column(&self, num_rows: usize) -> AccColumnRef {
        let mut bloom_filters = Box::new(AccBloomFilterColumn {
            bloom_filters: vec![],
        });
        bloom_filters.resize(num_rows);
        bloom_filters
    }

    fn partial_update(
        &self,
        accs: &mut AccColumnRef,
        acc_idx: IdxSelection<'_>,
        partial_args: &[ArrayRef],
        partial_arg_idx: IdxSelection<'_>,
    ) -> Result<()> {
        let accs = downcast_any!(accs, mut AccBloomFilterColumn).unwrap();
        let bloom_filter = match acc_idx {
            IdxSelection::Single(idx) => {
                if idx >= accs.num_records() {
                    accs.resize(idx + 1);
                }

                let bf = &mut accs.bloom_filters[idx];
                if bf.is_none() {
                    *bf = Some(SparkBloomFilter::new_with_expected_num_items(
                        self.estimated_num_items,
                        self.num_bits,
                    ));
                }
                bf.as_mut().unwrap()
            }
            _ => return df_unimplemented_err!("AggBloomFilter only supports one bloom filter"),
        };
        if partial_arg_idx != IdxSelection::Range(0, partial_args[0].len()) {
            return df_unimplemented_err!("AggBloomFilter only supports updating the whole array");
        }

        match &self.child_data_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                let long_values = cast(&partial_args[0], &DataType::Int64)?;
                for long_value in long_values.as_primitive::<Int64Type>().iter().flatten() {
                    bloom_filter.put_long(long_value);
                }
            }
            DataType::Utf8 => {
                for string_value in partial_args[0].as_string::<i32>().iter().flatten() {
                    bloom_filter.put_binary(string_value.as_bytes());
                }
            }
            DataType::Binary => {
                for binary_value in partial_args[0].as_binary::<i32>().iter().flatten() {
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
        accs: &mut AccColumnRef,
        acc_idx: IdxSelection<'_>,
        merging_accs: &mut AccColumnRef,
        merging_acc_idx: IdxSelection<'_>,
    ) -> Result<()> {
        let accs = downcast_any!(accs, mut AccBloomFilterColumn).unwrap();
        let merging_accs = downcast_any!(merging_accs, mut AccBloomFilterColumn).unwrap();

        idx_for_zipped! {
            ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                if acc_idx < accs.num_records() {
                    let acc_bloom_filter = &mut accs.bloom_filters[acc_idx];
                    let merging_acc_bloom_filter = std::mem::replace(&mut merging_accs.bloom_filters[merging_acc_idx], None);

                    if let Some(merging_acc_bloom_filter) = merging_acc_bloom_filter {
                        if let Some(acc_bloom_filter) = acc_bloom_filter {
                            acc_bloom_filter.put_all(&merging_acc_bloom_filter);
                        } else {
                            *acc_bloom_filter = Some(merging_acc_bloom_filter);
                        }
                    }
                } else {
                    let merging_acc_bloom_filter = std::mem::replace(&mut merging_accs.bloom_filters[merging_acc_idx], None);
                    accs.bloom_filters.push(merging_acc_bloom_filter);
                }
            }
        }
        Ok(())
    }

    fn final_merge(&self, accs: &mut AccColumnRef, acc_idx: IdxSelection<'_>) -> Result<ArrayRef> {
        let accs = downcast_any!(accs, mut AccBloomFilterColumn).unwrap();
        let mut binary_builder = BinaryBuilder::with_capacity(acc_idx.len(), 0);
        let mut buf = vec![];

        idx_for! {
            (acc_idx in acc_idx) => {
                if let Some(bloom_filter) = &mut accs.bloom_filters[acc_idx] {
                    bloom_filter.shrink_to_fit();
                    bloom_filter.write_to(&mut buf)?;
                    binary_builder.append_value(&buf);
                    buf.clear();
                } else {
                    binary_builder.append_null();
                }
            }
        }
        Ok(Arc::new(binary_builder.finish()))
    }
}

struct AccBloomFilterColumn {
    bloom_filters: Vec<Option<SparkBloomFilter>>,
}

impl AccColumn for AccBloomFilterColumn {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn resize(&mut self, len: usize) {
        self.bloom_filters.resize(len, None);
    }

    fn shrink_to_fit(&mut self) {
        self.bloom_filters.shrink_to_fit();
    }

    fn num_records(&self) -> usize {
        self.bloom_filters.len()
    }

    fn mem_used(&self) -> usize {
        self.bloom_filters
            .iter()
            .flatten()
            .map(|bf| bf.mem_size())
            .sum()
    }

    fn freeze_to_rows(&self, idx: IdxSelection<'_>, array: &mut [Vec<u8>]) -> Result<()> {
        idx_for! {
            (idx in idx) => {
                let w = &mut array[idx];
                if let Some(bloom_filter) = &self.bloom_filters[idx] {
                    w.write_u8(1)?;
                    bloom_filter.write_to(w)?;
                } else {
                    w.write_u8(0)?;
                }
            }
        }
        Ok(())
    }

    fn unfreeze_from_rows(&mut self, array: &[&[u8]], offsets: &mut [usize]) -> Result<()> {
        let mut idx = self.num_records();
        self.resize(idx + array.len());

        for (data, offset) in array.iter().zip(offsets) {
            let mut cursor = Cursor::new(*data);
            cursor.set_position(*offset as u64);

            if cursor.read_u8()? == 1 {
                self.bloom_filters[idx] = Some(SparkBloomFilter::read_from(&mut cursor)?);
            } else {
                self.bloom_filters[idx] = None;
            }
            *offset = cursor.position() as usize;
            idx += 1;
        }
        Ok(())
    }

    fn spill(&self, idx: IdxSelection<'_>, w: &mut SpillCompressedWriter) -> Result<()> {
        idx_for! {
            (idx in idx) => {
                if let Some(bloom_filter) = &self.bloom_filters[idx] {
                    w.write_u8(1)?;
                    bloom_filter.write_to(w)?;
                } else {
                    w.write_u8(0)?;
                }
            }
        }
        Ok(())
    }

    fn unspill(&mut self, num_rows: usize, r: &mut SpillCompressedReader) -> Result<()> {
        let idx = self.num_records();
        self.resize(idx + num_rows);

        for i in idx..idx + num_rows {
            if r.read_u8()? == 1 {
                self.bloom_filters[i] = Some(SparkBloomFilter::read_from(r)?);
            } else {
                self.bloom_filters[i] = None;
            }
        }
        Ok(())
    }
}
