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

use std::{hash::Hasher, sync::Arc};

use arrow::{
    array::{
        make_array, Array, ArrayRef, AsArray, BinaryArray, BinaryBuilder, MutableArrayData,
        RecordBatch, RecordBatchOptions,
    },
    compute::filter_record_batch,
    datatypes::{DataType, Field, FieldRef, Schema, SchemaRef},
    row::Rows,
};
use datafusion::common::Result;
use datafusion_ext_commons::{batch_size, df_execution_err};
use gxhash::GxHasher;
use itertools::Itertools;
use num::Integer;
use once_cell::sync::OnceCell;

use crate::joins::Idx;

pub struct JoinHashMap {
    batches: Vec<RecordBatch>, // data batch + key column
    keys: Vec<BinaryArray>,
    data_batches: Vec<RecordBatch>, // batches excluding the last key column
    batch_size: usize,              // all batches except the last one must have the same size
    num_entries: usize,
}

impl JoinHashMap {
    pub fn try_new(hash_map_batches: Vec<RecordBatch>) -> Result<Self> {
        if hash_map_batches.is_empty() {
            return df_execution_err!("JoinHashMap should have at least one entry");
        }
        let batch_size = hash_map_batches[0].num_rows();

        if hash_map_batches[..hash_map_batches.len() - 1]
            .iter()
            .any(|batch| batch.num_rows() != batch_size)
        {
            return df_execution_err!("JoinHashMap expect batch size {batch_size}");
        }
        Ok(Self {
            num_entries: hash_map_batches.iter().map(|batch| batch.num_rows()).sum(),
            keys: join_keys_from_hash_map_batches(&hash_map_batches),
            data_batches: hash_map_batches
                .iter()
                .cloned()
                .map(|mut batch| {
                    batch.remove_column(batch.num_columns() - 1);
                    batch
                })
                .collect(),
            batches: hash_map_batches,
            batch_size,
        })
    }

    pub fn data_schema(&self) -> SchemaRef {
        self.data_batches()[0].schema()
    }

    pub fn data_batches(&self) -> &[RecordBatch] {
        &self.data_batches
    }

    pub fn hash_map_batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    pub fn into_hash_map_batches(self) -> Vec<RecordBatch> {
        self.batches
    }

    pub fn inserted(&self, batch_idx: usize, row_idx: usize) -> bool {
        self.keys[batch_idx].is_valid(row_idx)
    }

    pub fn search<'a, K: AsRef<[u8]> + 'a>(&'a self, key: K) -> impl Iterator<Item = Idx> + 'a {
        struct EntryIterator<'a, K: AsRef<[u8]> + 'a> {
            key: K,
            join_hash_map_keys: &'a [BinaryArray],
            batch_size: usize,
            num_entries: usize,
            entry: usize,
        }
        impl<'a, K: AsRef<[u8]> + 'a> Iterator for EntryIterator<'a, K> {
            type Item = Idx;

            fn next(&mut self) -> Option<Self::Item> {
                loop {
                    // get current idx and advance to next entry
                    let idx = self.entry.div_rem(&self.batch_size);
                    self.entry = 1 + ((self.entry + 1) % (self.num_entries - 1));

                    // check current entry
                    let keys = &self.join_hash_map_keys[idx.0];
                    if keys.is_null(idx.1) {
                        return None;
                    }
                    if keys.value(idx.1) == self.key.as_ref() {
                        return Some(idx);
                    }
                }
            }
        }

        let entry = 1 + ((join_hash(key.as_ref()) as usize) % (self.num_entries - 1));
        EntryIterator {
            key,
            join_hash_map_keys: &self.keys,
            batch_size: self.batch_size,
            num_entries: self.num_entries,
            entry,
        }
    }

    pub fn iter_values(&self) -> impl Iterator<Item = RecordBatch> {
        self.batches
            .clone()
            .into_iter()
            .filter_map(|mut batch| {
                let keys = batch.remove_column(batch.num_columns() - 1);
                let filtered = arrow::compute::is_not_null(&keys)
                    .and_then(|valid_array| Ok(filter_record_batch(&batch, &valid_array)?))
                    .expect("error filtering record batch");
                let non_empty = filtered.num_rows() > 0;
                non_empty.then(|| filtered)
            })
            .filter(|batch| batch.num_rows() > 0)
    }
}

pub fn build_join_hash_map(
    batches: &[RecordBatch],
    batch_schema: SchemaRef,
    keys: &[Rows],
) -> Result<JoinHashMap> {
    let batch_size = batch_size();
    let num_valid_entries = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
    let num_entries = (num_valid_entries + 16) * 5 + 1;

    // build idx_map
    let mut idx_map = vec![None; num_entries];
    for (batch_idx, keys) in keys.iter().enumerate() {
        for (row_idx, key) in keys.iter().enumerate() {
            let hash = join_hash(key) as usize;
            let mut entry = 1 + (hash % (num_entries - 1)); // 0 is reserved

            // find an empty slot, then insert the key index
            while idx_map[entry].is_some() {
                entry = 1 + ((entry + 1) % (num_entries - 1)); // 0 is reserved
            }
            idx_map[entry] = Some((batch_idx, row_idx));
        }
    }

    // build hash map batches
    let hash_map_schema = join_hash_map_schema(&batch_schema);
    let mut hash_map_batches = vec![];
    let mut batch_array_datas = vec![vec![]; batch_schema.fields().len()];
    for batch in batches {
        for (col_idx, col) in batch.columns().iter().enumerate() {
            batch_array_datas[col_idx].push(col.to_data());
        }
    }
    for batch_entries in idx_map.iter().chunks(batch_size).into_iter() {
        let mut keys_builder = BinaryBuilder::with_capacity(batch_size, 0);
        let mut data_cols_builder: Vec<MutableArrayData> = batch_array_datas
            .iter()
            .map(|col_array_datas| {
                MutableArrayData::new(col_array_datas.iter().collect(), true, batch_size)
            })
            .collect();

        for entry in batch_entries {
            if let &Some((batch_idx, row_idx)) = entry {
                keys_builder.append_value(keys[batch_idx].row(row_idx).as_ref());
                for col_builder in data_cols_builder.iter_mut() {
                    col_builder.extend(batch_idx, row_idx, row_idx + 1);
                }
            } else {
                keys_builder.append_null();
                for col_builder in data_cols_builder.iter_mut() {
                    col_builder.extend_nulls(1);
                }
            }
        }
        let keys: ArrayRef = Arc::new(keys_builder.finish());
        let data_cols: Vec<ArrayRef> = data_cols_builder
            .into_iter()
            .map(|col_builder| make_array(col_builder.freeze()))
            .collect();
        let batch_num_rows = keys.len();
        let hash_map_batch = RecordBatch::try_new_with_options(
            hash_map_schema.clone(),
            [data_cols, vec![keys]].concat(),
            &RecordBatchOptions::new().with_row_count(Some(batch_num_rows)),
        )?;
        hash_map_batches.push(hash_map_batch);
    }
    Ok(JoinHashMap {
        keys: join_keys_from_hash_map_batches(&hash_map_batches),
        data_batches: hash_map_batches
            .iter()
            .cloned()
            .map(|mut batch| {
                batch.remove_column(batch.num_columns() - 1);
                batch
            })
            .collect(),
        batches: hash_map_batches,
        batch_size,
        num_entries,
    })
}

#[inline]
pub fn join_hash_map_schema(data_schema: &SchemaRef) -> SchemaRef {
    Arc::new(Schema::new(
        data_schema
            .fields()
            .iter()
            .map(|field| Arc::new(field.as_ref().clone().with_nullable(true)))
            .chain(std::iter::once(join_key_field()))
            .collect::<Vec<_>>(),
    ))
}

#[inline]
fn join_key_field() -> FieldRef {
    static BHJ_KEY_FIELD: OnceCell<FieldRef> = OnceCell::new();
    BHJ_KEY_FIELD
        .get_or_init(|| Arc::new(Field::new("~KEY", DataType::Binary, true)))
        .clone()
}

#[inline]
fn join_hash(key: impl AsRef<[u8]>) -> u32 {
    let mut h = GxHasher::with_seed(0x10c736ed99f9c14e);
    h.write(key.as_ref());
    h.finish() as u32
}

#[inline]
fn join_keys_from_hash_map_batches(hash_map_batches: &[RecordBatch]) -> Vec<BinaryArray> {
    hash_map_batches
        .iter()
        .map(|batch| {
            batch
                .columns()
                .last()
                .expect("hashmap key column not found")
                .as_binary()
                .clone()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{AsArray, Int32Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Int32Type, Schema},
        row::{RowConverter, SortField},
    };
    use datafusion::{assert_batches_sorted_eq, common::Result};

    #[test]
    fn test_join_hash_map() -> Result<()> {
        // generate a string key-value record batch for testing
        let batch_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            batch_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![
                    "a0", "a111", "a222", "a333", "a333", "a444",
                ])),
                Arc::new(Int32Array::from(vec![0, 111, 222, 3331, 3332, 444])),
            ],
        )?;

        // generate hashmap
        let row_converter = RowConverter::new(vec![SortField::new(DataType::Utf8)])?;
        let keys = row_converter.convert_columns(&[batch.column(0).clone()])?;
        let key333 = keys.row(3).owned();
        let hashmap = super::build_join_hash_map(&[batch], batch_schema, &[keys])?;

        // test searching a333
        let mut iter = hashmap.search(key333);
        let idx = iter.next().unwrap();
        let hash_map_batch = &hashmap.hash_map_batches()[idx.0];
        assert_eq!(
            hash_map_batch.column(0).as_string::<i32>().value(idx.1),
            "a333"
        );
        assert_eq!(
            hash_map_batch
                .column(1)
                .as_primitive::<Int32Type>()
                .value(idx.1),
            3331
        );
        let idx = iter.next().unwrap();
        let hash_map_batch = &hashmap.hash_map_batches()[idx.0];
        assert_eq!(
            hash_map_batch.column(0).as_string::<i32>().value(idx.1),
            "a333"
        );
        assert_eq!(
            hash_map_batch
                .column(1)
                .as_primitive::<Int32Type>()
                .value(idx.1),
            3332
        );
        let idx = iter.next();
        assert_eq!(idx, None);

        // test searching inexistent key
        let mut iter = hashmap.search(b"inexistent");
        let idx = iter.next();
        assert_eq!(idx, None);

        // test iter values
        let value_batch = hashmap.iter_values().next().unwrap();
        assert_batches_sorted_eq!(
            vec![
                "+------+-------+",
                "| key  | value |",
                "+------+-------+",
                "| a0   | 0     |",
                "| a111 | 111   |",
                "| a222 | 222   |",
                "| a333 | 3331  |",
                "| a333 | 3332  |",
                "| a444 | 444   |",
                "+------+-------+",
            ],
            &[value_batch]
        );
        Ok(())
    }
}
