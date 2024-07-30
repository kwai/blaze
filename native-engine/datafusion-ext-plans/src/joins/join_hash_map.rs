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
    io::{Cursor, Read, Write},
    slice::{from_raw_parts, from_raw_parts_mut},
    sync::Arc,
};

use arrow::{
    array::{Array, ArrayRef, AsArray, BinaryBuilder, RecordBatch},
    datatypes::{DataType, Field, FieldRef, Schema, SchemaRef},
};
use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};
use datafusion::{common::Result, physical_expr::PhysicalExprRef};
use datafusion_ext_commons::spark_hash::create_murmur3_hashes;
use hashbrown::HashMap;
use itertools::Itertools;
use once_cell::sync::OnceCell;

use crate::common::batch_selection::take_batch;

pub struct Table {
    entry_offsets: Vec<u32>,
    entry_lens: Vec<u32>,
    item_indices: Vec<u32>,
    item_hashes: Vec<i32>,
}

impl Table {
    pub fn new_empty() -> Self {
        let num_entries = Self::num_entries_of_rows(0);
        Self {
            entry_offsets: vec![0; num_entries],
            entry_lens: vec![0; num_entries],
            item_indices: vec![],
            item_hashes: vec![],
        }
    }

    pub fn try_from_key_columns(
        num_rows: usize,
        data_batch: RecordBatch,
        key_columns: &[ArrayRef],
    ) -> Result<(Self, RecordBatch)> {
        // returns the new data batch sorted by hashes

        assert!(
            num_rows < 1073741824,
            "join hash table: number of rows exceeded 2^30: {num_rows}"
        );

        let num_entries = Self::num_entries_of_rows(num_rows) as u32;
        let item_hashes = join_create_hashes(num_rows, &key_columns)?;
        let item_valids = (0..num_rows)
            .map(|row_idx| key_columns.iter().all(|key| key.is_valid(row_idx)))
            .collect::<Vec<_>>();

        // sort record batch by hashes for better compression and data locality
        // null values are placed in the front of each entry
        let (indices, item_hashes, item_valids): (Vec<usize>, Vec<i32>, Vec<bool>) = item_hashes
            .into_iter()
            .zip(item_valids)
            .enumerate()
            .map(|(row_idx, (hash, valid))| (row_idx, hash, valid))
            .sorted_unstable_by_key(|&(_row_idx, hash, valid)| (hash, valid))
            .multiunzip();
        let data_batch = take_batch(data_batch, indices)?;

        let mut entries_to_row_indices: HashMap<u32, Vec<u32>> = HashMap::new();
        for (row_idx, hash) in item_hashes.iter().enumerate() {
            let entry = *hash as u32 % num_entries;
            entries_to_row_indices
                .entry(entry)
                .or_default()
                .push(row_idx as u32);
        }

        let mut entry_offsets = Vec::with_capacity(num_entries as usize);
        let mut entry_lens = Vec::with_capacity(num_entries as usize);
        let mut item_indices = Vec::with_capacity(num_rows);
        for entry in 0..num_entries {
            match entries_to_row_indices.get(&entry) {
                Some(row_indices) => {
                    entry_offsets.push(item_indices.len() as u32);
                    entry_lens.push(row_indices.len() as u32);
                    item_indices.extend_from_slice(row_indices);
                }
                None => {
                    entry_offsets.push(item_indices.len() as u32);
                    entry_lens.push(0);
                }
            }

            // exclude null values from entry
            let cur_entry_idx = entry_offsets.len() - 1;
            while entry_lens[cur_entry_idx] > 0
                && !item_valids[item_indices[entry_offsets[cur_entry_idx] as usize] as usize]
            {
                entry_offsets[cur_entry_idx] += 1;
                entry_lens[cur_entry_idx] -= 1;
            }
        }
        let new = Self {
            entry_offsets,
            entry_lens,
            item_indices,
            item_hashes,
        };
        Ok((new, data_batch))
    }

    pub fn try_from_raw_bytes(raw_bytes: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(raw_bytes);
        let num_rows = cursor.read_u32::<NativeEndian>()? as usize;
        let num_entries = Self::num_entries_of_rows(num_rows);

        let mut new = Self {
            entry_offsets: vec![0; num_entries],
            entry_lens: vec![0; num_entries],
            item_indices: vec![0; num_rows],
            item_hashes: vec![0; num_rows],
        };

        unsafe {
            // safety: read integer arrays as raw bytes
            cursor.read_exact(from_raw_parts_mut(
                new.entry_offsets.as_mut_ptr() as *mut u8,
                num_entries * 4,
            ))?;
            cursor.read_exact(from_raw_parts_mut(
                new.entry_lens.as_mut_ptr() as *mut u8,
                num_entries * 4,
            ))?;
            cursor.read_exact(from_raw_parts_mut(
                new.item_indices.as_mut_ptr() as *mut u8,
                num_rows * 4,
            ))?;
            cursor.read_exact(from_raw_parts_mut(
                new.item_hashes.as_mut_ptr() as *mut u8,
                num_rows * 4,
            ))?;
        }
        Ok(new)
    }

    pub fn try_into_raw_bytes(self) -> Result<Vec<u8>> {
        let num_entries = self.entry_offsets.len();
        let num_rows = self.item_indices.len();
        let mut raw_bytes = Vec::with_capacity(num_entries * 8 + num_rows * 4 + 4);

        raw_bytes.write_u32::<NativeEndian>(num_rows as u32)?;
        unsafe {
            // safety: write integer arrays as raw bytes
            raw_bytes.write_all(from_raw_parts(
                self.entry_offsets.as_ptr() as *const u8,
                num_entries * 4,
            ))?;
            raw_bytes.write_all(from_raw_parts(
                self.entry_lens.as_ptr() as *const u8,
                num_entries * 4,
            ))?;
            raw_bytes.write_all(from_raw_parts(
                self.item_indices.as_ptr() as *const u8,
                num_rows * 4,
            ))?;
            raw_bytes.write_all(from_raw_parts(
                self.item_hashes.as_ptr() as *const u8,
                num_rows * 4,
            ))?;
        }
        Ok(raw_bytes)
    }

    pub fn entry<'a>(&'a self, hash: i32) -> Option<impl Iterator<Item = u32> + 'a> {
        let entry = (hash as u32) % (self.entry_offsets.len() as u32);
        let len = self.entry_lens[entry as usize] as usize;
        if len > 0 {
            let offset = self.entry_offsets[entry as usize] as usize;
            Some(
                self.item_indices[offset..][..len]
                    .iter()
                    .cloned()
                    .filter(move |&idx| self.item_hashes[idx as usize] == hash),
            )
        } else {
            None
        }
    }

    fn num_entries_of_rows(num_rows: usize) -> usize {
        num_rows * 3 + 1
    }
}

pub struct JoinHashMap {
    data_batch: RecordBatch,
    key_columns: Vec<ArrayRef>,
    table: Table,
}

impl JoinHashMap {
    pub fn try_from_data_batch(
        data_batch: RecordBatch,
        key_exprs: &[PhysicalExprRef],
    ) -> Result<JoinHashMap> {
        let key_columns: Vec<ArrayRef> = key_exprs
            .iter()
            .map(|expr| {
                Ok(expr
                    .evaluate(&data_batch)?
                    .into_array(data_batch.num_rows())?)
            })
            .collect::<Result<_>>()?;

        let (table, data_batch) =
            Table::try_from_key_columns(data_batch.num_rows(), data_batch, &key_columns)?;
        Ok(JoinHashMap {
            data_batch,
            key_columns,
            table,
        })
    }

    pub fn try_from_hash_map_batch(
        hash_map_batch: RecordBatch,
        key_exprs: &[PhysicalExprRef],
    ) -> Result<Self> {
        let mut data_batch = hash_map_batch.clone();
        let table = Table::try_from_raw_bytes(
            data_batch
                .remove_column(data_batch.num_columns() - 1)
                .as_binary::<i32>()
                .value(0),
        )?;
        let key_columns: Vec<ArrayRef> = key_exprs
            .iter()
            .map(|expr| {
                Ok(expr
                    .evaluate(&data_batch)?
                    .into_array(data_batch.num_rows())?)
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            data_batch,
            key_columns,
            table,
        })
    }

    pub fn try_new_empty(
        hash_map_schema: SchemaRef,
        key_exprs: &[PhysicalExprRef],
    ) -> Result<Self> {
        let table = Table::new_empty();
        let data_batch = RecordBatch::new_empty(hash_map_schema);
        let key_columns: Vec<ArrayRef> = key_exprs
            .iter()
            .map(|expr| {
                Ok(expr
                    .evaluate(&data_batch)?
                    .into_array(data_batch.num_rows())?)
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            data_batch,
            key_columns,
            table,
        })
    }

    pub fn distinct(&mut self) -> Result<()> {
        let comparators = self
            .key_columns
            .iter()
            .map(|array| Ok(arrow::array::build_compare(&array, &array)?))
            .collect::<Result<Vec<_>>>()?;
        let total_eq = |i, j| {
            comparators
                .iter()
                .all(|comparator| comparator(i, j).is_eq())
        };

        for entry in 0..self.table.entry_offsets.len() {
            if self.table.entry_lens[entry] <= 1 {
                continue;
            }
            let entry_offset = self.table.entry_offsets[entry] as usize;
            let mut entry_end = entry_offset + self.table.entry_lens[entry] as usize;
            let mut i = entry_offset + 1;

            while i < entry_end {
                let item_i = self.table.item_indices[i] as usize;
                let hash_i = self.table.item_hashes[item_i];
                let mut removed = false;

                for j in entry_offset..i {
                    let item_j = self.table.item_indices[j] as usize;
                    let hash_j = self.table.item_hashes[item_j];
                    if hash_j == hash_i && total_eq(item_j, item_i) {
                        // remove an duplicated key, remove it from entry
                        self.table.item_indices.swap(i, entry_end - 1);
                        entry_end -= 1;
                        removed = true;
                        break;
                    }
                }
                if !removed {
                    i += 1;
                }
            }
            self.table.entry_lens[entry] = (entry_end - entry_offset) as u32;
        }
        Ok(())
    }

    pub fn data_schema(&self) -> SchemaRef {
        self.data_batch().schema()
    }

    pub fn data_batch(&self) -> &RecordBatch {
        &self.data_batch
    }

    pub fn key_columns(&self) -> &[ArrayRef] {
        &self.key_columns
    }

    pub fn entry_indices<'a>(&'a self, hash: i32) -> Option<impl Iterator<Item = u32> + 'a> {
        self.table.entry(hash)
    }

    pub fn into_hash_map_batch(self) -> Result<RecordBatch> {
        let schema = join_hash_map_schema(&self.data_batch.schema());
        if self.data_batch.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(schema));
        }
        let mut table_col_builder = BinaryBuilder::new();
        table_col_builder.append_value(&self.table.try_into_raw_bytes()?);
        for _ in 1..self.data_batch.num_rows() {
            table_col_builder.append_null();
        }
        let table_col: ArrayRef = Arc::new(table_col_builder.finish());
        Ok(RecordBatch::try_new(
            schema,
            vec![self.data_batch.columns().to_vec(), vec![table_col]].concat(),
        )?)
    }
}

#[inline]
pub fn join_data_schema(hash_map_schema: &SchemaRef) -> SchemaRef {
    Arc::new(Schema::new(
        hash_map_schema
            .fields()
            .iter()
            .take(hash_map_schema.fields().len() - 1) // exclude hash map column
            .cloned()
            .collect::<Vec<_>>(),
    ))
}

#[inline]
pub fn join_hash_map_schema(data_schema: &SchemaRef) -> SchemaRef {
    Arc::new(Schema::new(
        data_schema
            .fields()
            .iter()
            .map(|field| Arc::new(field.as_ref().clone().with_nullable(true)))
            .chain(std::iter::once(join_table_field()))
            .collect::<Vec<_>>(),
    ))
}

#[inline]
pub fn join_create_hashes(num_rows: usize, key_columns: &[ArrayRef]) -> Result<Vec<i32>> {
    const JOIN_HASH_RANDOM_SEED: i32 = 0x90ec4058u32 as i32;
    let mut hashes = vec![JOIN_HASH_RANDOM_SEED; num_rows];
    create_murmur3_hashes(key_columns, &mut hashes)?;
    Ok(hashes)
}

#[inline]
fn join_table_field() -> FieldRef {
    static BHJ_KEY_FIELD: OnceCell<FieldRef> = OnceCell::new();
    BHJ_KEY_FIELD
        .get_or_init(|| Arc::new(Field::new("~TABLE", DataType::Binary, true)))
        .clone()
}
