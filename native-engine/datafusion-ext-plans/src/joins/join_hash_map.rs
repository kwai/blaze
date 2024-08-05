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
    fmt::{Debug, Formatter},
    io::{Cursor, Read, Write},
    slice::{from_raw_parts, from_raw_parts_mut},
    sync::Arc,
};

use arrow::{
    array::{Array, ArrayRef, AsArray, BinaryBuilder, RecordBatch},
    buffer::NullBuffer,
    datatypes::{DataType, Field, FieldRef, Schema, SchemaRef},
};
use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};
use datafusion::{common::Result, physical_expr::PhysicalExprRef};
use datafusion_ext_commons::spark_hash::create_hashes;
use once_cell::sync::OnceCell;

use crate::joins::bhj::make_eq_comparator_multiple_arrays;

pub struct Table {
    entries: Vec<Entry>,
    items: Vec<Item>,
}

#[derive(Clone, Copy, Default, Debug)]
#[repr(C)]
struct Entry {
    offset: u32,
    len: u32,
}

#[derive(Clone, Copy, Default, Debug)]
#[repr(C)]
struct Item {
    idx: u32,
    hash: i32,
}

impl Table {
    pub fn new_empty() -> Self {
        let num_entries = Self::num_entries_of_rows(0);
        Self {
            entries: vec![Entry::default(); num_entries],
            items: vec![],
        }
    }

    pub fn try_from_key_columns(
        num_rows: usize,
        data_batch: RecordBatch,
        key_columns: &[ArrayRef],
    ) -> Result<(Self, RecordBatch)> {
        assert!(
            num_rows < 1073741824,
            "join hash table: number of rows exceeded 2^30: {num_rows}"
        );

        let num_entries = Self::num_entries_of_rows(num_rows);
        let item_hashes = join_create_hashes(num_rows, &key_columns)?;
        let item_valids = key_columns
            .iter()
            .map(|col| col.nulls().cloned())
            .reduce(|nb1, nb2| NullBuffer::union(nb1.as_ref(), nb2.as_ref()))
            .flatten();
        let item_is_valid = |row_idx| match &item_valids {
            Some(nb) => nb.is_valid(row_idx),
            None => true,
        };

        let entries_to_row_indices = (0..num_rows)
            .into_iter()
            .filter(|&row_idx| item_is_valid(row_idx)) // exclude null keys
            .map(|row_idx| (row_idx as u32, (item_hashes[row_idx] as usize % num_entries) as u32))
            .collect::<Vec<_>>();

        // init entry offsets
        let mut entry_counts = vec![0; num_entries];
        for &(_, entry_idx) in &entries_to_row_indices {
            entry_counts[entry_idx as usize] += 1;
        }
        let mut entries = vec![Entry::default(); num_entries];
        let mut offset = 0;
        for (entry_idx, entry_count) in entry_counts.into_iter().enumerate() {
            entries[entry_idx].offset = offset;
            offset += entry_count;
        }

        let mut items = vec![Item::default(); num_rows];
        for (row_idx, entry_idx) in entries_to_row_indices {
            if !item_is_valid(row_idx as usize) {
                continue;
            }
            let entry = &mut entries[entry_idx as usize];
            entry.len += 1;

            let item_idx = entry.offset + entry.len - 1;
            items[item_idx as usize] = Item {
                idx: row_idx,
                hash: item_hashes[row_idx as usize],
            };
        }
        items.resize(num_rows, Item::default());

        let new = Self { entries, items };
        Ok((new, data_batch))
    }

    pub fn try_from_raw_bytes(raw_bytes: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(raw_bytes);
        let num_rows = cursor.read_u32::<NativeEndian>()? as usize;
        let num_entries = Self::num_entries_of_rows(num_rows);

        let mut new = Self {
            entries: vec![Entry::default(); num_entries],
            items: vec![Item::default(); num_rows],
        };

        unsafe {
            // safety: read integer arrays as raw bytes
            cursor.read_exact(from_raw_parts_mut(
                new.entries.as_mut_ptr() as *mut u8,
                num_entries * 8,
            ))?;
            cursor.read_exact(from_raw_parts_mut(
                new.items.as_mut_ptr() as *mut u8,
                num_rows * 8,
            ))?;
        }
        Ok(new)
    }

    pub fn try_into_raw_bytes(self) -> Result<Vec<u8>> {
        let num_entries = self.entries.len();
        let num_rows = self.items.len();
        let mut raw_bytes = Vec::with_capacity(num_entries * 8 + num_rows * 8 + 4);

        raw_bytes.write_u32::<NativeEndian>(num_rows as u32)?;
        unsafe {
            // safety: write integer arrays as raw bytes
            raw_bytes.write_all(from_raw_parts(
                self.entries.as_ptr() as *const u8,
                num_entries * 8,
            ))?;
            raw_bytes.write_all(from_raw_parts(
                self.items.as_ptr() as *const u8,
                num_rows * 8,
            ))?;
        }
        Ok(raw_bytes)
    }

    pub fn entry<'a>(&'a self, hash: i32) -> Option<impl Iterator<Item = u32> + 'a> {
        let entry = self.entries[(hash as usize) % self.entries.len()];
        if entry.len > 0 {
            Some(
                self.items[entry.offset as usize..][..entry.len as usize]
                    .iter()
                    .filter(move |item| item.hash == hash)
                    .map(move |item| item.idx),
            )
        } else {
            None
        }
    }

    fn num_entries_of_rows(num_rows: usize) -> usize {
        num_rows * 5 + 1
    }
}

pub struct JoinHashMap {
    data_batch: RecordBatch,
    key_columns: Vec<ArrayRef>,
    table: Table,
}

impl Debug for JoinHashMap {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "JoinHashMap(..)")
    }
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
        let eq = make_eq_comparator_multiple_arrays(&self.key_columns, &self.key_columns)?;

        for entry in self.table.entries.iter_mut().filter(|entry| entry.len > 1) {
            let entry_offset = entry.offset as usize;
            let mut entry_end = entry_offset + entry.len as usize;
            let mut i = entry_offset + 1;

            while i < entry_end {
                let item_i = self.table.items[i];
                let mut removed = false;

                for j in entry_offset..i {
                    let item_j = self.table.items[j];
                    if item_j.hash == item_i.hash && eq(item_j.idx as usize, item_i.idx as usize) {
                        // remove an duplicated key, remove it from entry
                        self.table.items.swap(i, entry_end - 1);
                        entry_end -= 1;
                        removed = true;
                        break;
                    }
                }
                if !removed {
                    i += 1;
                }
            }
            entry.len = (entry_end - entry_offset) as u32;
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

    pub fn num_entries(&self) -> usize {
        self.table.entries.len()
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
    const JOIN_HASH_RANDOM_SEED: i32 = 0x30ec4058i32;
    let mut hashes = vec![JOIN_HASH_RANDOM_SEED; num_rows];
    create_hashes(key_columns, &mut hashes, |v, h| {
        gxhash::gxhash32(v, h as i64) as i32
    })?;
    Ok(hashes)
}

#[inline]
fn join_table_field() -> FieldRef {
    static BHJ_KEY_FIELD: OnceCell<FieldRef> = OnceCell::new();
    BHJ_KEY_FIELD
        .get_or_init(|| Arc::new(Field::new("~TABLE", DataType::Binary, true)))
        .clone()
}
