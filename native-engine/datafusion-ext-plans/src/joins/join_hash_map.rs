// Copyright 2022 The Auron Authors
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
    hash::{BuildHasher, Hasher},
    io::{Cursor, Read, Write},
    simd::{Simd, cmp::SimdPartialEq},
    sync::Arc,
};

use arrow::{
    array::{Array, ArrayRef, AsArray, BinaryBuilder, RecordBatch},
    datatypes::{DataType, Field, FieldRef, Schema, SchemaRef},
};
use datafusion::{common::Result, physical_expr::PhysicalExprRef};
use datafusion_ext_commons::{
    SliceAsRawBytes, UninitializedInit,
    io::{read_len, write_len},
    prefetch_read_data,
    spark_hash::create_hashes,
    unchecked,
};
use itertools::Itertools;
use once_cell::sync::OnceCell;
use unchecked_index::UncheckedIndex;

// empty:  lead=0, value=0
// range:  lead=0, value=start, mapped_indices[start-1]=len
// single: lead=1, value=idx
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct MapValue(u32);

impl MapValue {
    pub const EMPTY: MapValue = MapValue(0);

    pub fn new_single(idx: u32) -> Self {
        Self(1 << 31 | idx)
    }

    pub fn new_range(start: u32) -> Self {
        Self(start)
    }

    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }

    pub fn is_single(&self) -> bool {
        self.0 >> 31 == 1
    }

    pub fn is_range(&self) -> bool {
        self.0 >> 31 == 0 && !self.is_empty()
    }

    pub fn get_single(&self) -> u32 {
        self.0 & 0x7fffffff
    }

    pub fn get_range<'a>(&self, map: &'a JoinHashMap) -> &'a [u32] {
        let start = self.0 as usize;
        let len = map.table.mapped_indices[start - 1] as usize;
        let end = start + len;
        &map.table.mapped_indices[start..end]
    }
}

const MAP_VALUE_GROUP_SIZE: usize = 8;

#[derive(Clone, Copy, Default)]
#[repr(align(64))] // ensure one group can be cached into a cache line
struct MapValueGroup {
    hashes: Simd<u32, MAP_VALUE_GROUP_SIZE>,
    values: [MapValue; MAP_VALUE_GROUP_SIZE],
}
const _MAP_VALUE_GROUP_SIZE_CHECKER: [(); 64] = [(); size_of::<MapValueGroup>()];

struct Table {
    num_valid_items: usize,
    map_mod_bits: u32,
    map: UncheckedIndex<Vec<MapValueGroup>>,
    mapped_indices: UncheckedIndex<Vec<u32>>,
}

impl Table {
    fn create_from_key_columns(num_rows: usize, key_columns: &[ArrayRef]) -> Result<Self> {
        assert!(
            num_rows < 1073741824,
            "join hash table: number of rows exceeded 2^30: {num_rows}"
        );
        let hashes = join_create_hashes(num_rows, key_columns);
        Self::craete_from_key_columns_and_hashes(num_rows, key_columns, hashes)
    }

    fn craete_from_key_columns_and_hashes(
        num_rows: usize,
        key_columns: &[ArrayRef],
        hashes: Vec<u32>,
    ) -> Result<Self> {
        assert!(
            num_rows < 1073741824,
            "join hash table: number of rows exceeded 2^30: {num_rows}"
        );

        let key_is_valid = |row_idx| key_columns.iter().all(|col| col.is_valid(row_idx));
        let mut mapped_indices = unchecked!(vec![]);
        let mut num_valid_items = 0;

        // collect map items
        let mut map_items = unchecked!(vec![]);
        for (hash, chunk) in hashes
            .into_iter()
            .enumerate()
            .filter(|(idx, _)| key_is_valid(*idx))
            .map(|(idx, hash)| {
                num_valid_items += 1;
                (idx as u32, hash)
            })
            .sorted_unstable_by_key(|&(idx, hash)| (hash, idx))
            .chunk_by(|(_, hash)| *hash)
            .into_iter()
        {
            let pos = mapped_indices.len() as u32;
            mapped_indices.push(0);
            mapped_indices.extend(chunk.map(|(idx, _hash)| idx));

            let start = pos + 1;
            let len = mapped_indices.len() as u32 - start;
            mapped_indices[pos as usize] = len;

            map_items.push((
                hash,
                match len {
                    0 => unreachable!(),
                    1 => {
                        let single = mapped_indices.pop().unwrap();
                        let _len = mapped_indices.pop().unwrap();
                        MapValue::new_single(single)
                    }
                    _ => MapValue::new_range(start),
                },
            ));
        }

        // build map
        let map_mod_bits = (map_items.len().max(128) * 2 / MAP_VALUE_GROUP_SIZE)
            .next_power_of_two()
            .trailing_zeros();
        let mut map = unchecked!(vec![MapValueGroup::default(); 1usize << map_mod_bits]);

        macro_rules! entries {
            [$i:expr] => (map_items[$i].0 % (1 << map_mod_bits))
        }

        const PREFETCH_AHEAD: usize = 4;
        for i in 0..map_items.len() {
            if i + PREFETCH_AHEAD < map_items.len() {
                prefetch_read_data!(&map[entries![i + PREFETCH_AHEAD] as usize]);
            }

            let mut e = entries![i] as usize;
            loop {
                let empty = map[e].hashes.simd_eq(Simd::splat(0));
                if let Some(empty_pos) = empty.first_set() {
                    map[e].hashes.as_mut_array()[empty_pos] = map_items[i].0;
                    map[e].values[empty_pos] = map_items[i].1;
                    break;
                }
                e += 1;
                e %= 1 << map_mod_bits;
            }
        }

        Ok(Table {
            num_valid_items,
            map_mod_bits,
            map,
            mapped_indices,
        })
    }

    pub fn read_from(mut r: impl Read) -> Result<Self> {
        // read map
        let num_valid_items = read_len(&mut r)?;
        let map_mod_bits = read_len(&mut r)? as u32;
        let mut map = Vec::uninitialized_init(1usize << map_mod_bits);
        r.read_exact(map.as_raw_bytes_mut())?;

        // read mapped indices
        let mapped_indices_len = read_len(&mut r)?;
        let mut mapped_indices = Vec::with_capacity(mapped_indices_len);
        for _ in 0..mapped_indices_len {
            mapped_indices.push(read_len(&mut r)? as u32);
        }

        Ok(Self {
            num_valid_items,
            map_mod_bits,
            map: unchecked!(map),
            mapped_indices: unchecked!(mapped_indices),
        })
    }

    pub fn write_to(self, mut w: impl Write) -> Result<()> {
        // write map
        write_len(self.num_valid_items, &mut w)?;
        write_len(self.map_mod_bits as usize, &mut w)?;
        w.write_all(self.map.as_raw_bytes())?;

        // write mapped indices
        write_len(self.mapped_indices.len(), &mut w)?;
        for &v in self.mapped_indices.as_slice() {
            write_len(v as usize, &mut w)?;
        }
        Ok(())
    }

    pub fn lookup_many(&self, hashes: Vec<u32>) -> Vec<MapValue> {
        let mut hashes = unchecked!(hashes);
        const PREFETCH_AHEAD: usize = 4;

        macro_rules! entries {
            [$i:expr] => (hashes[$i] % (1 << self.map_mod_bits))
        }

        macro_rules! prefetch_at {
            ($i:expr) => {{
                if $i < hashes.len() {
                    prefetch_read_data!(&self.map[entries!($i) as usize]);
                }
            }};
        }

        for i in 0..PREFETCH_AHEAD {
            prefetch_at!(i);
        }

        for i in 0..hashes.len() {
            prefetch_at!(i + PREFETCH_AHEAD);
            let mut e = entries![i] as usize;
            loop {
                let hash_matched = self.map[e].hashes.simd_eq(Simd::splat(hashes[i]));
                let empty = self.map[e].hashes.simd_eq(Simd::splat(0));

                if let Some(pos) = (hash_matched | empty).first_set() {
                    hashes[i] = unsafe {
                        // safety: transmute MapValue(u32) to u32
                        std::mem::transmute(self.map[e].values[pos])
                    };
                    break;
                }
                e += 1;
                e %= 1 << self.map_mod_bits;
            }
        }

        unsafe {
            // safety: transmute Vec<u32> to Vec<MapValue(u32)>
            std::mem::transmute(hashes)
        }
    }
}

pub struct JoinHashMap {
    data_batch: RecordBatch,
    key_columns: Vec<ArrayRef>,
    table: Table,
}

// safety: JoinHashMap is Send + Sync
unsafe impl Send for JoinHashMap {}
unsafe impl Sync for JoinHashMap {}

impl Debug for JoinHashMap {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "JoinHashMap(..)")
    }
}

impl JoinHashMap {
    pub fn create_from_data_batch(
        data_batch: RecordBatch,
        key_exprs: &[PhysicalExprRef],
    ) -> Result<Self> {
        let key_columns: Vec<ArrayRef> = key_exprs
            .iter()
            .map(|expr| {
                Ok(expr
                    .evaluate(&data_batch)?
                    .into_array(data_batch.num_rows())?)
            })
            .collect::<Result<_>>()?;

        let table = Table::create_from_key_columns(data_batch.num_rows(), &key_columns)?;

        Ok(Self {
            data_batch,
            key_columns,
            table,
        })
    }

    pub fn create_from_data_batch_and_hashes(
        data_batch: RecordBatch,
        key_columns: Vec<ArrayRef>,
        hashes: Vec<u32>,
    ) -> Result<Self> {
        let table =
            Table::craete_from_key_columns_and_hashes(data_batch.num_rows(), &key_columns, hashes)?;

        Ok(Self {
            data_batch,
            key_columns,
            table,
        })
    }
    pub fn create_empty(hash_map_schema: SchemaRef, key_exprs: &[PhysicalExprRef]) -> Result<Self> {
        let data_batch = RecordBatch::new_empty(hash_map_schema);
        Self::create_from_data_batch(data_batch, key_exprs)
    }

    pub fn record_batch_contains_hash_map(batch: &RecordBatch) -> bool {
        let table_data_column = batch.column(batch.num_columns() - 1);
        table_data_column.is_valid(0)
    }

    pub fn load_from_hash_map_batch(
        hash_map_batch: RecordBatch,
        key_exprs: &[PhysicalExprRef],
    ) -> Result<Self> {
        let mut data_batch = hash_map_batch.clone();

        let table_data_column = data_batch.remove_column(data_batch.num_columns() - 1);
        let mut table_data = Cursor::new(table_data_column.as_binary::<i32>().value(0));
        let table = Table::read_from(&mut table_data)?;

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

    pub fn into_hash_map_batch(self) -> Result<RecordBatch> {
        let schema = join_hash_map_schema(&self.data_batch.schema());
        if self.data_batch.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(schema));
        }

        let mut table_col_builder = BinaryBuilder::new();
        let mut table_data = vec![];
        self.table.write_to(&mut table_data)?;
        table_col_builder.append_value(&table_data);

        for _ in 1..self.data_batch.num_rows() {
            table_col_builder.append_null();
        }
        let table_col: ArrayRef = Arc::new(table_col_builder.finish());

        Ok(RecordBatch::try_new(
            schema,
            vec![self.data_batch.columns().to_vec(), vec![table_col]].concat(),
        )?)
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

    pub fn is_all_nulls(&self) -> bool {
        self.table.num_valid_items == 0
    }

    pub fn is_empty(&self) -> bool {
        self.data_batch.num_rows() == 0
    }

    pub fn lookup_many(&self, hashes: Vec<u32>) -> Vec<MapValue> {
        self.table.lookup_many(hashes)
    }

    pub fn get_range(&self, map_value: MapValue) -> &[u32] {
        map_value.get_range(self)
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
pub fn join_create_hashes(num_rows: usize, key_columns: &[ArrayRef]) -> Vec<u32> {
    const JOIN_HASH_RANDOM_SEED: u32 = 0x1E39FA04;
    const HASHER: foldhash::fast::FixedState =
        foldhash::fast::FixedState::with_seed(JOIN_HASH_RANDOM_SEED as u64);
    let mut hashes = create_hashes(num_rows, key_columns, JOIN_HASH_RANDOM_SEED, |v, h| {
        let mut hasher = HASHER.build_hasher();
        hasher.write_u32(h);
        hasher.write(v);
        hasher.finish() as u32
    });

    // use 31-bit non-zero hash
    for h in &mut hashes {
        *h |= 0x80000000;
    }
    hashes
}

#[inline]
pub fn join_table_field() -> FieldRef {
    static BHJ_KEY_FIELD: OnceCell<FieldRef> = OnceCell::new();
    BHJ_KEY_FIELD
        .get_or_init(|| Arc::new(Field::new("~TABLE", DataType::Binary, true)))
        .clone()
}
