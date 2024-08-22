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
    hash::{BuildHasher, Hasher},
    io::Cursor,
    mem::MaybeUninit,
    num::NonZero,
    sync::Arc,
};

use arrow::{
    array::{Array, ArrayRef, AsArray, BinaryBuilder, RecordBatch},
    buffer::NullBuffer,
    datatypes::{DataType, Field, FieldRef, Schema, SchemaRef},
};
use datafusion::{common::Result, physical_expr::PhysicalExprRef};
use datafusion_ext_commons::{
    frozen_hash_map::FrozenHashMap,
    io::{read_len, write_len},
    rdxsort::RadixSortIterExt,
    spark_hash::create_hashes,
};
use hashbrown::HashMap;
use itertools::Itertools;
use once_cell::sync::OnceCell;

use crate::unchecked;

enum MapType {
    Unfrozen(HashMap<u32, Bucket, SimpleHashBuilder>),
    Frozen(FrozenHashMap),
}

impl MapType {
    fn get_map(&self) -> &HashMap<u32, Bucket, SimpleHashBuilder> {
        match self {
            MapType::Unfrozen(map) => map,
            MapType::Frozen(map) => map.as_map(),
        }
    }
}

macro_rules! non_zero {
    ($e:expr) => {{
        unsafe { NonZero::new_unchecked($e) }
    }};
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Bucket {
    Single(u32),
    Range(u32, NonZero<u32>),
}

impl Bucket {
    pub const fn empty() -> Self {
        Bucket::Range(0, non_zero!(1))
    }

    pub fn is_empty(&self) -> bool {
        *self == Self::empty()
    }
}

// SimpleHasher/HashBuilder is specified for hashing a u32 hash codes
#[derive(Clone, Copy, Default)]
struct SimpleHashBuilder;

#[derive(Clone, Copy, Default)]
struct SimpleHasher(u32);

impl BuildHasher for SimpleHashBuilder {
    type Hasher = SimpleHasher;

    fn build_hasher(&self) -> SimpleHasher {
        SimpleHasher::default()
    }
}

impl Hasher for SimpleHasher {
    fn finish(&self) -> u64 {
        self.0 as u64 | (self.0 as u64).reverse_bits()
    }

    fn write(&mut self, _bytes: &[u8]) {
        unimplemented!()
    }

    fn write_u32(&mut self, i: u32) {
        self.0 = i
    }
}

struct Table {
    map_container: Box<MapType>,

    // bypass lifetime checking
    map: MaybeUninit<&'static HashMap<u32, Bucket, SimpleHashBuilder>>,
    mapped_indices: Vec<u32>,
}

impl Table {
    fn create_from_key_columns(num_rows: usize, key_columns: &[ArrayRef]) -> Result<Self> {
        assert!(
            num_rows < 1073741824,
            "join hash table: number of rows exceeded 2^30: {num_rows}"
        );

        let key_valids = key_columns
            .iter()
            .map(|col| col.logical_nulls())
            .reduce(|nb1, nb2| NullBuffer::union(nb1.as_ref(), nb2.as_ref()))
            .flatten();
        let key_is_valid = |row_idx| match &key_valids {
            Some(nb) => nb.is_valid(row_idx),
            None => true,
        };

        let mut map = HashMap::with_capacity_and_hasher(num_rows, SimpleHashBuilder);
        let mut mapped_indices = Vec::with_capacity(num_rows);

        for (hash, chunk) in join_create_hashes(num_rows, key_columns)
            .into_iter()
            .enumerate()
            .filter(|(idx, _)| key_is_valid(*idx))
            .map(|(idx, hash)| (idx as u32, hash))
            .radix_sorted_unstable_by_key(|&(_idx, hash)| hash)
            .chunk_by(|(_, hash)| *hash)
            .into_iter()
        {
            let start = mapped_indices.len() as u32;
            mapped_indices.extend(chunk.map(|(idx, _hash)| idx));

            let len = mapped_indices.len() as u32 - start;
            map.insert_unique_unchecked(
                hash,
                match len {
                    0 => unreachable!(),
                    1 => {
                        let single = mapped_indices.pop().unwrap();
                        Bucket::Single(single)
                    }
                    len => Bucket::Range(start, non_zero!(len)),
                },
            );
        }

        if map.len() < num_rows / 2 {
            map.shrink_to_fit();
        }

        let mut new = Table {
            map_container: Box::new(MapType::Unfrozen(map)),
            map: MaybeUninit::uninit(),
            mapped_indices,
        };
        new.map = MaybeUninit::new(unsafe {
            // safety: bypass lifetime checking
            std::mem::transmute(new.map_container.get_map())
        });
        Ok(new)
    }

    pub fn load_from_raw_bytes(raw_bytes: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(raw_bytes);

        // read frozen map
        let frozen = FrozenHashMap::load(&mut cursor)?;

        // read mapped indices
        let mut mapped_indices = vec![0; read_len(&mut cursor)?];
        for i in &mut mapped_indices {
            *i = read_len(&mut cursor)? as u32;
        }

        let mut new = Table {
            map_container: Box::new(MapType::Frozen(frozen)),
            map: MaybeUninit::uninit(),
            mapped_indices,
        };
        new.map = MaybeUninit::new(unsafe {
            // safety: bypass lifetime checking
            std::mem::transmute(new.map_container.get_map())
        });
        Ok(new)
    }

    pub fn try_into_raw_bytes(self) -> Result<Vec<u8>> {
        let frozen = FrozenHashMap::construct(&self.map());
        let mut raw_bytes =
            Vec::with_capacity((self.mapped_indices.len() + 1) * 8 + frozen.stored_bytes_len());

        // write frozen map
        frozen.store(&mut raw_bytes)?;

        // write mapped indices
        write_len(self.mapped_indices.len(), &mut raw_bytes)?;
        for i in self.mapped_indices {
            write_len(i as usize, &mut raw_bytes)?;
        }

        raw_bytes.shrink_to_fit();
        Ok(raw_bytes)
    }

    pub fn lookup(&self, hash: u32) -> Bucket {
        self.map().get(&hash).cloned().unwrap_or(Bucket::empty())
    }

    fn map(&self) -> &HashMap<u32, Bucket, SimpleHashBuilder> {
        unsafe {
            // safety: self.map is always initialized after created
            self.map.assume_init()
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

    pub fn create_empty(hash_map_schema: SchemaRef, key_exprs: &[PhysicalExprRef]) -> Result<Self> {
        let data_batch = RecordBatch::new_empty(hash_map_schema);
        Self::create_from_data_batch(data_batch, key_exprs)
    }

    pub fn load_from_hash_map_batch(
        hash_map_batch: RecordBatch,
        key_exprs: &[PhysicalExprRef],
    ) -> Result<Self> {
        let mut data_batch = hash_map_batch.clone();
        let table = Table::load_from_raw_bytes(
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
        self.table.map().is_empty()
    }

    pub fn is_empty(&self) -> bool {
        self.data_batch.num_rows() == 0
    }

    pub fn lookup(&self, hash: u32) -> Bucket {
        self.table.lookup(hash)
    }

    pub const fn empty_bucket() -> Bucket {
        Bucket::empty()
    }

    pub fn get_range(&self, start: u32, len: u32) -> &[u32] {
        let mapped_indices = unchecked!(&self.table.mapped_indices);
        &mapped_indices[start as usize..(start + len) as usize]
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
    create_hashes(num_rows, key_columns, JOIN_HASH_RANDOM_SEED, |v, h| {
        gxhash::gxhash32(v, h as i64)
    })
}

#[inline]
fn join_table_field() -> FieldRef {
    static BHJ_KEY_FIELD: OnceCell<FieldRef> = OnceCell::new();
    BHJ_KEY_FIELD
        .get_or_init(|| Arc::new(Field::new("~TABLE", DataType::Binary, true)))
        .clone()
}
