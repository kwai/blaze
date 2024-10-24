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
    hash::BuildHasher,
    simd::{cmp::SimdPartialEq, Simd},
};

use datafusion::common::Result;
use datafusion_ext_commons::{
    bytes_arena::{BytesArena, BytesArenaRef},
    prefetch_write_data, unchecked, UncheckedIndexIntoInner,
};
use unchecked_index::UncheckedIndex;

const MAP_VALUE_GROUP_SIZE: usize = 8;

#[derive(Clone, Copy, Default)]
#[repr(align(64))] // ensure one group can be cached into a cache line
struct MapValueGroup {
    hashes: Simd<u32, MAP_VALUE_GROUP_SIZE>,
    values: [u32; MAP_VALUE_GROUP_SIZE],
}
const _MAP_VALUE_GROUP_SIZE_CHECKER: [(); 64] = [(); size_of::<MapValueGroup>()];

struct Table {
    pub map: UncheckedIndex<Vec<MapValueGroup>>,
    pub map_mod_bits: u32,
    pub key_store: BytesArena,
    pub key_addrs: UncheckedIndex<Vec<BytesArenaRef>>,
}

impl Default for Table {
    fn default() -> Self {
        Self {
            map: unchecked!(vec![]),
            map_mod_bits: 0,
            key_store: BytesArena::default(),
            key_addrs: unchecked!(vec![]),
        }
    }
}

impl Table {
    fn len(&self) -> usize {
        self.key_addrs.len()
    }

    fn mem_size(&self) -> usize {
        size_of_val(self)
            + self.map.capacity() * size_of::<MapValueGroup>()
            + self.key_addrs.capacity() * size_of::<BytesArenaRef>()
            + self.key_store.mem_size()
    }

    fn reserve(&mut self, num_new_items: usize) {
        let num_reserved_items = self.len() + num_new_items;
        let new_map_mod_bits = (num_reserved_items.max(128) * 2 / MAP_VALUE_GROUP_SIZE)
            .next_power_of_two()
            .trailing_zeros();

        if new_map_mod_bits > self.map_mod_bits {
            self.rehash(new_map_mod_bits);
        }
    }

    fn upsert_many<K: AsRef<[u8]>>(
        &mut self,
        num_records: usize,
        key: impl Fn(usize) -> K,
        insert_or_update: impl FnMut(usize, bool, u32) -> Result<()>,
    ) -> Result<()> {
        let mut insert_or_update = insert_or_update;
        let hashes = unchecked!((0..num_records)
            .map(|i| agg_hash(key(i)))
            .collect::<Vec<_>>());
        let entries = unchecked!(hashes
            .iter()
            .map(|&hash| hash % (1 << self.map_mod_bits))
            .collect::<Vec<_>>());

        for i in 0..hashes.len() {
            const PREFETCH_AHEAD: usize = 4;
            if i + PREFETCH_AHEAD < hashes.len() {
                prefetch_write_data!(&self.map[entries[i + PREFETCH_AHEAD] as usize]);
            }

            self.upsert_one_impl(
                key(i).as_ref(),
                hashes[i],
                entries[i] as usize,
                |existed, record_idx| insert_or_update(i, existed, record_idx),
            )?;
        }
        Ok(())
    }

    fn upsert_one(
        &mut self,
        key: impl AsRef<[u8]>,
        insert_or_update: impl FnOnce(bool, u32) -> Result<()>,
    ) -> Result<()> {
        let key = key.as_ref();
        let hash = agg_hash(key);
        let entry = (hash % (1 << self.map_mod_bits)) as usize;
        self.upsert_one_impl(key, hash, entry, insert_or_update)
    }

    #[inline]
    fn upsert_one_impl(
        &mut self,
        key: &[u8],
        hash: u32,
        mut entries: usize,
        insert_or_update: impl FnOnce(bool, u32) -> Result<()>,
    ) -> Result<()> {
        loop {
            let hash_matched = self.map[entries].hashes.simd_eq(Simd::splat(hash));
            for i in hash_matched
                .to_array()
                .into_iter()
                .enumerate()
                .filter(|&(_, matched)| matched)
                .map(|(i, _)| i)
            {
                let record_idx = self.map[entries].values[i];
                if self.key_addrs[record_idx as usize].as_ref() == key {
                    return insert_or_update(true, record_idx);
                }
            }

            let empty = self.map[entries].hashes.simd_eq(Simd::splat(0));
            if let Some(empty_pos) = empty.first_set() {
                let record_idx = self.len() as u32;
                self.map[entries].hashes[empty_pos] = hash;
                self.map[entries].values[empty_pos] = record_idx;

                let key_addr = self.key_store.add(key);
                self.key_addrs.push(key_addr);
                return insert_or_update(false, record_idx);
            }
            entries += 1;
            entries %= 1 << self.map_mod_bits;
        }
    }

    #[inline]
    fn rehash(&mut self, map_mod_bits: u32) {
        let mut rehashed_map = unchecked!(vec![MapValueGroup::default(); 1 << map_mod_bits]);
        let zeros = Simd::splat(0);
        let new_mods = Simd::splat(1 << map_mod_bits);

        for group in self.map.drain(..) {
            let new_entries = group.hashes % new_mods;
            for &e in new_entries.as_array().iter().rev() {
                prefetch_write_data!(&rehashed_map[e as usize]);
            }

            for j in 0..MAP_VALUE_GROUP_SIZE {
                if group.hashes[j] != 0 {
                    let mut e = new_entries[j] as usize;
                    loop {
                        if let Some(empty_pos) = rehashed_map[e].hashes.simd_eq(zeros).first_set() {
                            rehashed_map[e].hashes[empty_pos] = group.hashes[j];
                            rehashed_map[e].values[empty_pos] = group.values[j];
                            break;
                        }
                        e += 1;
                        e %= 1 << map_mod_bits;
                    }
                }
            }
        }
        self.map_mod_bits = map_mod_bits;
        self.map = rehashed_map;
    }
}

// map<key: bytes, value: u32> where value is the index of accumulators
#[derive(Default)]
pub struct AggHashMap {
    map: Table,
}

impl AggHashMap {
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn mem_size(&self) -> usize {
        self.map.mem_size()
    }

    pub fn upsert_one_record(
        &mut self,
        key: impl AsRef<[u8]>,
        insert_or_update: impl FnOnce(bool, u32) -> Result<()>,
    ) -> Result<()> {
        self.map.reserve(1);
        self.map.upsert_one(key, insert_or_update)
    }

    pub fn upsert_records<K: AsRef<[u8]>>(
        &mut self,
        num_records: usize,
        key: impl Fn(usize) -> K,
        insert_or_update: impl FnMut(usize, bool, u32) -> Result<()>,
    ) -> Result<()> {
        self.map.reserve(num_records);
        self.map.upsert_many(num_records, key, insert_or_update)
    }

    pub fn take_keys(&mut self) -> (BytesArena, Vec<BytesArenaRef>) {
        let key_store = std::mem::take(&mut self.map.key_store);
        let key_addrs = std::mem::take(&mut *self.map.key_addrs);
        self.map.map.fill(MapValueGroup::default());
        (key_store, key_addrs)
    }

    pub fn into_keys(self) -> (BytesArena, Vec<BytesArenaRef>) {
        let key_store = self.map.key_store;
        let key_addrs = self.map.key_addrs.into_inner();
        (key_store, key_addrs)
    }
}

#[inline]
pub fn agg_hash(value: impl AsRef<[u8]>) -> u32 {
    // 32-bits non-zero hash
    const AGG_HASH_SEED_HASHING: i64 = 0x3F6F1B93;
    const HASHER: foldhash::fast::FixedState =
        foldhash::fast::FixedState::with_seed(AGG_HASH_SEED_HASHING as u64);
    HASHER.hash_one(value.as_ref()) as u32 | 0x80000000
}
