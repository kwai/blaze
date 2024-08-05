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

use std::{iter::Filter, vec::IntoIter};

use datafusion_ext_commons::bytes_arena::{BytesArena, BytesArenaAddr};

use crate::unchecked;

#[derive(Default, Clone, Copy)]
pub struct AggHashMapItem {
    pub key_addr: BytesArenaAddr,
    pub non_zero_hash: u32,
    pub acc_idx: u32,
}

pub enum AggHashMapLookupEntry<'a> {
    Occupied(&'a mut AggHashMapItem),
    Vaccant(&'a mut AggHashMapItem),
}

#[derive(Default)]
pub struct AggHashMap {
    items: Vec<AggHashMapItem>,
}

impl AggHashMap {
    pub fn lookup<'a>(
        &'a mut self,
        bytes: &BytesArena,
        non_zero_hash: u32,
        key: impl AsRef<[u8]>,
    ) -> AggHashMapLookupEntry<'a> {
        let mut bucket = (non_zero_hash as usize) % self.items.len();
        let mut items = unchecked!(&mut self.items);

        loop {
            let item_hash = items[bucket].non_zero_hash;
            if item_hash == 0 {
                return AggHashMapLookupEntry::Vaccant(unsafe {
                    // safety: items[bucket] has lifetime 'a
                    std::mem::transmute(&mut items[bucket])
                });
            }
            if item_hash == non_zero_hash && bytes.get(items[bucket].key_addr) == key.as_ref() {
                return AggHashMapLookupEntry::Occupied(unsafe {
                    // safety: items[bucket] has lifetime 'a
                    std::mem::transmute(&mut items[bucket])
                });
            }
            bucket += 1;
            bucket %= items.len();
        }
    }

    pub fn mem_size(&self) -> usize {
        self.items.capacity() * size_of::<AggHashMapItem>()
    }

    pub fn ensure_capacity(&mut self, len: usize) {
        if self.items.len() > len * 2 + 16 {
            return;
        }
        let new_capacity = len * 4 + 16;
        let old_self = std::mem::replace(
            self,
            Self {
                items: vec![AggHashMapItem::default(); new_capacity],
            },
        );
        let mut new_items = unchecked!(&mut self.items);

        for item in old_self {
            let mut bucket = (item.non_zero_hash as usize) % new_items.len();
            while new_items[bucket].non_zero_hash != 0 {
                bucket += 1;
                bucket %= new_items.len();
            }
            new_items[bucket] = item;
        }
    }
}

impl IntoIterator for AggHashMap {
    type Item = AggHashMapItem;
    type IntoIter = Filter<IntoIter<AggHashMapItem>, fn(&AggHashMapItem) -> bool>;

    fn into_iter(self) -> Self::IntoIter {
        self.items
            .into_iter()
            .filter(|item| item.non_zero_hash != 0)
    }
}
