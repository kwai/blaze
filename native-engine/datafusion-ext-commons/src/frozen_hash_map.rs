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

// this mod mainly comes from https://crates.io/crates/frozen-hashbrown
// but modified to support hashbrown's HashMap

use std::{
    alloc::Layout,
    fmt::Debug,
    io::{Read, Write},
    ptr::NonNull,
};

use byteorder::ReadBytesExt;
use datafusion::common::Result;

use crate::df_execution_err;

#[cfg(not(target_pointer_width = "64"))]
compile_error!("Only support 64-bit platforms");

struct Group {}

cfg_if::cfg_if! {
    if #[cfg(all(
        target_feature = "sse2",
        any(target_arch = "x86", target_arch = "x86_64"),
        not(miri)
    ))] {
        impl Group {
            const WIDTH: usize = 16;
        }
    } else if #[cfg(all(target_arch = "aarch64", target_feature = "neon"))] {
        impl Group {
            const WIDTH: usize = 8;
        }
    } else {
        // generic
        impl Group {
            const WIDTH: usize = 8;
        }
    }
}

#[derive(Clone)]
pub struct FrozenHashMap {
    table_layout: TableLayout,
    hashmap: RawHashMap,
    memory: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
struct RawHashMap {
    table: RawTable,
}

#[derive(Debug, Clone, Copy)]
struct RawTable {
    table: RawTableInner,
}

#[derive(Debug, Clone, Copy)]
struct RawTableInner {
    bucket_mask: usize,
    ctrl: NonNull<u8>,
    _growth_left: usize,
    items: usize,
}

#[derive(Debug, Copy, Clone)]
struct TableLayout {
    size: usize,
    ctrl_align: usize,
}

impl TableLayout {
    fn new(layout: Layout) -> Self {
        Self {
            size: layout.size(),
            ctrl_align: if layout.align() > Group::WIDTH {
                layout.align()
            } else {
                Group::WIDTH
            },
        }
    }

    fn calculate_layout_for(&self, buckets: usize) -> Option<(Layout, usize)> {
        assert!(buckets.is_power_of_two());

        let TableLayout { size, ctrl_align } = *self;
        // Manual layout calculation since Layout methods are not yet stable.
        let ctrl_offset =
            size.checked_mul(buckets)?.checked_add(ctrl_align - 1)? & !(ctrl_align - 1);
        let len = ctrl_offset.checked_add(buckets + Group::WIDTH)?;

        Some((
            unsafe { Layout::from_size_align_unchecked(len, ctrl_align) },
            ctrl_offset,
        ))
    }
}

impl RawTableInner {
    fn allocation(&self, table_layout: &TableLayout) -> Option<(*const u8, Layout)> {
        if self.is_empty_singleton() {
            None
        } else {
            let (layout, ctrl_offset) = table_layout.calculate_layout_for(self.buckets())?;
            Some((unsafe { self.ctrl.as_ptr().sub(ctrl_offset) }, layout))
        }
    }

    fn reallocation(&self, table_layout: &TableLayout) -> Option<(usize, Layout)> {
        if self.is_empty_singleton() {
            None
        } else {
            let (layout, ctrl_offset) = table_layout.calculate_layout_for(self.buckets())?;
            Some((ctrl_offset, layout))
        }
    }

    fn buckets(&self) -> usize {
        self.bucket_mask + 1
    }

    fn is_empty_singleton(&self) -> bool {
        self.bucket_mask == 0
    }
}

impl FrozenHashMap {
    pub fn construct<K, V, S>(hashmap: &hashbrown::HashMap<K, V, S>) -> Self
    where
        K: Clone + Copy,
        V: Clone + Copy,
    {
        Self::construct_with(
            unsafe {
                core::slice::from_raw_parts(
                    std::mem::transmute(hashmap as *const _),
                    size_of::<hashbrown::HashMap<K, V>>(),
                )
            },
            TableLayout::new(Layout::new::<(K, V)>()),
        )
    }

    fn construct_with(hashmap: &[u8], table_layout: TableLayout) -> Self {
        assert_eq!(size_of::<RawHashMap>(), hashmap.len());
        let hashmap: RawHashMap = unsafe { std::ptr::read_unaligned(hashmap.as_ptr() as *const _) };
        let memory = if let Some((location, layout)) = hashmap.table.table.allocation(&table_layout)
        {
            let location: &[u8] =
                unsafe { core::slice::from_raw_parts(location as *const u8, layout.size()) };
            location.to_vec()
        } else {
            vec![]
        };
        Self {
            table_layout,
            hashmap,
            memory,
        }
    }

    pub fn stored_bytes_len(&self) -> usize {
        let mut len = 0;
        len += size_of::<TableLayout>();
        len += size_of::<RawHashMap>();
        len += size_of_val(&self.memory.len());
        len += self.memory.len();
        len
    }

    pub fn store(self, mut output: impl Write) -> Result<()> {
        output.write_all(unsafe {
            core::slice::from_raw_parts(
                std::mem::transmute(&self.table_layout as *const _),
                size_of::<TableLayout>(),
            )
        })?;
        output.write_all(unsafe {
            core::slice::from_raw_parts(
                std::mem::transmute(&self.hashmap as *const _),
                size_of::<RawHashMap>(),
            )
        })?;
        output.write_all(&self.memory.len().to_ne_bytes())?;
        output.write_all(&self.memory)?;
        Ok(())
    }

    /// None means failed to load
    pub fn load(mut input: impl Read) -> Result<Self> {
        let chunk = size_of::<TableLayout>();
        let mut raw = vec![0; chunk];
        input.read_exact(&mut raw)?;

        let table_layout: TableLayout =
            unsafe { *std::mem::transmute::<_, &TableLayout>(raw.as_ptr()) };

        let chunk = size_of::<RawHashMap>();
        let mut raw = vec![0; chunk];
        input.read_exact(&mut raw)?;
        let hashmap: RawHashMap = unsafe { *std::mem::transmute::<_, &RawHashMap>(raw.as_ptr()) };

        let ll = [
            input.read_u8()?,
            input.read_u8()?,
            input.read_u8()?,
            input.read_u8()?,
            input.read_u8()?,
            input.read_u8()?,
            input.read_u8()?,
            input.read_u8()?,
        ];
        let length = usize::from_ne_bytes(ll);

        let mut memory = vec![0; length];
        input.read_exact(&mut memory)?;
        let mut loaded = Self {
            table_layout,
            hashmap,
            memory,
        };

        if let Some((offset, layout)) = loaded
            .hashmap
            .table
            .table
            .reallocation(&loaded.table_layout)
        {
            assert_eq!(layout.size(), loaded.memory.len());
            let address = loaded.memory.as_ptr() as usize + offset;
            loaded.hashmap.table.table.ctrl = unsafe { NonNull::new_unchecked(address as *mut u8) };
            Ok(loaded)
        } else {
            df_execution_err!("error loading hashmap")
        }
    }

    pub fn as_map<K, V, S>(&self) -> &hashbrown::HashMap<K, V, S>
    where
        K: Clone + Copy,
        V: Clone + Copy,
    {
        assert_eq!(
            size_of::<RawHashMap>(),
            size_of::<hashbrown::HashMap<K, V>>()
        );
        unsafe { std::mem::transmute::<_, &hashbrown::HashMap<K, V, S>>(&self.hashmap) }
    }

    pub fn len(&self) -> usize {
        self.hashmap.len()
    }
}

impl RawHashMap {
    fn len(&self) -> usize {
        self.table.table.items
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_frozen_hash_map() {
        let mut map: hashbrown::HashMap<i32, i32> = hashbrown::HashMap::new();
        map.insert(1, 2);
        map.insert(3, 4);
        map.insert(5, 6);
        map.insert(7, 8);
        let mut frozen = vec![];
        FrozenHashMap::construct(&map).store(&mut frozen).unwrap();

        let loaded_hashmap = FrozenHashMap::load(&mut Cursor::new(&frozen)).unwrap();
        assert_eq!(loaded_hashmap.len(), 4);

        let map: &hashbrown::HashMap<i32, i32> = loaded_hashmap.as_map();
        assert_eq!(map.len(), 4);
        assert_eq!(map[&1], 2);
        assert_eq!(map[&3], 4);
        assert_eq!(map[&5], 6);
        assert_eq!(map[&7], 8);
    }
}
