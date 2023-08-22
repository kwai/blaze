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

const BUF_CAPACITY_TARGET: usize = 262144;
const BUF_CAPACITY_ALMOST_FULL: usize = BUF_CAPACITY_TARGET * 4 / 5; // wasting at most 20% space

pub struct BytesArena {
    bufs: Vec<Vec<u8>>,
    bufs_frozen_mem_size: usize,
}

impl Default for BytesArena {
    fn default() -> Self {
        // does not pre-allocate memory for first buf
        Self {
            bufs: vec![Vec::with_capacity(0)],
            bufs_frozen_mem_size: 0,
        }
    }
}

impl BytesArena {
    pub fn add(&mut self, bytes: &[u8]) -> u64 {
        // assume bytes_len < 2^32
        let cur_buf_len = self.cur_buf().len();
        let len = bytes.len();

        // freeze current buf if it's almost full and has no enough space for the given bytes
        if cur_buf_len > BUF_CAPACITY_ALMOST_FULL && cur_buf_len + len > BUF_CAPACITY_TARGET {
            self.freeze_cur_buf();
        }

        let id = self.bufs.len() - 1;
        let offset = self.cur_buf().len();
        self.cur_buf_mut().extend_from_slice(bytes);
        make_arena_addr(id, offset, len)
    }

    pub fn get(&self, addr: u64) -> &[u8] {
        let (id, offset, len) = unapply_arena_addr(addr);
        unsafe {
            // safety - performance critical, assume addr is valid
            self.bufs
                .get_unchecked(id)
                .get_unchecked(offset..offset + len)
        }
    }

    /// specialized for merging two parts in sort-exec
    /// works like an IntoIterator, free memory of visited items
    pub(crate) fn specialized_get_and_drop_last(&mut self, addr: u64) -> &[u8] {
        let (id, offset, len) = unapply_arena_addr(addr);
        if id > 0 && !self.bufs[id - 1].is_empty() {
            self.bufs[id - 1].truncate(0); // drop last buf
            self.bufs[id - 1].shrink_to_fit();
        }
        unsafe {
            // safety - performance critical, assume addr is valid
            self.bufs
                .get_unchecked(id)
                .get_unchecked(offset..offset + len)
        }
    }

    pub fn mem_size(&self) -> usize {
        self.bufs_frozen_mem_size + self.cur_buf().capacity()
    }

    fn cur_buf(&self) -> &Vec<u8> {
        self.bufs.last().unwrap() // has always at least one buf
    }

    fn cur_buf_mut(&mut self) -> &mut Vec<u8> {
        self.bufs.last_mut().unwrap() // has always at least one buf
    }

    fn freeze_cur_buf(&mut self) {
        let frozen_mem_size = self.cur_buf().capacity();
        self.bufs_frozen_mem_size += frozen_mem_size;
        self.bufs.push(Vec::with_capacity(BUF_CAPACITY_TARGET));
    }
}

fn make_arena_addr(id: usize, offset: usize, len: usize) -> u64 {
    (id as u64 * BUF_CAPACITY_TARGET as u64 + offset as u64) << 32 | len as u64
}

fn unapply_arena_addr(addr: u64) -> (usize, usize, usize) {
    let id_offset = addr >> 32;
    let id = id_offset / (BUF_CAPACITY_TARGET as u64);
    let offset = id_offset % (BUF_CAPACITY_TARGET as u64);
    let len = addr << 32 >> 32;
    (id as usize, offset as usize, len as usize)
}
