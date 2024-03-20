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
    pub fn add(&mut self, bytes: &[u8]) -> BytesArenaAddr {
        // assume bytes_len < 2^32
        let cur_buf_len = self.cur_buf().len();
        let len = bytes.len();

        // freeze current buf if it's almost full and has no enough space for the given
        // bytes
        if cur_buf_len > BUF_CAPACITY_ALMOST_FULL && cur_buf_len + len > BUF_CAPACITY_TARGET {
            self.freeze_cur_buf();
        }

        let id = self.bufs.len() - 1;
        let offset = self.cur_buf().len();
        self.cur_buf_mut().extend_from_slice(bytes);
        BytesArenaAddr::new(id, offset, len)
    }

    pub fn get(&self, addr: BytesArenaAddr) -> &[u8] {
        let unpacked = addr.unpack();
        unsafe {
            // safety - performance critical, assume addr is valid
            self.bufs
                .get_unchecked(unpacked.id)
                .get_unchecked(unpacked.offset..unpacked.offset + unpacked.len)
        }
    }

    pub fn clear(&mut self) {
        *self = Self::default();
    }

    /// specialized for merging two parts in sort-exec
    /// works like an IntoIterator, free memory of visited items
    pub fn specialized_get_and_drop_last(&mut self, addr: BytesArenaAddr) -> &[u8] {
        let unpacked = addr.unpack();
        if unpacked.id > 0 && !self.bufs[unpacked.id - 1].is_empty() {
            self.bufs[unpacked.id - 1].truncate(0); // drop last buf
            self.bufs[unpacked.id - 1].shrink_to_fit();
        }
        unsafe {
            // safety - performance critical, assume addr is valid
            self.bufs
                .get_unchecked(unpacked.id)
                .get_unchecked(unpacked.offset..unpacked.offset + unpacked.len)
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

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct BytesArenaAddr(u64);

impl BytesArenaAddr {
    pub fn new(id: usize, offset: usize, len: usize) -> Self {
        Self((id as u64 * BUF_CAPACITY_TARGET as u64 + offset as u64) << 32 | len as u64)
    }

    pub fn unpack(self) -> UnpackedBytesArenaAddr {
        let id_offset = self.0 >> 32;
        let id = (id_offset / (BUF_CAPACITY_TARGET as u64)) as usize;
        let offset = (id_offset % (BUF_CAPACITY_TARGET as u64)) as usize;
        let len = (self.0 << 32 >> 32) as usize;

        UnpackedBytesArenaAddr { id, offset, len }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct UnpackedBytesArenaAddr {
    pub id: usize,
    pub offset: usize,
    pub len: usize,
}
