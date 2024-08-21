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
const HUGE_LEN: usize = 16384;

pub struct BytesArena {
    huges: Vec<Box<[u8]>>,
    bufs: Vec<Vec<u8>>,
    mem_size: usize,
}

impl Default for BytesArena {
    fn default() -> Self {
        // does not pre-allocate memory for first buf
        Self {
            huges: vec![],
            bufs: vec![Vec::with_capacity(0)],
            mem_size: 0,
        }
    }
}

impl BytesArena {
    pub fn add(&mut self, bytes: &[u8]) -> BytesArenaRef {
        let bytes_len = bytes.len();
        if bytes_len >= HUGE_LEN {
            self.mem_size += bytes_len + size_of::<Box<[u8]>>();
            self.huges.push(bytes.to_vec().into());
            return BytesArenaRef {
                ptr_addr: self.huges.last().unwrap().as_ptr() as usize,
                len: bytes_len as u32,
            };
        }

        // allocate a new buf if cur_buf cannot hold bytes
        if self.cur_buf().len() + bytes_len > self.cur_buf().capacity() {
            self.mem_size += BUF_CAPACITY_TARGET;
            self.bufs.push(Vec::with_capacity(BUF_CAPACITY_TARGET));
        }

        let cur_buf = self.cur_buf_mut();
        let start = cur_buf.len();
        cur_buf.extend_from_slice(bytes);

        BytesArenaRef {
            ptr_addr: cur_buf.as_ptr() as usize + start,
            len: bytes_len as u32,
        }
    }

    pub fn mem_size(&self) -> usize {
        self.mem_size + self.cur_buf().capacity()
    }

    fn cur_buf(&self) -> &Vec<u8> {
        self.bufs.last().unwrap() // has always at least one buf
    }

    fn cur_buf_mut(&mut self) -> &mut Vec<u8> {
        self.bufs.last_mut().unwrap() // has always at least one buf
    }
}

#[derive(Clone, Copy)]
pub struct BytesArenaRef {
    ptr_addr: usize,
    len: u32,
}

impl BytesArenaRef {
    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl AsRef<[u8]> for BytesArenaRef {
    fn as_ref(&self) -> &[u8] {
        unsafe {
            // safety: assume corresponding BytesArena is alive
            std::slice::from_raw_parts(self.ptr_addr as *const u8, self.len as usize)
        }
    }
}
