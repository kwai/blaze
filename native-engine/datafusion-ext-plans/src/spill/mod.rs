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

mod onheap_spill;

use std::fmt::{Debug, Formatter};
use bytesize::ByteSize;
use datafusion::common::Result;
use datafusion::execution::DiskManager;
use std::io::{BufReader, Cursor, Read, Seek, SeekFrom, Write};
use tempfile::NamedTempFile;

pub use crate::spill::onheap_spill::OnHeapSpill;

/// 3-level spill structure
///  L1: data is serialized and compressed into bytes, stored in off-heap memory.
///  L2: L1 data is moved into on-heap through o.a.s.blaze.memory.HeapSpillManager.
///  L3: L1 data is failed moving to L2, so spill to DiskManager.
pub enum Spill {
    // L1 with compressed data + pos
    L1(Vec<u8>),

    // L2 with on-heap spill
    L2(OnHeapSpill, usize),

    // L3 with temp file
    L3(NamedTempFile, usize),
}

impl Debug for Spill {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Spill::L1(data) =>
                write!(f, "L1-spill (size={})", ByteSize(data.len() as u64)),
            Spill::L2(_, len) =>
                write!(f, "L2-spill (size={})", ByteSize(*len as u64)),
            Spill::L3(_, len) =>
                write!(f, "L3-spill (size={})", ByteSize(*len as u64)),
        }
    }
}

impl Spill {
    pub fn new_l1(mut data: Vec<u8>) -> Self {
        data.shrink_to_fit();
        Spill::L1(data)
    }

    pub fn try_new_l2(data: &[u8]) -> Result<Self> {
        let len = data.len();
        let on_heap_spill = OnHeapSpill::try_new(data)?;
        Ok(Self::L2(on_heap_spill, len))
    }

    pub fn new_l2_from(on_heap_spill: OnHeapSpill) -> Self {
        let len = on_heap_spill.size();
        Self::L2(on_heap_spill, len)
    }

    pub fn try_new_l3(data: &[u8], disk_manager: &DiskManager) -> Result<Self> {
        let len = data.len();
        let mut tmp_file = disk_manager.create_tmp_file("blaze L3 spill")?;
        tmp_file.write_all(data)?;
        tmp_file.rewind()?;
        Ok(Self::L3(tmp_file, len))
    }

    pub fn try_new_l3_from(mut tmp_file: NamedTempFile) -> Result<Self> {
        tmp_file.seek(SeekFrom::End(0))?;
        let len = tmp_file.stream_position()? as usize;
        tmp_file.rewind()?;
        Ok(Self::L3(tmp_file, len))
    }

    pub fn level(&self) -> u8 {
        match self {
            Spill::L1(..) => 1,
            Spill::L2(..) => 2,
            Spill::L3(..) => 3,
        }
    }

    pub fn offheap_mem_size(&self) -> usize {
        if let Spill::L1(data) = self {
            return data.len();
        }
        0
    }

    pub fn onheap_mem_size(&self) -> usize {
        if let Spill::L2(_, len) = self {
            return *len;
        }
        0
    }

    pub fn disk_size(&self) -> usize {
        if let Spill::L3(_, len) = self {
            return *len;
        }
        0
    }

    pub fn into_reader(self) -> Box<dyn Read + Send> {
        match self {
            Spill::L1(data) => Box::new(Cursor::new(data)),
            Spill::L2(heap_spill, _) => Box::new(heap_spill),
            Spill::L3(tmp_file, _) => Box::new(tmp_file),
        }
    }

    pub fn into_buf_reader(self) -> BufReader<Box<dyn Read + Send>> {
        match self {
            Spill::L1(data) => BufReader::new(Box::new(Cursor::new(data))),
            Spill::L2(heap_spill, _) => {
                const BUFREAD_CAPACITY: usize = 65536;
                BufReader::with_capacity(BUFREAD_CAPACITY, Box::new(heap_spill))
            }
            Spill::L3(tmp_file, _) => {
                const BUFREAD_CAPACITY: usize = 65536;
                BufReader::with_capacity(BUFREAD_CAPACITY, Box::new(tmp_file))
            }
        }
    }

    pub fn to_l2(&self) -> Result<Self> {
        if let Spill::L1(data) = self {
            return Self::try_new_l2(data);
        }
        unimplemented!("only supports converting L1 to L2")
    }

    pub fn to_l3(&self, disk_manager: &DiskManager) -> Result<Self> {
        if let Spill::L1(data) = self {
            return Self::try_new_l3(data, disk_manager);
        }
        unimplemented!("only supports converting L1 to L3")
    }
}

pub fn dump_spills_statistics<'a>(spills: impl IntoIterator<Item = &'a Spill>) -> String {
    let mut l1_count = 0;
    let mut l2_count = 0;
    let mut l3_count = 0;
    let mut l1_size = 0;
    let mut l2_size = 0;
    let mut l3_size = 0;

    for spill in spills {
        let _ = match spill.level() {
            1 => (l1_count += 1, l1_size += spill.offheap_mem_size()),
            2 => (l2_count += 1, l2_size += spill.onheap_mem_size()),
            _ => (l3_count += 1, l3_size += spill.disk_size()),
        };
    }
    format!(
        "L1/L2/L3={}/{}/{}, off-heap={}, on-heap={}, disk={}",
        l1_count,
        l2_count,
        l3_count,
        ByteSize(l1_size as u64),
        ByteSize(l2_size as u64),
        ByteSize(l3_size as u64),
    )
}
