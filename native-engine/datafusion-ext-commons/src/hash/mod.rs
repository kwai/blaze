// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod mur;
pub mod xxhash;

fn read32(data: &[u8], offset: usize) -> u32 {
    let v = unsafe {
        // safety: boundary check is done by caller
        std::ptr::read_unaligned(data.as_ptr().add(offset) as *const u32)
    };
    if cfg!(target_endian = "big") {
        return v.swap_bytes();
    }
    v
}

fn read64(data: &[u8], offset: usize) -> u64 {
    let v = unsafe {
        // safety: boundary check is done by caller
        std::ptr::read_unaligned(data.as_ptr().add(offset) as *const u64)
    };
    if cfg!(target_endian = "big") {
        return v.swap_bytes();
    }
    v
}
