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

pub mod batch_selection;
pub mod cached_exprs_evaluator;
pub mod column_pruning;
pub mod execution_context;
pub mod ipc_compression;
pub mod make_eq_comparator;
pub mod timer_helper;

pub trait SliceAsRawBytes {
    fn as_raw_bytes<'a>(&self) -> &'a [u8];
    fn as_raw_bytes_mut<'a>(&mut self) -> &'a mut [u8];
}

impl<T: Sized + Copy> SliceAsRawBytes for [T] {
    fn as_raw_bytes<'a>(&self) -> &'a [u8] {
        let bytes_ptr = self.as_ptr() as *const u8;
        unsafe {
            // safety: access raw bytes
            std::slice::from_raw_parts(bytes_ptr, size_of::<T>() * self.len())
        }
    }

    fn as_raw_bytes_mut<'a>(&mut self) -> &'a mut [u8] {
        let bytes_ptr = self.as_mut_ptr() as *mut u8;
        unsafe {
            // safety: access raw bytes
            std::slice::from_raw_parts_mut(bytes_ptr, size_of::<T>() * self.len())
        }
    }
}
