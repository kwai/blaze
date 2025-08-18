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

#![allow(internal_features)]
#![feature(core_intrinsics)]
#![feature(slice_swap_unchecked)]
#![feature(vec_into_raw_parts)]

use auron_jni_bridge::conf::{
    BATCH_SIZE, IntConf, SUGGESTED_BATCH_MEM_SIZE, SUGGESTED_BATCH_MEM_SIZE_KWAY_MERGE,
};
use once_cell::sync::OnceCell;
use smallvec::SmallVec;
use unchecked_index::UncheckedIndex;

pub mod algorithm;
pub mod arrow;
pub mod hadoop_fs;
pub mod hash;
pub mod io;
pub mod scalar_value;
pub mod spark_bit_array;
pub mod spark_bloom_filter;
pub mod spark_hash;
pub mod uda;

#[macro_export]
macro_rules! df_execution_err {
    ($($arg:tt)*) => {
        Err(datafusion::common::DataFusionError::Execution(format!($($arg)*)))
    }
}
#[macro_export]
macro_rules! df_unimplemented_err {
    ($($arg:tt)*) => {
        Err(datafusion::common::DataFusionError::NotImplemented(format!($($arg)*)))
    }
}
#[macro_export]
macro_rules! df_external_err {
    ($($arg:tt)*) => {
        Err(datafusion::common::DataFusionError::External(format!($($arg)*)))
    }
}

#[macro_export]
macro_rules! downcast_any {
    ($value:expr,mut $ty:ty) => {{
        match $value.as_any_mut().downcast_mut::<$ty>() {
            Some(v) => Ok(v),
            None => $crate::df_execution_err!("error downcasting to {}", stringify!($ty)),
        }
    }};
    ($value:expr, $ty:ty) => {{
        match $value.as_any().downcast_ref::<$ty>() {
            Some(v) => Ok(v),
            None => $crate::df_execution_err!("error downcasting to {}", stringify!($ty)),
        }
    }};
}

pub fn batch_size() -> usize {
    const CACHED_BATCH_SIZE: OnceCell<usize> = OnceCell::new();
    *CACHED_BATCH_SIZE.get_or_init(|| BATCH_SIZE.value().unwrap_or(10000) as usize)
}

pub fn suggested_batch_mem_size() -> usize {
    static V: OnceCell<usize> = OnceCell::new();
    *V.get_or_init(|| SUGGESTED_BATCH_MEM_SIZE.value().unwrap_or(8388608) as usize)
}

pub fn suggested_kway_merge_batch_mem_size() -> usize {
    static V: OnceCell<usize> = OnceCell::new();
    *V.get_or_init(|| {
        SUGGESTED_BATCH_MEM_SIZE_KWAY_MERGE
            .value()
            .unwrap_or(1048576) as usize
    })
}

pub fn compute_suggested_batch_size_for_output(mem_size: usize, num_rows: usize) -> usize {
    let suggested_batch_mem_size = suggested_batch_mem_size();
    compute_batch_size_with_target_mem_size(mem_size, num_rows, suggested_batch_mem_size)
}

pub fn compute_suggested_batch_size_for_kway_merge(mem_size: usize, num_rows: usize) -> usize {
    let suggested_batch_mem_size = suggested_kway_merge_batch_mem_size();
    compute_batch_size_with_target_mem_size(mem_size, num_rows, suggested_batch_mem_size)
}

fn compute_batch_size_with_target_mem_size(
    mem_size: usize,
    num_rows: usize,
    target_mem_size: usize,
) -> usize {
    let batch_size = batch_size();
    let batch_size_min = 20;
    if num_rows == 0 {
        return batch_size;
    }
    let est_mem_size_per_row = mem_size.max(16) / num_rows.max(1);
    let est_sub_batch_size = target_mem_size / est_mem_size_per_row.max(16);
    est_sub_batch_size.min(batch_size).max(batch_size_min)
}

#[macro_export]
macro_rules! unchecked {
    ($e:expr) => {{
        // safety: bypass bounds checking, used in performance critical path
        #[allow(unused_unsafe)]
        unsafe {
            unchecked_index::unchecked_index($e)
        }
    }};
}

#[macro_export]
macro_rules! assume {
    ($e:expr) => {{
        // safety: use assume
        #[allow(unused_unsafe)]
        unsafe {
            std::intrinsics::assume($e)
        }
    }};
}

#[macro_export]
macro_rules! prefetch_read_data {
    ($e:expr) => {{
        // safety: use prefetch
        let locality = 3;
        #[allow(unused_unsafe)]
        unsafe {
            std::intrinsics::prefetch_read_data($e, locality)
        }
    }};
}
#[macro_export]
macro_rules! prefetch_write_data {
    ($e:expr) => {{
        // safety: use prefetch
        let locality = 3;
        #[allow(unused_unsafe)]
        unsafe {
            std::intrinsics::prefetch_write_data($e, locality)
        }
    }};
}

#[macro_export]
macro_rules! likely {
    ($e:expr) => {{ std::intrinsics::likely($e) }};
}

#[macro_export]
macro_rules! unlikely {
    ($e:expr) => {{ std::intrinsics::unlikely($e) }};
}

pub trait UncheckedIndexIntoInner<T> {
    fn into_inner(self) -> T;
}

impl<T: Sized> UncheckedIndexIntoInner<T> for UncheckedIndex<T> {
    fn into_inner(self) -> T {
        let no_drop = std::mem::ManuallyDrop::new(self);
        unsafe { std::ptr::read(&**no_drop) }
    }
}

pub trait UninitializedInit<T> {
    fn uninitialized_init(len: usize) -> T;
}

impl<T: Copy> UninitializedInit<Vec<T>> for Vec<T> {
    fn uninitialized_init(len: usize) -> Vec<T> {
        let mut v = Vec::with_capacity(len);
        unsafe { v.set_len(len) };
        v
    }
}

impl<T: Copy, const N: usize> UninitializedInit<SmallVec<T, N>> for SmallVec<T, N> {
    fn uninitialized_init(len: usize) -> SmallVec<T, N> {
        let mut v = SmallVec::with_capacity(len);
        unsafe { v.set_len(len) };
        v
    }
}

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
