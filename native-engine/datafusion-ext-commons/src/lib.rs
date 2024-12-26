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

#![allow(internal_features)]
#![feature(core_intrinsics)]
#![feature(new_uninit)]
#![feature(slice_swap_unchecked)]
#![feature(vec_into_raw_parts)]

use blaze_jni_bridge::conf::{IntConf, BATCH_SIZE};
use once_cell::sync::OnceCell;
use unchecked_index::UncheckedIndex;

pub mod algorithm;
pub mod arrow;
pub mod hadoop_fs;
pub mod hash;
pub mod io;
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

// bigger for better radix sort performance
pub const fn staging_mem_size_for_partial_sort() -> usize {
    1048576
}

// bigger for better radix sort performance
// aggregate merging is row-based, so use bigger memory size
pub const fn staging_mem_size_for_agg_merge() -> usize {
    16777216
}

// use bigger batch memory size writing shuffling data
pub const fn suggested_output_batch_mem_size() -> usize {
    25165824
}

// use smaller batch memory size for kway merging since there will be multiple
// batches in memory at the same time
pub const fn suggested_kway_merge_batch_mem_size() -> usize {
    1048576
}

pub fn compute_suggested_batch_size_for_output(mem_size: usize, num_rows: usize) -> usize {
    let suggested_batch_mem_size = suggested_output_batch_mem_size();
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
    ($e:expr) => {{
        std::intrinsics::likely($e)
    }};
}

#[macro_export]
macro_rules! unlikely {
    ($e:expr) => {{
        std::intrinsics::unlikely($e)
    }};
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
