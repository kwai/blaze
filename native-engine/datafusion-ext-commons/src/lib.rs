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

#![feature(new_uninit)]
#![feature(slice_swap_unchecked)]
#![feature(vec_into_raw_parts)]

use blaze_jni_bridge::{
    conf::{IntConf, BATCH_SIZE},
    is_jni_bridge_inited,
};
use once_cell::sync::OnceCell;

pub mod array_size;
pub mod bytes_arena;
pub mod cast;
pub mod ds;
pub mod ffi_helper;
pub mod hadoop_fs;
pub mod io;
pub mod rdxsort;
pub mod slim_bytes;
pub mod spark_hash;
pub mod streams;
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
    const CACHED_BATCH_SIZE: OnceCell<i32> = OnceCell::new();
    let batch_size = *CACHED_BATCH_SIZE
        .get_or_try_init(|| {
            if is_jni_bridge_inited() {
                BATCH_SIZE.value()
            } else {
                Ok(10000) // for testing
            }
        })
        .expect("error getting configured batch size") as usize;
    batch_size
}

// bigger for better radix sort performance
pub const fn staging_mem_size_for_partial_sort() -> usize {
    8388608
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
