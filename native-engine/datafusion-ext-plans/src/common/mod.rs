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

use blaze_jni_bridge::conf::{IntConf, BATCH_SIZE};
use once_cell::sync::OnceCell;

pub mod batch_selection;
pub mod batch_statisitcs;
pub mod cached_exprs_evaluator;
pub mod column_pruning;
pub mod ipc_compression;
pub mod output;

// for better cache usage
pub const fn staging_mem_size_for_partial_sort() -> usize {
    4194304 * 8 / 10
}

// use bigger batch memory size writing shuffling data
pub const fn suggested_output_batch_mem_size() -> usize {
    33554432
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
    const CACHED_BATCH_SIZE: OnceCell<i32> = OnceCell::new();
    let batch_size = *CACHED_BATCH_SIZE
        .get_or_try_init(|| BATCH_SIZE.value())
        .expect("error getting configured batch size") as usize;

    let mem_size_est_max = 4096 * batch_size;
    let est_mem_size_per_row = (mem_size + mem_size_est_max) / (num_rows + batch_size);
    let est_sub_batch_size = target_mem_size / est_mem_size_per_row;
    est_sub_batch_size.min(batch_size)
}
