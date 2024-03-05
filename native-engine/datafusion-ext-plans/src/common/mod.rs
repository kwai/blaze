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

use datafusion_ext_commons::batch_size;

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
    let est_mem_size_per_row = mem_size / num_rows;
    let est_sub_batch_size = target_mem_size / est_mem_size_per_row;
    est_sub_batch_size.min(batch_size).max(batch_size_min)
}
