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

#![feature(adt_const_params)]
#![feature(core_intrinsics)]
#![feature(get_mut_unchecked)]
#![feature(portable_simd)]
#![feature(ptr_as_ref_unchecked)]

// execution plan implementations
pub mod agg_exec;
pub mod broadcast_join_build_hash_map_exec;
pub mod broadcast_join_exec;
pub mod debug_exec;
pub mod empty_partitions_exec;
pub mod expand_exec;
pub mod ffi_reader_exec;
pub mod filter_exec;
pub mod generate_exec;
pub mod ipc_reader_exec;
pub mod ipc_writer_exec;
pub mod limit_exec;
pub mod parquet_exec;
pub mod parquet_sink_exec;
pub mod project_exec;
pub mod rename_columns_exec;
pub mod rss_shuffle_writer_exec;
pub mod shuffle_writer_exec;
pub mod sort_exec;
pub mod sort_merge_join_exec;
pub mod window_exec;

// memory management
pub mod memmgr;

// helper modules
pub mod agg;
pub mod common;
pub mod generate;
pub mod joins;
mod shuffle;
pub mod window;

#[macro_export]
macro_rules! unchecked {
    ($e:expr) => {{
        // safety: bypass bounds checking, used in performance critical path
        unsafe { unchecked_index::unchecked_index($e) }
    }};
}

#[macro_export]
macro_rules! prefetch_read_data {
    ($e:expr) => {{
        // safety: use prefetch
        let locality = 3;
        unsafe { std::intrinsics::prefetch_read_data($e, locality) }
    }};
}
