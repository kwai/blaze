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

#![feature(get_mut_unchecked)]
#![feature(adt_const_params)]

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
pub mod hash_join_exec;
pub mod ipc_reader_exec;
pub mod ipc_writer_exec;
pub mod limit_exec;
pub mod orc_exec;
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
