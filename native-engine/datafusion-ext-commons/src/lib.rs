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
#![feature(io_error_other)]
#![feature(slice_swap_unchecked)]

pub mod bytes_arena;
pub mod cast;
pub mod hadoop_fs;
pub mod io;
pub mod loser_tree;
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
