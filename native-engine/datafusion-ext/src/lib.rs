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

use std::sync::Arc;

use datafusion::common::DataFusionError;
use datafusion::logical_expr::ScalarFunctionImplementation;

pub mod debug_exec;
pub mod empty_partitions_exec;
pub mod expr;
pub mod ffi_reader_exec;
pub mod file_format;
pub mod hdfs_object_store;
pub mod ipc_reader_exec;
pub mod ipc_writer_exec;
pub mod jni_bridge;
pub mod limit_exec;
pub mod rename_columns_exec;
pub mod shuffle_writer_exec;
pub mod sort_merge_join_exec;
pub mod spark_fallback_to_jvm_expr;

mod spark_ext_function;
mod spark_hash;
mod util;

pub trait ResultExt<T> {
    fn unwrap_or_fatal(self) -> T;
    fn to_io_result(self) -> std::io::Result<T>;
}
impl<T, E> ResultExt<T> for Result<T, E>
where
    E: std::error::Error,
{
    fn unwrap_or_fatal(self) -> T {
        match self {
            Ok(value) => value,
            Err(err) => {
                if jni_exception_check!().unwrap_or(false) {
                    let _ = jni_exception_describe!();
                    let _ = jni_exception_clear!();
                }
                let errmsg = format!("uncaught error in native code: {:?}", err);

                std::panic::catch_unwind(move || jni_fatal_error!(errmsg))
                    .unwrap_or_else(|_| std::process::abort())
            }
        }
    }

    fn to_io_result(self) -> std::io::Result<T> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("{:?}", err),
            )),
        }
    }
}

pub fn create_spark_ext_function(
    name: &str,
) -> datafusion::error::Result<ScalarFunctionImplementation> {
    Ok(match name {
        "UnscaledValue" => Arc::new(spark_ext_function::spark_unscaled_value),
        "MakeDecimal" => Arc::new(spark_ext_function::spark_make_decimal),
        _ => Err(DataFusionError::NotImplemented(format!(
            "spark ext function not implemented: {}",
            name
        )))?,
    })
}
