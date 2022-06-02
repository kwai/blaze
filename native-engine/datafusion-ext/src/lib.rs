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

use datafusion::datasource::object_store_registry::ObjectStoreRegistry;
use once_cell::sync::OnceCell;

use hdfs_object_store::HDFSSingleFileObjectStore;
use std::sync::Arc;

pub mod empty_partitions_exec;
pub mod hdfs_object_store; // note: can be changed to priv once plan transforming is removed
pub mod jni_bridge;
pub mod rename_columns_exec;
pub mod shuffle_reader_exec;
pub mod shuffle_writer_exec;

mod batch_buffer;
mod spark_hash;

pub fn global_object_store_registry() -> &'static ObjectStoreRegistry {
    static OBJECT_STORE_REGISTRY: OnceCell<ObjectStoreRegistry> = OnceCell::new();
    OBJECT_STORE_REGISTRY.get_or_init(|| {
        let osr = ObjectStoreRegistry::default();
        let hdfs_object_store = Arc::new(HDFSSingleFileObjectStore);
        osr.register_store("hdfs".to_owned(), hdfs_object_store.clone());
        osr.register_store("viewfs".to_owned(), hdfs_object_store);
        osr
    })
}

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
