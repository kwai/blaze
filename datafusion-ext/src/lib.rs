pub mod hdfs_object_store; // note: can be changed to priv once plan transforming is removed
pub mod jni_bridge;
pub mod shuffle_reader_exec;
pub mod shuffle_writer_exec;
pub mod util;

mod batch_buffer;

use datafusion::datasource::object_store::ObjectStoreRegistry;
use hdfs_object_store::HDFSSingleFileObjectStore;
use std::sync::Arc;

pub fn global_object_store_registry() -> &'static ObjectStoreRegistry {
    lazy_static::lazy_static! {
        static ref OBJECT_STORE_REGISTRY: ObjectStoreRegistry = {
            let osr = ObjectStoreRegistry::default();
            osr.register_store("hdfs".to_owned(), Arc::new(HDFSSingleFileObjectStore::new()));
            osr
        };
    }
    &OBJECT_STORE_REGISTRY
}
