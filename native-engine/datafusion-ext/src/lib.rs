use std::sync::Arc;

use dashmap::DashMap;
use datafusion::datasource::object_store_registry::ObjectStoreRegistry;

use hdfs_object_store::HDFSSingleFileObjectStore;

pub mod hdfs_object_store; // note: can be changed to priv once plan transforming is removed
pub mod jni_bridge;
pub mod shuffle_reader_exec;
pub mod shuffle_writer_exec;
pub mod util;

mod batch_buffer;
mod spark_hash;

lazy_static::lazy_static! {
    static ref OBJECT_STORE_REGISTRY: ObjectStoreRegistry = {
        let osr = ObjectStoreRegistry::default();
        let hdfs_object_store = Arc::new(HDFSSingleFileObjectStore);
        osr.register_store("hdfs".to_owned(), hdfs_object_store.clone());
        osr.register_store("viewfs".to_owned(), hdfs_object_store);
        osr
    };

    static ref JENV_JOB_IDS: Arc<DashMap<usize, String>> = Arc::default();
}

pub fn global_object_store_registry() -> &'static ObjectStoreRegistry {
    &OBJECT_STORE_REGISTRY
}
