use std::sync::Arc;
use std::sync::Mutex;

use datafusion::datasource::object_store::ObjectStoreRegistry;
use hdfs_object_store::HDFSSingleFileObjectStore;

pub mod hdfs_object_store; // note: can be changed to priv once plan transforming is removed
pub mod jni_bridge;
pub mod shuffle_reader_exec;
pub mod shuffle_writer_exec;
pub mod util;

mod batch_buffer;

lazy_static::lazy_static! {
    static ref OBJECT_STORE_REGISTRY: ObjectStoreRegistry = {
        let osr = ObjectStoreRegistry::default();
        let hdfs_object_store = Arc::new(HDFSSingleFileObjectStore);
        osr.register_store("hdfs".to_owned(), hdfs_object_store.clone());
        osr.register_store("viewfs".to_owned(), hdfs_object_store.clone());
        osr
    };

    static ref JOB_ID: Arc<Mutex<String>> = Arc::new(Mutex::default());
}

pub fn global_object_store_registry() -> &'static ObjectStoreRegistry {
    &OBJECT_STORE_REGISTRY
}

pub fn set_job_id(job_id: &str) {
    let mut global_job_id = JOB_ID.lock().unwrap();
    global_job_id.clear();
    global_job_id.push_str(job_id);
}

pub fn get_job_id() -> String {
    JOB_ID.lock().unwrap().clone()
}
