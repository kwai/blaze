use std::sync::Arc;

use dashmap::DashMap;
use datafusion::datasource::object_store::ObjectStoreRegistry;
use jni::JNIEnv;

use hdfs_object_store::HDFSSingleFileObjectStore;
use jni_bridge::JavaClasses;

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

    static ref JENV_JOB_IDS: Arc<DashMap<usize, String>> = Arc::default();
}

pub fn global_object_store_registry() -> &'static ObjectStoreRegistry {
    &OBJECT_STORE_REGISTRY
}

pub fn set_job_id(job_id: &str) {
    let env_addr = unsafe {
        // safety: transmute to raw pointer addr, only used as jobid map key
        std::mem::transmute::<JNIEnv, usize>(JavaClasses::get_thread_jnienv())
    };
    JENV_JOB_IDS.insert(env_addr, job_id.to_owned());
}

pub fn get_job_id() -> String {
    let env_addr = unsafe {
        // safety: transmute to raw pointer addr, only used as jobid map key
        std::mem::transmute::<JNIEnv, usize>(JavaClasses::get_thread_jnienv())
    };
    JENV_JOB_IDS
        .get(&env_addr)
        .expect("job id not set in current thread")
        .clone()
}
