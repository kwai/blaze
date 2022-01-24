use datafusion::error::DataFusionError;
use jni::JavaVM;
use jni::JNIEnv;

pub struct Util;
impl Util {
    pub fn wrap_default_data_fusion_io_error(err: impl std::fmt::Debug) -> DataFusionError {
        DataFusionError::IoError(std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", err)))
    }

    pub fn jvm_clone(jvm: &JavaVM) -> JavaVM {
        unsafe {
            // safety - JavaVM instance is global unique in blaze-rs
            JavaVM::from_raw(jvm.get_java_vm_pointer()).unwrap()
        }
    }

    pub fn jni_env_clone(env: &JNIEnv) -> JNIEnv<'static> {
        unsafe {
            // safety - JNIEnv lives during the whole jni all, so can be regarded as static
            JNIEnv::from_raw(env.get_native_interface()).unwrap()

        }
    }
}
