use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::{
    jni_call, jni_call_static, jni_new_direct_byte_buffer, jni_new_global_ref,
    jni_new_object, jni_new_string,
};
use datafusion::error::{DataFusionError, Result};
use jni::objects::{GlobalRef, JObject};

pub struct FsProvider(GlobalRef, Mutex<HashMap<String, Arc<Fs>>>);

impl FsProvider {
    pub fn new(fs_provider: GlobalRef) -> Self {
        Self(fs_provider, Mutex::new(HashMap::new()))
    }

    pub fn provide(&self, path: &str) -> Result<Arc<Fs>> {
        let scheme = path.split_once('/').map(|split| split.0).unwrap_or("");
        let cache = &self.1;

        // first try to find an existed fs with same scheme
        if let Some(fs) = cache.lock().unwrap().get(scheme) {
            return Ok(fs.clone());
        }

        // provide and cache a new fs
        let fs = Arc::new(Fs::new(jni_new_global_ref!(jni_call!(
            ScalaFunction1(self.0.as_obj()).apply(jni_new_string!(path)?) -> JObject
        )?)?));
        cache.lock().unwrap().insert(scheme.to_owned(), fs.clone());
        Ok(fs)
    }
}

pub struct Fs(GlobalRef);

impl Fs {
    pub fn new(fs: GlobalRef) -> Self {
        Self(fs)
    }

    pub fn open(&self, path: &str) -> Result<FsDataInputStream> {
        let path = jni_new_object!(HadoopPath, jni_new_string!(path)?)?;
        let fin = jni_call!(
            HadoopFileSystem(self.0.as_obj()).open(path) -> JObject
        )?;
        Ok(FsDataInputStream(jni_new_global_ref!(fin)?))
    }
}

pub struct FsDataInputStream(GlobalRef);

impl FsDataInputStream {
    pub fn read_fully(&self, pos: u64, buf: &mut [u8]) -> Result<()> {
        jni_call!(HadoopFSDataInputStream(self.0.as_obj()).seek(pos as i64) -> ())?;

        let mut total_read_size = 0;
        let channel =
            jni_call_static!(JavaChannels.newChannel(self.0.as_obj()) -> JObject)?;
        let buffer = jni_new_direct_byte_buffer!(buf)?;

        while total_read_size < buf.len() {
            let read_size = jni_call!(
                JavaReadableByteChannel(channel).read(buffer) -> i32
            )?;
            if read_size == -1 {
                return Err(DataFusionError::IoError(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "FSDataInputStream.read() got unexpected EOF".to_string(),
                )));
            }
            total_read_size += read_size as usize;
        }
        Ok(())
    }
}

impl Drop for FsDataInputStream {
    fn drop(&mut self) {
        if let Err(e) = jni_call!(
            HadoopFSDataInputStream(self.0.as_obj()).close() -> ()
        ) {
            log::warn!("error closing hadoop FSDatainputStream: {:?}", e);
        }
    }
}
