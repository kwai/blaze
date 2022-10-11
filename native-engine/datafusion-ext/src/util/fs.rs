use datafusion::error::{DataFusionError, Result};
use jni::objects::{GlobalRef, JObject};
use crate::{jni_call, jni_call_static, jni_new_direct_byte_buffer, jni_new_global_ref, jni_new_object, jni_new_string};

pub struct FsProvider(GlobalRef);

impl FsProvider {
    pub fn new(fs_provider: GlobalRef) -> Self {
        Self(fs_provider)
    }

    pub fn provide(&self, path: &str) -> Result<Fs> {
        log::info!("get hadoop filesystem object from path: {}", path);
        let fs = jni_call!(
            ScalaFunction1(self.0.as_obj()).apply(jni_new_string!(path)?) -> JObject
        )?;
        Ok(Fs::new(jni_new_global_ref!(fs)?))
    }
}

pub struct Fs(GlobalRef);

impl Fs {
    pub fn new(fs: GlobalRef) -> Self {
        Self(fs)
    }

    pub fn open(&self, path: &str) -> Result<FsDataInputStream> {
        log::info!("hadoop fs opening {}", &path);
        let path = jni_new_object!(HadoopPath, jni_new_string!(path)?)?;
        let fin = jni_call!(
            HadoopFileSystem(self.0.as_obj()).open(path) -> JObject
        )?;
        Ok(FsDataInputStream(jni_new_global_ref!(fin)?))
    }
}

pub struct FsDataInputStream(GlobalRef);

impl FsDataInputStream {
    pub fn read_fully(&self, pos: u64, buf: &mut[u8]) -> Result<()> {
        jni_call!(HadoopFSDataInputStream(self.0.as_obj()).seek(pos as i64) -> ())?;

        let mut total_read_size = 0;
        let channel = jni_call_static!(JavaChannels.newChannel(self.0.as_obj()) -> JObject)?;
        let buffer = jni_new_direct_byte_buffer!(buf)?;

        while total_read_size < buf.len() {
            let read_size = jni_call!(
                JavaReadableByteChannel(channel).read(buffer) -> i32
            )?;
            if read_size == -1 {
                return Err(DataFusionError::IoError(
                    std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        format!("FSDataInputStream.read() got unexpected EOF")
                    ))
                );
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