use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::{BufReader, Read};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datafusion_data_access::object_store::{
    FileMetaStream, ListEntryStream, ObjectReader, ObjectStore,
};
use datafusion::datafusion_data_access::Result;
use datafusion::datafusion_data_access::SizedFile;
use futures::AsyncRead;
use jni::objects::{GlobalRef, JObject};
use jni::sys::jint;

use crate::jni_bridge::JavaClasses;
use crate::jni_bridge_new_object;
use crate::ResultExt;
use crate::{jni_bridge_call_method, jni_map_error};
use crate::{jni_bridge_call_static_method, jni_global_ref};

#[derive(Clone)]
pub struct HDFSSingleFileObjectStore;

impl Debug for HDFSSingleFileObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HDFSObjectStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for HDFSSingleFileObjectStore {
    async fn list_file(&self, _prefix: &str) -> Result<FileMetaStream> {
        unreachable!()
    }

    async fn list_dir(
        &self,
        _prefix: &str,
        _delimiter: Option<String>,
    ) -> Result<ListEntryStream> {
        unreachable!()
    }

    fn file_reader(&self, file: SizedFile) -> Result<Arc<dyn ObjectReader>> {
        log::debug!("HDFSSingleFileStore.file_reader: {:?}", file);

        let env = JavaClasses::get_thread_jnienv();
        let path = file.path.clone();
        let get_hdfs_input_stream = || -> datafusion::error::Result<GlobalRef> {
            let fs = jni_bridge_call_static_method!(env, JniBridge.getHDFSFileSystem -> JObject)?;
            let path_str = jni_map_error!(env.new_string(path))?;
            let path = jni_bridge_new_object!(env, HadoopPath, path_str)?;
            Ok(jni_global_ref!(
                env,
                jni_bridge_call_method!(env, HadoopFileSystem.open -> JObject, fs, path)?
            )?)
        };
        Ok(Arc::new(HDFSObjectReader {
            file,
            hdfs_input_stream: Arc::new(get_hdfs_input_stream().to_io_result()?),
        }))
    }
}

#[derive(Clone)]
struct HDFSObjectReader {
    file: SizedFile,
    hdfs_input_stream: Arc<GlobalRef>,
}

#[async_trait]
impl ObjectReader for HDFSObjectReader {
    async fn chunk_reader(
        &self,
        _start: u64,
        _length: usize,
    ) -> Result<Box<dyn AsyncRead>> {
        unimplemented!()
    }

    fn sync_chunk_reader(
        &self,
        start: u64,
        _: usize,
    ) -> Result<Box<dyn Read + Send + Sync>> {
        self.get_reader(start)
    }

    fn sync_reader(&self) -> Result<Box<dyn Read + Send + Sync>> {
        self.sync_chunk_reader(0, 0)
    }

    fn length(&self) -> u64 {
        self.file.size
    }
}

impl HDFSObjectReader {
    fn get_reader(&self, start: u64) -> Result<Box<dyn Read + Send + Sync>> {
        let reader = BufReader::new(HDFSFileReader {
            hdfs_input_stream: self.hdfs_input_stream.clone(),
            pos: start,
        });
        Ok(Box::new(reader))
    }
}

#[derive(Clone)]
struct HDFSFileReader {
    pub hdfs_input_stream: Arc<GlobalRef>,
    pub pos: u64,
}

impl Drop for HDFSFileReader {
    fn drop(&mut self) {
        // never panic in drop, otherwise the jvm process will be aborted
        let env = JavaClasses::get_thread_jnienv();
        let _ = jni_bridge_call_method!(
            env,
            HadoopFSDataInputStream.close -> (),
            self.hdfs_input_stream.as_obj()
        );
    }
}
impl Read for HDFSFileReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let env = JavaClasses::get_thread_jnienv();

        log::debug!("HDFSFileReader.read: size={}", buf.len());
        let buf = jni_map_error!(env.new_direct_byte_buffer(buf)).to_io_result()?;
        let read_size = jni_bridge_call_static_method!(
            env,
            JniBridge.readFSDataInputStream -> jint,
            self.hdfs_input_stream.as_obj(),
            buf,
            self.pos as i64,
        )
        .to_io_result()? as usize;

        log::debug!("HDFSFileReader.read result: read_size={}", read_size);
        self.pos += read_size as u64;
        Ok(read_size)
    }
}
