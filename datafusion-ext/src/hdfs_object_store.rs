use parking_lot::Mutex;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::Read;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::object_store::FileMetaStream;
use datafusion::datasource::object_store::ListEntryStream;
use datafusion::datasource::object_store::ObjectReader;
use datafusion::datasource::object_store::ObjectStore;
use datafusion::datasource::object_store::SizedFile;
use datafusion::error::Result;
use futures::AsyncRead;
use jni::errors::Result as JniResult;
use jni::objects::JObject;
use jni::objects::JValue;
use log::info;

use crate::error::Result as BlazeResult;
use crate::jni_bridge::JavaClasses;
use crate::jni_bridge_call_method;
use crate::jni_bridge_call_static_method;
use crate::jni_bridge_new_object;

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
        info!("HDFSSingleFileStore.file_reader: {:?}", file);
        Ok(Arc::new(HDFSObjectReader {
            file,
            opened: Mutex::new(None),
        }))
    }
}

struct HDFSObjectReader {
    file: SizedFile,
    opened: Mutex<Option<HDFSFileReader>>,
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
        _length: usize,
    ) -> Result<Box<dyn Read + Send + Sync>> {
        self.get_reader_at(start)
    }

    fn sync_reader(&self) -> Result<Box<dyn Read + Send + Sync>> {
        self.get_reader_at(0)
    }

    fn length(&self) -> u64 {
        self.file.size
    }
}

impl HDFSObjectReader {
    fn get_reader_at(&self, start: u64) -> Result<Box<dyn Read + Send + Sync>> {
        let mut reader_opt = self.opened.lock();
        if reader_opt.is_some() {
            info!(
                "HDFSObjectReader.seek on existing reader: file={:?}, seekPos={}",
                self.file, start
            );
            let reader = reader_opt.as_mut().unwrap();
            reader.new_pos = start;
            Ok(Box::new(reader.clone()) as Box<dyn Read + Send + Sync>)
        } else {
            let reader = HDFSFileReader::try_new(&*self.file.path.clone(), start)?;
            *reader_opt = Some(reader.clone());
            Ok(Box::new(reader) as Box<dyn Read + Send + Sync>)
        }
    }
}

#[derive(Clone)]
struct HDFSFileReader {
    pub hdfs_input_stream: JObject<'static>,
    pub cur_pos: u64,
    pub new_pos: u64,
}

unsafe impl Send for HDFSFileReader {}
unsafe impl Sync for HDFSFileReader {}

impl HDFSFileReader {
    pub fn try_new(path: &str, pos: u64) -> BlazeResult<HDFSFileReader> {
        info!("HDFSFileReader.try_new: path={}, pos={}", path, pos);
        Ok(HDFSFileReader {
            hdfs_input_stream: {
                let env = JavaClasses::get_thread_jnienv();
                let fs = jni_bridge_call_static_method!(
                    env,
                    JniBridge.getHDFSFileSystem,
                    JValue::Object(env.new_string(path)?.into())
                )?
                .l()?;
                info!("got hdfs");
                let path = jni_bridge_new_object!(
                    env,
                    HadoopPath,
                    JValue::Object(env.new_string(path)?.into())
                )?;
                info!("got path");
                let hdfs_input_stream = jni_bridge_call_method!(
                    env,
                    HadoopFileSystem.open,
                    fs,
                    JValue::Object(path)
                )?
                .l()?;
                hdfs_input_stream
            },
            cur_pos: 0,
            new_pos: pos,
        })
    }
}

impl Drop for HDFSFileReader {
    fn drop(&mut self) {
        let env = JavaClasses::get_thread_jnienv();
        jni_bridge_call_method!(
            env,
            HadoopFSDataInputStream.close,
            self.hdfs_input_stream
        )
        .expect("FSDataInputStream.close() exception");
    }
}

impl Read for HDFSFileReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        info!("HDFSFileReader.read: size={}", buf.len());
        Ok(())
            .and_then(|_| {
                let env = JavaClasses::get_thread_jnienv();
                let buf = env.new_direct_byte_buffer(buf)?;
                if self.cur_pos != self.new_pos {
                    jni_bridge_call_method!(
                        env,
                        HadoopFSDataInputStream.seek,
                        self.hdfs_input_stream,
                        JValue::Long(self.new_pos as i64)
                    )?;
                    self.cur_pos = self.new_pos;
                }

                let read_size = jni_bridge_call_static_method!(
                    env,
                    JniBridge.readFSDataInputStream,
                    JValue::Object(self.hdfs_input_stream),
                    JValue::Object(buf.into())
                )?
                .i()? as usize;

                self.cur_pos += read_size as u64;
                info!("HDFSFileReader.read result: read_size={}", read_size);
                JniResult::Ok(read_size)
            })
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))
    }
}
