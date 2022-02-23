use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::Read;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use datafusion::datasource::object_store::FileMeta;
use datafusion::datasource::object_store::FileMetaStream;
use datafusion::datasource::object_store::ListEntryStream;
use datafusion::datasource::object_store::ObjectReader;
use datafusion::datasource::object_store::ObjectStore;
use datafusion::datasource::object_store::SizedFile;
use datafusion::error::DataFusionError;
use datafusion::error::Result;
use futures::AsyncRead;
use futures::Stream;
use jni::errors::Result as JniResult;
use jni::objects::JObject;
use jni::objects::JValue;
use log::info;

use crate::jni_bridge::JavaClasses;
use crate::jni_bridge_call_method;
use crate::jni_bridge_call_static_method;
use crate::jni_bridge_new_object;
use crate::util::Util;

#[derive(Clone)]
pub struct HDFSSingleFileObjectStore;

impl Debug for HDFSSingleFileObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HDFSObjectStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for HDFSSingleFileObjectStore {
    async fn list_file(&self, prefix: &str) -> Result<FileMetaStream> {
        info!("HDFSSingleFileStore.list_file: {}", prefix);
        Util::to_datafusion_external_result(Ok(()).and_then(|_| {
            let env = JavaClasses::get_thread_jnienv();
            let fs =
                jni_bridge_call_static_method!(env, JniBridge.getHDFSFileSystem)?.l()?;

            let path = jni_bridge_new_object!(
                env,
                HadoopPath,
                JValue::Object(env.new_string(prefix)?.into())
            )?;

            let file_status = jni_bridge_call_method!(
                env,
                HadoopFileSystem.getFileStatus,
                fs,
                JValue::Object(path)
            )?
            .l()?;

            let file_size =
                jni_bridge_call_method!(env, HadoopFileStatus.getLen, file_status)?.j()?
                    as u64;

            let file_meta = FileMeta {
                sized_file: SizedFile {
                    path: prefix.to_owned(),
                    size: file_size,
                },
                last_modified: None,
            };

            struct HDFSSingleFileMetaStream {
                file_meta: FileMeta,
                ended: bool,
            }
            impl Stream for HDFSSingleFileMetaStream {
                type Item = Result<FileMeta>;
                fn poll_next(
                    self: Pin<&mut Self>,
                    _cx: &mut Context<'_>,
                ) -> Poll<Option<Result<FileMeta>>> {
                    let self_mut = self.get_mut();
                    if !self_mut.ended {
                        self_mut.ended = true;
                        return Poll::Ready(Some(Ok(self_mut.file_meta.clone())));
                    }
                    Poll::Ready(None)
                }
            }
            JniResult::Ok(Box::pin(HDFSSingleFileMetaStream {
                file_meta,
                ended: false,
            }) as FileMetaStream)
        }))
    }

    async fn list_dir(
        &self,
        _prefix: &str,
        _delimiter: Option<String>,
    ) -> Result<ListEntryStream> {
        Result::Err(DataFusionError::NotImplemented(
            "HDFSSingleFileObjectStore::list_dir not supported".to_owned(),
        ))
    }

    fn file_reader(&self, file: SizedFile) -> Result<Arc<dyn ObjectReader>> {
        info!("HDFSSingleFileStore.file_reader: {:?}", file);
        Ok(Arc::new(HDFSObjectReader { file }))
    }
}

struct HDFSObjectReader {
    file: SizedFile,
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

    fn sync_reader(&self) -> Result<Box<dyn Read + Send + Sync>> {
        self.get_reader(0)
    }

    fn sync_chunk_reader(
        &self,
        start: u64,
        _length: usize,
    ) -> Result<Box<dyn Read + Send + Sync>> {
        self.get_reader(start)
    }

    fn length(&self) -> u64 {
        self.file.size
    }
}

impl HDFSObjectReader {
    fn get_reader(&self, start: u64) -> Result<Box<dyn Read + Send + Sync>> {
        Util::to_datafusion_external_result(Ok(()).and_then(|_| {
            let reader = Box::new(HDFSFileReader::try_new(&self.file.path, start)?);
            JniResult::Ok(reader as Box<dyn Read + Send + Sync>)
        }))
    }
}

struct HDFSFileReader {
    pub hdfs_input_stream: JObject<'static>,
    pub pos: u64,
}

unsafe impl Send for HDFSFileReader {}
unsafe impl Sync for HDFSFileReader {}

impl HDFSFileReader {
    pub fn try_new(path: &str, pos: u64) -> JniResult<HDFSFileReader> {
        Ok(HDFSFileReader {
            hdfs_input_stream: {
                let env = JavaClasses::get_thread_jnienv();
                let fs =
                    jni_bridge_call_static_method!(env, JniBridge.getHDFSFileSystem)?
                        .l()?;
                let path = jni_bridge_new_object!(
                    env,
                    HadoopPath,
                    JValue::Object(env.new_string(path)?.into())
                )?;
                let hdfs_input_stream = jni_bridge_call_method!(
                    env,
                    HadoopFileSystem.open,
                    fs,
                    JValue::Object(path)
                )?
                .l()?;
                JniResult::Ok(hdfs_input_stream)
            }?,
            pos,
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
        Ok(())
            .and_then(|_| {
                let env = JavaClasses::get_thread_jnienv();
                let buf = env.new_direct_byte_buffer(buf)?;
                if self.pos != 0 {
                    jni_bridge_call_method!(
                        env,
                        HadoopFSDataInputStream.seek,
                        self.hdfs_input_stream,
                        JValue::Long(self.pos as i64)
                    )?;
                }

                let read_size = jni_bridge_call_static_method!(
                    env,
                    JniBridge.readFSDataInputStream,
                    JValue::Object(self.hdfs_input_stream),
                    JValue::Object(buf.into())
                )?
                .i()? as usize;

                self.pos += read_size as u64;
                JniResult::Ok(read_size)
            })
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))
    }
}
