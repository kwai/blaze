use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::Read;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

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
use jni::errors::Error as JniError;
use jni::errors::Result as JniResult;
use jni::objects::JObject;
use jni::objects::JValue;
use log::info;
use tokio::runtime::Builder;
use tokio::runtime::Runtime;

use crate::jni_bridge::JavaClasses;
use crate::util::Util;

#[derive(Clone)]
pub struct HDFSSingleFileObjectStore {
    pub tokio_runtime: Arc<Mutex<Runtime>>,
}

impl HDFSSingleFileObjectStore {
    pub fn new() -> Self {
        let tokio_runtime = Builder::new_multi_thread()
            .worker_threads(1) // single thread
            .thread_keep_alive(Duration::from_nanos(u64::MAX))
            .build()
            .unwrap();
        HDFSSingleFileObjectStore {
            tokio_runtime: Arc::new(Mutex::new(tokio_runtime)),
        }
    }
}

impl Debug for HDFSSingleFileObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HDFSObjectStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for HDFSSingleFileObjectStore {
    async fn list_file(&self, prefix: &str) -> Result<FileMetaStream> {
        info!("HDFSSingleFileStore.list_file: {}", prefix);
        return self.tokio_runtime.lock().unwrap().block_on(async {
            let list_file_impl = || -> JniResult<FileMetaStream> {
                let env = JavaClasses::get_thread_jnienv();
                let fs = env
                    .call_static_method_unchecked(
                        JavaClasses::get().cJniBridge.class,
                        JavaClasses::get().cJniBridge.method_get_hdfs_file_system,
                        JavaClasses::get()
                            .cJniBridge
                            .method_get_hdfs_file_system_ret
                            .clone(),
                        &[],
                    )?
                    .l()?;

                let path = env.new_object_unchecked(
                    JavaClasses::get().cHadoopPath.class,
                    JavaClasses::get().cHadoopPath.ctor,
                    &[JValue::Object(env.new_string(prefix)?.into())],
                )?;

                let file_status = env
                    .call_method_unchecked(
                        fs,
                        JavaClasses::get().cHadoopFileSystem.method_get_file_status,
                        JavaClasses::get()
                            .cHadoopFileSystem
                            .method_get_file_status_ret
                            .clone(),
                        &[JValue::Object(path)],
                    )?
                    .l()?;

                let file_meta = FileMeta {
                    sized_file: SizedFile {
                        path: prefix.to_owned(),
                        size: env
                            .call_method_unchecked(
                                file_status,
                                JavaClasses::get().cHadoopFileStatus.method_get_len,
                                JavaClasses::get()
                                    .cHadoopFileStatus
                                    .method_get_len_ret
                                    .clone(),
                                &[],
                            )?
                            .j()? as u64,
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
                Ok(Box::pin(HDFSSingleFileMetaStream {
                    file_meta,
                    ended: false,
                }))
            };
            Util::to_datafusion_external_result(list_file_impl())
        });
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
        Ok(Arc::new(HDFSObjectReader {
            object_store: self.clone(),
            file,
        }))
    }
}

struct HDFSObjectReader {
    object_store: HDFSSingleFileObjectStore,
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
        return self
            .object_store
            .tokio_runtime
            .lock()
            .unwrap()
            .block_on(async {
                let reader_jni = || -> JniResult<Box<dyn Read + Send + Sync>> {
                    let env = JavaClasses::get_thread_jnienv();
                    let fs = env
                        .call_static_method_unchecked(
                            JavaClasses::get().cJniBridge.class,
                            JavaClasses::get().cJniBridge.method_get_hdfs_file_system,
                            JavaClasses::get()
                                .cJniBridge
                                .method_get_hdfs_file_system_ret
                                .clone(),
                            &[],
                        )?
                        .l()?;

                    let path = env.new_object_unchecked(
                        JavaClasses::get().cHadoopPath.class,
                        JavaClasses::get().cHadoopPath.ctor,
                        &[JValue::Object(env.new_string(&self.file.path)?.into())],
                    )?;

                    let hdfs_input_stream = env
                        .call_method_unchecked(
                            fs,
                            JavaClasses::get().cHadoopFileSystem.method_open,
                            JavaClasses::get().cHadoopFileSystem.method_open_ret.clone(),
                            &[JValue::Object(path)],
                        )?
                        .l()?;

                    let reader = Box::new(HDFSFileReader::try_new(
                        self.object_store.clone(),
                        hdfs_input_stream,
                        start,
                    ));
                    Ok(reader)
                };
                Util::to_datafusion_external_result(reader_jni())
            });
    }
}

struct HDFSFileReader {
    pub object_store: HDFSSingleFileObjectStore,
    pub hdfs_input_stream: JObject<'static>,
    pub pos: u64,
}
unsafe impl Send for HDFSFileReader {}
unsafe impl Sync for HDFSFileReader {}

impl HDFSFileReader {
    pub fn try_new(
        object_store: HDFSSingleFileObjectStore,
        hdfs_input_stream: JObject<'static>,
        pos: u64,
    ) -> HDFSFileReader {
        HDFSFileReader {
            object_store,
            hdfs_input_stream,
            pos,
        }
    }
}

impl Read for HDFSFileReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        return self
            .object_store
            .tokio_runtime
            .lock()
            .unwrap()
            .block_on(async {
                let env = JavaClasses::get_thread_jnienv();
                let buf = env.new_direct_byte_buffer(buf)?;
                if self.pos != 0 {
                    env.call_method_unchecked(
                        self.hdfs_input_stream,
                        JavaClasses::get().cHadoopFSDataInputStream.method_seek,
                        JavaClasses::get()
                            .cHadoopFSDataInputStream
                            .method_seek_ret
                            .clone(),
                        &[JValue::Long(self.pos as i64)],
                    )?;
                }

                let read_size = env
                    .call_method_unchecked(
                        self.hdfs_input_stream,
                        JavaClasses::get().cHadoopFSDataInputStream.method_read,
                        JavaClasses::get()
                            .cHadoopFSDataInputStream
                            .method_read_ret
                            .clone(),
                        &[JValue::Object(buf.into())],
                    )?
                    .i()? as usize;

                self.pos += read_size as u64;
                Ok(read_size)
            })
            .map_err(|err: JniError| {
                std::io::Error::new(std::io::ErrorKind::Other, err)
            });
    }
}
