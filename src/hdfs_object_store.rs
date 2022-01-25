use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::{Read, Seek, SeekFrom};
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
use futures::AsyncRead;
use futures::Stream;
use jni::errors::Error as JniError;
use jni::errors::Result as JniResult;
use jni::objects::JObject;
use jni::objects::JValue;
use jni::JavaVM;
use tokio::runtime::Builder;
use tokio::runtime::Runtime;

use crate::jni_bridge::JavaClasses;
use crate::util::Util;
use crate::DFResult;

trait ReadSeek: Read + Seek {}

pub struct HDFSSingleFileObjectStore {
    pub jvm: JavaVM,
    pub tokio_runtime: Arc<Mutex<Runtime>>,
}

impl Clone for HDFSSingleFileObjectStore {
    fn clone(&self) -> Self {
        HDFSSingleFileObjectStore {
            jvm: Util::jvm_clone(&self.jvm),
            tokio_runtime: self.tokio_runtime.clone(),
        }
    }
}

impl HDFSSingleFileObjectStore {
    pub fn new(jvm: JavaVM) -> Self {
        let tokio_runtime = Builder::new_multi_thread()
            .worker_threads(1) // single thread
            .thread_keep_alive(Duration::from_nanos(u64::MAX))
            .build()
            .unwrap();
        HDFSSingleFileObjectStore {
            jvm,
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
    async fn list_file(&self, prefix: &str) -> DFResult<FileMetaStream> {
        info!("HDFSSingleFileStore.list_file: {}", prefix);
        return self.tokio_runtime.lock().unwrap().block_on(async {
            let list_file_impl = || -> JniResult<FileMetaStream> {
                let env = self.jvm.attach_current_thread_permanently()?;

                let fs = env
                    .call_static_method_unchecked(
                        JavaClasses::get().cJniBridge.class.clone(),
                        JavaClasses::get()
                            .cJniBridge
                            .method_get_hdfs_file_system
                            .clone(),
                        JavaClasses::get()
                            .cJniBridge
                            .method_get_hdfs_file_system_ret
                            .clone(),
                        &[],
                    )?
                    .l()?;

                let path = env.new_object_unchecked(
                    JavaClasses::get().cHadoopPath.class.clone(),
                    JavaClasses::get().cHadoopPath.ctor.clone(),
                    &[JValue::Object(env.new_string(prefix)?.into())],
                )?;

                let file_status = env
                    .call_method_unchecked(
                        fs,
                        JavaClasses::get()
                            .cHadoopFileSystem
                            .method_get_file_status
                            .clone(),
                        JavaClasses::get()
                            .cHadoopFileSystem
                            .method_get_file_status_ret
                            .clone(),
                        &[JValue::Object(path.into())],
                    )?
                    .l()?;

                let file_meta = FileMeta {
                    sized_file: SizedFile {
                        path: prefix.to_owned(),
                        size: env
                            .call_method_unchecked(
                                file_status,
                                JavaClasses::get()
                                    .cHadoopFileStatus
                                    .method_get_len
                                    .clone(),
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
                    type Item = DFResult<FileMeta>;
                    fn poll_next(
                        self: Pin<&mut Self>,
                        _cx: &mut Context<'_>,
                    ) -> Poll<Option<DFResult<FileMeta>>> {
                        let self_mut = self.get_mut();
                        if !self_mut.ended {
                            self_mut.ended = true;
                            return Poll::Ready(Some(Ok(self_mut.file_meta.clone())));
                        }
                        return Poll::Ready(None);
                    }
                }
                return Ok(Box::pin(HDFSSingleFileMetaStream {
                    file_meta,
                    ended: false,
                }));
            };
            return list_file_impl().map_err(Util::wrap_default_data_fusion_io_error);
        });
    }

    async fn list_dir(
        &self,
        _prefix: &str,
        _delimiter: Option<String>,
    ) -> DFResult<ListEntryStream> {
        DFResult::Err(DataFusionError::NotImplemented(
            "HDFSSingleFileObjectStore::list_dir not supported".to_owned(),
        ))
    }

    fn file_reader(&self, file: SizedFile) -> DFResult<Arc<dyn ObjectReader>> {
        info!("HDFSSingleFileStore.file_reader: {:?}", file);
        return Ok(Arc::new(HDFSObjectReader {
            object_store: self.clone(),
            file,
        }));
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
    ) -> DFResult<Box<dyn AsyncRead>> {
        unimplemented!()
    }

    fn sync_reader(&self) -> DFResult<Box<dyn Read + Send + Sync>> {
        return Ok(self.get_seekable_reader()?);
    }

    fn sync_chunk_reader(
        &self,
        start: u64,
        _length: usize,
    ) -> DFResult<Box<dyn Read + Send + Sync>> {
        return Ok(self.get_seekable_reader().and_then(|mut reader| {
            reader.seek(SeekFrom::Start(start))?;
            Ok(reader)
        })?);
    }

    fn length(&self) -> u64 {
        self.file.size
    }
}

impl HDFSObjectReader {
    fn get_seekable_reader(&self) -> DFResult<Box<dyn ReadSeek + Send + Sync>> {
        return self
            .object_store
            .tokio_runtime
            .lock()
            .unwrap()
            .block_on(async {
                let reader_jni = || -> JniResult<Box<dyn ReadSeek + Send + Sync>> {
                    let env = Util::jni_env_clone(
                        &self.object_store.jvm.attach_current_thread_permanently()?,
                    );

                    let fs = env
                        .call_static_method_unchecked(
                            JavaClasses::get().cJniBridge.class.clone(),
                            JavaClasses::get()
                                .cJniBridge
                                .method_get_hdfs_file_system
                                .clone(),
                            JavaClasses::get()
                                .cJniBridge
                                .method_get_hdfs_file_system_ret
                                .clone(),
                            &[],
                        )?
                        .l()?;

                    let path = env.new_object_unchecked(
                        JavaClasses::get().cHadoopPath.class.clone(),
                        JavaClasses::get().cHadoopPath.ctor.clone(),
                        &[JValue::Object(env.new_string(&self.file.path)?.into())],
                    )?;

                    let hdfs_input_stream = env
                        .call_method_unchecked(
                            fs,
                            JavaClasses::get().cHadoopFileSystem.method_open.clone(),
                            JavaClasses::get().cHadoopFileSystem.method_open_ret.clone(),
                            &[JValue::Object(path)],
                        )?
                        .l()?;

                    let reader = Box::new(HDFSFileReader::try_new(
                        self.object_store.clone(),
                        hdfs_input_stream,
                        self.file.size,
                    ));
                    Ok(reader)
                };
                return reader_jni().map_err(Util::wrap_default_data_fusion_io_error);
            });
    }
}

struct HDFSFileReader {
    pub object_store: HDFSSingleFileObjectStore,
    pub hdfs_input_stream: JObject<'static>,
    pub file_size: u64,
    pub pos: u64,
}
unsafe impl Send for HDFSFileReader {}
unsafe impl Sync for HDFSFileReader {}

impl HDFSFileReader {
    pub fn try_new(
        object_store: HDFSSingleFileObjectStore,
        hdfs_input_stream: JObject<'static>,
        file_size: u64,
    ) -> HDFSFileReader {
        HDFSFileReader {
            object_store,
            hdfs_input_stream,
            file_size,
            pos: 0,
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
                let env = &self.object_store.jvm.attach_current_thread_permanently()?;
                let buf = env.new_direct_byte_buffer(buf)?;

                let read_size = env
                    .call_method_unchecked(
                        self.hdfs_input_stream,
                        JavaClasses::get()
                            .cHadoopFSDataInputStream
                            .method_read
                            .clone(),
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
impl Seek for HDFSFileReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let seek_absolutely = |pos: u64| -> std::io::Result<u64> {
            return self
                .object_store
                .tokio_runtime
                .lock()
                .unwrap()
                .block_on(async {
                    let env =
                        &self.object_store.jvm.attach_current_thread_permanently()?;
                    env.call_method_unchecked(
                        self.hdfs_input_stream,
                        JavaClasses::get()
                            .cHadoopFSDataInputStream
                            .method_seek
                            .clone(),
                        JavaClasses::get()
                            .cHadoopFSDataInputStream
                            .method_seek_ret
                            .clone(),
                        &[JValue::Long(pos as i64)],
                    )?;
                    Ok(pos)
                })
                .map_err(|err: JniError| {
                    std::io::Error::new(std::io::ErrorKind::Other, err)
                });
        };

        match pos {
            SeekFrom::Start(start) => self.pos = start,
            SeekFrom::End(offset) => self.pos = self.file_size - (-offset) as u64,
            SeekFrom::Current(offset) => self.pos += offset as u64,
        }
        return seek_absolutely(self.pos);
    }
}
impl ReadSeek for HDFSFileReader {}
