// Copyright 2022 The Blaze Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_trait::async_trait;
use datafusion::datafusion_data_access::object_store::{
    FileMetaStream, ListEntryStream, ObjectReader, ObjectStore,
};
use datafusion::datafusion_data_access::Result;
use datafusion::datafusion_data_access::SizedFile;
use futures::AsyncRead;
use jni::objects::{GlobalRef, JObject};
use jni::sys::jint;

use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::{BufReader, Read};
use std::sync::Arc;

use crate::jni_call;
use crate::jni_call_static;
use crate::jni_new_direct_byte_buffer;
use crate::jni_new_global_ref;
use crate::jni_new_object;
use crate::jni_new_string;
use crate::ResultExt;

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
        log::warn!("HDFSSingleFileStore.file_reader: {:?}", file);

        let path = file.path.clone();
        let get_hdfs_input_stream = || -> datafusion::error::Result<GlobalRef> {
            let fs = jni_call_static!(JniBridge.getHDFSFileSystem() -> JObject)?;
            let path_str = jni_new_string!(path)?;
            let path = jni_new_object!(HadoopPath, path_str)?;
            Ok(jni_new_global_ref!(
                jni_call!(HadoopFileSystem(fs).open(path) -> JObject)?
            )?)
        };
        Ok(Arc::new(HDFSObjectReader {
            file,
            hdfs_input_stream: Arc::new(FSInputStreamWrapper(
                get_hdfs_input_stream().to_io_result()?,
            )),
        }))
    }
}

#[derive(Clone)]
struct HDFSObjectReader {
    file: SizedFile,
    hdfs_input_stream: Arc<FSInputStreamWrapper>,
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
        length: usize,
    ) -> Result<Box<dyn Read + Send + Sync>> {
        self.get_reader(start, length)
    }

    fn sync_reader(&self) -> Result<Box<dyn Read + Send + Sync>> {
        self.sync_chunk_reader(0, 0)
    }

    fn length(&self) -> u64 {
        self.file.size
    }
}

impl HDFSObjectReader {
    fn get_reader(
        &self,
        start: u64,
        length: usize,
    ) -> Result<Box<dyn Read + Send + Sync>> {
        let max_read_size =
            length.min(self.file.size.saturating_sub(start + length as u64) as usize);
        let buf_len = max_read_size.min(1048576);

        let reader = BufReader::with_capacity(
            buf_len,
            HDFSFileReader {
                hdfs_input_stream: self.hdfs_input_stream.clone(),
                length,
                start,
                pos: start,
            },
        );
        Ok(Box::new(reader))
    }
}

#[derive(Clone)]
struct HDFSFileReader {
    pub hdfs_input_stream: Arc<FSInputStreamWrapper>,
    pub length: usize,
    pub start: u64,
    pub pos: u64,
}

impl Read for HDFSFileReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.pos.saturating_sub(self.start) as usize;
        let rest = self.length.saturating_sub(read);
        let buf_len = buf.len().min(rest);

        let buf = jni_new_direct_byte_buffer!(&mut buf[..buf_len]).to_io_result()?;
        let read_size = jni_call_static!(
            JniBridge.readFSDataInputStream(
                self.hdfs_input_stream.as_obj(),
                buf,
                self.pos as i64,
            ) -> jint
        )
        .to_io_result()? as usize;

        self.pos += read_size as u64;
        Ok(read_size)
    }
}

struct FSInputStreamWrapper(GlobalRef);

impl FSInputStreamWrapper {
    pub fn as_obj(&self) -> JObject {
        self.0.as_obj()
    }
}

impl Drop for FSInputStreamWrapper {
    fn drop(&mut self) {
        // never panic in drop, otherwise the jvm process will be aborted
        let _ = jni_call!(HadoopFSDataInputStream(self.0.as_obj()).close() -> ());
    }
}
