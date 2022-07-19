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

use jni::objects::{GlobalRef, JObject};
use jni::sys::jint;

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Range;
use std::sync::Arc;
use futures::stream::BoxStream;
use object_store::{GetResult, ListResult, ObjectMeta, ObjectStore};
use object_store::path::Path;

use crate::{jni_call, jni_new_global_ref};
use crate::jni_call_static;
use crate::jni_new_direct_byte_buffer;
use crate::jni_new_object;
use crate::jni_new_string;
use jni::sys::jlong;

const NUM_HDFS_WORKER_THREADS: usize = 64;

#[derive(Clone)]
pub struct HDFSSingleFileObjectStore {
    spawner: Arc<tokio::runtime::Runtime>,
    fs: GlobalRef,
}

impl HDFSSingleFileObjectStore {
    pub fn try_new() -> datafusion::error::Result<Self> {
        let spawner = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(NUM_HDFS_WORKER_THREADS)
                .build()?
        );
        let fs = jni_new_global_ref!(
            jni_call_static!(JniBridge.getHDFSFileSystem() -> JObject)?
        )?;
        Ok(Self {
            spawner,
            fs,
        })
    }
}

impl Debug for HDFSSingleFileObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HDFSObjectStore")
    }
}

impl Display for HDFSSingleFileObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HDFSObjectStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for HDFSSingleFileObjectStore {

    async fn put(&self, _location: &Path, _bytes: bytes::Bytes) -> object_store::Result<()> {
        todo!()
    }

    async fn get(&self, _location: &Path) -> object_store::Result<GetResult> {
        todo!()
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> object_store::Result<bytes::Bytes> {
        let fs = self.fs.clone();
        let location = normalize_location(location);

        self.spawner.spawn_blocking(move || -> datafusion::error::Result<bytes::Bytes> {
            let fs = fs.as_obj();
            let path = jni_new_object!(HadoopPath, jni_new_string!(location.clone())?)?;
            let fin = jni_call!(HadoopFileSystem(fs).open(path) -> JObject)?;

            let mut buf = vec![0; range.len()];

            jni_call_static!(
                JniBridge.readFSDataInputStream(
                    fin,
                    jni_new_direct_byte_buffer!(&mut buf)?,
                    range.start as i64,
                ) -> jint
            )?;
            jni_call!(HadoopFSDataInputStream(fin).close() -> ())?;
            Ok(bytes::Bytes::from(buf))
        })
        .await?
        .map_err(|err| object_store::Error::Generic {
            store: "HDFSObjectStore",
            source: Box::new(err),
        })
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let fs = self.fs.clone();
        let location = normalize_location(location);

        self.spawner.spawn_blocking(move || -> datafusion::error::Result<ObjectMeta> {
            let fs = fs.as_obj();
            let path = jni_new_object!(HadoopPath, jni_new_string!(location.clone())?)?;
            let fstatus = jni_call!(HadoopFileSystem(fs).getFileStatus(path) -> JObject)?;
            let flen = jni_call!(HadoopFileStatus(fstatus).getLen() -> jlong)?;

            Ok(ObjectMeta {
                location: location.clone().into(),
                last_modified: chrono::MIN_DATETIME,
                size: flen as usize,
            })
        })
        .await?
        .map_err(|err| object_store::Error::Generic {
            store: "HDFSObjectStore",
            source: Box::new(err),
        })
    }

    async fn delete(&self, _location: &Path) -> object_store::Result<()> {
        todo!()
    }

    async fn list(&self, _prefix: Option<&Path>) -> object_store::Result<BoxStream<'_, object_store::Result<ObjectMeta>>> {
        todo!()
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> object_store::Result<ListResult> {
        todo!()
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        todo!()
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        todo!()
    }
}

fn normalize_location(location: impl AsRef<str>) -> String {
    const HDFS_LOCATION_PREFIX: &str = "hdfs:/-/";

    // NOTE:
    //  all input hdfs locations shoule be prefixed with this string. see object
    //  store registration codes.
    let location = location.as_ref();
    if location.starts_with(HDFS_LOCATION_PREFIX) {
        let trimmed = location
            .to_string()
            .trim_start_matches(HDFS_LOCATION_PREFIX)
            .trim_start_matches("/")
            .to_owned();
        let decoded = String::from_utf8(base64::decode(trimmed).unwrap()).unwrap();
        return decoded;
    }
    location.to_string()
}