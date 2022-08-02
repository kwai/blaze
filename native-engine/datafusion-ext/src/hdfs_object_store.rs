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

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Range;

use datafusion::error::Result;
use futures::stream::BoxStream;
use jni::sys::jlong;
use object_store::path::Path;
use object_store::{GetResult, ListResult, ObjectMeta, ObjectStore};
use once_cell::sync::OnceCell;

use crate::jni_call_static;
use crate::jni_new_direct_byte_buffer;
use crate::jni_new_string;

pub struct HDFSSingleFileObjectStore {
    spawner: OnceCell<tokio::runtime::Runtime>,
}

impl HDFSSingleFileObjectStore {
    pub fn new() -> Self {
        Self {
            spawner: OnceCell::new(),
        }
    }

    fn spawner(&self) -> &tokio::runtime::Runtime {
        self.spawner.get_or_init(|| {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
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
    async fn put(
        &self,
        _location: &Path,
        _bytes: bytes::Bytes,
    ) -> object_store::Result<()> {
        todo!()
    }

    async fn get(&self, _location: &Path) -> object_store::Result<GetResult> {
        todo!()
    }

    async fn get_range(
        &self,
        location: &Path,
        range: Range<usize>,
    ) -> object_store::Result<bytes::Bytes> {
        let location = normalize_location(location);

        self.spawner()
            .spawn_blocking(move || -> Result<bytes::Bytes> {
                let mut buf = vec![0; range.len()];

                jni_call_static!(
                    HDFSObjectStoreBridge.read(
                        jni_new_string!(&location)?,
                        range.start as i64,
                        jni_new_direct_byte_buffer!(&mut buf)?,
                    ) -> ()
                )?;
                Ok(bytes::Bytes::from(buf))
            })
            .await?
            .map_err(|err| object_store::Error::Generic {
                store: "HDFSObjectStore",
                source: Box::new(err),
            })
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let location = normalize_location(location);

        self.spawner()
            .spawn_blocking(move || -> Result<ObjectMeta> {
                let size = jni_call_static!(
                    HDFSObjectStoreBridge.size(jni_new_string!(&location)?) -> jlong
                )?;
                Ok(ObjectMeta {
                    location: location.into(),
                    last_modified: chrono::MIN_DATETIME,
                    size: size as usize,
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

    async fn list(
        &self,
        _prefix: Option<&Path>,
    ) -> object_store::Result<BoxStream<'_, object_store::Result<ObjectMeta>>> {
        todo!()
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        todo!()
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        todo!()
    }

    async fn copy_if_not_exists(
        &self,
        _from: &Path,
        _to: &Path,
    ) -> object_store::Result<()> {
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
            .trim_start_matches('/')
            .to_owned();
        let decoded = String::from_utf8(base64::decode(trimmed).unwrap()).unwrap();
        return decoded;
    }
    location.to_string()
}
