// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{ops::Range, sync::Arc};

use base64::{prelude::BASE64_URL_SAFE_NO_PAD, Engine};
use bytes::Bytes;
use datafusion::common::DataFusionError;
use datafusion_ext_commons::{
    df_execution_err,
    hadoop_fs::{FsDataInputStream, FsProvider},
};
use object_store::ObjectMeta;
use once_cell::sync::OnceCell;

#[derive(Clone)]
pub struct InternalFileReader {
    fs_provider: Arc<FsProvider>,
    meta: ObjectMeta,
    input: OnceCell<Arc<FsDataInputStream>>,
}

impl InternalFileReader {
    pub fn new(fs_provider: Arc<FsProvider>, meta: ObjectMeta) -> Self {
        Self {
            fs_provider,
            meta,
            input: OnceCell::new(),
        }
    }

    fn get_input(&self) -> datafusion::common::Result<Arc<FsDataInputStream>> {
        let input = self
            .input
            .get_or_try_init(|| {
                let path = BASE64_URL_SAFE_NO_PAD
                    .decode(self.meta.location.filename().expect("missing filename"))
                    .map(|bytes| String::from_utf8_lossy(&bytes).to_string())
                    .or_else(|_| {
                        let filename = self.meta.location.filename();
                        df_execution_err!("cannot decode filename: {filename:?}")
                    })?;
                let fs = self.fs_provider.provide(&path)?;
                Ok(Arc::new(fs.open(&path)?))
            })
            .map_err(|e| DataFusionError::External(e))?;
        Ok(input.clone())
    }

    pub fn read_fully(&self, range: Range<usize>) -> datafusion::common::Result<Bytes> {
        let mut bytes = vec![0u8; range.len()];
        self.get_input()?
            .read_fully(range.start as u64, &mut bytes)?;
        Ok(Bytes::from(bytes))
    }

    pub fn get_meta(&self) -> ObjectMeta {
        self.meta.clone()
    }
}
