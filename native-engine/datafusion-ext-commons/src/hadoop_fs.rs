// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use auron_jni_bridge::{
    jni_call, jni_call_static, jni_new_direct_byte_buffer, jni_new_global_ref, jni_new_object,
    jni_new_string,
};
use datafusion::{error::Result, physical_plan::metrics::Time};
use jni::objects::{GlobalRef, JObject};

use crate::df_execution_err;

#[derive(Clone)]
pub struct Fs {
    fs: GlobalRef,
    io_time: Time,
}

impl Fs {
    pub fn new(fs: GlobalRef, io_time_metric: &Time) -> Self {
        Self {
            fs,
            io_time: io_time_metric.clone(),
        }
    }

    pub fn mkdirs(&self, path: &str) -> Result<()> {
        let _timer = self.io_time.timer();
        let path_str = jni_new_string!(path)?;
        let path_uri = jni_new_object!(JavaURI(path_str.as_obj()))?;
        let path = jni_new_object!(HadoopPath(path_uri.as_obj()))?;
        let succeeded = jni_call!(
            HadoopFileSystem(self.fs.as_obj()).mkdirs(path.as_obj()) -> bool
        )?;
        if !succeeded {
            df_execution_err!("fs.mkdirs not succeeded")?;
        }
        Ok(())
    }

    pub fn open(&self, path: &str) -> Result<Arc<FsDataInputWrapper>> {
        let _timer = self.io_time.timer();
        let path = jni_new_string!(path)?;
        let wrapper = jni_call_static!(
            JniBridge.openFileAsDataInputWrapper(self.fs.as_obj(), path.as_obj()) -> JObject
        )?;

        Ok(Arc::new(FsDataInputWrapper {
            obj: jni_new_global_ref!(wrapper.as_obj())?,
            io_time: self.io_time.clone(),
        }))
    }

    pub fn create(&self, path: &str) -> Result<Arc<FsDataOutputWrapper>> {
        let _timer = self.io_time.timer();
        let path = jni_new_string!(path)?;
        let wrapper = jni_call_static!(
            JniBridge.createFileAsDataOutputWrapper(self.fs.as_obj(), path.as_obj()) -> JObject
        )?;

        Ok(Arc::new(FsDataOutputWrapper {
            obj: jni_new_global_ref!(wrapper.as_obj())?,
            io_time: self.io_time.clone(),
        }))
    }
}

pub struct FsDataInputWrapper {
    obj: GlobalRef,
    io_time: Time,
}

impl FsDataInputWrapper {
    pub fn read_fully(&self, pos: u64, buf: &mut [u8]) -> Result<()> {
        let _timer = self.io_time.timer();
        let buf = jni_new_direct_byte_buffer!(buf)?;

        jni_call!(AuronFSDataInputWrapper(self.obj.as_obj())
            .readFully(pos as i64, buf.as_obj()) -> ())?;
        Ok(())
    }
}

impl Drop for FsDataInputWrapper {
    fn drop(&mut self) {
        let _timer = self.io_time.timer();
        if let Err(e) = jni_call!(JavaAutoCloseable(self.obj.as_obj()).close() -> ()) {
            log::warn!("error closing hadoop FSDataInputStream: {:?}", e);
        }
    }
}

pub struct FsDataOutputWrapper {
    obj: GlobalRef,
    io_time: Time,
}

impl FsDataOutputWrapper {
    pub fn write_fully(&self, buf: &[u8]) -> Result<()> {
        let _timer = self.io_time.timer();
        let buf = jni_new_direct_byte_buffer!(buf)?;
        jni_call!(AuronFSDataOutputWrapper(self.obj.as_obj()).writeFully(buf.as_obj()) -> ())?;
        Ok(())
    }

    pub fn close(self) -> Result<()> {
        jni_call!(JavaAutoCloseable(self.obj.as_obj()).close() -> ())
    }
}

impl Drop for FsDataOutputWrapper {
    fn drop(&mut self) {
        let _ = jni_call!(JavaAutoCloseable(self.obj.as_obj()).close() -> ());
    }
}

#[derive(Clone)]
pub struct FsProvider {
    fs_provider: GlobalRef,
    io_time: Time,
}

impl FsProvider {
    pub fn new(fs_provider: GlobalRef, io_time_metric: &Time) -> Self {
        Self {
            fs_provider,
            io_time: io_time_metric.clone(),
        }
    }

    pub fn provide(&self, path: &str) -> Result<Fs> {
        let _timer = self.io_time.timer();
        let fs = jni_call!(
            ScalaFunction1(self.fs_provider.as_obj()).apply(
                jni_new_string!(path)?.as_obj()
            ) -> JObject
        )?;
        Ok(Fs::new(jni_new_global_ref!(fs.as_obj())?, &self.io_time))
    }
}
