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

use blaze_jni_bridge::{
    jni_call, jni_call_static, jni_new_direct_byte_buffer, jni_new_global_ref, jni_new_object,
    jni_new_string,
};
use datafusion::{error::Result, physical_plan::metrics::Time};
use jni::objects::{GlobalRef, JObject};

use crate::df_execution_err;

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

    pub fn open(&self, path: &str) -> Result<FsDataInputStream> {
        let _timer = self.io_time.timer();
        let path_str = jni_new_string!(path)?;
        let path_uri = jni_new_object!(JavaURI(path_str.as_obj()))?;
        let path = jni_new_object!(HadoopPath(path_uri.as_obj()))?;
        let fin = jni_call!(
            HadoopFileSystem(self.fs.as_obj()).open(path.as_obj()) -> JObject
        )?;

        Ok(FsDataInputStream {
            stream: jni_new_global_ref!(fin.as_obj())?,
            io_time: self.io_time.clone(),
        })
    }

    pub fn create(&self, path: &str) -> Result<FsDataOutputStream> {
        let _timer = self.io_time.timer();
        let path_str = jni_new_string!(path)?;
        let path_uri = jni_new_object!(JavaURI(path_str.as_obj()))?;
        let path = jni_new_object!(HadoopPath(path_uri.as_obj()))?;
        let fin = jni_call!(
            HadoopFileSystem(self.fs.as_obj()).create(path.as_obj()) -> JObject
        )?;

        Ok(FsDataOutputStream {
            stream: jni_new_global_ref!(fin.as_obj())?,
            io_time: self.io_time.clone(),
        })
    }
}

pub struct FsDataInputStream {
    stream: GlobalRef,
    io_time: Time,
}

impl FsDataInputStream {
    pub fn read_fully(&self, pos: u64, buf: &mut [u8]) -> Result<()> {
        let _timer = self.io_time.timer();
        let buf = jni_new_direct_byte_buffer!(buf)?;

        jni_call_static!(JniUtil.readFullyFromFSDataInputStream(
            self.stream.as_obj(), pos as i64, buf.as_obj()) -> ()
        )?;
        Ok(())
    }
}

impl Drop for FsDataInputStream {
    fn drop(&mut self) {
        let _timer = self.io_time.timer();
        if let Err(e) = jni_call!(JavaAutoCloseable(self.stream.as_obj()).close() -> ()) {
            log::warn!("error closing hadoop FSDataInputStream: {:?}", e);
        }
    }
}

pub struct FsDataOutputStream {
    stream: GlobalRef,
    io_time: Time,
}

impl FsDataOutputStream {
    pub fn write_fully(&self, buf: &[u8]) -> Result<()> {
        let _timer = self.io_time.timer();
        let buf = jni_new_direct_byte_buffer!(buf)?;

        jni_call_static!(JniUtil.writeFullyToFSDataOutputStream(
            self.stream.as_obj(), buf.as_obj()) -> ()
        )?;
        Ok(())
    }

    pub fn close(self) -> Result<()> {
        jni_call!(JavaAutoCloseable(self.stream.as_obj()).close() -> ())
    }
}

impl Drop for FsDataOutputStream {
    fn drop(&mut self) {
        let _ = jni_call!(JavaAutoCloseable(self.stream.as_obj()).close() -> ());
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
