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

use blaze_commons::{
    jni_call, jni_call_static, jni_new_direct_byte_buffer, jni_new_global_ref,
    jni_new_object, jni_new_string,
};
use datafusion::error::Result;
use datafusion::physical_plan::metrics::Time;
use jni::objects::{GlobalRef, JObject};

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
                jni_new_string!(path)?
            ) -> JObject
        )?;
        Ok(Fs::new(jni_new_global_ref!(fs)?, &self.io_time))
    }
}

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

    pub fn open(&self, path: &str) -> Result<FsDataInputStream> {
        let _timer = self.io_time.timer();
        let path = jni_new_object!(HadoopPath, jni_new_string!(path)?)?;
        let fin = jni_call!(
            HadoopFileSystem(self.fs.as_obj()).open(path) -> JObject
        )?;
        Ok(FsDataInputStream {
            stream: jni_new_global_ref!(fin)?,
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
        jni_call_static!(
            JniUtil.readFullyFromFSDataInputStream(
                self.stream.as_obj(),
                pos as i64,
                jni_new_direct_byte_buffer!(buf)?
            ) -> ()
        )?;
        Ok(())
    }
}

impl Drop for FsDataInputStream {
    fn drop(&mut self) {
        let _timer = self.io_time.timer();
        if let Err(e) = jni_call!(
            HadoopFSDataInputStream(self.stream.as_obj()).close() -> ()
        ) {
            log::warn!("error closing hadoop FSDatainputStream: {:?}", e);
        }
    }
}
