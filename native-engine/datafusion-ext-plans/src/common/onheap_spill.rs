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

use blaze_commons::{jni_call, jni_call_static, jni_new_direct_byte_buffer, jni_new_global_ref};
use datafusion::common::Result;
use jni::objects::GlobalRef;
use jni::sys::jlong;
use std::io::{BufReader, BufWriter, Read, Write};
use std::sync::Arc;

/// A spill structure which cooperates with BlazeOnHeapSpillManager
#[derive(Clone)]
pub struct OnHeapSpill(Arc<RawOnHeapSpill>);
impl OnHeapSpill {
    pub fn try_new() -> Result<Self> {
        let hsm = jni_call_static!(JniBridge.getTaskOnHeapSpillManager() -> JObject)?;
        let spill_id = jni_call!(BlazeOnHeapSpillManager(hsm.as_obj()).newSpill() -> i32)?;

        Ok(Self(Arc::new(RawOnHeapSpill {
            hsm: jni_new_global_ref!(hsm.as_obj())?,
            spill_id,
        })))
    }

    pub fn complete(&self) -> Result<()> {
        jni_call!(BlazeOnHeapSpillManager(self.0.hsm.as_obj())
            .completeSpill(self.0.spill_id) -> ())?;
        Ok(())
    }

    pub fn get_disk_usage(&self) -> Result<u64> {
        let usage = jni_call!(BlazeOnHeapSpillManager(self.0.hsm.as_obj())
            .getSpillDiskUsage(self.0.spill_id) -> jlong)? as u64;
        Ok(usage)
    }

    pub fn get_buf_reader(&self) -> BufReader<Box<dyn Read + Send>> {
        BufReader::with_capacity(65536, Box::new(self.clone()))
    }

    pub fn get_buf_writer(&self) -> BufWriter<Box<dyn Write + Send>> {
        BufWriter::with_capacity(65536, Box::new(self.clone()))
    }
}

impl Write for OnHeapSpill {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let write_len = buf.len();
        let buf = jni_new_direct_byte_buffer!(buf)?;

        jni_call!(BlazeOnHeapSpillManager(
            self.0.hsm.as_obj()).writeSpill(self.0.spill_id, buf.as_obj()) -> ()
        )?;
        Ok(write_len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Read for OnHeapSpill {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let buf = jni_new_direct_byte_buffer!(buf)?;
        let read_len = jni_call!(BlazeOnHeapSpillManager(
            self.0.hsm.as_obj()).readSpill(self.0.spill_id, buf.as_obj()) -> i32
        )?;
        Ok(read_len as usize)
    }
}

struct RawOnHeapSpill {
    hsm: GlobalRef,
    spill_id: i32,
}

impl Drop for RawOnHeapSpill {
    fn drop(&mut self) {
        let _ = jni_call!(BlazeOnHeapSpillManager(self.hsm.as_obj())
            .releaseSpill(self.spill_id) -> ());
    }
}
