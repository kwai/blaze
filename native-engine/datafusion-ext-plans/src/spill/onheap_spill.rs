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
};
use datafusion::common::Result;
use jni::objects::{GlobalRef, JObject};
use std::io::{BufReader, Read, Write};

/// A spill structure which cooperates with BlazeOnHeapSpillManager
pub struct OnHeapSpill {
    hsm: GlobalRef,
    spill_id: i32,
}

impl OnHeapSpill {
    pub fn try_new() -> Result<Self> {
        let hsm = jni_call_static!(JniBridge.getTaskOnHeapSpillManager() -> JObject)?;
        let spill_id = jni_call!(BlazeOnHeapSpillManager(hsm).newSpill() -> i32)?;
        Ok(Self {
            hsm: jni_new_global_ref!(hsm)?,
            spill_id,
        })
    }

    pub fn id(&self) -> i32 {
        self.spill_id
    }

    pub fn complete(&mut self) -> Result<()> {
        jni_call!(BlazeOnHeapSpillManager(self.hsm.as_obj())
            .completeSpill(self.spill_id) -> ())?;
        Ok(())
    }

    pub fn into_buf_reader(self) -> BufReader<Box<dyn Read + Send>> {
        BufReader::with_capacity(65536, Box::new(self))
    }

    fn drop_impl(&mut self) -> Result<()> {
        jni_call!(BlazeOnHeapSpillManager(self.hsm.as_obj())
            .releaseSpill(self.spill_id) -> ())?;
        Ok(())
    }
}

impl Write for OnHeapSpill {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        jni_call!(BlazeOnHeapSpillManager(self.hsm.as_obj()).writeSpill(
            self.spill_id,
            jni_new_direct_byte_buffer!(
                std::slice::from_raw_parts_mut(buf.as_ptr() as *mut u8, buf.len())
            )?,
        ) -> ())?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Read for OnHeapSpill {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(jni_call!(
            BlazeOnHeapSpillManager(self.hsm.as_obj()).readSpill(
                self.spill_id,
                jni_new_direct_byte_buffer!(buf)?,
            ) -> i32
        )
        .map(|s| s as usize)?)
    }
}

impl Drop for OnHeapSpill {
    fn drop(&mut self) {
        let _ = self.drop_impl(); // drop without panics
    }
}
