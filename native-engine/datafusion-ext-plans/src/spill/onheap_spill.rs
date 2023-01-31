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

use std::io::{Read, Write};
use datafusion::common::{DataFusionError, Result};
use jni::objects::{GlobalRef, JObject};
use blaze_commons::{jni_call, jni_call_static, jni_new_direct_byte_buffer, jni_new_global_ref};

/// A spill structure which cooperates with BlazeOnHeapSpillManager
pub struct OnHeapSpill {
    hsm: GlobalRef,
    spill_id: i32,
    size: usize,
}

impl OnHeapSpill {
    pub fn try_new_incompleted(capacity: usize) -> Result<Self> {
        let hsm = jni_new_global_ref!(
            jni_call_static!(JniBridge.getTaskOnHeapSpillManager() -> JObject)?
        )?;

        // try to allocate a new spill
        let spill_id = jni_call!(
            BlazeOnHeapSpillManager(hsm.as_obj()).newSpill(capacity as i64) -> i32
        )?;
        if spill_id == -1 {
            return Err(DataFusionError::ResourcesExhausted(
                format!("cannot allocate on-heap spill")
            ));
        }
        Ok(Self {
            hsm,
            spill_id,
            size: 0,
        })
    }

    pub fn try_new(data: &[u8]) -> Result<Self> {
        let mut spill = Self::try_new_incompleted(data.len())?;
        spill.write_all(data)?;
        spill.complete()?;
        Ok(spill)
    }

    pub fn complete(&mut self) -> Result<()> {
        jni_call!(BlazeOnHeapSpillManager(self.hsm.as_obj())
            .completeSpill(self.spill_id) -> ())?;
        Ok(())
    }

    pub fn size(&self) -> usize {
        self.size
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

        self.size += buf.len();
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
        ).map(|s| s as usize)?)
    }
}

impl Drop for OnHeapSpill {
    fn drop(&mut self) {
        let _ = self.drop_impl(); // drop without panics
    }
}
