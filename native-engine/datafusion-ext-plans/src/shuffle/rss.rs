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

use std::io::Write;

use blaze_jni_bridge::{jni_call, jni_new_direct_byte_buffer};
use jni::objects::GlobalRef;

pub struct RssWriter {
    rss_partition_writer: GlobalRef,
    partition_id: usize,
}

impl RssWriter {
    pub fn new(rss_partition_writer: GlobalRef, partition_id: usize) -> Self {
        Self {
            rss_partition_writer,
            partition_id,
        }
    }
}

impl Write for RssWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let buf_len = buf.len();
        let buf = jni_new_direct_byte_buffer!(&buf)?;
        jni_call!(
            BlazeRssPartitionWriterBase(self.rss_partition_writer.as_obj())
                .write(self.partition_id as i32, buf.as_obj(), buf_len as i32) -> ()
        )?;
        Ok(buf_len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
