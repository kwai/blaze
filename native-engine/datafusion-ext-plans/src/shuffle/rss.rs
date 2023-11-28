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

use std::io::Cursor;

use arrow::record_batch::RecordBatch;
use blaze_jni_bridge::{jni_call, jni_new_direct_byte_buffer};
use datafusion::common::Result;
use datafusion_ext_commons::io::write_one_batch;
use jni::objects::GlobalRef;

pub fn rss_write_batch(
    rss_partition_writer: &GlobalRef,
    partition_id: usize,
    batch: RecordBatch,
    uncompressed_size: &mut usize,
) -> Result<()> {
    let mut data = vec![];

    write_one_batch(
        &batch,
        &mut Cursor::new(&mut data),
        true,
        Some(uncompressed_size),
    )?;
    let data_len = data.len();
    let buf = jni_new_direct_byte_buffer!(&data)?;
    jni_call!(
        BlazeRssPartitionWriterBase(rss_partition_writer.as_obj())
        .write(partition_id as i32, buf.as_obj(), data_len as i32) -> ()
    )?;
    Ok(())
}

pub fn rss_flush(rss_partition_writer: &GlobalRef) -> Result<()> {
    jni_call!(BlazeRssPartitionWriterBase(rss_partition_writer.as_obj()).flush() -> ())?;
    Ok(())
}
