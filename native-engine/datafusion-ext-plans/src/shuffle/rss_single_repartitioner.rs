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

use async_trait::async_trait;
use blaze_jni_bridge::{jni_call, jni_new_direct_byte_buffer};
use datafusion::{arrow::record_batch::RecordBatch, common::Result, physical_plan::metrics::Count};
use datafusion_ext_commons::io::write_one_batch;
use jni::objects::GlobalRef;

use crate::shuffle::ShuffleRepartitioner;

pub struct RssSingleShuffleRepartitioner {
    rss_partition_writer: GlobalRef,
    data_size_metric: Count,
}

impl RssSingleShuffleRepartitioner {
    pub fn new(rss_partition_writer: GlobalRef, data_size_metric: Count) -> Self {
        Self {
            rss_partition_writer,
            data_size_metric,
        }
    }
}

#[async_trait]
impl ShuffleRepartitioner for RssSingleShuffleRepartitioner {
    async fn insert_batch(&self, input: RecordBatch) -> Result<()> {
        let mut cursor = Cursor::new(Vec::<u8>::new());
        let mut num_bytes_written_uncompressed = 0;
        write_one_batch(
            &input,
            &mut cursor,
            true,
            Some(&mut num_bytes_written_uncompressed),
        )?;
        self.data_size_metric.add(num_bytes_written_uncompressed);

        let rss_data = cursor.into_inner();
        let length = rss_data.len();
        let rss_buffer = jni_new_direct_byte_buffer!(&rss_data)?;

        if length != 0 {
            jni_call!(BlazeRssPartitionWriterBase(self.rss_partition_writer.as_obj())
                .write(0_i32, rss_buffer.as_obj(), length as i32) -> ())?;
        }
        Ok(())
    }

    async fn shuffle_write(&self) -> Result<()> {
        Ok(())
    }
}
