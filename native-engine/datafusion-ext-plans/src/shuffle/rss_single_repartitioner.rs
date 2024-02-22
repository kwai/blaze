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

use std::sync::Arc;

use async_trait::async_trait;
use blaze_jni_bridge::jni_call;
use datafusion::{arrow::record_batch::RecordBatch, common::Result, physical_plan::metrics::Count};
use datafusion_ext_commons::df_execution_err;
use jni::objects::GlobalRef;
use parking_lot::Mutex;

use crate::{
    common::ipc_compression::IpcCompressionWriter,
    shuffle::{rss::RssWriter, ShuffleRepartitioner},
};

pub struct RssSingleShuffleRepartitioner {
    rss: GlobalRef,
    rss_partition_writer: Arc<Mutex<IpcCompressionWriter<RssWriter>>>,
    data_size_metric: Count,
}

impl RssSingleShuffleRepartitioner {
    pub fn new(rss_partition_writer: GlobalRef, data_size_metric: Count) -> Self {
        Self {
            rss: rss_partition_writer.clone(),
            rss_partition_writer: Arc::new(Mutex::new(IpcCompressionWriter::new(
                RssWriter::new(rss_partition_writer, 0),
                true,
            ))),
            data_size_metric,
        }
    }
}

#[async_trait]
impl ShuffleRepartitioner for RssSingleShuffleRepartitioner {
    async fn insert_batch(&self, input: RecordBatch) -> Result<()> {
        let rss_partition_writer = self.rss_partition_writer.clone();
        let uncompressed_size =
            tokio::task::spawn_blocking(move || rss_partition_writer.lock().write_batch(input))
                .await
                .or_else(|err| df_execution_err!("{err}"))??;
        self.data_size_metric.add(uncompressed_size);
        Ok(())
    }

    async fn shuffle_write(&self) -> Result<()> {
        self.rss_partition_writer.lock().flush()?;

        let rss = self.rss.clone();
        let fut = tokio::task::spawn_blocking(
            move || jni_call!(BlazeRssPartitionWriterBase(rss.as_obj()).close() -> ()),
        );
        fut.await.or_else(|err| df_execution_err!("{err}"))??;
        Ok(())
    }
}
