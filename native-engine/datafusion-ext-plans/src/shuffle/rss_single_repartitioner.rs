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
use datafusion::{arrow::record_batch::RecordBatch, common::Result};
use datafusion_ext_commons::df_execution_err;
use jni::objects::GlobalRef;
use parking_lot::Mutex;

use crate::{
    common::ipc_compression::IpcCompressionWriter,
    shuffle::{rss::RssWriter, ShuffleRepartitioner},
};

pub struct RssSingleShuffleRepartitioner {
    rss_partition_writer: Arc<Mutex<IpcCompressionWriter<RssWriter>>>,
}

impl RssSingleShuffleRepartitioner {
    pub fn new(rss_partition_writer: GlobalRef) -> Self {
        Self {
            rss_partition_writer: Arc::new(Mutex::new(IpcCompressionWriter::new(
                RssWriter::new(rss_partition_writer, 0),
                true,
            ))),
        }
    }
}

#[async_trait]
impl ShuffleRepartitioner for RssSingleShuffleRepartitioner {
    async fn insert_batch(&self, input: RecordBatch) -> Result<()> {
        let rss_partition_writer = self.rss_partition_writer.clone();
        tokio::task::spawn_blocking(move || rss_partition_writer.lock().write_batch(input))
            .await
            .or_else(|err| df_execution_err!("{err}"))??;
        Ok(())
    }

    async fn shuffle_write(&self) -> Result<()> {
        self.rss_partition_writer.lock().flush()?;
        Ok(())
    }
}
