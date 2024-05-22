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

use std::{
    fs::{File, OpenOptions},
    io::{Seek, Write},
    sync::Arc,
};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::{common::Result, physical_plan::metrics::BaselineMetrics};
use tokio::sync::Mutex;

use crate::{common::ipc_compression::IpcCompressionWriter, shuffle::ShuffleRepartitioner};

pub struct SingleShuffleRepartitioner {
    output_data_file: String,
    output_index_file: String,
    output_data: Arc<Mutex<Option<IpcCompressionWriter<File>>>>,
    metrics: BaselineMetrics,
}

impl SingleShuffleRepartitioner {
    pub fn new(
        output_data_file: String,
        output_index_file: String,
        metrics: BaselineMetrics,
    ) -> Self {
        Self {
            output_data_file,
            output_index_file,
            output_data: Arc::new(Mutex::default()),
            metrics,
        }
    }

    fn get_output_writer<'a>(
        &self,
        output_data: &'a mut Option<IpcCompressionWriter<File>>,
    ) -> Result<&'a mut IpcCompressionWriter<File>> {
        if output_data.is_none() {
            *output_data = Some(IpcCompressionWriter::new(
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&self.output_data_file)?,
                true,
            ));
        }
        Ok(output_data.as_mut().unwrap())
    }
}

#[async_trait]
impl ShuffleRepartitioner for SingleShuffleRepartitioner {
    async fn insert_batch(&self, input: RecordBatch) -> Result<()> {
        let _timer = self.metrics.elapsed_compute().timer();
        let mut output_data = self.output_data.lock().await;
        let output_writer = self.get_output_writer(&mut *output_data)?;
        output_writer.write_batch(input)?;
        Ok(())
    }

    async fn shuffle_write(&self) -> Result<()> {
        let _timer = self.metrics.elapsed_compute().timer();
        let output_data = std::mem::take(&mut *self.output_data.lock().await);

        // write index file
        if let Some(output_writer) = output_data {
            let mut output_file = output_writer.finish_into_inner()?;
            let offset = output_file.stream_position()?;
            let mut output_index = File::create(&self.output_index_file)?;
            output_index.write_all(&[0u8; 8])?;
            output_index.write_all(&(offset as i64).to_le_bytes()[..])?;
            output_index.sync_data()?;
        } else {
            // write empty data file and index file
            let output_data = File::create(&self.output_data_file)?;
            output_data.set_len(0)?;
            let mut output_index = File::create(&self.output_index_file)?;
            output_index.write_all(&[0u8; 16])?;
            output_index.sync_data()?;
        }
        Ok(())
    }
}
