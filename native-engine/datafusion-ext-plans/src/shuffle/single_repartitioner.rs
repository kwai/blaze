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
use datafusion::{common::Result, physical_plan::metrics::Time};
use tokio::sync::Mutex;

use crate::{
    common::{
        ipc_compression::IpcCompressionWriter,
        timer_helper::{TimedWriter, TimerHelper},
    },
    shuffle::ShuffleRepartitioner,
};

pub struct SingleShuffleRepartitioner {
    output_data_file: String,
    output_index_file: String,
    output_data: Arc<Mutex<Option<IpcCompressionWriter<TimedWriter<File>>>>>,
    output_io_time: Time,
}

impl SingleShuffleRepartitioner {
    pub fn new(output_data_file: String, output_index_file: String, output_io_time: Time) -> Self {
        Self {
            output_data_file,
            output_index_file,
            output_data: Arc::new(Mutex::default()),
            output_io_time,
        }
    }

    fn get_output_writer<'a>(
        &self,
        output_data: &'a mut Option<IpcCompressionWriter<TimedWriter<File>>>,
    ) -> Result<&'a mut IpcCompressionWriter<TimedWriter<File>>> {
        if output_data.is_none() {
            *output_data = Some(IpcCompressionWriter::new(
                self.output_io_time.wrap_writer(
                    OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(&self.output_data_file)?,
                ),
            ));
        }
        Ok(output_data.as_mut().unwrap())
    }
}

#[async_trait]
impl ShuffleRepartitioner for SingleShuffleRepartitioner {
    async fn insert_batch(&self, input: RecordBatch) -> Result<()> {
        let mut output_data = self.output_data.lock().await;
        let output_writer = self.get_output_writer(&mut *output_data)?;
        output_writer.write_batch(input.num_rows(), input.columns())?;
        Ok(())
    }

    async fn shuffle_write(&self) -> Result<()> {
        let mut output_data = std::mem::take(&mut *self.output_data.lock().await);

        // write index file
        if let Some(output_writer) = output_data.as_mut() {
            let mut output_index = self.output_io_time.wrap_writer(
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&self.output_index_file)?,
            );
            output_writer.finish_current_buf()?;
            let offset = output_writer.inner_mut().0.stream_position()?;
            output_index.write_all(&[0u8; 8])?;
            output_index.write_all(&(offset as i64).to_le_bytes()[..])?;
        } else {
            // write empty data file and index file
            let _output_data = self.output_io_time.wrap_writer(
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&self.output_data_file)?,
            );
            let mut output_index = self.output_io_time.wrap_writer(
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&self.output_index_file)?,
            );
            output_index.write_all(&[0u8; 16])?;
        }
        Ok(())
    }
}
