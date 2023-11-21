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
};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::{
    common::Result,
    error::DataFusionError,
    physical_plan::metrics::{BaselineMetrics, Count},
};
use datafusion_ext_commons::io::write_one_batch;
use once_cell::sync::OnceCell;

use crate::shuffle::ShuffleRepartitioner;

pub struct SingleShuffleRepartitioner {
    output_data_file: String,
    output_index_file: String,
    output_data: OnceCell<File>,
    metrics: BaselineMetrics,
    data_size_metric: Count,
}

impl SingleShuffleRepartitioner {
    pub fn new(
        output_data_file: String,
        output_index_file: String,
        metrics: BaselineMetrics,
        data_size_metric: Count,
    ) -> Self {
        Self {
            output_data_file,
            output_index_file,
            output_data: OnceCell::new(),
            metrics,
            data_size_metric,
        }
    }

    fn get_output_data(&self) -> Result<&File> {
        self.output_data
            .get_or_try_init(|| {
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&self.output_data_file)
            })
            .map_err(DataFusionError::IoError)
    }
}

#[async_trait]
impl ShuffleRepartitioner for SingleShuffleRepartitioner {
    async fn insert_batch(&self, input: RecordBatch) -> Result<()> {
        let _timer = self.metrics.elapsed_compute().timer();
        let mut num_bytes_written_uncompressed = 0;
        write_one_batch(
            &input,
            &mut self.get_output_data()?.try_clone()?,
            true,
            Some(&mut num_bytes_written_uncompressed),
        )?;
        self.data_size_metric.add(num_bytes_written_uncompressed);
        Ok(())
    }

    async fn shuffle_write(&self) -> Result<()> {
        self.get_output_data()?.sync_data()?;

        let offset = self.get_output_data()?.stream_position()?;
        let mut output_index = File::create(&self.output_index_file)?;
        output_index.write_all(&[0u8; 8])?;
        output_index.write_all(&(offset as i64).to_le_bytes()[..])?;
        output_index.sync_data()?;
        Ok(())
    }
}
