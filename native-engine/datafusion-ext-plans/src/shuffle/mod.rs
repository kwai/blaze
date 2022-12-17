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

use std::io::{Seek, Write};
use std::sync::Arc;
use async_trait::async_trait;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::common::Result;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::DiskManager;
use datafusion::physical_plan::{Partitioning, SendableRecordBatchStream};
use datafusion::physical_plan::coalesce_batches::concat_batches;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::BaselineMetrics;
use futures::StreamExt;
use tempfile::NamedTempFile;
use datafusion_ext_commons::spark_hash::{create_hashes, pmod};

pub mod sort_repartitioner;
pub mod bucket_repartitioner;
pub mod single_repartitioner;

#[async_trait]
pub trait ShuffleRepartitioner: Send + Sync {
    fn name(&self) -> &str;
    async fn insert_batch(&self, input: RecordBatch) -> Result<()>;
    async fn shuffle_write(&self) -> Result<()>;
}

impl dyn ShuffleRepartitioner {
    pub async fn execute(
        self: Arc<Self>,
        mut input: SendableRecordBatchStream,
        batch_size: usize,
        metrics: BaselineMetrics,
    ) -> Result<SendableRecordBatchStream> {
        let mut staging_batches = vec![];
        let mut num_staging_rows = 0;

        macro_rules! flush_staging_batches {
            () => {{
                let batch = concat_batches(
                    &input.schema(),
                    &std::mem::take(&mut staging_batches),
                    num_staging_rows)?;
                let batch_mem_size = batch.get_array_memory_size();
                log::info!(
                    "{} flushing record batch with {} rows, bytes size={}",
                    self.name(),
                    batch.num_rows(),
                    batch_mem_size,
                );
                metrics.record_output(batch.num_rows());
                self.insert_batch(batch).await?;
            }}
        }

        while let Some(batch) = input.next().await {
            let _timer = metrics.elapsed_compute().timer();
            let batch = batch?;

            if batch.num_rows() == 0 {
                continue;
            }
            num_staging_rows += batch.num_rows();
            staging_batches.push(batch);

            // NOTE: in shuffle writer exec, the output_rows metrics represents the
            // number of rows those are written to output data file.
            if num_staging_rows >= batch_size {
                flush_staging_batches!();
                num_staging_rows = 0;
            }
        }

        let _timer = metrics.elapsed_compute().timer();
        if !staging_batches.is_empty() {
            flush_staging_batches!()
        }
        self.shuffle_write().await?;

        // shuffle writer always has empty output
        Ok(Box::pin(MemoryStream::try_new(
            vec![],
            input.schema(),
            None,
        )?))
    }
}

struct InMemSpillInfo {
    frozen: Vec<u8>,
    offsets: Vec<u64>,
}
impl InMemSpillInfo {
    fn mem_size(&self) -> usize {
        self.frozen.len() + self.offsets.len() * 8
    }

    fn into_file_spill(self, disk_manager: &DiskManager) -> Result<FileSpillInfo> {
        let mut file = disk_manager.create_tmp_file()?;
        file.write_all(&self.frozen)?;
        file.rewind()?;
        Ok(FileSpillInfo {
            file,
            offsets: self.offsets,
        })
    }
}

struct FileSpillInfo {
    file: NamedTempFile,
    offsets: Vec<u64>,
}

fn evaluate_hashes(
    partitioning: &Partitioning,
    batch: &RecordBatch,
) -> ArrowResult<Vec<u32>>{
    match partitioning {
        Partitioning::Hash(exprs, _) => {
            let mut hashes_buf = vec![];
            let arrays = exprs
                .iter()
                .map(|expr| Ok(expr.evaluate(&batch)?.into_array(batch.num_rows())))
                .collect::<Result<Vec<_>>>()?;

            // use identical seed as spark hash partition
            hashes_buf.resize(arrays[0].len(), 42);

            // compute hash array
            create_hashes(&arrays, &mut hashes_buf)?;
            Ok(hashes_buf)
        }
        _ => unreachable!("unsupported partitioning: {:?}", partitioning)
    }
}

fn evaluate_partition_ids(
    hashes: &[u32],
    num_partitions: usize
) -> Vec<u32> {
    hashes
        .iter()
        .map(|hash| pmod(*hash, num_partitions) as u32)
        .collect()

}
