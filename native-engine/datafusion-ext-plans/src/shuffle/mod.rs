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

use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc,
};

use arrow::{error::Result as ArrowResult, record_batch::RecordBatch};
use async_trait::async_trait;
use bytesize::ByteSize;
use datafusion::{
    common::Result,
    error::DataFusionError,
    execution::context::TaskContext,
    physical_plan::{
        metrics::{BaselineMetrics, Count},
        Partitioning, SendableRecordBatchStream,
    },
};
use datafusion_ext_commons::{
    array_size::ArraySize,
    spark_hash::{create_murmur3_hashes, pmod},
    streams::coalesce_stream::CoalesceInput,
};
use futures::StreamExt;

use crate::{common::output::TaskOutputter, memmgr::spill::Spill};

pub mod single_repartitioner;
pub mod sort_repartitioner;

mod buffered_data;
mod rss;
pub mod rss_single_repartitioner;
pub mod rss_sort_repartitioner;

#[async_trait]
pub trait ShuffleRepartitioner: Send + Sync {
    async fn insert_batch(&self, input: RecordBatch) -> Result<()>;
    async fn shuffle_write(&self) -> Result<()>;
}

impl dyn ShuffleRepartitioner {
    pub async fn execute(
        self: Arc<Self>,
        context: Arc<TaskContext>,
        partition: usize,
        input: SendableRecordBatchStream,
        metrics: BaselineMetrics,
        data_size_metric: Count,
    ) -> Result<SendableRecordBatchStream> {
        let input_schema = input.schema();

        // coalesce input
        let mut coalesced = context.coalesce_with_default_batch_size(input, &metrics)?;

        // process all input batches
        context.output_with_sender("Shuffle", input_schema, move |_| async move {
            let batches_num_rows = AtomicUsize::default();
            let batches_mem_size = AtomicUsize::default();
            while let Some(batch) = coalesced.next().await.transpose()? {
                let _timer = metrics.elapsed_compute().timer();
                let batch_num_rows = batch.num_rows();
                let batch_mem_size = batch.get_array_mem_size();
                if batches_num_rows.load(SeqCst) == 0 {
                    log::info!(
                        "[partition={partition}] start shuffle writing, first batch num_rows={}, mem_size={}",
                        batch_num_rows,
                        ByteSize(batch_mem_size as u64),
                    );
                }
                batches_num_rows.fetch_add(batch_num_rows, SeqCst);
                batches_mem_size.fetch_add(batch_mem_size, SeqCst);
                metrics.record_output(batch.num_rows());
                self.insert_batch(batch)
                    .await
                    .map_err(|err| err.context("shuffle: executing insert_batch() error"))?;
            }
            data_size_metric.add(batches_mem_size.load(SeqCst));

            let _timer = metrics.elapsed_compute().timer();
            log::info!(
                "[partition={partition}] finishing shuffle writing, num_rows={}, mem_size={}",
                batches_num_rows.load(SeqCst),
                ByteSize(batches_mem_size.load(SeqCst) as u64),
            );
            self.shuffle_write()
                .await
                .map_err(|err| err.context("shuffle: executing shuffle_write() error"))?;
            log::info!("[partition={partition}] finishing shuffle writing");
            Ok::<_, DataFusionError>(())
        })
    }
}

struct ShuffleSpill {
    spill: Box<dyn Spill>,
    offsets: Vec<u64>,
}

fn evaluate_hashes(partitioning: &Partitioning, batch: &RecordBatch) -> ArrowResult<Vec<i32>> {
    match partitioning {
        Partitioning::Hash(exprs, _) => {
            let mut hashes_buf = vec![];
            let arrays = exprs
                .iter()
                .map(|expr| Ok(expr.evaluate(batch)?.into_array(batch.num_rows())?))
                .collect::<Result<Vec<_>>>()?;

            // use identical seed as spark hash partition
            hashes_buf.resize(arrays[0].len(), 42);

            // compute hash array
            create_murmur3_hashes(&arrays, &mut hashes_buf)?;
            Ok(hashes_buf)
        }
        _ => unreachable!("unsupported partitioning: {:?}", partitioning),
    }
}

fn evaluate_partition_ids(hashes: &[i32], num_partitions: usize) -> Vec<u32> {
    hashes
        .iter()
        .map(|hash| pmod(*hash, num_partitions) as u32)
        .collect()
}
