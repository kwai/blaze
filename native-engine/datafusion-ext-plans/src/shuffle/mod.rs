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
    physical_plan::{Partitioning, SendableRecordBatchStream},
};
use datafusion_ext_commons::{arrow::array_size::ArraySize, spark_hash::create_murmur3_hashes};
use futures::StreamExt;

use crate::{common::execution_context::ExecutionContext, memmgr::spill::Spill};

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
    pub fn execute(
        self: Arc<Self>,
        exec_ctx: Arc<ExecutionContext>,
        input: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream> {
        let data_size_counter = exec_ctx.register_counter_metric("data_size");
        let mut coalesced = exec_ctx.coalesce_with_default_batch_size(input);

        // process all input batches
        Ok(exec_ctx
            .clone()
            .output_with_sender("Shuffle", move |_| async move {
                let batches_num_rows = AtomicUsize::default();
                let batches_mem_size = AtomicUsize::default();
                while let Some(batch) = coalesced.next().await.transpose()? {
                    let _timer = exec_ctx.baseline_metrics().elapsed_compute().timer();
                    let batch_num_rows = batch.num_rows();
                    let batch_mem_size = batch.get_array_mem_size();
                    if batches_num_rows.load(SeqCst) == 0 {
                        log::info!(
                            "start shuffle writing, first batch num_rows={}, mem_size={}",
                            batch_num_rows,
                            ByteSize(batch_mem_size as u64),
                        );
                    }
                    batches_num_rows.fetch_add(batch_num_rows, SeqCst);
                    batches_mem_size.fetch_add(batch_mem_size, SeqCst);
                    exec_ctx.baseline_metrics().record_output(batch.num_rows());
                    self.insert_batch(batch)
                        .await
                        .map_err(|err| err.context("shuffle: executing insert_batch() error"))?;
                }
                data_size_counter.add(batches_mem_size.load(SeqCst));

                let _timer = exec_ctx.baseline_metrics().elapsed_compute().timer();
                log::info!(
                    "finishing shuffle writing, num_rows={}, mem_size={}",
                    batches_num_rows.load(SeqCst),
                    ByteSize(batches_mem_size.load(SeqCst) as u64),
                );
                self.shuffle_write()
                    .await
                    .map_err(|err| err.context("shuffle: executing shuffle_write() error"))?;
                log::info!("finishing shuffle writing");
                Ok::<_, DataFusionError>(())
            }))
    }
}

struct ShuffleSpill {
    spill: Box<dyn Spill>,
    offsets: Vec<u64>,
}

fn evaluate_hashes(partitioning: &Partitioning, batch: &RecordBatch) -> ArrowResult<Vec<i32>> {
    match partitioning {
        Partitioning::Hash(exprs, _) => {
            let arrays = exprs
                .iter()
                .map(|expr| Ok(expr.evaluate(batch)?.into_array(batch.num_rows())?))
                .collect::<Result<Vec<_>>>()?;

            // compute hash array, use identical seed as spark hash partition
            Ok(create_murmur3_hashes(arrays[0].len(), &arrays, 42))
        }
        _ => unreachable!("unsupported partitioning: {:?}", partitioning),
    }
}

fn evaluate_partition_ids(mut hashes: Vec<i32>, num_partitions: usize) -> Vec<u32> {
    // evaluate part_id = pmod(hash, num_partitions)
    for h in &mut hashes {
        *h = h.rem_euclid(num_partitions as i32);
    }

    unsafe {
        // safety: transmute Vec<i32> to Vec<u32>
        std::mem::transmute(hashes)
    }
}

fn evaluate_robin_partition_ids(partitioning: &Partitioning, batch: &RecordBatch) -> Vec<u32> {
    let partition_num = partitioning.partition_count();
    let num_rows = batch.num_rows();
    let mut vec_u32 = Vec::with_capacity(num_rows);
    if num_rows == 0 {
        return vec_u32;
    }
    for i in 0..num_rows {
        vec_u32.push((i % partition_num) as u32);
    }
    vec_u32
}
