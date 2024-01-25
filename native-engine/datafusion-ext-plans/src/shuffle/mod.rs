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

use arrow::{error::Result as ArrowResult, record_batch::RecordBatch};
use async_trait::async_trait;
use datafusion::{
    common::Result,
    error::DataFusionError,
    execution::context::TaskContext,
    physical_plan::{metrics::BaselineMetrics, Partitioning, SendableRecordBatchStream},
};
use datafusion_ext_commons::{
    rdxsort::radix_sort_u16_with_max_key_by,
    spark_hash::{create_hashes, pmod},
    streams::coalesce_stream::CoalesceInput,
};
use futures::StreamExt;
use itertools::Itertools;

use crate::{
    common::{output::TaskOutputter, BatchTaker},
    memmgr::onheap_spill::Spill,
};

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
        input: SendableRecordBatchStream,
        metrics: BaselineMetrics,
    ) -> Result<SendableRecordBatchStream> {
        let input_schema = input.schema();

        // coalesce input
        let mut coalesced = context.coalesce_with_default_batch_size(input, &metrics)?;

        // process all input batches
        context.output_with_sender("Shuffle", input_schema, |_| async move {
            while let Some(batch) = coalesced.next().await.transpose()? {
                let _timer = metrics.elapsed_compute().timer();
                metrics.record_output(batch.num_rows());
                self.insert_batch(batch)
                    .await
                    .map_err(|err| err.context("shuffle: executing insert_batch() error"))?;
            }
            let _timer = metrics.elapsed_compute().timer();
            self.shuffle_write()
                .await
                .map_err(|err| err.context("shuffle: executing shuffle_write() error"))?;
            Ok::<_, DataFusionError>(())
        })
    }
}

struct ShuffleSpill {
    spill: Box<dyn Spill>,
    offsets: Vec<u64>,
}

fn evaluate_hashes(partitioning: &Partitioning, batch: &RecordBatch) -> ArrowResult<Vec<u32>> {
    match partitioning {
        Partitioning::Hash(exprs, _) => {
            let mut hashes_buf = vec![];
            let arrays = exprs
                .iter()
                .map(|expr| Ok(expr.evaluate(batch)?.into_array(batch.num_rows())))
                .collect::<Result<Vec<_>>>()?;

            // use identical seed as spark hash partition
            hashes_buf.resize(arrays[0].len(), 42);

            // compute hash array
            create_hashes(&arrays, &mut hashes_buf)?;
            Ok(hashes_buf)
        }
        _ => unreachable!("unsupported partitioning: {:?}", partitioning),
    }
}

fn evaluate_partition_ids(hashes: &[u32], num_partitions: usize) -> Vec<u32> {
    hashes
        .iter()
        .map(|hash| pmod(*hash, num_partitions) as u32)
        .collect()
}

fn sort_batch_by_partition_id(
    batch: RecordBatch,
    partitioning: &Partitioning,
) -> Result<(Vec<u32>, RecordBatch)> {
    let hashes = evaluate_hashes(partitioning, &batch)?;
    let partition_indices = evaluate_partition_ids(&hashes, partitioning.partition_count());

    // use quick sort if partition count >= 65536
    if partitioning.partition_count() >= 65536 || batch.num_rows() >= 65536 {
        let (sorted_partition_indices, sorted_row_indices): (Vec<u32>, Vec<u32>) =
            partition_indices
                .into_iter()
                .zip(0..batch.num_rows() as u32)
                .sorted_unstable_by_key(|(partition_id, _)| *partition_id)
                .unzip();
        let sorted_batch = BatchTaker(&batch).take(sorted_row_indices)?;
        return Ok((sorted_partition_indices, sorted_batch));
    }

    // otherwise use radix sort
    let mut sorted_partition_and_row_indices: Vec<(u16, u16)> = partition_indices
        .into_iter()
        .map(|partition_id| partition_id as u16)
        .zip(0..batch.num_rows() as u16)
        .collect();
    radix_sort_u16_with_max_key_by(
        &mut sorted_partition_and_row_indices,
        partitioning.partition_count().saturating_sub(1) as u16,
        |item| item.0,
    );
    let (sorted_partition_indices, sorted_row_indices): (Vec<u32>, Vec<u16>) =
        sorted_partition_and_row_indices
            .into_iter()
            .map(|(partition_id, row_id)| (partition_id as u32, row_id))
            .unzip();
    let sorted_batch = BatchTaker(&batch).take(sorted_row_indices)?;
    return Ok((sorted_partition_indices, sorted_batch));
}
