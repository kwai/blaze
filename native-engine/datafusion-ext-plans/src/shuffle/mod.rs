// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    fmt,
    fs::{File, OpenOptions},
    os::unix::fs::PermissionsExt,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
};

use arrow::{
    array::ArrayRef,
    error::Result as ArrowResult,
    record_batch::RecordBatch,
    row::{Row, RowConverter, Rows, SortField},
};
use async_trait::async_trait;
use bytesize::ByteSize;
use datafusion::{
    common::Result,
    error::DataFusionError,
    physical_expr::{PhysicalExprRef, PhysicalSortExpr},
    physical_plan::SendableRecordBatchStream,
};
use datafusion_ext_commons::{arrow::array_size::BatchSize, spark_hash::create_murmur3_hashes};
use futures::StreamExt;
use parking_lot::Mutex as SyncMutex;

use crate::common::execution_context::ExecutionContext;

pub mod single_repartitioner;
pub mod sort_repartitioner;

pub mod buffered_data;
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
                    let batch_mem_size = batch.get_batch_mem_size();
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

#[derive(Debug, Clone)]
pub enum Partitioning {
    /// Allocate batches using a round-robin algorithm and the specified number
    /// of partitions
    RoundRobinPartitioning(usize),
    /// Allocate rows based on a hash of one of more expressions and the
    /// specified number of partitions
    HashPartitioning(Vec<PhysicalExprRef>, usize),
    /// Single partitioning scheme with a known number of partitions
    SinglePartitioning(),
    /// Range partitioning
    RangePartitioning(Vec<PhysicalSortExpr>, usize, Arc<Rows>),
}

impl Partitioning {
    /// Returns the number of partitions in this partitioning scheme
    pub fn partition_count(&self) -> usize {
        use Partitioning::*;
        match self {
            RoundRobinPartitioning(n) | HashPartitioning(_, n) | RangePartitioning(_, n, _) => *n,
            SinglePartitioning() => 1,
        }
    }
}

impl fmt::Display for Partitioning {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Partitioning::RoundRobinPartitioning(size) => write!(f, "RoundRobinBatch({size})"),
            Partitioning::HashPartitioning(phy_exprs, size) => {
                let phy_exprs_str = phy_exprs
                    .iter()
                    .map(|e| format!("{e}"))
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(f, "Hash([{phy_exprs_str}], {size})")
            }
            Partitioning::SinglePartitioning() => {
                write!(f, "SinglePartitioning()")
            }
            Partitioning::RangePartitioning(sort_exprs, size, bounds) => {
                let phy_exprs_str = sort_exprs
                    .iter()
                    .map(|e| format!("{e}"))
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(f, "Range([{phy_exprs_str}], {size}, {:?})", bounds)
            }
        }
    }
}

fn evaluate_hashes(partitioning: &Partitioning, batch: &RecordBatch) -> ArrowResult<Vec<i32>> {
    match partitioning {
        Partitioning::HashPartitioning(exprs, _) => {
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

fn evaluate_robin_partition_ids(
    partitioning: &Partitioning,
    batch: &RecordBatch,
    start_rows: usize,
) -> Vec<u32> {
    let partition_num = partitioning.partition_count();
    let num_rows = batch.num_rows();
    let mut vec_u32 = Vec::with_capacity(num_rows);
    for i in 0..num_rows {
        vec_u32.push(((i + start_rows) % partition_num) as u32);
    }
    vec_u32
}

fn evaluate_range_partition_ids(
    batch: &RecordBatch,
    sort_expr: &Vec<PhysicalSortExpr>,
    bound_rows: &Arc<Rows>,
) -> Result<Vec<u32>> {
    let num_rows = batch.num_rows();

    let sort_row_converter = Arc::new(SyncMutex::new(RowConverter::new(
        sort_expr
            .iter()
            .map(|expr: &PhysicalSortExpr| {
                Ok(SortField::new_with_options(
                    expr.expr.data_type(&batch.schema())?,
                    expr.options,
                ))
            })
            .collect::<Result<Vec<SortField>>>()?,
    )?));

    let key_cols: Vec<ArrayRef> = sort_expr
        .iter()
        .map(|expr| {
            expr.expr
                .evaluate(&batch)
                .and_then(|cv| cv.into_array(batch.num_rows()))
        })
        .collect::<Result<_>>()?;
    let key_rows = sort_row_converter.lock().convert_columns(&key_cols)?;
    let mut vec_u32 = Vec::with_capacity(num_rows);
    for key_row in key_rows.iter() {
        let partition = get_partition(key_row, bound_rows, true);
        vec_u32.push(partition);
    }
    Ok(vec_u32)
}

fn get_partition(key_row: Row, bound_rows: &Arc<Rows>, ascending: bool) -> u32 {
    let mut partition = 0;
    let num_rows = bound_rows.num_rows();
    if num_rows <= 128 {
        // If we have less than 128 partitions naive search
        while partition < num_rows && key_row > bound_rows.row(partition) {
            partition += 1;
        }
    } else {
        // Determine which binary search method to use only once.
        partition = binary_search(bound_rows, key_row, 0, num_rows as isize);
        // binarySearch either returns the match location or -[insertion point]-1
        if partition > num_rows {
            partition = num_rows
        }
    }
    if !ascending {
        partition = num_rows - partition
    }
    partition as u32
}

fn binary_search(rows: &Arc<Rows>, target: Row, from_index: isize, to_index: isize) -> usize {
    let mut low: isize = from_index;
    let mut high: isize = to_index - 1;

    while low <= high {
        let mid = (low + high) >> 1;
        let mid_val = rows.row(mid as usize);

        if mid_val < target {
            low = mid + 1;
        } else if mid_val > target {
            high = mid - 1;
        } else {
            return mid as usize; // key found
        }
    }
    return low as usize; // key not found.
}

pub fn open_shuffle_file<P: AsRef<Path>>(path: P) -> std::io::Result<File> {
    let path_ref = path.as_ref();
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path_ref)?;

    // Set the shuffle file permissions to 0644 to keep it consistent with the
    // permissions of the built-in shuffler manager in Spark.
    std::fs::set_permissions(path_ref, std::fs::Permissions::from_mode(0o644))?;

    Ok(file)
}
