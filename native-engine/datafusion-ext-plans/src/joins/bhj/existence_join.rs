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
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering::Relaxed},
        Arc,
    },
};

use arrow::{
    array::{BooleanArray, RecordBatch},
    buffer::NullBuffer,
    datatypes::Schema,
    row::Rows,
};
use async_trait::async_trait;
use bitvec::{bitvec, prelude::BitVec};
use datafusion::{common::Result, physical_plan::metrics::Time};

use crate::{
    broadcast_join_exec::Joiner,
    common::{batch_selection::interleave_batches, output::WrappedRecordBatchSender},
    joins::{
        bhj::{
            ProbeSide,
            ProbeSide::{L, R},
        },
        join_hash_map::JoinHashMap,
        Idx, JoinParams,
    },
};

#[derive(std::marker::ConstParamTy, Clone, Copy, PartialEq, Eq)]
pub struct JoinerParams {
    probe_side: ProbeSide,
}

impl JoinerParams {
    const fn new(probe_side: ProbeSide) -> Self {
        Self { probe_side }
    }
}

const LEFT_PROBED: JoinerParams = JoinerParams::new(L);
const RIGHT_PROBED: JoinerParams = JoinerParams::new(R);

pub type LeftProbedExistenceJoiner = ExistenceJoiner<LEFT_PROBED>;
pub type RightProbedExistenceJoiner = ExistenceJoiner<RIGHT_PROBED>;

pub struct ExistenceJoiner<const P: JoinerParams> {
    join_params: JoinParams,
    output_sender: Arc<WrappedRecordBatchSender>,
    map: JoinHashMap,
    map_joined: Vec<BitVec>,
    send_output_time: Time,
    output_rows: AtomicUsize,
}

impl<const P: JoinerParams> ExistenceJoiner<P> {
    pub fn new(
        join_params: JoinParams,
        map: JoinHashMap,
        output_sender: Arc<WrappedRecordBatchSender>,
    ) -> Self {
        let map_joined = map
            .data_batches()
            .iter()
            .map(|batch| bitvec![0; batch.num_rows()])
            .collect();

        Self {
            join_params,
            output_sender,
            map,
            map_joined,
            send_output_time: Time::new(),
            output_rows: AtomicUsize::new(0),
        }
    }

    async fn flush(
        &self,
        probed_batch: RecordBatch,
        build_indices: Vec<Idx>,
        exists: Vec<bool>,
    ) -> Result<()> {
        let cols = match P.probe_side {
            L => probed_batch,
            R => interleave_batches(
                self.map.data_schema(),
                self.map.data_batches(),
                &build_indices,
            )?,
        };
        let exists_col = Arc::new(BooleanArray::from(exists));

        let output_batch = RecordBatch::try_new(
            self.join_params.output_schema.clone(),
            [cols.columns().to_vec(), vec![exists_col]].concat(),
        )?;
        self.output_rows.fetch_add(output_batch.num_rows(), Relaxed);

        let timer = self.send_output_time.timer();
        self.output_sender.send(Ok(output_batch), None).await;
        drop(timer);
        Ok(())
    }
}

#[async_trait]
impl<const P: JoinerParams> Joiner for ExistenceJoiner<P> {
    async fn join(
        mut self: Pin<&mut Self>,
        probed_batch: RecordBatch,
        probed_key: Rows,
        probed_key_has_null: Option<NullBuffer>,
    ) -> Result<()> {
        let mut exists = vec![];

        for (row_idx, key) in probed_key.iter().enumerate() {
            if !probed_key_has_null
                .as_ref()
                .map(|nb| nb.is_null(row_idx))
                .unwrap_or(false)
            {
                match P.probe_side {
                    L => {
                        exists.push(self.map.search(key).next().is_some());
                        if exists.len() >= self.join_params.batch_size {
                            let exists = std::mem::take(&mut exists);
                            self.as_mut()
                                .flush(probed_batch.clone(), vec![], exists)
                                .await?;
                        }
                    }
                    R => {
                        for idx in self.map.search(key) {
                            // safety: bypass mutability checker
                            let map_joined = unsafe {
                                &mut *(&self.map_joined[idx.0] as *const BitVec as *mut BitVec)
                            };
                            map_joined.set(idx.1, true);
                        }
                    }
                }
            }
        }
        if !exists.is_empty() {
            self.flush(probed_batch.clone(), vec![], exists).await?;
        }
        Ok(())
    }

    async fn finish(mut self: Pin<&mut Self>) -> Result<()> {
        if P.probe_side == R {
            let probed_empty_batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
            let map_joined = std::mem::take(&mut self.map_joined);

            for (batch_idx, batch_joined) in map_joined.into_iter().enumerate() {
                let mut build_indices: Vec<Idx> = Vec::with_capacity(batch_joined.len());
                let mut exists = vec![];

                for (row_idx, joined) in batch_joined.into_iter().enumerate() {
                    if self.map.inserted(batch_idx, row_idx) {
                        build_indices.push((batch_idx, row_idx));
                        exists.push(joined);
                    }
                }
                if !build_indices.is_empty() {
                    self.as_mut()
                        .flush(probed_empty_batch.clone(), build_indices, exists)
                        .await?;
                }
            }
        }
        Ok(())
    }

    fn total_send_output_time(&self) -> usize {
        self.send_output_time.value()
    }

    fn num_output_rows(&self) -> usize {
        self.output_rows.load(Relaxed)
    }
}
