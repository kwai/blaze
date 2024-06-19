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
    array::{RecordBatch, RecordBatchOptions},
    buffer::NullBuffer,
    datatypes::Schema,
    row::Rows,
};
use async_trait::async_trait;
use bitvec::{bitvec, prelude::BitVec};
use datafusion::{common::Result, physical_plan::metrics::Time};

use crate::{
    broadcast_join_exec::Joiner,
    common::{
        batch_selection::{interleave_batches, take_batch},
        output::WrappedRecordBatchSender,
    },
    joins::{
        bhj::{
            semi_join::ProbeSide::{L, R},
            ProbeSide,
        },
        join_hash_map::JoinHashMap,
        Idx, JoinParams,
    },
};

#[derive(std::marker::ConstParamTy, Clone, Copy, PartialEq, Eq)]
pub struct JoinerParams {
    probe_side: ProbeSide,
    probe_is_join_side: bool,
    semi: bool,
}

impl JoinerParams {
    const fn new(probe_side: ProbeSide, probe_is_join_side: bool, semi: bool) -> Self {
        Self {
            probe_side,
            probe_is_join_side,
            semi,
        }
    }
}

const LEFT_PROBED_LEFT_SEMI: JoinerParams = JoinerParams::new(L, true, true);
const LEFT_PROBED_LEFT_ANTI: JoinerParams = JoinerParams::new(L, true, false);
const LEFT_PROBED_RIGHT_SEMI: JoinerParams = JoinerParams::new(L, false, true);
const LEFT_PROBED_RIGHT_ANTI: JoinerParams = JoinerParams::new(L, false, false);
const RIGHT_PROBED_LEFT_SEMI: JoinerParams = JoinerParams::new(R, false, true);
const RIGHT_PROBED_LEFT_ANTI: JoinerParams = JoinerParams::new(R, false, false);
const RIGHT_PROBED_RIGHT_SEMI: JoinerParams = JoinerParams::new(R, true, true);
const RIGHT_PROBED_RIGHT_ANTI: JoinerParams = JoinerParams::new(R, true, false);

pub type LeftProbedLeftSemiJoiner = SemiJoiner<LEFT_PROBED_LEFT_SEMI>;
pub type LeftProbedLeftAntiJoiner = SemiJoiner<LEFT_PROBED_LEFT_ANTI>;
pub type LeftProbedRightSemiJoiner = SemiJoiner<LEFT_PROBED_RIGHT_SEMI>;
pub type LeftProbedRightAntiJoiner = SemiJoiner<LEFT_PROBED_RIGHT_ANTI>;
pub type RightProbedLeftSemiJoiner = SemiJoiner<RIGHT_PROBED_LEFT_SEMI>;
pub type RightProbedLeftAntiJoiner = SemiJoiner<RIGHT_PROBED_LEFT_ANTI>;
pub type RightProbedRightSemiJoiner = SemiJoiner<RIGHT_PROBED_RIGHT_SEMI>;
pub type RightProbedRightAntiJoiner = SemiJoiner<RIGHT_PROBED_RIGHT_ANTI>;

pub struct SemiJoiner<const P: JoinerParams> {
    join_params: JoinParams,
    output_sender: Arc<WrappedRecordBatchSender>,
    map_joined: Vec<BitVec>,
    map: JoinHashMap,
    send_output_time: Time,
    output_rows: AtomicUsize,
}

impl<const P: JoinerParams> SemiJoiner<P> {
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
        probe_indices: Vec<u32>,
        build_indices: Vec<Idx>,
    ) -> Result<()> {
        let num_rows;
        let cols = if P.probe_is_join_side {
            num_rows = probe_indices.len();
            take_batch(probed_batch.clone(), probe_indices)?
        } else {
            num_rows = build_indices.len();
            interleave_batches(
                self.map.data_schema(),
                self.map.data_batches(),
                &build_indices,
            )?
        };
        let output_batch = RecordBatch::try_new_with_options(
            self.join_params.output_schema.clone(),
            cols.columns().to_vec(),
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )?;
        self.output_rows.fetch_add(output_batch.num_rows(), Relaxed);

        let timer = self.send_output_time.timer();
        self.output_sender.send(Ok(output_batch), None).await;
        drop(timer);
        Ok(())
    }
}

#[async_trait]
impl<const P: JoinerParams> Joiner for SemiJoiner<P> {
    async fn join(
        mut self: Pin<&mut Self>,
        probed_batch: RecordBatch,
        probed_key: Rows,
        probed_key_has_null: Option<NullBuffer>,
    ) -> Result<()> {
        let mut probe_indices: Vec<u32> = vec![];

        for (row_idx, row) in probed_key.iter().enumerate() {
            let mut found = false;
            if !probed_key_has_null
                .as_ref()
                .map(|nb| nb.is_null(row_idx))
                .unwrap_or(false)
            {
                for idx in self.map.search(&row) {
                    found = true;
                    if P.probe_is_join_side {
                        if P.semi {
                            probe_indices.push(row_idx as u32);
                        }
                        break;
                    } else {
                        // safety: bypass mutability checker
                        let map_joined = unsafe {
                            &mut *(&self.map_joined[idx.0] as *const BitVec as *mut BitVec)
                        };
                        map_joined.set(idx.1, true);
                    }
                }
            }
            if P.probe_is_join_side && !P.semi && !found {
                probe_indices.push(row_idx as u32);
            }

            if probe_indices.len() >= self.join_params.batch_size {
                let probe_indices = std::mem::take(&mut probe_indices);
                self.as_mut()
                    .flush(probed_batch.clone(), probe_indices, vec![])
                    .await?;
            }
        }
        if !probe_indices.is_empty() {
            self.flush(probed_batch.clone(), probe_indices, vec![])
                .await?;
        }
        Ok(())
    }

    async fn finish(mut self: Pin<&mut Self>) -> Result<()> {
        if !P.probe_is_join_side {
            let probed_empty_batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
            let map_joined = std::mem::take(&mut self.map_joined);

            for (batch_idx, batch_joined) in map_joined.into_iter().enumerate() {
                let mut build_indices: Vec<Idx> = Vec::with_capacity(batch_joined.len());

                for (row_idx, joined) in batch_joined.into_iter().enumerate() {
                    if P.semi && joined
                        || !P.semi && !joined && self.map.inserted(batch_idx, row_idx)
                    {
                        build_indices.push((batch_idx, row_idx));
                    }
                }
                if !build_indices.is_empty() {
                    self.as_mut()
                        .flush(probed_empty_batch.clone(), vec![], build_indices)
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
