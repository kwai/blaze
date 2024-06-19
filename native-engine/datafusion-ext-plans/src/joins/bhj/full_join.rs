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

use arrow::{array::RecordBatch, buffer::NullBuffer, datatypes::Schema, row::Rows};
use async_trait::async_trait;
use bitvec::{bitvec, prelude::BitVec};
use datafusion::{common::Result, physical_plan::metrics::Time};

use crate::{
    broadcast_join_exec::Joiner,
    common::{
        batch_selection::{interleave_batches, take_batch_opt},
        output::WrappedRecordBatchSender,
    },
    joins::{
        bhj::{
            full_join::ProbeSide::{L, R},
            ProbeSide,
        },
        join_hash_map::JoinHashMap,
        Idx, JoinParams,
    },
};

#[derive(std::marker::ConstParamTy, Clone, Copy, PartialEq, Eq)]
pub struct JoinerParams {
    probe_side: ProbeSide,
    probe_side_outer: bool,
    build_side_outer: bool,
}

impl JoinerParams {
    const fn new(probe_side: ProbeSide, probe_side_outer: bool, build_side_outer: bool) -> Self {
        Self {
            probe_side,
            probe_side_outer,
            build_side_outer,
        }
    }
}

const LEFT_PROBED_INNER: JoinerParams = JoinerParams::new(L, false, false);
const LEFT_PROBED_LEFT: JoinerParams = JoinerParams::new(L, true, false);
const LEFT_PROBED_RIGHT: JoinerParams = JoinerParams::new(L, false, true);
const LEFT_PROBED_OUTER: JoinerParams = JoinerParams::new(L, true, true);

const RIGHT_PROBED_INNER: JoinerParams = JoinerParams::new(R, false, false);
const RIGHT_PROBED_LEFT: JoinerParams = JoinerParams::new(R, false, true);
const RIGHT_PROBED_RIGHT: JoinerParams = JoinerParams::new(R, true, false);
const RIGHT_PROBED_OUTER: JoinerParams = JoinerParams::new(R, true, true);

pub type LeftProbedInnerJoiner = FullJoiner<LEFT_PROBED_INNER>;
pub type LeftProbedLeftJoiner = FullJoiner<LEFT_PROBED_LEFT>;
pub type LeftProbedRightJoiner = FullJoiner<LEFT_PROBED_RIGHT>;
pub type LeftProbedFullOuterJoiner = FullJoiner<LEFT_PROBED_OUTER>;
pub type RightProbedInnerJoiner = FullJoiner<RIGHT_PROBED_INNER>;
pub type RightProbedLeftJoiner = FullJoiner<RIGHT_PROBED_LEFT>;
pub type RightProbedRightJoiner = FullJoiner<RIGHT_PROBED_RIGHT>;
pub type RightProbedFullOuterJoiner = FullJoiner<RIGHT_PROBED_OUTER>;

pub struct FullJoiner<const P: JoinerParams> {
    join_params: JoinParams,
    output_sender: Arc<WrappedRecordBatchSender>,
    map: JoinHashMap,
    map_joined: Vec<BitVec>,
    send_output_time: Time,
    output_rows: AtomicUsize,
}

impl<const P: JoinerParams> FullJoiner<P> {
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
            send_output_time: Time::default(),
            output_rows: AtomicUsize::new(0),
        }
    }

    async fn flush(
        &self,
        probed_batch: RecordBatch,
        probe_indices: Vec<Option<u32>>,
        build_indices: Vec<Idx>,
    ) -> Result<()> {
        let pcols = take_batch_opt(probed_batch.clone(), probe_indices)?;
        let bcols = interleave_batches(
            self.map.data_schema(),
            self.map.data_batches(),
            &build_indices,
        )?;
        let output_batch = RecordBatch::try_new(
            self.join_params.output_schema.clone(),
            match P.probe_side {
                L => [pcols.columns().to_vec(), bcols.columns().to_vec()].concat(),
                R => [bcols.columns().to_vec(), pcols.columns().to_vec()].concat(),
            },
        )?;
        self.output_rows.fetch_add(output_batch.num_rows(), Relaxed);

        let timer = self.send_output_time.timer();
        self.output_sender.send(Ok(output_batch), None).await;
        drop(timer);
        Ok(())
    }
}

#[async_trait]
impl<const P: JoinerParams> Joiner for FullJoiner<P> {
    async fn join(
        mut self: Pin<&mut Self>,
        probed_batch: RecordBatch,
        probed_key: Rows,
        probed_key_has_null: Option<NullBuffer>,
    ) -> Result<()> {
        let mut probe_indices: Vec<Option<u32>> = vec![];
        let mut build_indices: Vec<Idx> = vec![];

        for (row_idx, row) in probed_key.iter().enumerate() {
            let mut found = false;
            if !probed_key_has_null
                .as_ref()
                .map(|nb| nb.is_null(row_idx))
                .unwrap_or(false)
            {
                for idx in self.map.search(&row) {
                    found = true;
                    probe_indices.push(Some(row_idx as u32));
                    build_indices.push(idx);

                    if P.build_side_outer {
                        // safety: bypass mutability checker
                        let map_joined = unsafe {
                            &mut *(&self.map_joined[idx.0] as *const BitVec as *mut BitVec)
                        };
                        map_joined.set(idx.1, true);
                    }
                }
            }
            if P.probe_side_outer && !found {
                probe_indices.push(Some(row_idx as u32));
                build_indices.push(Idx::default());
            }

            if probe_indices.len() >= self.join_params.batch_size {
                let probe_indices = std::mem::take(&mut probe_indices);
                let build_indices = std::mem::take(&mut build_indices);
                self.as_mut()
                    .flush(probed_batch.clone(), probe_indices, build_indices)
                    .await?;
            }
        }
        if !probe_indices.is_empty() {
            self.flush(probed_batch.clone(), probe_indices, build_indices)
                .await?;
        }
        Ok(())
    }

    async fn finish(mut self: Pin<&mut Self>) -> Result<()> {
        if P.build_side_outer {
            let probed_schema = match P.probe_side {
                L => self.join_params.left_schema.clone(),
                R => self.join_params.right_schema.clone(),
            };
            let probed_empty_batch = RecordBatch::new_empty(Arc::new(Schema::new(
                probed_schema
                    .fields()
                    .iter()
                    .map(|field| field.as_ref().clone().with_nullable(true))
                    .collect::<Vec<_>>(),
            )));
            let map_joined = std::mem::take(&mut self.map_joined);

            for (batch_idx, batch_joined) in map_joined.into_iter().enumerate() {
                let mut probe_indices: Vec<Option<u32>> = Vec::with_capacity(batch_joined.len());
                let mut build_indices: Vec<Idx> = Vec::with_capacity(batch_joined.len());

                for (row_idx, joined) in batch_joined.into_iter().enumerate() {
                    if !joined && self.map.inserted(batch_idx, row_idx) {
                        probe_indices.push(None);
                        build_indices.push((batch_idx, row_idx));
                    }
                }
                if !probe_indices.is_empty() {
                    self.as_mut()
                        .flush(probed_empty_batch.clone(), probe_indices, build_indices)
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
