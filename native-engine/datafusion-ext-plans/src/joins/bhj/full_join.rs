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
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::Relaxed},
    },
};

use arrow::{
    array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, UInt32Array, new_null_array},
    buffer::NullBuffer,
};
use async_trait::async_trait;
use bitvec::{bitvec, prelude::BitVec};
use datafusion::{common::Result, physical_plan::metrics::Time};
use datafusion_ext_commons::{
    arrow::{eq_comparator::EqComparator, selection::take_cols},
    likely,
};

use crate::{
    broadcast_join_exec::Joiner,
    common::{execution_context::WrappedRecordBatchSender, timer_helper::TimerHelper},
    joins::{
        JoinParams,
        bhj::{
            ProbeSide,
            full_join::ProbeSide::{L, R},
        },
        join_hash_map::{JoinHashMap, join_create_hashes},
    },
};

#[derive(std::marker::ConstParamTy, Clone, Copy, PartialEq, Eq)]
pub struct JoinerParams {
    probe_side: ProbeSide,
    probe_side_outer: bool,
    build_side_outer: bool,
}

impl JoinerParams {
    pub const fn new(
        probe_side: ProbeSide,
        probe_side_outer: bool,
        build_side_outer: bool,
    ) -> Self {
        Self {
            probe_side,
            probe_side_outer,
            build_side_outer,
        }
    }
}

pub const LEFT_PROBED_INNER: JoinerParams = JoinerParams::new(L, false, false);
pub const LEFT_PROBED_LEFT: JoinerParams = JoinerParams::new(L, true, false);
pub const LEFT_PROBED_RIGHT: JoinerParams = JoinerParams::new(L, false, true);
pub const LEFT_PROBED_OUTER: JoinerParams = JoinerParams::new(L, true, true);

pub const RIGHT_PROBED_INNER: JoinerParams = JoinerParams::new(R, false, false);
pub const RIGHT_PROBED_LEFT: JoinerParams = JoinerParams::new(R, false, true);
pub const RIGHT_PROBED_RIGHT: JoinerParams = JoinerParams::new(R, true, false);
pub const RIGHT_PROBED_OUTER: JoinerParams = JoinerParams::new(R, true, true);

pub type LProbedInnerJoiner = FullJoiner<LEFT_PROBED_INNER>;
pub type LProbedLeftJoiner = FullJoiner<LEFT_PROBED_LEFT>;
pub type LProbedRightJoiner = FullJoiner<LEFT_PROBED_RIGHT>;
pub type LProbedFullOuterJoiner = FullJoiner<LEFT_PROBED_OUTER>;
pub type RProbedInnerJoiner = FullJoiner<RIGHT_PROBED_INNER>;
pub type RProbedLeftJoiner = FullJoiner<RIGHT_PROBED_LEFT>;
pub type RProbedRightJoiner = FullJoiner<RIGHT_PROBED_RIGHT>;
pub type RProbedFullOuterJoiner = FullJoiner<RIGHT_PROBED_OUTER>;

pub struct FullJoiner<const P: JoinerParams> {
    join_params: JoinParams,
    output_sender: Arc<WrappedRecordBatchSender>,
    map: Arc<JoinHashMap>,
    map_joined: BitVec,
    output_rows: AtomicUsize,
}

impl<const P: JoinerParams> FullJoiner<P> {
    pub fn new(
        join_params: JoinParams,
        map: Arc<JoinHashMap>,
        output_sender: Arc<WrappedRecordBatchSender>,
    ) -> Self {
        let map_joined = bitvec![0; map.data_batch().num_rows()];
        Self {
            join_params,
            output_sender,
            map,
            map_joined,
            output_rows: AtomicUsize::new(0),
        }
    }

    fn create_probed_key_columns(&self, probed_batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
        let probed_key_exprs = match P.probe_side {
            L => &self.join_params.left_keys,
            R => &self.join_params.right_keys,
        };
        let probed_key_columns: Vec<ArrayRef> = probed_key_exprs
            .iter()
            .map(|expr| {
                Ok(expr
                    .evaluate(probed_batch)?
                    .into_array(probed_batch.num_rows())?)
            })
            .collect::<Result<_>>()?;
        Ok(probed_key_columns)
    }

    async fn flush(
        &self,
        probe_cols: Vec<ArrayRef>,
        build_cols: Vec<ArrayRef>,
        num_rows: usize,
    ) -> Result<()> {
        let output_batch = RecordBatch::try_new_with_options(
            self.join_params.output_schema.clone(),
            match P.probe_side {
                L => [probe_cols, build_cols].concat(),
                R => [build_cols, probe_cols].concat(),
            },
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )?;
        self.output_rows.fetch_add(output_batch.num_rows(), Relaxed);
        self.output_sender.send(output_batch).await;
        Ok(())
    }

    async fn flush_hash_joined(
        mut self: Pin<&mut Self>,
        probed_batch: &RecordBatch,
        hash_joined_probe_indices: Vec<u32>,
        hash_joined_build_inner_indices: Vec<u32>,
        hash_joined_build_outer_indices: Vec<Option<u32>>,
        build_output_time: &Time,
    ) -> Result<()> {
        let _build_output_timer = build_output_time.timer();
        let probe_indices = hash_joined_probe_indices;
        let build_indices: UInt32Array = if P.probe_side_outer {
            hash_joined_build_outer_indices.into()
        } else {
            hash_joined_build_inner_indices.into()
        };
        let num_rows = probe_indices.len();

        assert_eq!(probe_indices.len(), build_indices.len());

        let pprojected = match P.probe_side {
            L => self
                .join_params
                .projection
                .project_left(probed_batch.columns()),
            R => self
                .join_params
                .projection
                .project_right(probed_batch.columns()),
        };
        let mprojected = match P.probe_side {
            L => self
                .join_params
                .projection
                .project_right(self.map.data_batch().columns()),
            R => self
                .join_params
                .projection
                .project_left(self.map.data_batch().columns()),
        };

        // fast path for the case where every probed records have 1-to-1 joined
        let pcols = if probe_indices.len() == probed_batch.num_rows()
            && probe_indices
                .iter()
                .zip(0..probed_batch.num_rows() as u32)
                .all(|(&idx, i)| idx == i)
        {
            pprojected
        } else {
            take_cols(&pprojected, probe_indices)?
        };

        if P.build_side_outer {
            for idx in build_indices.iter().flatten() {
                self.map_joined.set(idx as usize, true);
            }
        }
        let bcols = take_cols(&mprojected, build_indices)?;

        build_output_time
            .exclude_timer_async(self.flush(pcols, bcols, num_rows))
            .await?;
        Ok(())
    }
}

#[async_trait]
impl<const P: JoinerParams> Joiner for FullJoiner<P> {
    async fn join(
        mut self: Pin<&mut Self>,
        probed_batch: RecordBatch,
        probed_side_hash_time: &Time,
        probed_side_search_time: &Time,
        probed_side_compare_time: &Time,
        build_output_time: &Time,
    ) -> Result<()> {
        let mut hash_joined_probe_indices = vec![];
        let mut hash_joined_build_inner_indices = vec![];
        let mut hash_joined_build_outer_indices = vec![];

        let batch_size = self.join_params.batch_size.max(probed_batch.num_rows());
        let probed_key_columns = self.create_probed_key_columns(&probed_batch)?;
        let probed_hashes = probed_side_hash_time
            .with_timer(|| join_create_hashes(probed_batch.num_rows(), &probed_key_columns));

        let map = self.map.clone();
        let eq = EqComparator::try_new(&probed_key_columns, map.key_columns())?;

        let probed_valids = probed_key_columns
            .iter()
            .map(|col| col.logical_nulls())
            .reduce(|nb1, nb2| NullBuffer::union(nb1.as_ref(), nb2.as_ref()))
            .flatten();

        let map_values = probed_side_search_time.with_timer(|| {
            let probed_hashes = if let Some(probed_valids) = &probed_valids {
                probed_hashes
                    .iter()
                    .enumerate()
                    .filter_map(|(row_idx, &hash)| probed_valids.is_valid(row_idx).then_some(hash))
                    .collect()
            } else {
                probed_hashes
            };
            map.lookup_many(probed_hashes)
        });

        let _probed_side_compare_timer = probed_side_compare_time.timer();
        let mut hashes_idx = 0;

        for row_idx in 0..probed_batch.num_rows() {
            let mut joined = false;

            if probed_valids
                .as_ref()
                .map(|nb| nb.is_valid(row_idx))
                .unwrap_or(true)
            {
                let map_value = map_values[hashes_idx];
                hashes_idx += 1;

                let mut join = |map_idx| {
                    if likely!(eq.eq(row_idx, map_idx as usize)) {
                        if P.probe_side_outer {
                            hash_joined_probe_indices.push(row_idx as u32);
                            hash_joined_build_outer_indices.push(Some(map_idx));
                        } else {
                            hash_joined_probe_indices.push(row_idx as u32);
                            hash_joined_build_inner_indices.push(map_idx);
                        }
                        joined = true;
                    }
                };

                match map_value {
                    map_value if map_value.is_single() => {
                        join(map_value.get_single());
                    }
                    map_value if map_value.is_range() => {
                        for &map_idx in map.get_range(map_value) {
                            join(map_idx);
                        }
                    }
                    _ => {} // map_value.is_empty
                }
            }

            if P.probe_side_outer && !joined {
                hash_joined_probe_indices.push(row_idx as u32);
                hash_joined_build_outer_indices.push(None);
            }

            if hash_joined_probe_indices.len() > batch_size {
                probed_side_compare_time
                    .exclude_timer_async(self.as_mut().flush_hash_joined(
                        &probed_batch,
                        std::mem::take(&mut hash_joined_probe_indices),
                        std::mem::take(&mut hash_joined_build_inner_indices),
                        std::mem::take(&mut hash_joined_build_outer_indices),
                        build_output_time,
                    ))
                    .await?;
            }
        }

        if !hash_joined_probe_indices.is_empty() {
            probed_side_compare_time
                .exclude_timer_async(self.as_mut().flush_hash_joined(
                    &probed_batch,
                    hash_joined_probe_indices,
                    hash_joined_build_inner_indices,
                    hash_joined_build_outer_indices,
                    build_output_time,
                ))
                .await?;
        }
        Ok(())
    }

    async fn finish(mut self: Pin<&mut Self>, build_output_time: &Time) -> Result<()> {
        let _build_output_timer = build_output_time.timer();

        // output unjoined rows of probed side
        let map_joined = std::mem::take(&mut self.map_joined);
        if P.build_side_outer {
            let map_unjoined_indices = map_joined
                .into_iter()
                .enumerate()
                .filter(|(_, joined)| !joined)
                .map(|(idx, _)| idx as u32)
                .collect::<Vec<_>>();

            let pschema = match P.probe_side {
                L => &self.join_params.left_schema,
                R => &self.join_params.right_schema,
            };
            let mprojected = match P.probe_side {
                L => self
                    .join_params
                    .projection
                    .project_right(self.map.data_batch().columns()),
                R => self
                    .join_params
                    .projection
                    .project_left(self.map.data_batch().columns()),
            };

            let num_rows = map_unjoined_indices.len();
            let pcols = pschema
                .fields()
                .iter()
                .map(|field| new_null_array(field.data_type(), num_rows))
                .collect::<Vec<_>>();
            let bcols = take_cols(&mprojected, map_unjoined_indices)?;
            build_output_time
                .exclude_timer_async(self.as_mut().flush(pcols, bcols, num_rows))
                .await?;
        }
        Ok(())
    }

    fn can_early_stop(&self) -> bool {
        if !P.probe_side_outer {
            return self.map.is_all_nulls();
        }
        false
    }

    fn num_output_rows(&self) -> usize {
        self.output_rows.load(Relaxed)
    }
}
