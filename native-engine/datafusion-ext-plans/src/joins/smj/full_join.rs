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

use std::{cmp::Ordering, pin::Pin, sync::Arc};

use arrow::array::{RecordBatch, RecordBatchOptions};
use async_trait::async_trait;
use datafusion::{common::Result, physical_plan::metrics::Time};
use datafusion_ext_commons::suggested_output_batch_mem_size;
use smallvec::{smallvec, SmallVec};

use crate::{
    common::{batch_selection::interleave_batches, output::WrappedRecordBatchSender},
    compare_cursor, cur_forward,
    joins::{Idx, JoinParams, StreamCursors},
    sort_merge_join_exec::Joiner,
};

pub struct FullJoiner<const L_OUTER: bool, const R_OUTER: bool> {
    join_params: JoinParams,
    output_sender: Arc<WrappedRecordBatchSender>,
    lindices: Vec<Idx>,
    rindices: Vec<Idx>,
    send_output_time: Time,
    output_rows: usize,
}

pub type InnerJoiner = FullJoiner<false, false>;
pub type LeftOuterJoiner = FullJoiner<true, false>;
pub type RightOuterJoiner = FullJoiner<false, true>;
pub type FullOuterJoiner = FullJoiner<true, true>;

impl<const L_OUTER: bool, const R_OUTER: bool> FullJoiner<L_OUTER, R_OUTER> {
    pub fn new(join_params: JoinParams, output_sender: Arc<WrappedRecordBatchSender>) -> Self {
        Self {
            join_params,
            output_sender,
            lindices: vec![],
            rindices: vec![],
            send_output_time: Time::new(),
            output_rows: 0,
        }
    }

    fn should_flush(&self, curs: &StreamCursors) -> bool {
        if self.lindices.len() >= self.join_params.batch_size {
            return true;
        }

        if curs.0.num_buffered_batches() + curs.1.num_buffered_batches() >= 6
            && curs.0.mem_size() + curs.1.mem_size() > suggested_output_batch_mem_size()
        {
            if let Some(first_lidx) = self.lindices.first() {
                if first_lidx.0 < curs.0.cur_idx.0 {
                    return true;
                }
            }
            if let Some(first_ridx) = self.rindices.first() {
                if first_ridx.0 < curs.1.cur_idx.0 {
                    return true;
                }
            }
        }
        false
    }

    async fn flush(mut self: Pin<&mut Self>, curs: &mut StreamCursors) -> Result<()> {
        let lindices = std::mem::take(&mut self.lindices);
        let rindices = std::mem::take(&mut self.rindices);
        let num_rows = lindices.len();
        assert_eq!(lindices.len(), rindices.len());

        let lcols = interleave_batches(
            curs.0.projected_batch_schema.clone(),
            &curs.0.projected_batches,
            &lindices,
        )?;
        let rcols = interleave_batches(
            curs.1.projected_batch_schema.clone(),
            &curs.1.projected_batches,
            &rindices,
        )?;
        let output_batch = RecordBatch::try_new_with_options(
            self.join_params.projection.schema.clone(),
            [lcols.columns(), rcols.columns()].concat(),
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )?;

        if output_batch.num_rows() > 0 {
            self.output_rows += output_batch.num_rows();

            let timer = self.send_output_time.timer();
            self.output_sender.send(Ok(output_batch), None).await;
            drop(timer);
        }
        Ok(())
    }
}

#[async_trait]
impl<const L_OUTER: bool, const R_OUTER: bool> Joiner for FullJoiner<L_OUTER, R_OUTER> {
    async fn join(mut self: Pin<&mut Self>, curs: &mut StreamCursors) -> Result<()> {
        while !curs.0.finished && !curs.1.finished {
            let mut lidx = curs.0.cur_idx;
            let mut ridx = curs.1.cur_idx;
            match compare_cursor!(curs) {
                Ordering::Less => {
                    if L_OUTER {
                        self.lindices.push(lidx);
                        self.rindices.push(Idx::default());
                    }
                    cur_forward!(curs.0);
                    if self.should_flush(curs) {
                        self.as_mut().flush(curs).await?;
                    }
                    curs.0
                        .set_min_reserved_idx(*self.lindices.first().unwrap_or(&lidx));
                }
                Ordering::Greater => {
                    if R_OUTER {
                        self.lindices.push(Idx::default());
                        self.rindices.push(ridx);
                    }
                    cur_forward!(curs.1);
                    if self.should_flush(curs) {
                        self.as_mut().flush(curs).await?;
                    }
                    curs.1
                        .set_min_reserved_idx(*self.rindices.first().unwrap_or(&ridx));
                }
                Ordering::Equal => {
                    cur_forward!(curs.0);
                    cur_forward!(curs.1);
                    self.lindices.push(lidx);
                    self.rindices.push(ridx);

                    let mut equal_lindices: SmallVec<[Idx; 16]> = smallvec![lidx];
                    let mut equal_rindices: SmallVec<[Idx; 16]> = smallvec![ridx];
                    let mut last_lidx = lidx;
                    let mut last_ridx = ridx;
                    lidx = curs.0.cur_idx;
                    ridx = curs.1.cur_idx;
                    let mut l_equal = !curs.0.finished && curs.0.key(lidx) == curs.0.key(last_lidx);
                    let mut r_equal = !curs.1.finished && curs.1.key(ridx) == curs.1.key(last_ridx);

                    while l_equal || r_equal {
                        if l_equal {
                            for &ridx in &equal_rindices {
                                self.lindices.push(lidx);
                                self.rindices.push(ridx);
                            }
                            if r_equal {
                                equal_lindices.push(lidx);
                            }
                            cur_forward!(curs.0);
                            last_lidx = lidx;
                            lidx = curs.0.cur_idx;
                        } else {
                            curs.1
                                .set_min_reserved_idx(*self.rindices.first().unwrap_or(&last_ridx));
                        }

                        if r_equal {
                            for &lidx in &equal_lindices {
                                self.lindices.push(lidx);
                                self.rindices.push(ridx);
                            }
                            if l_equal {
                                equal_rindices.push(ridx);
                            }
                            cur_forward!(curs.1);
                            last_ridx = ridx;
                            ridx = curs.1.cur_idx;
                        } else {
                            curs.0
                                .set_min_reserved_idx(*self.lindices.first().unwrap_or(&last_lidx));
                        }

                        if self.should_flush(curs) {
                            self.as_mut().flush(curs).await?;
                        }
                        l_equal = l_equal
                            && !curs.0.finished
                            && curs.0.key(lidx) == curs.0.key(last_lidx);
                        r_equal = r_equal
                            && !curs.1.finished
                            && curs.1.key(ridx) == curs.1.key(last_ridx);
                    }

                    if self.should_flush(curs) {
                        self.as_mut().flush(curs).await?;
                    }
                    curs.0
                        .set_min_reserved_idx(*self.lindices.first().unwrap_or(&curs.0.cur_idx));
                    curs.1
                        .set_min_reserved_idx(*self.rindices.first().unwrap_or(&curs.1.cur_idx));
                }
            }
        }

        // at least one side is finished, consume the other side if it is an outer side
        while L_OUTER && !curs.0.finished {
            let lidx = curs.0.cur_idx;
            self.lindices.push(lidx);
            self.rindices.push(Idx::default());
            cur_forward!(curs.0);
            if self.should_flush(curs) {
                self.as_mut().flush(curs).await?;
            }
            curs.0
                .set_min_reserved_idx(*self.lindices.first().unwrap_or(&lidx));
        }
        while R_OUTER && !curs.1.finished {
            let ridx = curs.1.cur_idx;
            self.lindices.push(Idx::default());
            self.rindices.push(ridx);
            cur_forward!(curs.1);
            if self.should_flush(curs) {
                self.as_mut().flush(curs).await?;
            }
            curs.1
                .set_min_reserved_idx(*self.rindices.first().unwrap_or(&ridx));
        }
        if !self.lindices.is_empty() {
            self.flush(curs).await?;
        }
        Ok(())
    }

    fn total_send_output_time(&self) -> usize {
        self.send_output_time.value()
    }

    fn num_output_rows(&self) -> usize {
        self.output_rows
    }
}
