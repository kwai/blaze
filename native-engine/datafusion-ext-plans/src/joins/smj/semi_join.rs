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

use crate::{
    common::{batch_selection::interleave_batches, output::WrappedRecordBatchSender},
    compare_cursor, cur_forward,
    joins::{
        smj::semi_join::SemiJoinSide::{L, R},
        Idx, JoinParams, StreamCursors,
    },
    sort_merge_join_exec::Joiner,
};

#[derive(std::marker::ConstParamTy, Clone, Copy, PartialEq, Eq)]
pub enum SemiJoinSide {
    L,
    R,
}

#[derive(std::marker::ConstParamTy, Clone, Copy, PartialEq, Eq)]
pub struct JoinerParams {
    join_side: SemiJoinSide,
    semi: bool,
}

impl JoinerParams {
    const fn new(join_side: SemiJoinSide, semi: bool) -> Self {
        Self { join_side, semi }
    }
}
pub struct SemiJoiner<const P: JoinerParams> {
    join_params: JoinParams,
    output_sender: Arc<WrappedRecordBatchSender>,
    indices: Vec<Idx>,
    send_output_time: Time,
    output_rows: usize,
}

const LEFT_SEMI: JoinerParams = JoinerParams::new(L, true);
const LEFT_ANTI: JoinerParams = JoinerParams::new(L, false);
const RIGHT_SEMI: JoinerParams = JoinerParams::new(R, true);
const RIGHT_ANTI: JoinerParams = JoinerParams::new(R, false);

pub type LeftSemiJoiner = SemiJoiner<LEFT_SEMI>;
pub type LeftAntiJoiner = SemiJoiner<LEFT_ANTI>;
pub type RightSemiJoiner = SemiJoiner<RIGHT_SEMI>;
pub type RightAntiJoiner = SemiJoiner<RIGHT_ANTI>;

impl<const P: JoinerParams> SemiJoiner<P> {
    pub fn new(join_params: JoinParams, output_sender: Arc<WrappedRecordBatchSender>) -> Self {
        Self {
            join_params,
            output_sender,
            indices: vec![],
            send_output_time: Time::new(),
            output_rows: 0,
        }
    }

    fn should_flush(&self, curs: &StreamCursors) -> bool {
        if self.indices.len() >= self.join_params.batch_size {
            return true;
        }

        if curs.0.num_buffered_batches() + curs.1.num_buffered_batches() >= 6
            && curs.0.mem_size() + curs.1.mem_size() > suggested_output_batch_mem_size()
        {
            if let Some(first_idx) = self.indices.first() {
                let cur_idx = match P.join_side {
                    L => curs.0.cur_idx,
                    R => curs.1.cur_idx,
                };
                if first_idx.0 < cur_idx.0 {
                    return true;
                }
            }
        }
        false
    }

    async fn flush(mut self: Pin<&mut Self>, curs: &mut StreamCursors) -> Result<()> {
        let indices = std::mem::take(&mut self.indices);
        let num_rows = indices.len();

        let cols = match P.join_side {
            L => interleave_batches(
                curs.0.projected_batch_schema.clone(),
                &curs.0.projected_batches,
                &indices,
            )?,
            R => interleave_batches(
                curs.1.projected_batch_schema.clone(),
                &curs.1.projected_batches,
                &indices,
            )?,
        };
        let output_batch = RecordBatch::try_new_with_options(
            self.join_params.projection.schema.clone(),
            cols.columns().to_vec(),
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
impl<const P: JoinerParams> Joiner for SemiJoiner<P> {
    async fn join(mut self: Pin<&mut Self>, curs: &mut StreamCursors) -> Result<()> {
        while !curs.0.finished && !curs.1.finished {
            let mut lidx = curs.0.cur_idx;
            let mut ridx = curs.1.cur_idx;

            match compare_cursor!(curs) {
                Ordering::Less => {
                    if P.join_side == L && !P.semi {
                        self.indices.push(lidx);
                    }
                    cur_forward!(curs.0);
                    if self.should_flush(curs) {
                        self.as_mut().flush(curs).await?;
                    }
                    curs.0.set_min_reserved_idx(match P.join_side {
                        L => *self.indices.first().unwrap_or(&lidx),
                        R => lidx,
                    });
                }
                Ordering::Greater => {
                    if P.join_side == R && !P.semi {
                        self.indices.push(ridx);
                    }
                    cur_forward!(curs.1);
                    if self.should_flush(curs) {
                        self.as_mut().flush(curs).await?;
                    }
                    curs.1.set_min_reserved_idx(match P.join_side {
                        L => ridx,
                        R => *self.indices.first().unwrap_or(&ridx),
                    });
                }
                Ordering::Equal => {
                    // output/skip left equal rows
                    loop {
                        if P.join_side == L && P.semi {
                            self.indices.push(lidx);
                            if self.should_flush(curs) {
                                self.as_mut().flush(curs).await?;
                            }
                        }
                        cur_forward!(curs.0);
                        curs.0.set_min_reserved_idx(match P.join_side {
                            L => *self.indices.first().unwrap_or(&lidx),
                            R => lidx,
                        });

                        if !curs.0.finished && curs.0.key(curs.0.cur_idx) == curs.0.key(lidx) {
                            lidx = curs.0.cur_idx;
                            continue;
                        }
                        break;
                    }

                    // output/skip right equal rows
                    loop {
                        if P.join_side == R && P.semi {
                            self.indices.push(ridx);
                            if self.should_flush(curs) {
                                self.as_mut().flush(curs).await?;
                            }
                        }
                        cur_forward!(curs.1);
                        curs.1.set_min_reserved_idx(match P.join_side {
                            L => ridx,
                            R => *self.indices.first().unwrap_or(&ridx),
                        });

                        if !curs.1.finished && curs.1.key(curs.1.cur_idx) == curs.1.key(ridx) {
                            ridx = curs.1.cur_idx;
                            continue;
                        }
                        break;
                    }
                }
            }
        }

        // at least one side is finished, consume the other side if it is an anti side
        if !P.semi {
            while P.join_side == L && !P.semi && !curs.0.finished {
                let lidx = curs.0.cur_idx;
                self.indices.push(lidx);
                cur_forward!(curs.0);
                if self.should_flush(curs) {
                    self.as_mut().flush(curs).await?;
                }
                curs.0.set_min_reserved_idx(match P.join_side {
                    L => *self.indices.first().unwrap_or(&lidx),
                    R => lidx,
                });
            }
            while P.join_side == R && !P.semi && !curs.1.finished {
                let ridx = curs.1.cur_idx;
                self.indices.push(ridx);
                cur_forward!(curs.1);
                if self.should_flush(curs) {
                    self.as_mut().flush(curs).await?;
                }
                curs.1.set_min_reserved_idx(match P.join_side {
                    L => ridx,
                    R => *self.indices.first().unwrap_or(&ridx),
                });
            }
        }
        if !self.indices.is_empty() {
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
