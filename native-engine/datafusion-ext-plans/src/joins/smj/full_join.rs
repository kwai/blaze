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

use std::{cmp::Ordering, pin::Pin, sync::Arc};

use arrow::array::{RecordBatch, RecordBatchOptions};
use async_trait::async_trait;
use datafusion::common::Result;
use datafusion_ext_commons::arrow::selection::create_batch_interleaver;
use itertools::Itertools;

use crate::{
    common::execution_context::WrappedRecordBatchSender,
    compare_cursor, cur_forward,
    joins::{Idx, JoinParams, stream_cursor::StreamCursor},
    sort_merge_join_exec::Joiner,
};

pub struct FullJoiner<const L_OUTER: bool, const R_OUTER: bool> {
    join_params: JoinParams,
    output_sender: Arc<WrappedRecordBatchSender>,
    lindices: Vec<Idx>,
    rindices: Vec<Idx>,
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
            output_rows: 0,
        }
    }

    fn should_flush(&self) -> bool {
        self.lindices.len() >= self.join_params.batch_size
    }

    async fn flush(
        mut self: Pin<&mut Self>,
        cur1: &mut StreamCursor,
        cur2: &mut StreamCursor,
    ) -> Result<()> {
        let lindices = std::mem::take(&mut self.lindices);
        let rindices = std::mem::take(&mut self.rindices);
        let num_rows = lindices.len();
        assert_eq!(lindices.len(), rindices.len());

        let lbatch_interleaver = create_batch_interleaver(cur1.batches(), false)?;
        let rbatch_interleaver = create_batch_interleaver(cur2.batches(), false)?;
        let lcols = lbatch_interleaver(&lindices)?;
        let rcols = rbatch_interleaver(&rindices)?;

        let output_batch = RecordBatch::try_new_with_options(
            self.join_params.projection.schema.clone(),
            [lcols.columns(), rcols.columns()].concat(),
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )?;

        if output_batch.num_rows() > 0 {
            self.output_rows += output_batch.num_rows();
            self.output_sender.send(output_batch).await;
        }
        Ok(())
    }
}

#[async_trait]
impl<const L_OUTER: bool, const R_OUTER: bool> Joiner for FullJoiner<L_OUTER, R_OUTER> {
    async fn join(
        mut self: Pin<&mut Self>,
        cur1: &mut StreamCursor,
        cur2: &mut StreamCursor,
    ) -> Result<()> {
        let mut equal_lindices = vec![];
        let mut equal_rindices = vec![];

        while !cur1.finished() && !cur2.finished() {
            if self.should_flush()
                || cur1.num_buffered_batches() > 1
                || cur2.num_buffered_batches() > 1
            {
                self.as_mut().flush(cur1, cur2).await?;
                cur1.clean_out_dated_batches();
                cur2.clean_out_dated_batches();
            }

            match compare_cursor!(cur1, cur2) {
                Ordering::Less => {
                    if L_OUTER {
                        self.lindices.push(cur1.cur_idx());
                        self.rindices.push(Idx::default());
                    }
                    cur_forward!(cur1);
                }
                Ordering::Greater => {
                    if R_OUTER {
                        self.lindices.push(Idx::default());
                        self.rindices.push(cur2.cur_idx());
                    }
                    cur_forward!(cur2);
                }
                Ordering::Equal => {
                    equal_lindices.clear();
                    equal_rindices.clear();
                    equal_lindices.push(cur1.cur_idx());
                    equal_rindices.push(cur2.cur_idx());
                    let l_key_idx = cur1.cur_idx();
                    let r_key_idx = cur2.cur_idx();
                    cur_forward!(cur1);
                    cur_forward!(cur2);

                    // iterate both stream, find smaller one, use it for probing
                    let mut has_multi_equal = false;
                    let mut l_equal = true;
                    let mut r_equal = true;
                    while l_equal && r_equal {
                        if l_equal {
                            l_equal = !cur1.finished() && cur1.cur_key() == cur1.key(l_key_idx);
                            if l_equal {
                                has_multi_equal = true;
                                equal_lindices.push(cur1.cur_idx());
                                cur_forward!(cur1);
                            }
                        }
                        if r_equal {
                            r_equal = !cur2.finished() && cur2.cur_key() == cur2.key(r_key_idx);
                            if r_equal {
                                has_multi_equal = true;
                                equal_rindices.push(cur2.cur_idx());
                                cur_forward!(cur2);
                            }
                        }
                    }

                    // fast path for one-to-one join
                    if !has_multi_equal {
                        self.lindices.push(l_key_idx);
                        self.rindices.push(r_key_idx);
                        continue;
                    }

                    for (&lidx, &ridx) in equal_lindices.iter().cartesian_product(&equal_rindices) {
                        self.lindices.push(lidx);
                        self.rindices.push(ridx);
                    }

                    if r_equal {
                        // stream right side
                        while !cur2.finished() && cur2.cur_key() == cur1.key(l_key_idx) {
                            for &lidx in &equal_lindices {
                                self.lindices.push(lidx);
                                self.rindices.push(cur2.cur_idx());
                            }
                            cur_forward!(cur2);
                            if self.should_flush() || cur2.num_buffered_batches() > 1 {
                                self.as_mut().flush(cur1, cur2).await?;
                                cur2.clean_out_dated_batches();
                            }
                        }
                    }

                    if l_equal {
                        // stream left side
                        while !cur1.finished() && cur1.cur_key() == cur2.key(r_key_idx) {
                            for &ridx in &equal_rindices {
                                self.lindices.push(cur1.cur_idx());
                                self.rindices.push(ridx);
                            }
                            cur_forward!(cur1);
                            if self.should_flush() || cur1.num_buffered_batches() > 1 {
                                self.as_mut().flush(cur1, cur2).await?;
                                cur1.clean_out_dated_batches();
                            }
                        }
                    }
                }
            }
        }

        if !self.lindices.is_empty() {
            self.as_mut().flush(cur1, cur2).await?;
            cur1.clean_out_dated_batches();
            cur2.clean_out_dated_batches();
        }

        // at least one side is finished, consume the other side if it is an outer side
        while L_OUTER && !cur1.finished() {
            let lidx = cur1.cur_idx();
            self.lindices.push(lidx);
            self.rindices.push(Idx::default());
            cur_forward!(cur1);
            if self.should_flush() {
                self.as_mut().flush(cur1, cur2).await?;
                cur1.clean_out_dated_batches();
            }
        }
        while R_OUTER && !cur2.finished() {
            let ridx = cur2.cur_idx();
            self.lindices.push(Idx::default());
            self.rindices.push(ridx);
            cur_forward!(cur2);
            if self.should_flush() {
                self.as_mut().flush(cur1, cur2).await?;
                cur2.clean_out_dated_batches();
            }
        }
        if !self.lindices.is_empty() {
            self.flush(cur1, cur2).await?;
        }
        Ok(())
    }

    fn num_output_rows(&self) -> usize {
        self.output_rows
    }
}
