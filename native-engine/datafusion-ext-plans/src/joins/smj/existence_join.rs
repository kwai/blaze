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

use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use async_trait::async_trait;
use datafusion::common::Result;
use datafusion_ext_commons::arrow::selection::create_batch_interleaver;

use crate::{
    common::execution_context::WrappedRecordBatchSender,
    compare_cursor, cur_forward,
    joins::{Idx, JoinParams, StreamCursors},
    sort_merge_join_exec::Joiner,
};

pub struct ExistenceJoiner {
    join_params: JoinParams,
    output_sender: Arc<WrappedRecordBatchSender>,
    indices: Vec<Idx>,
    exists: Vec<bool>,
    output_rows: usize,
}

impl ExistenceJoiner {
    pub fn new(join_params: JoinParams, output_sender: Arc<WrappedRecordBatchSender>) -> Self {
        Self {
            join_params,
            output_sender,
            indices: vec![],
            exists: vec![],
            output_rows: 0,
        }
    }

    fn should_flush(&self) -> bool {
        self.indices.len() >= self.join_params.batch_size
    }

    async fn flush(mut self: Pin<&mut Self>, curs: &mut StreamCursors) -> Result<()> {
        let indices = std::mem::take(&mut self.indices);
        let num_rows = indices.len();
        let batch_interleaver = create_batch_interleaver(&curs.0.projected_batches, false)?;
        let cols = batch_interleaver(&indices)?;

        let exists = std::mem::take(&mut self.exists);
        let exists_col: ArrayRef = Arc::new(arrow::array::BooleanArray::from(exists));

        let output_batch = RecordBatch::try_new_with_options(
            self.join_params.output_schema.clone(),
            [cols.columns().to_vec(), vec![exists_col]].concat(),
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
impl Joiner for ExistenceJoiner {
    async fn join(mut self: Pin<&mut Self>, curs: &mut StreamCursors) -> Result<()> {
        while !curs.0.finished && !curs.1.finished {
            if self.should_flush()
                || curs.0.num_buffered_batches() > 1
                || curs.1.num_buffered_batches() > 1
            {
                self.as_mut().flush(curs).await?;
                curs.0.clean_out_dated_batches();
                curs.1.clean_out_dated_batches();
            }

            match compare_cursor!(curs) {
                Ordering::Less => {
                    self.indices.push(curs.0.cur_idx);
                    self.exists.push(false);
                    cur_forward!(curs.0);
                }
                Ordering::Greater => {
                    cur_forward!(curs.1);
                }
                Ordering::Equal => {
                    let l_key_idx = curs.0.cur_idx;
                    let r_key_idx = curs.1.cur_idx;

                    self.indices.push(curs.0.cur_idx);
                    self.exists.push(true);
                    cur_forward!(curs.0);
                    cur_forward!(curs.1);

                    // iterate both stream, find smaller one, use it for probing
                    let mut l_equal = true;
                    let mut r_equal = true;
                    while l_equal && r_equal {
                        if l_equal {
                            l_equal = !curs.0.finished && curs.0.cur_key() == curs.0.key(l_key_idx);
                            if l_equal {
                                self.indices.push(curs.0.cur_idx);
                                self.exists.push(true);
                                cur_forward!(curs.0);
                            }
                        }
                        if r_equal {
                            r_equal = !curs.1.finished && curs.1.cur_key() == curs.1.key(r_key_idx);
                            if r_equal {
                                cur_forward!(curs.1);
                            }
                        }
                    }

                    if l_equal {
                        // stream left side
                        while !curs.0.finished && curs.0.cur_key() == curs.1.key(r_key_idx) {
                            self.indices.push(curs.0.cur_idx);
                            self.exists.push(true);
                            cur_forward!(curs.0);
                            if self.should_flush() || curs.0.num_buffered_batches() > 1 {
                                self.as_mut().flush(curs).await?;
                                curs.0.clean_out_dated_batches();
                            }
                        }
                    }

                    if r_equal {
                        // stream right side
                        while !curs.1.finished && curs.1.cur_key() == curs.0.key(l_key_idx) {
                            cur_forward!(curs.1);
                            if self.should_flush() || curs.1.num_buffered_batches() > 1 {
                                self.as_mut().flush(curs).await?;
                                curs.1.clean_out_dated_batches();
                            }
                        }
                    }
                }
            }
        }

        while !curs.0.finished {
            self.indices.push(curs.0.cur_idx);
            self.exists.push(false);
            cur_forward!(curs.0);
            if self.should_flush() {
                self.as_mut().flush(curs).await?;
                curs.0.clean_out_dated_batches();
            }
        }
        if !self.indices.is_empty() {
            self.flush(curs).await?;
        }
        Ok(())
    }

    fn num_output_rows(&self) -> usize {
        self.output_rows
    }
}
