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

use arrow::{
    array::{RecordBatch, RecordBatchOptions},
    buffer::NullBuffer,
    datatypes::{Schema, SchemaRef},
    row::{Row, RowConverter, Rows, SortField},
};
use datafusion::{
    common::{JoinSide, Result},
    execution::SendableRecordBatchStream,
    physical_expr::PhysicalExprRef,
    physical_plan::metrics::Time,
};
use datafusion_ext_commons::array_size::ArraySize;
use futures::{Future, StreamExt};
use parking_lot::Mutex;

use crate::{
    common::batch_selection::take_batch_opt,
    joins::{Idx, JoinParams},
};

pub struct StreamCursor {
    stream: SendableRecordBatchStream,
    key_converter: Arc<Mutex<RowConverter>>,
    key_exprs: Vec<PhysicalExprRef>,
    poll_time: Time,

    // IMPORTANT:
    // batches/rows/null_buffers always contains a `null batch` in the front
    projection: Vec<usize>,
    pub projected_batch_schema: SchemaRef,
    pub projected_batches: Vec<RecordBatch>,
    pub cur_idx: Idx,
    min_reserved_idx: Idx,
    keys: Vec<Arc<Rows>>,
    key_has_nulls: Vec<Option<NullBuffer>>,
    num_null_batches: usize,
    mem_size: usize,
    pub finished: bool,
}

impl StreamCursor {
    pub fn try_new(
        stream: SendableRecordBatchStream,
        join_params: &JoinParams,
        join_side: JoinSide,
        projection: &[usize],
    ) -> Result<Self> {
        let key_converter = Arc::new(Mutex::new(RowConverter::new(
            join_params
                .key_data_types
                .iter()
                .cloned()
                .zip(&join_params.sort_options)
                .map(|(dt, options)| SortField::new_with_options(dt, *options))
                .collect(),
        )?));
        let key_exprs = match join_side {
            JoinSide::Left => join_params.left_keys.clone(),
            JoinSide::Right => join_params.right_keys.clone(),
        };

        let empty_batch = RecordBatch::new_empty(Arc::new(Schema::new(
            stream
                .schema()
                .fields()
                .iter()
                .map(|f| f.as_ref().clone().with_nullable(true))
                .collect::<Vec<_>>(),
        )));
        let empty_keys = Arc::new(
            key_converter.lock().convert_columns(
                &key_exprs
                    .iter()
                    .map(|key| Ok(key.evaluate(&empty_batch)?.into_array(0)?))
                    .collect::<Result<Vec<_>>>()?,
            )?,
        );
        let null_batch = take_batch_opt(empty_batch, [Option::<usize>::None])?;
        let projected_null_batch = null_batch.project(projection)?;
        let null_nb = NullBuffer::new_null(1);

        Ok(Self {
            stream,
            key_exprs,
            key_converter,
            poll_time: Time::new(),
            projection: projection.to_vec(),
            projected_batch_schema: projected_null_batch.schema(),
            projected_batches: vec![projected_null_batch],
            cur_idx: (0, 0),
            min_reserved_idx: (0, 0),
            keys: vec![empty_keys],
            key_has_nulls: vec![Some(null_nb)],
            num_null_batches: 1,
            mem_size: 0,
            finished: false,
        })
    }

    pub fn next(&mut self) -> Option<impl Future<Output = Result<()>> + '_> {
        self.cur_idx.1 += 1;
        if self.cur_idx.1 >= self.projected_batches[self.cur_idx.0].num_rows() {
            self.cur_idx.0 += 1;
            self.cur_idx.1 = 0;
        }

        let should_load_next_batch = self.cur_idx.0 >= self.projected_batches.len();
        if should_load_next_batch {
            Some(async move {
                while let Some(batch) = {
                    let timer = self.poll_time.timer();
                    let batch = self.stream.next().await.transpose()?;
                    drop(timer);
                    batch
                } {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let key_columns = self
                        .key_exprs
                        .iter()
                        .map(|key| Ok(key.evaluate(&batch)?.into_array(batch.num_rows())?))
                        .collect::<Result<Vec<_>>>()?;
                    let key_has_nulls = key_columns
                        .iter()
                        .map(|c| c.nulls().cloned())
                        .reduce(|lhs, rhs| NullBuffer::union(lhs.as_ref(), rhs.as_ref()))
                        .unwrap_or(None);
                    let keys = Arc::new(self.key_converter.lock().convert_columns(&key_columns)?);

                    self.mem_size += batch.get_array_mem_size();
                    self.mem_size += key_has_nulls
                        .as_ref()
                        .map(|nb| nb.buffer().len())
                        .unwrap_or_default();
                    self.mem_size += keys.size();

                    self.projected_batches
                        .push(RecordBatch::try_new_with_options(
                            self.projected_batches[0].schema(),
                            self.projection
                                .iter()
                                .map(|&i| batch.column(i).clone())
                                .collect(),
                            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
                        )?);
                    self.key_has_nulls.push(key_has_nulls);
                    self.keys.push(keys);

                    // fill out-dated batches with null batches
                    if self.num_null_batches < self.min_reserved_idx.0 {
                        for i in self.num_null_batches..self.min_reserved_idx.0 {
                            self.mem_size -= self.projected_batches[i].get_array_mem_size();
                            self.mem_size -= self.key_has_nulls[i]
                                .as_ref()
                                .map(|nb| nb.buffer().len())
                                .unwrap_or_default();
                            self.mem_size -= self.keys[i].size();

                            self.projected_batches[i] = self.projected_batches[0].clone();
                            self.keys[i] = self.keys[0].clone();
                            self.key_has_nulls[i] = self.key_has_nulls[0].clone();
                            self.num_null_batches += 1;
                        }
                    }
                    return Ok(());
                }
                self.finished = true;
                return Ok(());
            })
        } else {
            None
        }
    }

    #[inline]
    pub fn is_null_key(&self, idx: Idx) -> bool {
        self.key_has_nulls[idx.0]
            .as_ref()
            .map(|nb| nb.is_null(idx.1))
            .unwrap_or(false)
    }

    #[inline]
    pub fn key<'a>(&'a self, idx: Idx) -> Row<'a> {
        let keys = &self.keys[idx.0];
        keys.row(idx.1)
    }

    #[inline]
    pub fn num_buffered_batches(&self) -> usize {
        self.projected_batches.len() - self.num_null_batches
    }

    #[inline]
    pub fn mem_size(&self) -> usize {
        self.mem_size
    }

    #[inline]
    pub fn set_min_reserved_idx(&mut self, idx: Idx) {
        self.min_reserved_idx = idx;
    }

    #[inline]
    pub fn total_poll_time(&self) -> usize {
        self.poll_time.value()
    }
}

#[macro_export]
macro_rules! cur_forward {
    ($cur:expr) => {{
        if let Some(fut) = $cur.next() {
            fut.await?;
        }
    }};
}
