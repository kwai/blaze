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
use datafusion::physical_plan::EmptyRecordBatchStream;
use datafusion_ext_commons::arrow::selection::take_batch;
use futures::{Future, StreamExt};
use parking_lot::Mutex;

use crate::{
    common::timer_helper::TimerHelper,
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
    keys: Vec<Arc<Rows>>,
    key_has_nulls: Vec<Option<NullBuffer>>,
    pub finished: bool,
    pub key_rows_batches: Option<Vec<(RecordBatch, Rows)>>,
}

impl StreamCursor {
    pub fn try_new(
        stream: SendableRecordBatchStream,
        poll_time: Time,
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
        let null_batch = take_batch(empty_batch, vec![Option::<u32>::None])?;
        let projected_null_batch = null_batch.project(projection)?;
        let null_nb = NullBuffer::new_null(1);

        Ok(Self {
            stream,
            key_exprs,
            key_converter,
            poll_time,
            projection: projection.to_vec(),
            projected_batch_schema: projected_null_batch.schema(),
            projected_batches: vec![projected_null_batch],
            cur_idx: (0, 0),
            keys: vec![empty_keys],
            key_has_nulls: vec![Some(null_nb)],
            finished: false,
            key_rows_batches: None,
        })
    }

    pub fn from_key_rows_batches(
        key_rows_batches: Vec<(RecordBatch, Rows)>,
        key_exprs: Vec<PhysicalExprRef>,
        projection: Vec<usize>,
        schema: SchemaRef,
    ) -> Self {
        let mut projected_batches = Vec::with_capacity(key_rows_batches.len());
        let mut keys = Vec::with_capacity(key_rows_batches.len());
        for (batch, rows) in key_rows_batches.into_iter() {
            projected_batches.push(batch);
            keys.push(Arc::new(rows));
        }
        let projected_batches_len = projected_batches.len();
        Self {
            stream: Box::pin(EmptyRecordBatchStream::new(schema.clone())),
            key_converter: Arc::new(Mutex::new(RowConverter::new(vec![]).unwrap())),
            key_exprs,
            poll_time: Time::new(),
            projection,
            projected_batch_schema: schema,
            projected_batches,
            cur_idx: (0, 0),
            keys,
            key_has_nulls: vec![None; projected_batches_len],
            finished: false,
            key_rows_batches: None,
        }
    }

    pub fn next(&mut self) -> Option<impl Future<Output=Result<()>> + '_> {
        self.cur_idx.1 += 1;
        if self.cur_idx.1 >= self.projected_batches[self.cur_idx.0].num_rows() {
            self.cur_idx.0 += 1;
            self.cur_idx.1 = 0;
        }

        let should_load_next_batch = self.cur_idx.0 >= self.projected_batches.len();
        if should_load_next_batch {
            Some(async move {
                while let Some(batch) = self
                    .poll_time
                    .with_timer_async(async { self.stream.next().await.transpose() })
                    .await?
                {
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
                        .map(|c| c.logical_nulls())
                        .reduce(|lhs, rhs| NullBuffer::union(lhs.as_ref(), rhs.as_ref()))
                        .unwrap_or(None);
                    let keys = Arc::new(self.key_converter.lock().convert_columns(&key_columns)?);

                    let projected_batch = RecordBatch::try_new_with_options(
                        self.projected_batches[0].schema(),
                        self.projection
                            .iter()
                            .map(|&i| batch.column(i).clone())
                            .collect(),
                        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
                    )?;
                    self.projected_batches.push(projected_batch);
                    self.key_has_nulls.push(key_has_nulls);
                    self.keys.push(keys);
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
    pub fn cur_key<'a>(&'a self) -> Row<'a> {
        self.key(self.cur_idx)
    }

    #[inline]
    pub fn num_buffered_batches(&self) -> usize {
        self.projected_batches.len() - 1
    }

    pub fn clean_out_dated_batches(&mut self) {
        if self.cur_idx.0 > 1 {
            self.projected_batches
                .splice(1..self.cur_idx.0, std::iter::empty());
            self.keys.splice(1..self.cur_idx.0, std::iter::empty());
            self.key_has_nulls
                .splice(1..self.cur_idx.0, std::iter::empty());
            self.cur_idx.0 = 1;
        }
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
