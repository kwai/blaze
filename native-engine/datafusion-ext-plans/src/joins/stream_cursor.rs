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

use std::{future::Future, pin::Pin, sync::Arc};

use arrow::{
    array::{ArrayRef, RecordBatch},
    buffer::NullBuffer,
    datatypes::{Schema, SchemaRef},
    row::{Row, RowConverter, Rows},
};
use arrow_schema::DataType;
use datafusion::{common::Result, physical_plan::metrics::Time};
use datafusion_ext_commons::arrow::selection::take_batch;
use futures::StreamExt;

use crate::{
    common::{
        key_rows_output::SendableRecordBatchWithKeyRowsStream, row_null_checker::RowNullChecker,
        timer_helper::TimerHelper,
    },
    joins::Idx,
};

/// Optimized StreamCursor that uses pre-computed key rows to avoid redundant
/// key conversion
pub struct StreamCursor {
    stream: SendableRecordBatchWithKeyRowsStream,
    key_null_checker: RowNullChecker,
    poll_time: Time,

    // IMPORTANT:
    // batches/rows/null_buffers always contains a `null batch` in the front
    pub batch_schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    pub cur_idx: Idx,
    keys: Vec<Arc<Rows>>,
    key_has_nulls: Vec<Option<NullBuffer>>,
    pub finished: bool,
}

impl StreamCursor {
    pub fn try_new(
        stream: SendableRecordBatchWithKeyRowsStream,
        poll_time: Time,
        key_data_types: &[DataType],
    ) -> Result<Self> {
        let empty_batch = RecordBatch::new_empty(Arc::new(Schema::new(
            stream
                .schema()
                .fields()
                .iter()
                .map(|f| f.as_ref().clone().with_nullable(true))
                .collect::<Vec<_>>(),
        )));

        let key_null_checker = RowNullChecker::new(
            &stream
                .keys()
                .iter()
                .zip(key_data_types)
                .map(|(k, dt)| (dt.clone(), k.options))
                .collect::<Vec<_>>(),
        );

        let empty_keys = Arc::new(RowConverter::new(vec![])?.convert_columns(&[] as &[ArrayRef])?);
        let null_batch = take_batch(empty_batch, vec![Option::<u32>::None])?;
        let null_nb = NullBuffer::new_null(1);

        Ok(Self {
            stream,
            key_null_checker,
            poll_time,
            batch_schema: null_batch.schema(),
            batches: vec![null_batch],
            cur_idx: (0, 0),
            keys: vec![empty_keys],
            key_has_nulls: vec![Some(null_nb)],
            finished: false,
        })
    }
}

impl StreamCursor {
    pub fn next(&mut self) -> Option<Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>> {
        self.cur_idx.1 += 1;
        if self.cur_idx.1 >= self.batches[self.cur_idx.0].num_rows() {
            self.cur_idx.0 += 1;
            self.cur_idx.1 = 0;
        }

        let should_load_next_batch = self.cur_idx.0 >= self.batches.len();
        if should_load_next_batch {
            Some(Box::pin(async move {
                while let Some(batch_with_keys) = self
                    .poll_time
                    .with_timer_async(async { self.stream.next().await.transpose() })
                    .await?
                {
                    if batch_with_keys.batch.num_rows() == 0 {
                        continue;
                    }

                    let keys = batch_with_keys.key_rows.clone();
                    let batch = batch_with_keys.batch;
                    let key_has_nulls = Some(self.key_null_checker.has_nulls(&keys))
                        .filter(|nb| nb.null_count() > 0);

                    self.batches.push(batch);
                    self.key_has_nulls.push(key_has_nulls);
                    self.keys.push(keys);
                    return Ok(());
                }
                self.finished = true;
                return Ok(());
            }))
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
    pub fn cur_idx(&self) -> Idx {
        self.cur_idx
    }

    #[inline]
    pub fn finished(&self) -> bool {
        self.finished
    }

    #[inline]
    pub fn num_buffered_batches(&self) -> usize {
        self.batches.len() - 1
    }

    pub fn clean_out_dated_batches(&mut self) {
        if self.cur_idx.0 > 1 {
            self.batches.splice(1..self.cur_idx.0, std::iter::empty());
            self.keys.splice(1..self.cur_idx.0, std::iter::empty());
            self.key_has_nulls
                .splice(1..self.cur_idx.0, std::iter::empty());
            self.cur_idx.0 = 1;
        }
    }

    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
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
