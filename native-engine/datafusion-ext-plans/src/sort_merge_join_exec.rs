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

use crate::common::output::{output_with_sender, WrappedRecordBatchSender};
use crate::common::{BatchTaker, BatchesInterleaver};
use arrow::array::*;
use arrow::buffer::NullBuffer;
use arrow::compute::{prep_null_mask_filter, SortOptions};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::{Row, RowConverter, Rows, SortField};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::JoinType;
use datafusion::logical_expr::JoinType::*;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::joins::utils::{
    build_join_schema, check_join_is_valid, JoinFilter, JoinOn, JoinSide,
};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet, ScopedTimerGuard,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    Statistics,
};
use datafusion_ext_commons::streams::coalesce_stream::CoalesceStream;
use futures::{StreamExt, TryStreamExt};
use parking_lot::Mutex as SyncMutex;
use std::any::Any;
use std::cmp::Ordering;
use std::fmt::Formatter;
use std::sync::Arc;

#[derive(Debug)]
pub struct SortMergeJoinExec {
    /// Left sorted joining execution plan
    left: Arc<dyn ExecutionPlan>,
    /// Right sorting joining execution plan
    right: Arc<dyn ExecutionPlan>,
    /// Set of common columns used to join on
    on: JoinOn,
    /// How the join is performed
    join_type: JoinType,
    /// Optional filter before outputting
    join_filter: Option<JoinFilter>,
    /// The schema once the join is applied
    schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Sort options of join columns used in sorting left and right execution plans
    sort_options: Vec<SortOptions>,
}

impl SortMergeJoinExec {
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
        join_filter: Option<JoinFilter>,
        sort_options: Vec<SortOptions>,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();

        if matches!(join_type, LeftSemi | LeftAnti | RightSemi | RightAnti,) {
            if join_filter.is_some() {
                return Err(DataFusionError::Plan(format!(
                    "Semi/Anti join with filter is not supported yet"
                )));
            }
        }

        check_join_is_valid(&left_schema, &right_schema, &on)?;
        if sort_options.len() != on.len() {
            return Err(DataFusionError::Plan(format!(
                "Expected number of sort options: {}, actual: {}",
                on.len(),
                sort_options.len()
            )));
        }

        let schema = Arc::new(build_join_schema(&left_schema, &right_schema, &join_type).0);

        Ok(Self {
            left,
            right,
            on,
            join_type,
            join_filter,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            sort_options,
        })
    }
}

impl DisplayAs for SortMergeJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "SortMergeJoin: join_type={:?}, on={:?}, schema={:?}",
            self.join_type, self.on, self.schema,
        )
    }
}

impl ExecutionPlan for SortMergeJoinExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.right.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        match self.join_type {
            Left | LeftSemi | LeftAnti => self.left.output_ordering(),
            Right | RightSemi | RightAnti => self.right.output_ordering(),
            Inner => self.left.output_ordering(),
            Full => None,
        }
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match &children[..] {
            [left, right] => Ok(Arc::new(SortMergeJoinExec::try_new(
                left.clone(),
                right.clone(),
                self.on.clone(),
                self.join_type,
                self.join_filter.clone(),
                self.sort_options.clone(),
            )?)),
            _ => Err(DataFusionError::Internal(
                "SortMergeJoin wrong number of children".to_string(),
            )),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let left = self.left.execute(partition, context.clone())?;
        let right = self.right.execute(partition, context.clone())?;
        let metrics = Arc::new(BaselineMetrics::new(&self.metrics, partition));

        let on_left: Vec<usize> = self.on.iter().map(|on| on.0.index()).collect();
        let on_right: Vec<usize> = self.on.iter().map(|on| on.1.index()).collect();

        let on_data_types = on_left
            .iter()
            .map(|&i| left.schema().field(i).data_type().clone())
            .collect::<Vec<_>>();
        let batch_size = context.session_config().batch_size();
        let sub_batch_size = batch_size / batch_size.ilog2() as usize;

        // use smaller batch size and coalesce batches at the end, to avoid buffer overflowing
        let join_params = JoinParams {
            join_type: self.join_type,
            output_schema: self.schema(),
            on_left,
            on_right,
            on_data_types,
            join_filter: self.join_filter.clone(),
            sort_options: self.sort_options.clone(),
            batch_size: sub_batch_size,
        };

        let metrics_cloned = metrics.clone();
        let output_schema = self.schema();
        let output_stream = Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(async move {
                output_with_sender("SortMergeJoin", context, output_schema, move |sender| {
                    execute_join(left, right, join_params, metrics_cloned, sender)
                })
            })
            .try_flatten(),
        ));

        let output_coalesced = Box::pin(CoalesceStream::new(
            output_stream,
            batch_size,
            metrics.elapsed_compute().clone(),
        ));
        Ok(output_coalesced)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}

#[derive(Clone)]
struct JoinParams {
    join_type: JoinType,
    output_schema: SchemaRef,
    on_left: Vec<usize>,
    on_right: Vec<usize>,
    on_data_types: Vec<DataType>,
    sort_options: Vec<SortOptions>,
    join_filter: Option<JoinFilter>,
    batch_size: usize,
}

async fn execute_join(
    lstream: SendableRecordBatchStream,
    rstream: SendableRecordBatchStream,
    join_params: JoinParams,
    metrics: Arc<BaselineMetrics>,
    sender: Arc<WrappedRecordBatchSender>,
) -> Result<()> {
    let elapsed_time = metrics.elapsed_compute().clone();
    let mut timer = elapsed_time.timer();

    let on_row_converter = Arc::new(SyncMutex::new(RowConverter::new(
        join_params
            .on_data_types
            .iter()
            .zip(&join_params.sort_options)
            .map(|(data_type, sort_option)| {
                SortField::new_with_options(data_type.clone(), *sort_option)
            })
            .collect(),
    )?));

    let mut lcur = StreamCursor::try_new(
        lstream,
        on_row_converter.clone(),
        join_params.on_left.clone(),
        &mut timer,
    )
    .await?;

    let mut rcur = StreamCursor::try_new(
        rstream,
        on_row_converter.clone(),
        join_params.on_right.clone(),
        &mut timer,
    )
    .await?;

    let join_type = join_params.join_type;
    let mut joiner = Joiner::new();
    let mut leqs = vec![];
    let mut reqs = vec![];

    macro_rules! joiner_accept_pair {
        ($lidx:expr, $ridx:expr) => {{
            let lidx = $lidx;
            let ridx = $ridx;
            let r = joiner.accept_pair(&join_params, &mut lcur, &mut rcur, lidx, ridx)?;
            if let Some(batch) = r {
                metrics.record_output(batch.num_rows());
                sender.send(Ok(batch), Some(&mut timer)).await;
            }
        }};
    }

    // process records until one side is exhausted
    while !lcur.finished && !rcur.finished {
        let r = compare_cursor(&lcur, lcur.cur_idx, &rcur, rcur.cur_idx);
        match r {
            Ordering::Less => {
                if matches!(join_type, Left | LeftAnti | Full) {
                    joiner_accept_pair!(Some(lcur.cur_idx), None);
                }
                lcur.next(&mut timer).await?;
                lcur.clear_outdated(joiner.l_min_reserved_bidx);
            }
            Ordering::Greater => {
                if matches!(join_type, Right | RightAnti | Full) {
                    joiner_accept_pair!(None, Some(rcur.cur_idx));
                }
                rcur.next(&mut timer).await?;
                rcur.clear_outdated(joiner.r_min_reserved_bidx);
            }
            Ordering::Equal => {
                let lidx0 = lcur.cur_idx;
                let ridx0 = rcur.cur_idx;
                leqs.push(lidx0);
                reqs.push(ridx0);
                lcur.next(&mut timer).await?;
                rcur.next(&mut timer).await?;

                let mut leq = true;
                let mut req = true;
                while leq && req {
                    if leq && !lcur.finished && lcur.row(lcur.cur_idx) == lcur.row(lidx0) {
                        leqs.push(lcur.cur_idx);
                        lcur.next(&mut timer).await?;
                    } else {
                        leq = false;
                    }
                    if req && !rcur.finished && rcur.row(rcur.cur_idx) == rcur.row(ridx0) {
                        reqs.push(rcur.cur_idx);
                        rcur.next(&mut timer).await?;
                    } else {
                        req = false;
                    }
                }

                match join_type {
                    Inner | Left | Right | Full => {
                        for &l in &leqs {
                            for &r in &reqs {
                                joiner_accept_pair!(Some(l), Some(r));
                            }
                        }
                    }
                    LeftSemi => {
                        for &l in &leqs {
                            joiner_accept_pair!(Some(l), None);
                        }
                    }
                    RightSemi => {
                        for &r in &reqs {
                            joiner_accept_pair!(None, Some(r));
                        }
                    }
                    LeftAnti | RightAnti => {}
                }

                if leq {
                    while !lcur.finished && lcur.row(lcur.cur_idx) == rcur.row(ridx0) {
                        match join_type {
                            Inner | Left | Right | Full => {
                                for &r in &reqs {
                                    joiner_accept_pair!(Some(lcur.cur_idx), Some(r));
                                }
                            }
                            LeftSemi => {
                                joiner_accept_pair!(Some(lcur.cur_idx), None);
                            }
                            RightSemi | LeftAnti | RightAnti => {}
                        }
                        lcur.next(&mut timer).await?;
                        lcur.clear_outdated(joiner.l_min_reserved_bidx);
                    }
                }
                if req {
                    while !rcur.finished && rcur.row(rcur.cur_idx) == lcur.row(lidx0) {
                        match join_type {
                            Inner | Left | Right | Full => {
                                for &l in &leqs {
                                    joiner_accept_pair!(Some(l), Some(rcur.cur_idx));
                                }
                            }
                            RightSemi => {
                                joiner_accept_pair!(None, Some(rcur.cur_idx));
                            }
                            LeftSemi | LeftAnti | RightAnti => {}
                        }
                        rcur.next(&mut timer).await?;
                        rcur.clear_outdated(joiner.r_min_reserved_bidx);
                    }
                }
                leqs.clear();
                reqs.clear();
                lcur.clear_outdated(joiner.l_min_reserved_bidx);
                rcur.clear_outdated(joiner.r_min_reserved_bidx);
            }
        }

        // flush joiner if cursors buffered too many batches
        if !joiner.is_empty() && lcur.num_buffered_batches() + rcur.num_buffered_batches() > 5 {
            if let Some(batch) = joiner.flush_pairs(&join_params, &mut lcur, &mut rcur)? {
                metrics.record_output(batch.num_rows());
                sender.send(Ok(batch), Some(&mut timer)).await;
            }
        }
    }

    // process rest records in inexhausted side
    if matches!(join_type, Left | LeftAnti | Full) {
        while !lcur.finished {
            joiner_accept_pair!(Some(lcur.cur_idx), None);
            lcur.next(&mut timer).await?;
            lcur.clear_outdated(joiner.l_min_reserved_bidx);
        }
    }
    if matches!(join_type, Right | RightAnti | Full) {
        while !rcur.finished {
            joiner_accept_pair!(None, Some(rcur.cur_idx));
            rcur.next(&mut timer).await?;
            rcur.clear_outdated(joiner.r_min_reserved_bidx);
        }
    }

    // flush joiner
    if !joiner.is_empty() {
        if let Some(batch) = joiner.flush_pairs(&join_params, &mut lcur, &mut rcur)? {
            metrics.record_output(batch.num_rows());
            sender.send(Ok(batch), Some(&mut timer)).await;
        }
    }
    Ok(())
}

struct StreamCursor {
    stream: SendableRecordBatchStream,
    on_row_converter: Arc<SyncMutex<RowConverter>>,
    on_columns: Vec<usize>,

    // IMPORTANT:
    // batches/rows/null_buffers always contains a `null batch` in the front
    batches: Vec<RecordBatch>,
    on_rows: Vec<Arc<Rows>>,
    on_row_null_buffers: Vec<Option<NullBuffer>>,
    cur_idx: (usize, usize),
    num_null_batches: usize,
    finished: bool,
}

impl StreamCursor {
    async fn try_new(
        stream: SendableRecordBatchStream,
        on_row_converter: Arc<SyncMutex<RowConverter>>,
        on_columns: Vec<usize>,
        stop_timer: &mut ScopedTimerGuard<'_>,
    ) -> Result<Self> {
        let empty_batch = RecordBatch::new_empty(Arc::new(Schema::new(
            stream
                .schema()
                .fields()
                .iter()
                .map(|f| f.as_ref().clone().with_nullable(true))
                .collect::<Vec<_>>(),
        )));
        let null_batch = BatchTaker(&empty_batch).take_opt([Option::<usize>::None])?;
        let null_on_rows = Arc::new(
            on_row_converter
                .lock()
                .convert_columns(null_batch.project(&on_columns)?.columns())?,
        );
        let null_nb = NullBuffer::new_null(1);

        let mut cursor = Self {
            stream,
            on_row_converter,
            on_columns,
            batches: vec![null_batch],
            on_rows: vec![null_on_rows],
            on_row_null_buffers: vec![Some(null_nb)],
            cur_idx: (1, 0),
            num_null_batches: 1,
            finished: false,
        };
        if !cursor.next_batch(stop_timer).await? {
            cursor.finished = true;
        }
        Ok(cursor)
    }

    async fn next(&mut self, stop_timer: &mut ScopedTimerGuard<'_>) -> Result<()> {
        let mut cur_idx = self.cur_idx;

        if cur_idx.1 + 1 < self.batches[cur_idx.0].num_rows() {
            cur_idx.1 += 1;
        } else {
            cur_idx.0 += 1;
            cur_idx.1 = 0;
            if !self.next_batch(stop_timer).await? {
                self.finished = true;
            }
        }
        self.cur_idx = cur_idx;
        Ok(())
    }

    async fn next_batch(&mut self, stop_timer: &mut ScopedTimerGuard<'_>) -> Result<bool> {
        stop_timer.stop();
        if let Some(batch) = self.stream.next().await.transpose()? {
            stop_timer.restart();
            let on_columns = batch.project(&self.on_columns)?.columns().to_vec();
            let on_row_null_buffer = on_columns
                .iter()
                .map(|c| c.nulls().cloned())
                .reduce(|lhs, rhs| NullBuffer::union(lhs.as_ref(), rhs.as_ref()))
                .unwrap_or(None);
            let on_rows = Arc::new(self.on_row_converter.lock().convert_columns(&on_columns)?);

            self.batches.push(batch);
            self.on_row_null_buffers.push(on_row_null_buffer);
            self.on_rows.push(on_rows);
            return Ok(true);
        } else {
            stop_timer.restart();
        }
        Ok(false)
    }

    #[inline]
    fn row<'a>(&'a self, idx: (usize, usize)) -> Row<'a> {
        let bidx = idx.0;
        let ridx = idx.1;
        self.on_rows[bidx].row(ridx)
    }

    #[inline]
    fn num_buffered_batches(&self) -> usize {
        self.batches.len() - self.num_null_batches
    }

    #[inline]
    fn clear_outdated(&mut self, min_reserved_bidx: usize) {
        // fill out-dated batches with null batches
        for i in self.num_null_batches..min_reserved_bidx.min(self.cur_idx.0) {
            self.batches[i] = self.batches[0].clone();
            self.on_rows[i] = self.on_rows[0].clone();
            self.on_row_null_buffers[i] = self.on_row_null_buffers[0].clone();
            self.num_null_batches += 1;
        }
    }
}

#[derive(Default)]
struct Joiner {
    ljoins: Vec<(usize, usize)>,
    rjoins: Vec<(usize, usize)>,
    l_min_reserved_bidx: usize,
    r_min_reserved_bidx: usize,
}

impl Joiner {
    fn new() -> Self {
        Self {
            ljoins: vec![],
            rjoins: vec![],
            l_min_reserved_bidx: usize::MAX,
            r_min_reserved_bidx: usize::MAX,
        }
    }

    fn accept_pair(
        &mut self,
        join_params: &JoinParams,
        lcur: &mut StreamCursor,
        rcur: &mut StreamCursor,
        l: Option<(usize, usize)>,
        r: Option<(usize, usize)>,
    ) -> Result<Option<RecordBatch>> {
        if let Some((bidx, ridx)) = l {
            self.ljoins.push((bidx, ridx));
            self.l_min_reserved_bidx = self.l_min_reserved_bidx.min(bidx);
        } else {
            self.ljoins.push((0, 0));
        }

        if let Some((bidx, ridx)) = r {
            self.rjoins.push((bidx, ridx));
            self.r_min_reserved_bidx = self.r_min_reserved_bidx.min(bidx);
        } else {
            self.rjoins.push((0, 0));
        }

        let batch_size = join_params.batch_size;
        if self.ljoins.len() >= batch_size || self.rjoins.len() >= batch_size {
            return self.flush_pairs(join_params, lcur, rcur);
        }
        Ok(None)
    }

    fn is_empty(&self) -> bool {
        self.ljoins.is_empty() && self.rjoins.is_empty()
    }

    fn flush_pairs(
        &mut self,
        join_params: &JoinParams,
        lcur: &mut StreamCursor,
        rcur: &mut StreamCursor,
    ) -> Result<Option<RecordBatch>> {
        self.l_min_reserved_bidx = usize::MAX;
        self.r_min_reserved_bidx = usize::MAX;

        if let Some(join_filter) = &join_params.join_filter {
            // get intermediate batch
            let intermediate_columns = join_filter
                .column_indices()
                .iter()
                .map(|ci| {
                    let (cur, joins) = match ci.side {
                        JoinSide::Left => (&lcur, &self.ljoins),
                        JoinSide::Right => (&rcur, &self.rjoins),
                    };
                    let arrays = cur
                        .batches
                        .iter()
                        .map(|b| b.column(ci.index).as_ref())
                        .collect::<Vec<_>>();
                    Ok(arrow::compute::interleave(&arrays, joins)?)
                })
                .collect::<Result<Vec<_>>>()?;
            let intermediate_batch =
                RecordBatch::try_new(Arc::new(join_filter.schema().clone()), intermediate_columns)?;

            // evalute filter
            let filtered_array = join_filter
                .expression()
                .evaluate(&intermediate_batch)?
                .into_array(intermediate_batch.num_rows());
            let filtered = as_boolean_array(&filtered_array);
            let filtered = if filtered.null_count() > 0 {
                prep_null_mask_filter(filtered)
            } else {
                filtered.clone()
            };

            // apply filter
            let mut retained = 0;
            for (i, selected) in filtered.values().iter().enumerate() {
                if selected {
                    self.ljoins[retained] = self.ljoins[i];
                    self.rjoins[retained] = self.rjoins[i];
                    retained += 1;
                }
            }
            self.ljoins.truncate(retained);
            self.rjoins.truncate(retained);
            if retained == 0 {
                return Ok(None);
            }
        }

        let output_columns = match join_params.join_type {
            LeftSemi | LeftAnti => BatchesInterleaver::new(lcur.batches[0].schema(), &lcur.batches)
                .interleave(&self.ljoins)?
                .columns()
                .to_vec(),
            RightSemi | RightAnti => {
                BatchesInterleaver::new(rcur.batches[0].schema(), &rcur.batches)
                    .interleave(&self.rjoins)?
                    .columns()
                    .to_vec()
            }
            _ => [
                BatchesInterleaver::new(lcur.batches[0].schema(), &lcur.batches)
                    .interleave(&self.ljoins)?
                    .columns(),
                BatchesInterleaver::new(rcur.batches[0].schema(), &rcur.batches)
                    .interleave(&self.rjoins)?
                    .columns(),
            ]
            .concat(),
        };
        self.ljoins.clear();
        self.rjoins.clear();
        let batch = RecordBatch::try_new(join_params.output_schema.clone(), output_columns)?;
        Ok(Some(batch))
    }
}

fn compare_cursor(
    lcur: &StreamCursor,
    lidx: (usize, usize),
    rcur: &StreamCursor,
    ridx: (usize, usize),
) -> Ordering {
    match (&lcur.on_rows.get(lidx.0), &rcur.on_rows.get(ridx.0)) {
        (None, _) => Ordering::Greater,
        (_, None) => Ordering::Less,
        (Some(lrows), Some(rrows)) => {
            let lkey = &lrows.row(lidx.1);
            let rkey = &rrows.row(ridx.1);
            match lkey.cmp(rkey) {
                Ordering::Greater => Ordering::Greater,
                Ordering::Less => Ordering::Less,
                _ => {
                    if let Some(nb) = &lcur.on_row_null_buffers[lidx.0] {
                        if nb.is_null(lidx.1) {
                            return Ordering::Less;
                        }
                    }
                    Ordering::Equal
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::sort_merge_join_exec::SortMergeJoinExec;
    use arrow;
    use arrow::array::*;
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::error::Result;
    use datafusion::logical_expr::JoinType;
    use datafusion::logical_expr::JoinType::*;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::common;
    use datafusion::physical_plan::joins::utils::*;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::sync::Arc;

    fn columns(schema: &Schema) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }

    fn build_table_i32(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Int32, false),
            Field::new(b.0, DataType::Int32, false),
            Field::new(c.0, DataType::Int32, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(a.1.clone())),
                Arc::new(Int32Array::from(b.1.clone())),
                Arc::new(Int32Array::from(c.1.clone())),
            ],
        )
        .unwrap()
    }

    fn build_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a, b, c);
        let schema = batch.schema();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    fn build_table_from_batches(batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
        let schema = batches.first().unwrap().schema();
        Arc::new(MemoryExec::try_new(&[batches], schema, None).unwrap())
    }

    fn build_date_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Date32, false),
            Field::new(b.0, DataType::Date32, false),
            Field::new(c.0, DataType::Date32, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Date32Array::from(a.1.clone())),
                Arc::new(Date32Array::from(b.1.clone())),
                Arc::new(Date32Array::from(c.1.clone())),
            ],
        )
        .unwrap();

        let schema = batch.schema();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    fn build_date64_table(
        a: (&str, &Vec<i64>),
        b: (&str, &Vec<i64>),
        c: (&str, &Vec<i64>),
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Date64, false),
            Field::new(b.0, DataType::Date64, false),
            Field::new(c.0, DataType::Date64, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Date64Array::from(a.1.clone())),
                Arc::new(Date64Array::from(b.1.clone())),
                Arc::new(Date64Array::from(c.1.clone())),
            ],
        )
        .unwrap();

        let schema = batch.schema();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    /// returns a table with 3 columns of i32 in memory
    pub fn build_table_i32_nullable(
        a: (&str, &Vec<Option<i32>>),
        b: (&str, &Vec<Option<i32>>),
        c: (&str, &Vec<Option<i32>>),
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(a.0, DataType::Int32, true),
            Field::new(b.0, DataType::Int32, true),
            Field::new(c.0, DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(a.1.clone())),
                Arc::new(Int32Array::from(b.1.clone())),
                Arc::new(Int32Array::from(c.1.clone())),
            ],
        )
        .unwrap();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    fn join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
    ) -> Result<SortMergeJoinExec> {
        let sort_options = vec![SortOptions::default(); on.len()];
        SortMergeJoinExec::try_new(left, right, on, join_type, None, sort_options)
    }

    fn join_with_options(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
        sort_options: Vec<SortOptions>,
    ) -> Result<SortMergeJoinExec> {
        SortMergeJoinExec::try_new(left, right, on, join_type, None, sort_options)
    }

    async fn join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let sort_options = vec![SortOptions::default(); on.len()];
        join_collect_with_options(left, right, on, join_type, sort_options).await
    }

    async fn join_collect_with_options(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
        sort_options: Vec<SortOptions>,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let join = join_with_options(left, right, on, join_type, sort_options)?;
        let columns = columns(&join.schema());

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;
        Ok((columns, batches))
    }

    async fn join_collect_batch_size_equals_two(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let session_ctx = SessionContext::with_config(SessionConfig::new().with_batch_size(2));
        let task_ctx = session_ctx.task_ctx();
        let join = join(left, right, on, join_type)?;
        let columns = columns(&join.schema());

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;
        Ok((columns, batches))
    }

    #[tokio::test]
    async fn join_inner_one() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 5]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, Inner).await?;

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 5  | 9  | 20 | 5  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_two() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2]),
            ("b2", &vec![1, 2, 2]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b2", &vec![1, 2, 2]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![
            (
                Column::new_with_schema("a1", &left.schema())?,
                Column::new_with_schema("a1", &right.schema())?,
            ),
            (
                Column::new_with_schema("b2", &left.schema())?,
                Column::new_with_schema("b2", &right.schema())?,
            ),
        ];

        let (_columns, batches) = join_collect(left, right, on, Inner).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b2 | c1 | a1 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 1  | 7  | 1  | 1  | 70 |",
            "| 2  | 2  | 8  | 2  | 2  | 80 |",
            "| 2  | 2  | 9  | 2  | 2  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_two_two() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 1, 2]),
            ("b2", &vec![1, 1, 2]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a1", &vec![1, 1, 3]),
            ("b2", &vec![1, 1, 2]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![
            (
                Column::new_with_schema("a1", &left.schema())?,
                Column::new_with_schema("a1", &right.schema())?,
            ),
            (
                Column::new_with_schema("b2", &left.schema())?,
                Column::new_with_schema("b2", &right.schema())?,
            ),
        ];

        let (_columns, batches) = join_collect(left, right, on, Inner).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b2 | c1 | a1 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 1  | 7  | 1  | 1  | 70 |",
            "| 1  | 1  | 7  | 1  | 1  | 80 |",
            "| 1  | 1  | 8  | 1  | 1  | 70 |",
            "| 1  | 1  | 8  | 1  | 1  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_with_nulls() -> Result<()> {
        let left = build_table_i32_nullable(
            ("a1", &vec![Some(1), Some(1), Some(2), Some(2)]),
            ("b2", &vec![None, Some(1), Some(2), Some(2)]), // null in key field
            ("c1", &vec![Some(1), None, Some(8), Some(9)]), // null in non-key field
        );
        let right = build_table_i32_nullable(
            ("a1", &vec![Some(1), Some(1), Some(2), Some(3)]),
            ("b2", &vec![None, Some(1), Some(2), Some(2)]),
            ("c2", &vec![Some(10), Some(70), Some(80), Some(90)]),
        );
        let on = vec![
            (
                Column::new_with_schema("a1", &left.schema())?,
                Column::new_with_schema("a1", &right.schema())?,
            ),
            (
                Column::new_with_schema("b2", &left.schema())?,
                Column::new_with_schema("b2", &right.schema())?,
            ),
        ];

        let (_, batches) = join_collect(left, right, on, Inner).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b2 | c1 | a1 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 1  |    | 1  | 1  | 70 |",
            "| 2  | 2  | 8  | 2  | 2  | 80 |",
            "| 2  | 2  | 9  | 2  | 2  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_with_nulls_with_options() -> Result<()> {
        let left = build_table_i32_nullable(
            ("a1", &vec![Some(2), Some(2), Some(1), Some(1)]),
            ("b2", &vec![Some(2), Some(2), Some(1), None]), // null in key field
            ("c1", &vec![Some(9), Some(8), None, Some(1)]), // null in non-key field
        );
        let right = build_table_i32_nullable(
            ("a1", &vec![Some(3), Some(2), Some(1), Some(1)]),
            ("b2", &vec![Some(2), Some(2), Some(1), None]),
            ("c2", &vec![Some(90), Some(80), Some(70), Some(10)]),
        );
        let on = vec![
            (
                Column::new_with_schema("a1", &left.schema())?,
                Column::new_with_schema("a1", &right.schema())?,
            ),
            (
                Column::new_with_schema("b2", &left.schema())?,
                Column::new_with_schema("b2", &right.schema())?,
            ),
        ];
        let (_, batches) = join_collect_with_options(
            left,
            right,
            on,
            Inner,
            vec![
                SortOptions {
                    descending: true,
                    nulls_first: false
                };
                2
            ],
            // null_equals_null=false
        )
        .await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b2 | c1 | a1 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 2  | 2  | 9  | 2  | 2  | 80 |",
            "| 2  | 2  | 8  | 2  | 2  | 80 |",
            "| 1  | 1  |    | 1  | 1  | 70 |",
            "+----+----+----+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_output_two_batches() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2]),
            ("b2", &vec![1, 2, 2]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b2", &vec![1, 2, 2]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![
            (
                Column::new_with_schema("a1", &left.schema())?,
                Column::new_with_schema("a1", &right.schema())?,
            ),
            (
                Column::new_with_schema("b2", &left.schema())?,
                Column::new_with_schema("b2", &right.schema())?,
            ),
        ];

        let (_, batches) = join_collect_batch_size_equals_two(left, right, on, Inner).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b2 | c1 | a1 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 1  | 7  | 1  | 1  | 70 |",
            "| 2  | 2  | 8  | 2  | 2  | 80 |",
            "| 2  | 2  | 9  | 2  | 2  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 1);
        // The output order is important as SMJ preserves sortedness
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_left_one() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, Left).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_one() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]), // 6 does not exist on the left
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, Right).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "|    |    |    | 30 | 6  | 90 |",
            "+----+----+----+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_full_one() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b2", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Column::new_with_schema("b1", &left.schema()).unwrap(),
            Column::new_with_schema("b2", &right.schema()).unwrap(),
        )];

        let (_, batches) = join_collect(left, right, on, Full).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "|    |    |    | 30 | 6  | 90 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_anti() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2, 3, 5]),
            ("b1", &vec![4, 5, 5, 7, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 8, 9, 11]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, LeftAnti).await?;
        let expected = vec![
            "+----+----+----+",
            "| a1 | b1 | c1 |",
            "+----+----+----+",
            "| 3  | 7  | 9  |",
            "| 5  | 7  | 11 |",
            "+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_semi() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2, 3]),
            ("b1", &vec![4, 5, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]), // 5 is double on the right
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, LeftSemi).await?;
        let expected = vec![
            "+----+----+----+",
            "| a1 | b1 | c1 |",
            "+----+----+----+",
            "| 1  | 4  | 7  |",
            "| 2  | 5  | 8  |",
            "| 2  | 5  | 8  |",
            "+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_with_duplicated_column_names() -> Result<()> {
        let left = build_table(
            ("a", &vec![1, 2, 3]),
            ("b", &vec![4, 5, 7]),
            ("c", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a", &vec![10, 20, 30]),
            ("b", &vec![1, 2, 7]),
            ("c", &vec![70, 80, 90]),
        );
        let on = vec![(
            // join on a=b so there are duplicate column names on unjoined columns
            Column::new_with_schema("a", &left.schema())?,
            Column::new_with_schema("b", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, Inner).await?;
        let expected = vec![
            "+---+---+---+----+---+----+",
            "| a | b | c | a  | b | c  |",
            "+---+---+---+----+---+----+",
            "| 1 | 4 | 7 | 10 | 1 | 70 |",
            "| 2 | 5 | 8 | 20 | 2 | 80 |",
            "+---+---+---+----+---+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_date32() -> Result<()> {
        let left = build_date_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![19107, 19108, 19108]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_date_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![19107, 19108, 19109]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, Inner).await?;

        let expected = vec![
            "+------------+------------+------------+------------+------------+------------+",
            "| a1         | b1         | c1         | a2         | b1         | c2         |",
            "+------------+------------+------------+------------+------------+------------+",
            "| 1970-01-02 | 2022-04-25 | 1970-01-08 | 1970-01-11 | 2022-04-25 | 1970-03-12 |",
            "| 1970-01-03 | 2022-04-26 | 1970-01-09 | 1970-01-21 | 2022-04-26 | 1970-03-22 |",
            "| 1970-01-04 | 2022-04-26 | 1970-01-10 | 1970-01-21 | 2022-04-26 | 1970-03-22 |",
            "+------------+------------+------------+------------+------------+------------+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_date64() -> Result<()> {
        let left = build_date64_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1650703441000, 1650903441000, 1650903441000]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_date64_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![1650703441000, 1650503441000, 1650903441000]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, Inner).await?;
        let expected = vec![
            "+-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+",
            "| a1                      | b1                  | c1                      | a2                      | b1                  | c2                      |",
            "+-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+",
            "| 1970-01-01T00:00:00.001 | 2022-04-23T08:44:01 | 1970-01-01T00:00:00.007 | 1970-01-01T00:00:00.010 | 2022-04-23T08:44:01 | 1970-01-01T00:00:00.070 |",
            "| 1970-01-01T00:00:00.002 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.008 | 1970-01-01T00:00:00.030 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.090 |",
            "| 1970-01-01T00:00:00.003 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.009 | 1970-01-01T00:00:00.030 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.090 |",
            "+-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+",
        ];

        // The output order is important as SMJ preserves sortedness
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_left_sort_order() -> Result<()> {
        let left = build_table(
            ("a1", &vec![0, 1, 2, 3, 4, 5]),
            ("b1", &vec![3, 4, 5, 6, 6, 7]),
            ("c1", &vec![4, 5, 6, 7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![0, 10, 20, 30, 40]),
            ("b2", &vec![2, 4, 6, 6, 8]),
            ("c2", &vec![50, 60, 70, 80, 90]),
        );
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b2", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, Left).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 0  | 3  | 4  |    |    |    |",
            "| 1  | 4  | 5  | 10 | 4  | 60 |",
            "| 2  | 5  | 6  |    |    |    |",
            "| 3  | 6  | 7  | 20 | 6  | 70 |",
            "| 3  | 6  | 7  | 30 | 6  | 80 |",
            "| 4  | 6  | 8  | 20 | 6  | 70 |",
            "| 4  | 6  | 8  | 30 | 6  | 80 |",
            "| 5  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_sort_order() -> Result<()> {
        let left = build_table(
            ("a1", &vec![0, 1, 2, 3]),
            ("b1", &vec![3, 4, 5, 7]),
            ("c1", &vec![6, 7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![0, 10, 20, 30]),
            ("b2", &vec![2, 4, 5, 6]),
            ("c2", &vec![60, 70, 80, 90]),
        );
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b2", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, Right).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "|    |    |    | 0  | 2  | 60 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "|    |    |    | 30 | 6  | 90 |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_left_multiple_batches() -> Result<()> {
        let left_batch_1 = build_table_i32(
            ("a1", &vec![0, 1, 2]),
            ("b1", &vec![3, 4, 5]),
            ("c1", &vec![4, 5, 6]),
        );
        let left_batch_2 = build_table_i32(
            ("a1", &vec![3, 4, 5, 6]),
            ("b1", &vec![6, 6, 7, 9]),
            ("c1", &vec![7, 8, 9, 9]),
        );
        let right_batch_1 = build_table_i32(
            ("a2", &vec![0, 10, 20]),
            ("b2", &vec![2, 4, 6]),
            ("c2", &vec![50, 60, 70]),
        );
        let right_batch_2 = build_table_i32(
            ("a2", &vec![30, 40]),
            ("b2", &vec![6, 8]),
            ("c2", &vec![80, 90]),
        );
        let left = build_table_from_batches(vec![left_batch_1, left_batch_2]);
        let right = build_table_from_batches(vec![right_batch_1, right_batch_2]);
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b2", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, Left).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 0  | 3  | 4  |    |    |    |",
            "| 1  | 4  | 5  | 10 | 4  | 60 |",
            "| 2  | 5  | 6  |    |    |    |",
            "| 3  | 6  | 7  | 20 | 6  | 70 |",
            "| 3  | 6  | 7  | 30 | 6  | 80 |",
            "| 4  | 6  | 8  | 20 | 6  | 70 |",
            "| 4  | 6  | 8  | 30 | 6  | 80 |",
            "| 5  | 7  | 9  |    |    |    |",
            "| 6  | 9  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_multiple_batches() -> Result<()> {
        let right_batch_1 = build_table_i32(
            ("a2", &vec![0, 1, 2]),
            ("b2", &vec![3, 4, 5]),
            ("c2", &vec![4, 5, 6]),
        );
        let right_batch_2 = build_table_i32(
            ("a2", &vec![3, 4, 5, 6]),
            ("b2", &vec![6, 6, 7, 9]),
            ("c2", &vec![7, 8, 9, 9]),
        );
        let left_batch_1 = build_table_i32(
            ("a1", &vec![0, 10, 20]),
            ("b1", &vec![2, 4, 6]),
            ("c1", &vec![50, 60, 70]),
        );
        let left_batch_2 = build_table_i32(
            ("a1", &vec![30, 40]),
            ("b1", &vec![6, 8]),
            ("c1", &vec![80, 90]),
        );
        let left = build_table_from_batches(vec![left_batch_1, left_batch_2]);
        let right = build_table_from_batches(vec![right_batch_1, right_batch_2]);
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b2", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, Right).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "|    |    |    | 0  | 3  | 4  |",
            "| 10 | 4  | 60 | 1  | 4  | 5  |",
            "|    |    |    | 2  | 5  | 6  |",
            "| 20 | 6  | 70 | 3  | 6  | 7  |",
            "| 30 | 6  | 80 | 3  | 6  | 7  |",
            "| 20 | 6  | 70 | 4  | 6  | 8  |",
            "| 30 | 6  | 80 | 4  | 6  | 8  |",
            "|    |    |    | 5  | 7  | 9  |",
            "|    |    |    | 6  | 9  | 9  |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_full_multiple_batches() -> Result<()> {
        let left_batch_1 = build_table_i32(
            ("a1", &vec![0, 1, 2]),
            ("b1", &vec![3, 4, 5]),
            ("c1", &vec![4, 5, 6]),
        );
        let left_batch_2 = build_table_i32(
            ("a1", &vec![3, 4, 5, 6]),
            ("b1", &vec![6, 6, 7, 9]),
            ("c1", &vec![7, 8, 9, 9]),
        );
        let right_batch_1 = build_table_i32(
            ("a2", &vec![0, 10, 20]),
            ("b2", &vec![2, 4, 6]),
            ("c2", &vec![50, 60, 70]),
        );
        let right_batch_2 = build_table_i32(
            ("a2", &vec![30, 40]),
            ("b2", &vec![6, 8]),
            ("c2", &vec![80, 90]),
        );
        let left = build_table_from_batches(vec![left_batch_1, left_batch_2]);
        let right = build_table_from_batches(vec![right_batch_1, right_batch_2]);
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b2", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, Full).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "|    |    |    | 0  | 2  | 50 |",
            "|    |    |    | 40 | 8  | 90 |",
            "| 0  | 3  | 4  |    |    |    |",
            "| 1  | 4  | 5  | 10 | 4  | 60 |",
            "| 2  | 5  | 6  |    |    |    |",
            "| 3  | 6  | 7  | 20 | 6  | 70 |",
            "| 3  | 6  | 7  | 30 | 6  | 80 |",
            "| 4  | 6  | 8  | 20 | 6  | 70 |",
            "| 4  | 6  | 8  | 30 | 6  | 80 |",
            "| 5  | 7  | 9  |    |    |    |",
            "| 6  | 9  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }
}
