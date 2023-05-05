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

use arrow::array::*;
use arrow::compute::kernels::take::take;
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};
use bitvec::vec::BitVec;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::joins::utils::{build_join_schema, check_join_is_valid, JoinOn};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet, Time,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use datafusion_ext_commons::array_builder::{
    builder_append_null, builder_extend, make_batch, new_array_builders,
};
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use parking_lot::Mutex as SyncMutex;
use std::any::Any;
use std::borrow::BorrowMut;
use std::cmp::Ordering;
use std::fmt::Formatter;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use crate::common::{output_with_sender, WrappedRecordBatchSender};

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
        sort_options: Vec<SortOptions>,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();

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
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            sort_options,
        })
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
            JoinType::Inner | JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti => {
                self.left.output_ordering()
            }
            JoinType::Right => self.right.output_ordering(),
            JoinType::Full => None,
            j @ (JoinType::RightSemi | JoinType::RightAnti) => {
                panic!("join type not supported: {:?}", j);
            }
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

        let on_left = self.on.iter().map(|on| on.0.clone()).collect();
        let on_right = self.on.iter().map(|on| on.1.clone()).collect();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(
                execute_join(
                    context,
                    self.join_type,
                    self.schema(),
                    left,
                    right,
                    on_left,
                    on_right,
                    self.sort_options.clone(),
                    metrics,
                )
                .map_err(|e| ArrowError::ExternalError(Box::new(e))),
            )
            .try_flatten(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "SortMergeJoin: join_type={:?}, on={:?}, schema={:?}",
                    self.join_type, self.on, &self.schema
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}

#[derive(Clone)]
struct JoinParams {
    join_type: JoinType,
    output_schema: SchemaRef,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    on_left: Vec<usize>,
    on_right: Vec<usize>,
    on_data_types: Vec<DataType>,
    sort_options: Vec<SortOptions>,
    batch_size: usize,
}

#[allow(clippy::too_many_arguments)]
async fn execute_join(
    context: Arc<TaskContext>,
    join_type: JoinType,
    output_schema: SchemaRef,
    left: SendableRecordBatchStream,
    right: SendableRecordBatchStream,
    on_left: Vec<Column>,
    on_right: Vec<Column>,
    sort_options: Vec<SortOptions>,
    metrics: Arc<BaselineMetrics>,
) -> Result<SendableRecordBatchStream> {
    let left_schema = left.schema();
    let right_schema = right.schema();
    let on_left = on_left.into_iter().map(|c| c.index()).collect::<Vec<_>>();
    let on_right = on_right.into_iter().map(|c| c.index()).collect::<Vec<_>>();
    let on_data_types = on_left
        .iter()
        .map(|&i| left_schema.field(i).data_type().clone())
        .collect::<Vec<_>>();
    let batch_size = context.session_config().batch_size();

    let join_params = JoinParams {
        join_type,
        output_schema,
        left_schema,
        right_schema,
        on_left,
        on_right,
        on_data_types,
        sort_options,
        batch_size,
    };
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

    let mut left_cursor =
        StreamCursor::try_new(left, on_row_converter.clone(), join_params.on_left.clone()).await?;
    let mut right_cursor = StreamCursor::try_new(
        right,
        on_row_converter.clone(),
        join_params.on_right.clone(),
    )
    .await?;

    let output_schema = join_params.output_schema.clone();
    output_with_sender("SortMergeJoin", output_schema, move |sender| async move {
        match join_params.join_type {
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                join_combined(
                    &mut left_cursor,
                    &mut right_cursor,
                    join_params,
                    metrics,
                    sender,
                ).await?;
            }
            JoinType::LeftSemi | JoinType::LeftAnti => {
                join_semi(
                    &mut left_cursor,
                    &mut right_cursor,
                    join_params,
                    metrics,
                    sender,
                ).await?;
            }
            other @ (JoinType::RightSemi | JoinType::RightAnti) => {
                panic!("join type not supported: {other}");
            }
        }
        Ok(())
    })
}

async fn join_combined(
    left_cursor: &mut StreamCursor,
    right_cursor: &mut StreamCursor,
    join_params: JoinParams,
    metrics: Arc<BaselineMetrics>,
    sender: WrappedRecordBatchSender,
) -> Result<()> {
    let total_time = Time::new();
    let io_time = Time::new();
    let total_timer = total_time.timer();

    let left_dts = join_params
        .left_schema
        .fields()
        .iter()
        .map(|field| field.data_type().clone())
        .collect::<Vec<_>>();
    let right_dts = join_params
        .right_schema
        .fields()
        .iter()
        .map(|field| field.data_type().clone())
        .collect::<Vec<_>>();
    let join_type = join_params.join_type;

    let mut staging_len = 0;
    let mut staging = new_array_builders(&join_params.output_schema, join_params.batch_size);
    let num_left_columns = join_params.left_schema.fields().len();

    macro_rules! flush_staging {
        () => {{
            let batch = make_batch(
                join_params.output_schema.clone(),
                std::mem::replace(&mut staging, {
                    new_array_builders(&join_params.output_schema, join_params.batch_size)
                }),
            )?;
            staging_len = 0;
            let _ = staging_len; // suppress value unused warnings

            metrics.record_output(batch.num_rows());
            let _ = io_time.timer();
            sender.send(Ok(batch)).await.ok();
        }};
    }
    macro_rules! cartesian_join_lr {
        ($batch1:expr, $range1:expr, $batch2:expr, $range2:expr) => {{
            for l in $range1.clone() {
                for r in $range2.clone() {
                    let (staging_l, staging_r) = staging.split_at_mut(num_left_columns);

                    $batch1.columns().iter().enumerate().for_each(|(i, c)| {
                        builder_extend(staging_l[i].as_mut(), c, &[l], c.data_type());
                    });
                    $batch2.columns().iter().enumerate().for_each(|(i, c)| {
                        builder_extend(staging_r[i].as_mut(), c, &[r], c.data_type())
                    });

                    staging_len += 1;
                    if staging_len >= join_params.batch_size {
                        flush_staging!();
                    }
                }
            }
        }};
    }

    macro_rules! cartesian_join_rl {
        ($batch1:expr, $range1:expr, $batch2:expr, $range2:expr) => {{
            for r in $range2.clone() {
                for l in $range1.clone() {
                    let (staging_l, staging_r) = staging.split_at_mut(num_left_columns);

                    $batch1.columns().iter().enumerate().for_each(|(i, c)| {
                        builder_extend(staging_l[i].as_mut(), c, &[l], c.data_type());
                    });
                    $batch2.columns().iter().enumerate().for_each(|(i, c)| {
                        builder_extend(staging_r[i].as_mut(), c, &[r], c.data_type())
                    });

                    staging_len += 1;
                    if staging_len >= join_params.batch_size {
                        flush_staging!();
                    }
                }
            }
        }};
    }

    macro_rules! cartesian_join_columnar_lr {
        ($batch1:expr, $range1:expr, $batch2:expr, $range2:expr) => {{
            let mut lindices = UInt64Builder::new();
            let mut rindices = UInt64Builder::new();

            for l in $range1.clone() {
                for r in $range2.clone() {
                    lindices.append_value(l as u64);
                    rindices.append_value(r as u64);
                }

                if lindices.len() >= join_params.batch_size || l + 1 == $range1.end {
                    let mut cols = Vec::with_capacity(join_params.output_schema.fields().len());
                    let larray = lindices.finish();
                    let rarray = rindices.finish();

                    for c in $batch1.columns() {
                        cols.push(take(c.as_ref(), &larray, None)?);
                    }
                    for c in $batch2.columns() {
                        cols.push(take(c.as_ref(), &rarray, None)?);
                    }
                    let batch = RecordBatch::try_new(join_params.output_schema.clone(), cols)?;

                    if staging_len > 0 {
                        // for keeping input order
                        flush_staging!();
                    }
                    metrics.record_output(batch.num_rows());
                    let _ = io_time.timer();
                    sender.send(Ok(batch)).await.ok();
                }
            }
        }};
    }

    macro_rules! cartesian_join_columnar_rl {
        ($batch1:expr, $range1:expr, $batch2:expr, $range2:expr) => {{
            let mut lindices = UInt64Builder::new();
            let mut rindices = UInt64Builder::new();

            for r in $range2.clone() {
                for l in $range1.clone() {
                    lindices.append_value(l as u64);
                    rindices.append_value(r as u64);
                }

                if rindices.len() >= join_params.batch_size || r + 1 == $range2.end {
                    let mut cols = Vec::with_capacity(join_params.output_schema.fields().len());
                    let larray = lindices.finish();
                    let rarray = rindices.finish();

                    for c in $batch1.columns() {
                        cols.push(take(c.as_ref(), &larray, None)?);
                    }
                    for c in $batch2.columns() {
                        cols.push(take(c.as_ref(), &rarray, None)?);
                    }

                    let batch = RecordBatch::try_new(join_params.output_schema.clone(), cols)?;

                    if staging_len > 0 {
                        // for keeping input order
                        flush_staging!();
                    }
                    metrics.record_output(batch.num_rows());
                    let _ = io_time.timer();
                    sender.send(Ok(batch)).await.ok();
                }
            }
        }};
    }

    loop {
        let left_finished = left_cursor.on_rows.is_none();
        let right_finished = right_cursor.on_rows.is_none();
        let all_finished = left_finished && right_finished;

        if left_finished && matches!(join_type, JoinType::Inner | JoinType::Left) {
            break;
        }
        if right_finished && matches!(join_type, JoinType::Inner | JoinType::Right) {
            break;
        }
        if all_finished && matches!(join_type, JoinType::Full) {
            break;
        }

        let ord = compare_cursor(left_cursor, right_cursor);
        match ord {
            Ordering::Less => {
                if matches!(join_type, JoinType::Left | JoinType::Full) {
                    let left_batch = left_cursor.batch.as_ref().unwrap();
                    let idx = left_cursor.idx;
                    let (staging_l, staging_r) = staging.split_at_mut(num_left_columns);

                    // append <left-columns + nulls> for left/full joins
                    left_batch.columns().iter().enumerate().for_each(|(i, c)| {
                        builder_extend(staging_l[i].as_mut(), c, &[idx], c.data_type());
                    });
                    right_dts.iter().enumerate().for_each(|(i, dt)| {
                        builder_append_null(staging_r[i].as_mut(), dt);
                    });

                    staging_len += 1;
                    if staging_len >= join_params.batch_size {
                        flush_staging!();
                    }
                }
                left_cursor.forward().await?;
                continue;
            }
            Ordering::Greater => {
                if matches!(join_type, JoinType::Right | JoinType::Full) {
                    let right_batch = right_cursor.batch.as_ref().unwrap();
                    let idx = right_cursor.idx;
                    let (staging_l, staging_r) = staging.split_at_mut(num_left_columns);

                    // append <nulls + right-columns> for right/full joins
                    left_dts.iter().enumerate().for_each(|(i, dt)| {
                        builder_append_null(staging_l[i].as_mut(), dt);
                    });
                    right_batch.columns().iter().enumerate().for_each(|(i, c)| {
                        builder_extend(staging_r[i].as_mut(), c, &[idx], c.data_type());
                    });

                    staging_len += 1;
                    if staging_len >= join_params.batch_size {
                        flush_staging!();
                    }
                }
                right_cursor.forward().await?;
                continue;
            }
            Ordering::Equal => {}
        }

        // NOTE:
        //  Perform cartesian join of two equal parts.
        //  In cartesian join, at least one side must be very small,
        //  otherwise the size of producted output will explode and
        //  cannot be executed.
        //  So here we read equal batches of two cursors side by side
        //  and find the smaller one. once it is found, it can be probed
        //  to each batch of the bigger side and the bigger side is no
        //  longer needed to be stored in memory.

        let on_row = left_cursor
            .on_rows
            .as_ref()
            .unwrap()
            .row(left_cursor.idx)
            .owned();
        let cursors = [left_cursor.borrow_mut(), right_cursor.borrow_mut()];
        let mut finished = [false, false];
        let mut num_rows = [0, 0];
        let mut cur_indices = [0, 0];
        let mut buffers = [
            Vec::<(RecordBatch, Range<usize>)>::new(),
            Vec::<(RecordBatch, Range<usize>)>::new(),
        ];

        macro_rules! flush_buffers {
            // at least one side is finished
            () => {{
                if finished[0] || finished[1] {
                    for (batch1, range1) in &buffers[0] {
                        for (batch2, range2) in &buffers[1] {
                            if range1.len() * range2.len() < 64 {
                                if join_params.join_type == JoinType::Right {
                                    cartesian_join_rl!(
                                        &batch1,
                                        range1.clone(),
                                        &batch2,
                                        range2.clone()
                                    );
                                } else {
                                    cartesian_join_lr!(
                                        &batch1,
                                        range1.clone(),
                                        &batch2,
                                        range2.clone()
                                    );
                                }
                            } else {
                                if join_params.join_type == JoinType::Right {
                                    cartesian_join_columnar_rl!(
                                        &batch1,
                                        range1.clone(),
                                        &batch2,
                                        range2.clone()
                                    );
                                } else {
                                    cartesian_join_columnar_lr!(
                                        &batch1,
                                        range1.clone(),
                                        &batch2,
                                        range2.clone()
                                    );
                                }
                            }
                        }
                    }
                    if finished[0] {
                        buffers[1].clear();
                    }
                    if finished[1] {
                        buffers[0].clear();
                    }
                }
            }};
        }

        // forward first row
        for i in 0..2 {
            let batch = cursors[i].batch.as_ref().unwrap();
            let idx = cursors[i].idx;
            if idx < batch.num_rows() - 1 {
                cur_indices[i] = idx;
            } else {
                buffers[i].push((batch.clone(), idx..idx + 1));
                num_rows[i] += 1;
                cur_indices[i] = 0;
            }
            cursors[i].forward().await?;
        }

        // read rest equal rows
        while finished != [true, true] {
            let i = match finished {
                // find unfinished smaller side
                [false, true] => 0,
                [true, false] => 1,
                _ => {
                    if num_rows[0] < num_rows[1] {
                        0
                    } else {
                        1
                    }
                }
            };

            if cursors[i].on_rows.is_none() {
                finished[i] = true;
                flush_buffers!();
                continue;
            }

            let batch = cursors[i].batch.as_ref().unwrap();
            let on_rows = cursors[i].on_rows.as_ref().unwrap();
            let idx = cursors[i].idx;

            if on_rows.row(idx).as_ref() == on_row.as_ref() {
                // equal -- if current batch is finished, append to buffer
                if idx == batch.num_rows() - 1 {
                    let range = cur_indices[i]..batch.num_rows();
                    num_rows[i] += range.len();
                    buffers[i].push((batch.clone(), range));
                    cur_indices[i] = 0;
                    flush_buffers!();
                }
                cursors[i].forward().await?;
            } else {
                // unequal -- if current batch is forwarded, append to buffer, then exit
                if idx > cur_indices[i] {
                    let range = cur_indices[i]..idx;
                    num_rows[i] += range.len();
                    buffers[i].push((batch.clone(), range));
                }
                finished[i] = true;
                flush_buffers!();
            }
        }
    }

    if staging_len > 0 {
        flush_staging!();
    }

    drop(total_timer);
    metrics.elapsed_compute().add_duration(Duration::from_nanos(
        (total_time.value()
            - io_time.value()
            - left_cursor.io_time.value()
            - right_cursor.io_time.value()) as u64,
    ));
    Ok(())
}

async fn join_semi(
    left_cursor: &mut StreamCursor,
    right_cursor: &mut StreamCursor,
    join_params: JoinParams,
    metrics: Arc<BaselineMetrics>,
    sender: WrappedRecordBatchSender,
) -> Result<()> {
    let total_time = Time::new();
    let io_time = Time::new();
    let total_timer = total_time.timer();

    let join_type = join_params.join_type;
    let mut staging_len = 0;
    let mut staging = new_array_builders(&join_params.left_schema, join_params.batch_size);

    macro_rules! flush_staging {
        () => {{
            let batch = make_batch(
                join_params.output_schema.clone(),
                std::mem::replace(&mut staging, {
                    new_array_builders(&join_params.output_schema, join_params.batch_size)
                }),
            )?;
            staging_len = 0;
            let _ = staging_len; // suppress value unused warnings

            metrics.record_output(batch.num_rows());
            let _ = io_time.timer();
            sender.send(Ok(batch)).await.ok();
        }};
    }
    macro_rules! output_left_current_record {
        () => {{
            let left_batch = left_cursor.batch.as_ref().unwrap();
            let idx = left_cursor.idx;
            left_batch.columns().iter().enumerate().for_each(|(i, c)| {
                builder_extend(staging[i].as_mut(), c, &[idx], c.data_type());
            });
            staging_len += 1;
            if staging_len >= join_params.batch_size {
                flush_staging!();
            }
        }};
    }

    while left_cursor.on_rows.is_some() {
        if join_type == JoinType::LeftSemi && right_cursor.on_rows.is_none() {
            break;
        }

        let ord = compare_cursor(left_cursor, right_cursor);
        match ord {
            Ordering::Less => {
                if join_type == JoinType::LeftAnti {
                    output_left_current_record!();
                }
                left_cursor.forward().await?;
                continue;
            }
            Ordering::Equal => {
                if join_type == JoinType::LeftSemi {
                    output_left_current_record!();
                }
                left_cursor.forward().await?;
                continue;
            }
            Ordering::Greater => {
                right_cursor.forward().await?;
                continue;
            }
        }
    }
    if staging_len > 0 {
        flush_staging!();
    }

    drop(total_timer);
    metrics.elapsed_compute().add_duration(Duration::from_nanos(
        (total_time.value()
            - io_time.value()
            - left_cursor.io_time.value()
            - right_cursor.io_time.value()) as u64,
    ));
    Ok(())
}

struct StreamCursor {
    stream: SendableRecordBatchStream,
    io_time: Time,
    on_row_converter: Arc<SyncMutex<RowConverter>>,
    on_columns: Vec<usize>,
    batch: Option<RecordBatch>,
    on_rows: Option<Rows>,
    on_row_nulls: BitVec,
    idx: usize,
}

impl StreamCursor {
    async fn try_new(
        mut stream: SendableRecordBatchStream,
        on_row_converter: Arc<SyncMutex<RowConverter>>,
        on_columns: Vec<usize>,
    ) -> Result<Self> {
        if let Some(batch) = stream.next().await.transpose()? {
            let mut cursor = Self {
                stream,
                io_time: Time::new(),
                on_row_converter,
                on_columns,
                on_rows: None,
                on_row_nulls: BitVec::new(),
                batch: Some(batch),
                idx: 0,
            };
            cursor.update_rows()?;
            return Ok(cursor);
        }

        Ok(Self {
            stream,
            io_time: Time::new(),
            on_row_converter,
            on_columns,
            on_rows: None,
            on_row_nulls: BitVec::new(),
            batch: None,
            idx: 0,
        })
    }

    fn update_rows(&mut self) -> Result<()> {
        if let Some(batch) = self.batch.as_ref() {
            let on_columns = batch.project(&self.on_columns)?.columns().to_vec();

            self.on_rows = Some(self.on_row_converter.lock().convert_columns(&on_columns)?);
            self.on_row_nulls = (0..batch.num_rows())
                .map(|idx| on_columns.iter().any(|column| column.is_null(idx)))
                .collect::<BitVec>();
        } else {
            self.on_rows = None;
            self.on_row_nulls = BitVec::new();
        }
        Ok(())
    }

    async fn forward(&mut self) -> Result<()> {
        if let Some(batch) = &self.batch {
            if self.idx + 1 < batch.num_rows() {
                self.idx += 1;
                return Ok(());
            }
            loop {
                match {
                    let _ = self.io_time.timer();
                    self.stream.next().await
                } {
                    Some(batch) => {
                        let batch = batch?;
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        self.batch = Some(batch);
                        self.idx = 0;
                        break;
                    }
                    None => {
                        self.batch = None;
                        self.idx = 0;
                        break;
                    }
                }
            }
            self.update_rows()?;
        } else {
            self.on_rows = None;
            self.on_row_nulls = BitVec::new();
        }
        Ok(())
    }
}

fn compare_cursor(left_cursor: &StreamCursor, right_cursor: &StreamCursor) -> Ordering {
    match (&left_cursor.on_rows, &right_cursor.on_rows) {
        (None, _) => Ordering::Greater,
        (_, None) => Ordering::Less,
        (Some(left_rows), Some(right_rows)) => {
            let left_key = &left_rows.row(left_cursor.idx);
            let right_key = &right_rows.row(right_cursor.idx);
            match left_key.cmp(right_key) {
                Ordering::Greater => Ordering::Greater,
                Ordering::Less => Ordering::Less,
                _ => {
                    if !left_cursor.on_row_nulls[left_cursor.idx] {
                        Ordering::Equal
                    } else {
                        Ordering::Less
                    }
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
        SortMergeJoinExec::try_new(left, right, on, join_type, sort_options)
    }

    fn join_with_options(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
        sort_options: Vec<SortOptions>,
    ) -> Result<SortMergeJoinExec> {
        SortMergeJoinExec::try_new(left, right, on, join_type, sort_options)
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

        let (_, batches) = join_collect(left, right, on, JoinType::Inner).await?;

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

        let (_columns, batches) = join_collect(left, right, on, JoinType::Inner).await?;
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

        let (_columns, batches) = join_collect(left, right, on, JoinType::Inner).await?;
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

        let (_, batches) = join_collect(left, right, on, JoinType::Inner).await?;
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
            JoinType::Inner,
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

        let (_, batches) =
            join_collect_batch_size_equals_two(left, right, on, JoinType::Inner).await?;
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

        let (_, batches) = join_collect(left, right, on, JoinType::Left).await?;
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

        let (_, batches) = join_collect(left, right, on, JoinType::Right).await?;
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

        let (_, batches) = join_collect(left, right, on, JoinType::Full).await?;
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

        let (_, batches) = join_collect(left, right, on, JoinType::LeftAnti).await?;
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

        let (_, batches) = join_collect(left, right, on, JoinType::LeftSemi).await?;
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

        let (_, batches) = join_collect(left, right, on, JoinType::Inner).await?;
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

        let (_, batches) = join_collect(left, right, on, JoinType::Inner).await?;

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

        let (_, batches) = join_collect(left, right, on, JoinType::Inner).await?;
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

        let (_, batches) = join_collect(left, right, on, JoinType::Left).await?;
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

        let (_, batches) = join_collect(left, right, on, JoinType::Right).await?;
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

        let (_, batches) = join_collect(left, right, on, JoinType::Left).await?;
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

        let (_, batches) = join_collect(left, right, on, JoinType::Right).await?;
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

        let (_, batches) = join_collect(left, right, on, JoinType::Full).await?;
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
