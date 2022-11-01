use crate::util::array_builder::{
    builder_append_null, builder_extend, make_batch, new_array_builders,
};
use datafusion::arrow::array::*;
use datafusion::arrow::compute::kernels::take::take;
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::logical_plan::JoinType;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::join_utils::{
    build_join_schema, check_join_is_valid, JoinOn,
};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
};
use datafusion::physical_plan::stream::{
    RecordBatchReceiverStream, RecordBatchStreamAdapter,
};
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use std::any::Any;
use std::borrow::BorrowMut;
use std::cmp::Ordering;
use std::fmt::Formatter;
use std::sync::Arc;

use futures::{StreamExt, TryFutureExt, TryStreamExt};
use itertools::izip;

use tokio::sync::mpsc::Sender;

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
    /// If null_equals_null is true, null == null else null != null
    null_equals_null: bool,
}

impl SortMergeJoinExec {
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
        sort_options: Vec<SortOptions>,
        null_equals_null: bool,
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

        let schema =
            Arc::new(build_join_schema(&left_schema, &right_schema, &join_type).0);

        Ok(Self {
            left,
            right,
            on,
            join_type,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            sort_options,
            null_equals_null,
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
            JoinType::Inner | JoinType::Left | JoinType::Semi | JoinType::Anti => {
                self.left.output_ordering()
            }
            JoinType::Right => self.right.output_ordering(),
            JoinType::Full => None,
        }
    }

    fn relies_on_input_order(&self) -> bool {
        true
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
                self.null_equals_null,
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
                    self.null_equals_null,
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
    null_equals_null: bool,
    batch_size: usize,
}

async fn execute_join(
    context: Arc<TaskContext>,
    join_type: JoinType,
    output_schema: SchemaRef,
    left: SendableRecordBatchStream,
    right: SendableRecordBatchStream,
    on_left: Vec<Column>,
    on_right: Vec<Column>,
    sort_options: Vec<SortOptions>,
    null_equals_null: bool,
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
        null_equals_null,
        batch_size,
    };
    let mut left_cursor = StreamCursor::try_new(left).await?;
    let mut right_cursor = StreamCursor::try_new(right).await?;

    let (sender, receiver) = tokio::sync::mpsc::channel(2);
    let join_params_clone = join_params.clone();
    let join_handle = tokio::task::spawn(async move {
        let result = match join_params.join_type {
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                join_combined(
                    &mut left_cursor,
                    &mut right_cursor,
                    join_params_clone,
                    metrics.clone(),
                    &sender,
                )
                .await
                .map_err(ArrowError::from)
            }

            JoinType::Semi | JoinType::Anti => join_semi(
                &mut left_cursor,
                &mut right_cursor,
                join_params_clone,
                metrics.clone(),
                &sender,
            )
            .await
            .map_err(ArrowError::from),
        };

        if let Err(e) = result {
            sender.send(Err(e)).await.ok();
        }
    });
    Ok(RecordBatchReceiverStream::create(
        &join_params.output_schema,
        receiver,
        join_handle,
    ))
}

async fn join_combined(
    left_cursor: &mut StreamCursor,
    right_cursor: &mut StreamCursor,
    join_params: JoinParams,
    metrics: Arc<BaselineMetrics>,
    sender: &Sender<ArrowResult<RecordBatch>>,
) -> Result<()> {
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
    let mut staging =
        new_array_builders(&join_params.output_schema, join_params.batch_size);
    let num_left_columns = join_params.left_schema.fields().len();

    macro_rules! flush_staging {
        () => {{
            let batch = make_batch(
                join_params.output_schema.clone(),
                std::mem::replace(&mut staging, {
                    new_array_builders(&join_params.output_schema, join_params.batch_size)
                }),
            )?;
            sender.send(Ok(batch)).await.ok();
            metrics.record_output(staging_len);
            staging_len = 0;
            let _ = staging_len; // suppress value unused warnings
        }};
    }
    macro_rules! cartesian_join_lr {
        ($batch1:expr, $range1:expr, $batch2:expr, $range2:expr) => {{
            for l in $range1.clone() {
                for r in $range2.clone() {
                    let (staging_l, staging_r) = staging.split_at_mut(num_left_columns);

                    $batch1.columns().iter().enumerate().for_each(|(i, c)| {
                        builder_extend(&mut staging_l[i], c, &[l], c.data_type());
                    });
                    $batch2.columns().iter().enumerate().for_each(|(i, c)| {
                        builder_extend(&mut staging_r[i], c, &[r], c.data_type())
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
                        builder_extend(&mut staging_l[i], c, &[l], c.data_type());
                    });
                    $batch2.columns().iter().enumerate().for_each(|(i, c)| {
                        builder_extend(&mut staging_r[i], c, &[r], c.data_type())
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
                    let mut cols =
                        Vec::with_capacity(join_params.output_schema.fields().len());
                    let larray = lindices.finish();
                    let rarray = rindices.finish();

                    for c in $batch1.columns() {
                        cols.push(take(c.as_ref(), &larray, None)?);
                    }
                    for c in $batch2.columns() {
                        cols.push(take(c.as_ref(), &rarray, None)?);
                    }
                    let batch =
                        RecordBatch::try_new(join_params.output_schema.clone(), cols)?;

                    if staging_len > 0 {
                        // for keeping input order
                        flush_staging!();
                    }

                    let batch_num_rows = batch.num_rows();
                    sender.send(Ok(batch)).await.ok();
                    metrics.record_output(batch_num_rows);
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
                    let mut cols =
                        Vec::with_capacity(join_params.output_schema.fields().len());
                    let larray = lindices.finish();
                    let rarray = rindices.finish();

                    for c in $batch1.columns() {
                        cols.push(take(c.as_ref(), &larray, None)?);
                    }
                    for c in $batch2.columns() {
                        cols.push(take(c.as_ref(), &rarray, None)?);
                    }

                    let batch =
                        RecordBatch::try_new(join_params.output_schema.clone(), cols)?;

                    if staging_len > 0 {
                        // for keeping input order
                        flush_staging!();
                    }

                    let batch_num_rows = batch.num_rows();
                    sender.send(Ok(batch)).await.ok();
                    metrics.record_output(batch_num_rows);
                }
            }
        }};
    }

    loop {
        let left_finished = left_cursor.current().is_none();
        let right_finished = right_cursor.current().is_none();
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
        let ord = compare_cursor(left_cursor, right_cursor, &join_params);

        if ord.is_lt() {
            if matches!(join_type, JoinType::Left | JoinType::Full) {
                let (left_batch, idx) = left_cursor.current().unwrap();
                let (staging_l, staging_r) = staging.split_at_mut(num_left_columns);

                // append <left-columns + nulls> for left/full joins
                left_batch.columns().iter().enumerate().for_each(|(i, c)| {
                    builder_extend(&mut staging_l[i], c, &[idx], c.data_type());
                });
                right_dts.iter().enumerate().for_each(|(i, dt)| {
                    builder_append_null(&mut staging_r[i], dt);
                });

                staging_len += 1;
                if staging_len >= join_params.batch_size {
                    flush_staging!();
                }
            }
            left_cursor.forward().await?;
            continue;
        }
        if ord.is_gt() {
            if matches!(join_type, JoinType::Right | JoinType::Full) {
                let (right_batch, idx) = right_cursor.current().unwrap();
                let (staging_l, staging_r) = staging.split_at_mut(num_left_columns);

                // append <nulls + right-columns> for right/full joins
                left_dts.iter().enumerate().for_each(|(i, dt)| {
                    builder_append_null(&mut staging_l[i], dt);
                });
                right_batch.columns().iter().enumerate().for_each(|(i, c)| {
                    builder_extend(&mut staging_r[i], c, &[idx], c.data_type());
                });

                staging_len += 1;
                if staging_len >= join_params.batch_size {
                    flush_staging!();
                }
            }
            right_cursor.forward().await?;
            continue;
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

        let cursors = [left_cursor.borrow_mut(), right_cursor.borrow_mut()];
        let ons = [&join_params.on_left, &join_params.on_right];

        let mut finished = [false, false];
        let mut first_batches = [None, None];
        let mut first_indices = [0, 0];
        let mut num_rows = [0, 0];
        let mut cur_indices = [0, 0];
        let mut buffers = [vec![], vec![]];

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
        // read first row of each cursor
        for i in 0..2 {
            let (batch, idx) = cursors[i]
                .current()
                .map(|(batch, start)| (batch.clone(), start))
                .unwrap();
            first_batches[i] = Some(batch.clone());
            first_indices[i] = idx;

            if idx < batch.num_rows() - 1 {
                cur_indices[i] = idx;
            } else {
                buffers[i].push((batch, idx..idx + 1));
                num_rows[i] += 1;
                cur_indices[i] = 0;
            };
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

            if cursors[i].current().is_none() {
                finished[i] = true;
                flush_buffers!();
                continue;
            }
            let (batch, idx) = cursors[i].current().unwrap();

            if row_equal(
                first_batches[i].as_ref().unwrap(),
                ons[i],
                first_indices[i],
                batch,
                ons[i],
                idx,
                &join_params.on_data_types,
                join_params.null_equals_null,
            ) {
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
    Ok(())
}

async fn join_semi(
    left_cursor: &mut StreamCursor,
    right_cursor: &mut StreamCursor,
    join_params: JoinParams,
    metrics: Arc<BaselineMetrics>,
    sender: &Sender<ArrowResult<RecordBatch>>,
) -> Result<()> {
    let join_type = join_params.join_type;
    let mut staging_len = 0;
    let mut staging =
        new_array_builders(&join_params.left_schema, join_params.batch_size);

    macro_rules! flush_staging {
        () => {{
            let batch = make_batch(
                join_params.output_schema.clone(),
                std::mem::replace(&mut staging, {
                    new_array_builders(&join_params.output_schema, join_params.batch_size)
                }),
            )?;
            sender.send(Ok(batch)).await.ok();
            metrics.record_output(staging_len);
            staging_len = 0;
            let _ = staging_len; // suppress value unused warnings
        }};
    }
    macro_rules! output_left_current_record {
        () => {{
            let (left_batch, idx) = left_cursor.current().unwrap();
            left_batch.columns().iter().enumerate().for_each(|(i, c)| {
                builder_extend(&mut staging[i], c, &[idx], c.data_type());
            });
            staging_len += 1;
            if staging_len >= join_params.batch_size {
                flush_staging!();
            }
        }};
    }

    while left_cursor.current().is_some() {
        if join_type == JoinType::Semi && right_cursor.current().is_none() {
            break;
        }

        let ord = compare_cursor(left_cursor, right_cursor, &join_params);
        match ord {
            Ordering::Less => {
                if join_type == JoinType::Anti {
                    output_left_current_record!();
                }
                left_cursor.forward().await?;
                continue;
            }
            Ordering::Equal => {
                if join_type == JoinType::Semi {
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
    Ok(())
}

fn compare_cursor(
    left: &StreamCursor,
    right: &StreamCursor,
    join_params: &JoinParams,
) -> Ordering {
    if left.current().is_none() {
        return Ordering::Greater;
    }
    if right.current().is_none() {
        return Ordering::Less;
    }
    let (left_batch, left_idx) = left.current().unwrap();
    let (right_batch, right_idx) = right.current().unwrap();
    row_compare(
        left_batch,
        &join_params.on_left,
        left_idx,
        right_batch,
        &join_params.on_right,
        right_idx,
        &join_params.on_data_types,
        &join_params.sort_options,
        join_params.null_equals_null,
    )
}

struct StreamCursor {
    stream: SendableRecordBatchStream,
    batch: Option<Arc<RecordBatch>>,
    idx: usize,
}

impl StreamCursor {
    async fn try_new(mut stream: SendableRecordBatchStream) -> Result<Self> {
        if let Some(batch) = stream.next().await {
            Ok(Self {
                stream,
                batch: Some(Arc::new(batch?)),
                idx: 0,
            })
        } else {
            Ok(Self {
                stream,
                batch: None,
                idx: 0,
            })
        }
    }

    fn current(&self) -> Option<(&Arc<RecordBatch>, usize)> {
        self.batch.as_ref().map(|batch| (batch, self.idx))
    }

    async fn forward(&mut self) -> Result<()> {
        if let Some(batch) = &self.batch {
            if self.idx + 1 < batch.num_rows() {
                self.idx += 1;
                return Ok(());
            }
            loop {
                match self.stream.next().await {
                    Some(batch) => {
                        let batch = batch?;
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        self.batch = Some(Arc::new(batch));
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
        }
        Ok(())
    }
}

fn row_compare(
    batch1: &RecordBatch,
    on_batch1: &[usize],
    idx1: usize,
    batch2: &RecordBatch,
    on_batch2: &[usize],
    idx2: usize,
    on_data_types: &[DataType],
    sort_options: &[SortOptions],
    null_equals_null: bool,
) -> Ordering {
    for (dt, sort_options, col1, col2) in izip!(
        on_data_types,
        sort_options,
        on_batch1.iter().map(|&i| batch1.column(i)),
        on_batch2.iter().map(|&i| batch2.column(i))
    ) {
        macro_rules! compare {
            ($arrowty:ident) => {{
                type A = paste::paste! {[< $arrowty Array >]};
                let col1 = col1.as_any().downcast_ref::<A>().unwrap();
                let col2 = col2.as_any().downcast_ref::<A>().unwrap();
                if col1.is_valid(idx1) && col2.is_valid(idx2) {
                    let v1 = &col1.value(idx1);
                    let v2 = &col2.value(idx2);
                    match v1.partial_cmp(v2).unwrap_or(Ordering::Less) {
                        ord @ (Ordering::Greater | Ordering::Less) => {
                            return if sort_options.descending {
                                ord.reverse()
                            } else {
                                ord
                            };
                        }
                        Ordering::Equal => {}
                    }
                } else if null_equals_null && col1.is_null(idx1) && col2.is_null(idx2) {
                    return Ordering::Equal;
                } else if col1.is_null(idx1) ^ !sort_options.nulls_first {
                    return Ordering::Less;
                } else {
                    return Ordering::Greater;
                }
            }};
        }
        match dt {
            DataType::Boolean => compare!(Boolean),
            DataType::Int8 => compare!(Int8),
            DataType::Int16 => compare!(Int16),
            DataType::Int32 => compare!(Int32),
            DataType::Int64 => compare!(Int64),
            DataType::UInt8 => compare!(UInt8),
            DataType::UInt16 => compare!(UInt16),
            DataType::UInt32 => compare!(UInt32),
            DataType::UInt64 => compare!(UInt64),
            DataType::Float32 => compare!(Float32),
            DataType::Float64 => compare!(Float64),
            DataType::Date32 => compare!(Date32),
            DataType::Date64 => compare!(Date64),
            DataType::Time32(TimeUnit::Second) => compare!(Time32Second),
            DataType::Time32(TimeUnit::Millisecond) => compare!(Time32Millisecond),
            DataType::Time64(TimeUnit::Microsecond) => compare!(Time64Microsecond),
            DataType::Time64(TimeUnit::Nanosecond) => compare!(Time64Nanosecond),
            DataType::Utf8 => compare!(String),
            DataType::LargeUtf8 => compare!(LargeString),
            DataType::Decimal128(_, _) => compare!(Decimal128),
            DataType::Decimal256(_, _) => compare!(Decimal256),
            _ => unimplemented!("data type not supported in sort-merge join"),
        }
    }
    Ordering::Equal
}

fn row_equal(
    batch1: &RecordBatch,
    on_batch1: &[usize],
    idx1: usize,
    batch2: &RecordBatch,
    on_batch2: &[usize],
    idx2: usize,
    on_data_types: &[DataType],
    null_equals_null: bool,
) -> bool {
    for (dt, col1, col2) in izip!(
        on_data_types,
        on_batch1.iter().map(|&i| batch1.column(i)),
        on_batch2.iter().map(|&i| batch2.column(i))
    ) {
        macro_rules! eq {
            ($arrowty:ident) => {{
                type A = paste::paste! {[< $arrowty Array >]};
                let col1 = col1.as_any().downcast_ref::<A>().unwrap();
                let col2 = col2.as_any().downcast_ref::<A>().unwrap();
                if col1.is_valid(idx1) && col2.is_valid(idx2) {
                    if col1.value(idx1) != col2.value(idx2) {
                        return false;
                    }
                } else if col1.is_null(idx1) && col2.is_null(idx2) {
                    if !null_equals_null {
                        return false;
                    }
                } else {
                    return false;
                }
            }};
        }
        match dt {
            DataType::Boolean => eq!(Boolean),
            DataType::Int8 => eq!(Int8),
            DataType::Int16 => eq!(Int16),
            DataType::Int32 => eq!(Int32),
            DataType::Int64 => eq!(Int64),
            DataType::UInt8 => eq!(UInt8),
            DataType::UInt16 => eq!(UInt16),
            DataType::UInt32 => eq!(UInt32),
            DataType::UInt64 => eq!(UInt64),
            DataType::Float32 => eq!(Float32),
            DataType::Float64 => eq!(Float64),
            DataType::Date32 => eq!(Date32),
            DataType::Date64 => eq!(Date64),
            DataType::Time32(TimeUnit::Second) => eq!(Time32Second),
            DataType::Time32(TimeUnit::Millisecond) => eq!(Time32Millisecond),
            DataType::Time64(TimeUnit::Microsecond) => eq!(Time64Microsecond),
            DataType::Time64(TimeUnit::Nanosecond) => eq!(Time64Nanosecond),
            DataType::Binary => eq!(Binary),
            DataType::LargeBinary => eq!(LargeBinary),
            DataType::Utf8 => eq!(String),
            DataType::LargeUtf8 => eq!(LargeString),
            DataType::Decimal128(_, _) => eq!(Decimal256),
            DataType::Decimal256(_, _) => eq!(Decimal256),
            _ => unimplemented!("data type not supported in sort-merge join"),
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use datafusion::arrow;
    use datafusion::arrow::array::*;
    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::error::Result;
    use datafusion::logical_plan::JoinType;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::common;
    use datafusion::physical_plan::join_utils::JoinOn;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::ExecutionPlan;
    use std::sync::Arc;

    use crate::plan::sort_merge_join_exec::SortMergeJoinExec;
    use datafusion::prelude::{SessionConfig, SessionContext};

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
        SortMergeJoinExec::try_new(left, right, on, join_type, sort_options, false)
    }

    fn join_with_options(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
        sort_options: Vec<SortOptions>,
        null_equals_null: bool,
    ) -> Result<SortMergeJoinExec> {
        SortMergeJoinExec::try_new(
            left,
            right,
            on,
            join_type,
            sort_options,
            null_equals_null,
        )
    }

    async fn join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let sort_options = vec![SortOptions::default(); on.len()];
        join_collect_with_options(left, right, on, join_type, sort_options, false).await
    }

    async fn join_collect_with_options(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
        sort_options: Vec<SortOptions>,
        null_equals_null: bool,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let join = join_with_options(
            left,
            right,
            on,
            join_type,
            sort_options,
            null_equals_null,
        )?;
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
        let session_ctx =
            SessionContext::with_config(SessionConfig::new().with_batch_size(2));
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
            true,
        )
        .await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b2 | c1 | a1 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 2  | 2  | 9  | 2  | 2  | 80 |",
            "| 2  | 2  | 8  | 2  | 2  | 80 |",
            "| 1  | 1  |    | 1  | 1  | 70 |",
            "| 1  |    | 1  | 1  |    | 10 |",
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

        let (_, batches) = join_collect(left, right, on, JoinType::Anti).await?;
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

        let (_, batches) = join_collect(left, right, on, JoinType::Semi).await?;
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
            "+------------+------------+------------+------------+------------+------------+",
            "| a1         | b1         | c1         | a2         | b1         | c2         |",
            "+------------+------------+------------+------------+------------+------------+",
            "| 1970-01-01 | 2022-04-23 | 1970-01-01 | 1970-01-01 | 2022-04-23 | 1970-01-01 |",
            "| 1970-01-01 | 2022-04-25 | 1970-01-01 | 1970-01-01 | 2022-04-25 | 1970-01-01 |",
            "| 1970-01-01 | 2022-04-25 | 1970-01-01 | 1970-01-01 | 2022-04-25 | 1970-01-01 |",
            "+------------+------------+------------+------------+------------+------------+",
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
