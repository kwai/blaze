// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{any::Any, fmt::Formatter, io::Write, sync::Arc};

use arrow::{
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use blaze_jni_bridge::{jni_call_static, jni_get_string, jni_new_global_ref, jni_new_string};
use datafusion::{
    common::{Result, ScalarValue, Statistics},
    execution::context::TaskContext,
    parquet::{
        arrow::{parquet_to_arrow_schema, ArrowWriter},
        basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel},
        file::properties::{EnabledStatistics, WriterProperties, WriterVersion},
        schema::{parser::parse_message_type, types::SchemaDescriptor},
    },
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        metrics::{BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricValue, MetricsSet, Time},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, Metric, Partitioning,
        SendableRecordBatchStream,
    },
};
use datafusion_ext_commons::{
    array_size::ArraySize,
    cast::cast,
    df_execution_err,
    hadoop_fs::{FsDataOutputStream, FsProvider},
};
use futures::{stream::once, StreamExt, TryStreamExt};
use parking_lot::Mutex;

use crate::common::output::TaskOutputter;

#[derive(Debug)]
pub struct ParquetSinkExec {
    fs_resource_id: String,
    input: Arc<dyn ExecutionPlan>,
    num_dyn_parts: usize,
    props: Vec<(String, String)>,
    metrics: ExecutionPlanMetricsSet,
}

impl ParquetSinkExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        fs_resource_id: String,
        num_dyn_parts: usize,
        props: Vec<(String, String)>,
    ) -> Self {
        Self {
            input,
            fs_resource_id,
            num_dyn_parts,
            props,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for ParquetSinkExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ParquetSink")
    }
}

impl ExecutionPlan for ParquetSinkExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.fs_resource_id.clone(),
            self.num_dyn_parts,
            self.props.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let metrics = BaselineMetrics::new(&self.metrics, partition);
        let elapsed_compute = metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer();

        // register io_time metric
        let io_time = Time::default();
        let io_time_metric = Arc::new(Metric::new(
            MetricValue::Time {
                name: "io_time".into(),
                time: io_time.clone(),
            },
            Some(partition),
        ));
        self.metrics.register(io_time_metric);

        // register bytes_written metric
        let bytes_written = Count::default();
        let bytes_written_metric = Arc::new(Metric::new(
            MetricValue::Count {
                name: "bytes_written".into(),
                count: bytes_written.clone(),
            },
            Some(partition),
        ));
        self.metrics.register(bytes_written_metric);

        let parquet_sink_context = Arc::new(ParquetSinkContext::try_new(
            &self.fs_resource_id,
            self.num_dyn_parts,
            &io_time,
            &self.props,
        )?);

        let input = self.input.execute(partition, context.clone())?;
        let output = Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            once(execute_parquet_sink(
                partition,
                context,
                parquet_sink_context,
                input,
                metrics,
                bytes_written,
            ))
            .try_flatten(),
        ));
        Ok(output)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        todo!()
    }
}

struct ParquetSinkContext {
    fs_provider: FsProvider,
    hive_schema: SchemaRef,
    num_dyn_parts: usize,
    row_group_block_size: usize,
    props: WriterProperties,
}

impl ParquetSinkContext {
    fn try_new(
        fs_resource_id: &str,
        num_dyn_parts: usize,
        io_time: &Time,
        props: &[(String, String)],
    ) -> Result<Self> {
        let fs_provider = {
            let resource_id = jni_new_string!(&fs_resource_id)?;
            let fs = jni_call_static!(JniBridge.getResource(resource_id.as_obj()) -> JObject)?;
            FsProvider::new(jni_new_global_ref!(fs.as_obj())?, io_time)
        };

        // parse hive schema from props
        let hive_schema = match props
            .iter()
            .find(|(key, _)| key == "parquet.hive.schema")
            .map(|(_, value)| value)
            .and_then(|value| parse_message_type(value.as_str()).ok())
            .and_then(|tp| parquet_to_arrow_schema(&SchemaDescriptor::new(Arc::new(tp)), None).ok())
            .map(Arc::new)
        {
            Some(hive_schema) => hive_schema,
            _ => df_execution_err!("missing parquet.hive.schema")?,
        };

        // parse row group byte size from props
        let row_group_block_size = props
            .iter()
            .find(|(key, _)| key == "parquet.block.size")
            .and_then(|(_, value)| value.parse::<usize>().ok())
            .unwrap_or(128 * 1024 * 1024);

        Ok(Self {
            fs_provider,
            hive_schema,
            num_dyn_parts,
            row_group_block_size,
            props: parse_writer_props(props),
        })
    }
}

async fn execute_parquet_sink(
    partition_id: usize,
    context: Arc<TaskContext>,
    parquet_sink_context: Arc<ParquetSinkContext>,
    mut input: SendableRecordBatchStream,
    metrics: BaselineMetrics,
    bytes_written: Count,
) -> Result<SendableRecordBatchStream> {
    let schema = input.schema();
    let part_writer: Arc<Mutex<Option<PartWriter>>> = Arc::default();

    context.output_with_sender("ParquetSink", schema.clone(), move |sender| async move {
        macro_rules! part_writer_init {
            ($batch:expr, $part_values:expr) => {{
                log::info!(
                    "[partition={partition_id}] starts writing partition: {:?}",
                    $part_values
                );
                let parquet_sink_context_cloned = parquet_sink_context.clone();
                *part_writer.lock() = Some({
                    // send identity batch, after that we can achieve a new output file
                    sender.send(Ok($batch.slice(0, 1)), None).await;
                    tokio::task::spawn_blocking(move || {
                        PartWriter::try_new(partition_id, parquet_sink_context_cloned, $part_values)
                    })
                    .await
                    .or_else(|e| df_execution_err!("closing parquet file error: {e}"))??
                });
            }};
        }
        macro_rules! part_writer_close {
            () => {{
                let maybe_writer = part_writer.lock().take();
                if let Some(w) = maybe_writer {
                    let file_stat = tokio::task::spawn_blocking(move || w.close())
                        .await
                        .or_else(|e| df_execution_err!("closing parquet file error: {e}"))??;
                    jni_call_static!(
                        BlazeNativeParquetSinkUtils.completeOutput(
                            jni_new_string!(&file_stat.path)?.as_obj(),
                            file_stat.num_rows as i64,
                            file_stat.num_bytes as i64,
                        ) -> ()
                    )?;
                    metrics.output_rows().add(file_stat.num_rows);
                    bytes_written.add(file_stat.num_bytes);
                }
            }}
        }

        // write parquet data
        while let Some(mut batch) = input.next().await.transpose()? {
            let _timer = metrics.elapsed_compute().timer();
            if batch.num_rows() == 0 {
                continue;
            }

            while batch.num_rows() > 0 {
                let part_values =
                    get_dyn_part_values(&batch, parquet_sink_context.num_dyn_parts, 0)?;
                let part_writer_outdated =
                    part_writer.lock().as_ref().map(|w| &w.part_values) != Some(&part_values);

                if part_writer_outdated {
                    part_writer_close!();
                    part_writer_init!(batch, &part_values);
                    continue;
                }

                // compute sub batch size
                let batch_mem_size = batch.get_array_mem_size();
                let num_sub_batches = (batch_mem_size / 1048576).max(1);
                let num_sub_batch_rows = (batch.num_rows() / num_sub_batches).max(16);

                // split batch into current part and rest parts, then write current part
                let m = rfind_part_values(&batch, &part_values)?;
                let cur_batch = batch.slice(0, m);
                batch = batch.slice(m, batch.num_rows() - m);

                // write cur batch
                let cur_batch = adapt_schema(&cur_batch, &parquet_sink_context.hive_schema)?;
                let mut offset = 0;
                while offset < cur_batch.num_rows() {
                    let part_writer = part_writer.clone();
                    let sub_batch_size = num_sub_batch_rows.min(cur_batch.num_rows() - offset);
                    let sub_batch = cur_batch.slice(offset, sub_batch_size);
                    offset += sub_batch_size;

                    tokio::task::spawn_blocking(move || {
                        let mut part_writer = part_writer.lock();
                        let w = part_writer.as_mut().unwrap();
                        w.write(&sub_batch)
                    })
                    .await
                    .or_else(|e| df_execution_err!("writing parquet file error: {e}"))??;
                }
            }
        }
        part_writer_close!();
        Ok(())
    })
}

fn adapt_schema(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let mut casted_cols = vec![];

    for (col_idx, casted_field) in schema.fields().iter().enumerate() {
        casted_cols.push(cast(batch.column(col_idx), casted_field.data_type())?);
    }
    Ok(RecordBatch::try_new_with_options(
        schema.clone(),
        casted_cols,
        &RecordBatchOptions::new().with_row_count(Some(num_rows)),
    )?)
}

fn rfind_part_values(batch: &RecordBatch, part_values: &[ScalarValue]) -> Result<usize> {
    for row_idx in (0..batch.num_rows()).rev() {
        if get_dyn_part_values(batch, part_values.len(), row_idx)? == part_values {
            return Ok(row_idx + 1);
        }
    }
    Ok(0)
}

fn parse_writer_props(prop_kvs: &[(String, String)]) -> WriterProperties {
    let mut builder = WriterProperties::builder();

    macro_rules! setprop {
        ($key:expr, $value:expr, $tnum:ty, $setfn:ident) => {{
            if let Ok(value) = $value.parse::<$tnum>() {
                builder.$setfn(value)
            } else {
                builder
            }
        }};
    }

    // apply default configuration from parquet-rs
    builder = builder.set_data_page_row_count_limit(20000);

    // do not use page-level statistics and bloom filter
    builder = builder.set_statistics_enabled(EnabledStatistics::Chunk);
    builder = builder.set_bloom_filter_enabled(false);

    // apply configuration
    for (key, value) in prop_kvs {
        builder = match key.as_ref() {
            "parquet.page.size" => setprop!(key, value, usize, set_data_page_size_limit),
            "parquet.page.row.count.limit" => {
                setprop!(key, value, usize, set_data_page_row_count_limit)
            }
            "parquet.enable.dictionary" => setprop!(key, value, bool, set_dictionary_enabled),
            "parquet.dictionary.page.size" => {
                setprop!(key, value, usize, set_dictionary_page_size_limit)
            }
            "parquet.statistics.truncate.length" => {
                setprop!(key, value, usize, set_max_statistics_size)
            }
            "parquet.writer.version" => {
                builder.set_writer_version(match value.to_ascii_uppercase().as_ref() {
                    "PARQUET_1_0" => WriterVersion::PARQUET_1_0,
                    "PARQUET_2_0" => WriterVersion::PARQUET_2_0,
                    _ => {
                        log::warn!("unsupported parquet writer version: {}", value);
                        WriterVersion::PARQUET_1_0
                    }
                })
            }
            "parquet.compression" => {
                builder.set_compression(match value.to_ascii_uppercase().as_ref() {
                    "UNCOMPRESSED" => Compression::UNCOMPRESSED,
                    "SNAPPY" => Compression::SNAPPY,
                    "GZIP" => Compression::GZIP(GzipLevel::default()),
                    "LZO" => Compression::LZO,
                    "BROTLI" => Compression::BROTLI(BrotliLevel::default()),
                    "LZ4" => Compression::LZ4,
                    "ZSTD" => {
                        let level_default = ZstdLevel::default().compression_level();
                        let level = prop_kvs
                            .iter()
                            .find(|(key, _)| key == "parquet.compression.codec.zstd.level")
                            .map(|(_, value)| value.parse::<i32>().unwrap_or(level_default))
                            .unwrap_or(level_default);
                        Compression::ZSTD(ZstdLevel::try_new(level).unwrap_or_default())
                    }
                    _ => {
                        log::warn!("unsupported parquet compression: {}", value);
                        Compression::UNCOMPRESSED
                    }
                })
            }
            _ => builder,
        }
    }
    builder.build()
}

#[derive(Debug)]
struct PartFileStat {
    path: String,
    num_rows: usize,
    num_bytes: usize,
}

struct PartWriter {
    partition_id: usize,
    path: String,
    parquet_sink_context: Arc<ParquetSinkContext>,
    parquet_writer: ArrowWriter<FSDataWriter>,
    part_values: Vec<ScalarValue>,
    rows_written: Count,
    bytes_written: Count,
}

impl PartWriter {
    fn try_new(
        partition_id: usize,
        parquet_sink_context: Arc<ParquetSinkContext>,
        part_values: &[ScalarValue],
    ) -> Result<Self> {
        if !part_values.is_empty() {
            log::info!(
                "[partition={partition_id}] starts outputting dynamic partition: {part_values:?}"
            );
        }
        let part_file = jni_get_string!(
            jni_call_static!(BlazeNativeParquetSinkUtils.getTaskOutputPath() -> JObject)?
                .as_obj()
                .into()
        )?;
        log::info!("[partition={partition_id}] starts writing parquet file: {part_file}");

        let fs = parquet_sink_context.fs_provider.provide(&part_file)?;
        let bytes_written = Count::new();
        let rows_written = Count::new();
        let fout = fs.create(&part_file)?;
        let data_writer = FSDataWriter::new(fout, &bytes_written);
        let parquet_writer = ArrowWriter::try_new(
            data_writer,
            parquet_sink_context.hive_schema.clone(),
            Some(parquet_sink_context.props.clone()),
        )?;
        Ok(Self {
            partition_id,
            path: part_file,
            parquet_sink_context,
            parquet_writer,
            part_values: part_values.to_vec(),
            rows_written,
            bytes_written,
        })
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let row_group_block_size = self.parquet_sink_context.row_group_block_size;
        self.parquet_writer.write(&batch)?;
        if self.parquet_writer.in_progress_size() >= row_group_block_size {
            self.parquet_writer.flush()?;
        }
        Ok(())
    }

    fn close(self) -> Result<PartFileStat> {
        let partition_id = self.partition_id;
        let mut parquet_writer = self.parquet_writer;
        parquet_writer.flush()?;
        let rows_written = parquet_writer
            .flushed_row_groups()
            .iter()
            .map(|rg| rg.num_rows() as usize)
            .sum();
        let data_writer = parquet_writer.into_inner()?;
        let bytes_written = data_writer.bytes_written.value();
        data_writer.close()?;

        self.rows_written.add(rows_written);
        self.bytes_written.add(bytes_written);
        let stat = PartFileStat {
            path: self.path,
            num_rows: rows_written,
            num_bytes: bytes_written,
        };
        log::info!("[partition={partition_id}] finished writing parquet file: {stat:?}");
        Ok(stat)
    }
}

fn get_dyn_part_values(
    batch: &RecordBatch,
    num_dyn_parts: usize,
    row_idx: usize,
) -> Result<Vec<ScalarValue>> {
    batch
        .columns()
        .iter()
        .skip(batch.num_columns() - num_dyn_parts)
        .map(|part_col| ScalarValue::try_from_array(part_col, row_idx))
        .collect()
}

// Write wrapper for FSDataOutputStream
struct FSDataWriter {
    inner: FsDataOutputStream,
    bytes_written: Count,
}

impl FSDataWriter {
    pub fn new(inner: FsDataOutputStream, bytes_written: &Count) -> Self {
        Self {
            inner,
            bytes_written: bytes_written.clone(),
        }
    }

    pub fn close(self) -> Result<()> {
        self.inner.close()
    }
}
impl Write for FSDataWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner
            .write_fully(&buf)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))?;
        self.bytes_written.add(buf.len());
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
