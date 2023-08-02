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

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use blaze_jni_bridge::{jni_call_static, jni_new_global_ref, jni_new_string};
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::execution::context::TaskContext;
use datafusion::parquet::arrow::{parquet_to_arrow_schema, ArrowWriter};
use datafusion::parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use datafusion::parquet::file::properties::{WriterProperties, WriterVersion};
use datafusion::parquet::schema::parser::parse_message_type;
use datafusion::parquet::schema::types::SchemaDescriptor;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricValue, MetricsSet, Time,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan, Metric, Partitioning,
    SendableRecordBatchStream,
};
use datafusion_ext_commons::cast::cast;
use datafusion_ext_commons::hadoop_fs::{FsDataOutputStream, FsProvider};
use futures::stream::once;
use futures::{StreamExt, TryStreamExt};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use std::any::Any;
use std::fmt::Formatter;
use std::io::Write;
use std::sync::Arc;

#[derive(Debug)]
pub struct ParquetSinkExec {
    fs_resource_id: String,
    path: String,
    input: Arc<dyn ExecutionPlan>,
    props: Vec<(String, String)>,
    metrics: ExecutionPlanMetricsSet,
}

impl ParquetSinkExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        fs_resource_id: String,
        path: String,
        props: Vec<(String, String)>,
    ) -> Self {
        Self {
            input,
            fs_resource_id,
            path,
            props,
            metrics: ExecutionPlanMetricsSet::new(),
        }
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
            self.path.clone(),
            self.props.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let fs_resource_id = self.fs_resource_id.clone();
        let path = self.path.clone();
        let props = self.props.clone();
        let metrics = BaselineMetrics::new(&self.metrics, partition);

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

        let input = self.input.execute(partition, context.clone())?;
        let output = Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            once(execute_parquet_sink(
                fs_resource_id,
                path,
                input,
                props,
                metrics,
                io_time,
                bytes_written,
            ))
            .try_flatten(),
        ));
        Ok(output)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ParquetSink [path={}]", self.path)
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}

async fn execute_parquet_sink(
    fs_resource_id: String,
    path: String,
    mut input: SendableRecordBatchStream,
    props: Vec<(String, String)>,
    metrics: BaselineMetrics,
    io_time: Time,
    bytes_written: Count,
) -> Result<SendableRecordBatchStream> {
    let mut timer = metrics.elapsed_compute().timer();

    // parse hive_schema from props
    let hive_schema = props
        .iter()
        .find(|(key, _)| key == "parquet.hive.schema")
        .map(|(_, value)| value)
        .and_then(|value| parse_message_type(value.as_str()).ok())
        .and_then(|tp| parquet_to_arrow_schema(&SchemaDescriptor::new(Arc::new(tp)), None).ok())
        .map(Arc::new)
        .ok_or(DataFusionError::Execution(format!(
            "missing parquet.hive.schema"
        )))?;

    // parse row group byte size from props
    let block_size = props
        .iter()
        .find(|(key, _)| key == "parquet.block.size")
        .and_then(|(_, value)| value.parse::<usize>().ok())
        .unwrap_or(128 * 1024 * 1024);

    let schema = input.schema();
    let props = parse_writer_props(&props);
    let parquet_writer: Arc<Mutex<OnceCell<ArrowWriter<FSDataWriter>>>> = Arc::default();
    timer.stop();

    // write parquet data
    while let Some(batch) = input.next().await.transpose()? {
        timer.restart();

        // adapt batch to output schema
        let batch = adapt_schema(batch, &hive_schema)?;

        // init parquet writer after first batch is received
        // to avoid creating empty file
        parquet_writer.lock().get_or_try_init(|| {
            create_parquet_writer(
                &fs_resource_id,
                &path,
                &hive_schema,
                &props,
                &io_time,
                &bytes_written,
            )
        })?;

        let parquet_writer = parquet_writer.clone();
        let metrics = metrics.clone();
        let fut = tokio::task::spawn_blocking(move || {
            let num_rows = batch.num_rows();
            let mut parquet_writer_locked = parquet_writer.lock();
            let parquet_writer = parquet_writer_locked.get_mut().unwrap();

            parquet_writer.write(&batch)?;
            if parquet_writer.in_progress_size() >= block_size {
                parquet_writer.flush()?;
            }
            metrics.record_output(num_rows);
            Ok::<_, DataFusionError>(())
        });
        fut.await
            .map_err(|err| DataFusionError::Execution(format!("{err}")))??;
        timer.stop();
    }

    timer.restart();
    let maybe_writer: Option<ArrowWriter<FSDataWriter>> = parquet_writer.lock().take();
    if let Some(w) = maybe_writer {
        let fut = tokio::task::spawn_blocking(move || {
            w.close()?;
            Ok::<_, DataFusionError>(())
        });
        fut.await
            .map_err(|err| DataFusionError::Execution(format!("{err}")))??;
    }

    // parquet sink does not provide any output records
    Ok(Box::pin(EmptyRecordBatchStream::new(schema)))
}

fn adapt_schema(batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let casted_cols = batch
        .columns()
        .iter()
        .zip(&schema.fields)
        .map(|(col, to_field)| cast(col, to_field.data_type()))
        .collect::<Result<_>>()?;
    Ok(RecordBatch::try_new(schema.clone(), casted_cols)?)
}

fn parse_writer_props(prop_kvs: &[(String, String)]) -> WriterProperties {
    let mut builder = WriterProperties::builder()
        .set_created_by(format!("blaze-build-{}", git_version::git_version!()));

    macro_rules! setprop {
        ($key:expr, $value:expr, $tnum:ty, $setfn:ident) => {{
            if let Ok(value) = $value.parse::<$tnum>() {
                builder.$setfn(value)
            } else {
                log::warn!("invalid parquet prop value: {}={}", $key, $value);
                builder
            }
        }};
    }

    for (key, value) in prop_kvs {
        builder = match key.as_ref() {
            "parquet.page.size" => setprop!(key, value, usize, set_data_page_size_limit),
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
                        WriterVersion::PARQUET_2_0
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
                    "ZSTD" => Compression::ZSTD(ZstdLevel::default()),
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

fn create_parquet_writer(
    fs_resource_id: &str,
    path: &str,
    schema: &SchemaRef,
    props: &WriterProperties,
    io_time: &Time,
    bytes_written: &Count,
) -> Result<ArrowWriter<FSDataWriter>> {
    // get fs object from jni bridge resource
    let fs_provider = {
        let resource_id = jni_new_string!(&fs_resource_id)?;
        let fs = jni_call_static!(JniBridge.getResource(resource_id.as_obj()) -> JObject)?;
        Arc::new(FsProvider::new(jni_new_global_ref!(fs.as_obj())?, io_time))
    };

    // create FSDataOutputStream
    let fs = fs_provider.provide(&path)?;
    let fout = fs.create(&path)?;
    let parquet_writer = ArrowWriter::try_new(
        FSDataWriter::new(fout, bytes_written),
        schema.clone(),
        Some(props.clone()),
    )?;
    Ok(parquet_writer)
}

// AsyncWrite wrapper for FSDataOutputStream
struct FSDataWriter {
    inner: Arc<FsDataOutputStream>,
    bytes_written: Count,
}

impl FSDataWriter {
    pub fn new(inner: FsDataOutputStream, bytes_written: &Count) -> Self {
        Self {
            inner: Arc::new(inner),
            bytes_written: bytes_written.clone(),
        }
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
