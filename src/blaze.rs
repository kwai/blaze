use std::io::BufWriter;
use std::sync::Arc;

use ballista_core::execution_plans::ShuffleReaderExec;
use ballista_core::serde::protobuf::TaskDefinition;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::file_format::FileScanConfig;
use datafusion::physical_plan::file_format::ParquetExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::file_format::PhysicalPlanConfig;
use datafusion::physical_plan::sort::SortExec;
use jni::JNIEnv;
use jni::objects::JByteBuffer;
use jni::objects::JObject;
use jni::objects::JValue;
use jni::signature::JavaType;
use jni::signature::Primitive;
use jni::JNIEnv;
use prost::Message;

use crate::blaze_shuffle_reader_exec::BlazeShuffleReaderExec;
use crate::hdfs_object_store::HDFSSingleFileObjectStore;
use crate::jni_bridge::JavaClasses;
use crate::util::Util;

#[timed(duration(printer = "info!"))]
pub fn blaze_call_native(
    env: &JNIEnv,
    task_definition: JByteBuffer,
    ipc_record_batch_data_consumer: JObject,
) {
    let _env_logger_init = env_logger::try_init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );
    info!("Blaze native computing started");

    info!("Initializing JavaClasses");
    let env = Util::jni_env_clone(env);
    JavaClasses::init(&env).expect("Error initializing JavaClasses");
    info!("Initializing JavaClasses succeeded");

    info!("Decoding task definition");
    let task_definition_raw = env
        .get_direct_buffer_address(task_definition)
        .expect("Error getting task definition");
    let task_definition: TaskDefinition =
        TaskDefinition::decode(&task_definition_raw[..])
            .expect("Error decoding task definition");
    info!("Decoding task definition succeeded");

    info!("Creating native execution plan");
    let task_id = task_definition
        .task_id
        .expect("Missing task_definition.task_id");
    let plan = &task_definition.plan.expect("Missing task_definition.plan");
    let execution_plan: Arc<dyn ExecutionPlan> =
        plan.try_into().expect("Error converting to ExecutionPlan");
    info!(
        "Creating native execution plan succeeded: task_id={:?}",
        task_id
    );

    // replace LocalObjectStore to HDFSObjectStore in parquet scan
    let execution_plan = replace_parquet_scan_object_store(execution_plan.clone(), &env);

    // replace ShuffleReaderExec with BlazeShuffleReaderExec
    let execution_plan =
        replace_shuffle_reader(execution_plan.clone(), &env, &task_id.job_id);

    // we can pass down shuffle dirs as well as max memory threshold by creating RuntimeEnv with
    // specific RuntimeConfig.
    // use the default one here as placeholder now.
    let runtime = Arc::new(RuntimeEnv::default());

    // set all SortExec preserve_partitioning to true, because all partitioning is done is spark side
    let execution_plan = set_sort_plan_preserve_partitioning(execution_plan.clone());

    info!("Executing plan:\n{}", datafusion::physical_plan::displayable(execution_plan.as_ref()).indent());
    let sync_tokio_runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let record_batch_stream = sync_tokio_runtime
        .block_on(execution_plan.execute(task_id.partition_id as usize, runtime))
        .expect("Error executing plan");
    let schema = record_batch_stream.schema().clone();

    let record_batches: Vec<RecordBatch> = sync_tokio_runtime
        .block_on(datafusion::physical_plan::common::collect(
            record_batch_stream,
        ))
        .expect("Error collecting record batches");
    info!("Executing plan finished");
    for metric in execution_plan.metrics().unwrap_or_default().iter() {
        info!(
            " -> metrics (partition={}): {}: {}",
            metric.partition().unwrap_or_default(),
            metric.value().name(),
            metric.value()
        )
    }
    let record_batches = record_batches
        .into_iter() // retain non-empty record batches
        .filter(|record_batch| record_batch.num_rows() > 0)
        .collect::<Vec<_>>();

    let consumer_class = env.find_class("java/util/function/Consumer").unwrap();
    let consumer_accept_method = env
        .get_method_id(consumer_class, "accept", "(Ljava/lang/Object;)V")
        .unwrap();

    if !record_batches.is_empty() {
        info!("Result schema:");
        for field in schema.fields() {
            info!(
                " -> col={}, type={}, nullable={}",
                field.name(),
                field.data_type(),
                field.is_nullable()
            )
        }

        let mut buf: Vec<u8> = vec![];
        let mut buf_writer = BufWriter::new(&mut buf);
        let mut arrow_writer = StreamWriter::try_new(&mut buf_writer, &*schema).unwrap();

        info!("Writing IPC");
        let mut num_rows_total = 0;
        for record_batch in record_batches.iter().filter(|batch| batch.num_rows() > 0) {
            num_rows_total += record_batch.num_rows();
            arrow_writer.write(record_batch).expect("Error writing IPC");
        }
        arrow_writer.finish().expect("Error finishing arrow writer");
        let buf_writer = arrow_writer.into_inner().unwrap();
        info!(
            "Writing IPC finished: rows={}, bytes={}",
            num_rows_total,
            buf_writer.get_ref().len()
        );

        info!("Invoking IPC data consumer");
        let byte_buffer = env
            .new_direct_byte_buffer(buf_writer.get_mut())
            .expect("Error creating ByteBuffer");
        env.call_method_unchecked(
            ipc_record_batch_data_consumer,
            consumer_accept_method,
            JavaType::Primitive(Primitive::Void),
            &[JValue::Object(byte_buffer.into())],
        )
        .expect("Error invoking IPC data consumer");
        info!("Invoking IPC data consumer succeeded");
    } else {
        info!("Invoking IPC data consumer (with null result)");
        env.call_method_unchecked(
            ipc_record_batch_data_consumer,
            consumer_accept_method,
            JavaType::Primitive(Primitive::Void),
            &[JValue::Object(JObject::null())],
        )
        .expect("Error invoking IPC data consumer");
        info!("Invoking IPC data consumer succeeded");
    }
    info!("Blaze native computing finished");
}

fn replace_parquet_scan_object_store(
    plan: Arc<dyn ExecutionPlan>,
    env: &JNIEnv,
) -> Arc<dyn ExecutionPlan> {
    let children = plan
        .children()
        .iter()
        .map(|child| replace_parquet_scan_object_store(child.clone(), env))
        .collect::<Vec<_>>();

    if let Some(parquet_exec) = plan.as_any().downcast_ref::<ParquetExec>() {
        let parquet_scan_base_config = unsafe {
            // safety: bypass visiblity of FileScanConfig
            &mut *std::mem::transmute::<*const FileScanConfig, *mut FileScanConfig>(
                parquet_exec.base_config() as *const FileScanConfig,
            )
        };
        parquet_scan_base_config.object_store =
            Arc::new(HDFSSingleFileObjectStore::new(env.get_java_vm().unwrap()));
    }
    if children.is_empty() {
        return plan;
    }
    return plan.with_new_children(children).unwrap();
}

fn replace_shuffle_reader(
    plan: Arc<dyn ExecutionPlan>,
    env: &JNIEnv,
    job_id: &str,
) -> Arc<dyn ExecutionPlan> {
    let children = plan
        .children()
        .iter()
        .map(|child| replace_shuffle_reader(child.clone(), env, job_id))
        .collect::<Vec<_>>();

    if let Some(shuffle_reader_exec) = plan.as_any().downcast_ref::<ShuffleReaderExec>() {
        let blaze_shuffle_reader: Arc<dyn ExecutionPlan> =
            Arc::new(BlazeShuffleReaderExec {
                jvm: env.get_java_vm().unwrap(),
                job_id: job_id.to_owned(),
                schema: shuffle_reader_exec.schema(),
            });
        return blaze_shuffle_reader;
    }
    if children.is_empty() {
        return plan;
    }
    return plan.with_new_children(children).unwrap();
}

fn set_sort_plan_preserve_partitioning(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let children = plan.children().iter()
        .map(|child| set_sort_plan_preserve_partitioning(child.clone()))
        .collect::<Vec<_>>();

    if let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() {
        return Arc::new(SortExec::new_with_partitioning(
            sort_exec.expr().to_vec(),
            sort_exec.input().clone(),
            true,
        ));
    }
    if children.is_empty() {
        return plan;
    }
    return plan.with_new_children(children).unwrap();
}
