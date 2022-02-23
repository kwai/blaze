use conquer_once::OnceCell;
use std::future;
use std::io::BufWriter;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_ext::jni_bridge::JavaClasses;
use datafusion_ext::jni_bridge_call_method;
use futures::TryFutureExt;
use futures::TryStreamExt;
use jni::errors::Result as JniResult;
use jni::objects::JByteBuffer;
use jni::objects::JClass;
use jni::objects::JObject;
use jni::objects::JValue;
use jni::JNIEnv;
use log::info;
use plan_serde::protobuf::TaskDefinition;
use prost::Message;
use tokio::runtime::Runtime;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

static TOKIO_RUNTIME: OnceCell<Runtime> = OnceCell::uninit();

#[allow(non_snake_case)]
#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_blaze_JniBridge_callNative(
    env: JNIEnv,
    _: JClass,
    taskDefinition: JByteBuffer,
    metricNode: JObject,
    ipcRecordBatchDataConsumer: JObject,
) {
    let start_time = std::time::Instant::now();
    if let Err(err) = std::panic::catch_unwind(|| {
        blaze_call_native(&env, taskDefinition, metricNode, ipcRecordBatchDataConsumer);
    }) {
        if !env.exception_check().unwrap() {
            env.throw_new(
                "java/lang/RuntimeException",
                if let Some(msg) = err.downcast_ref::<String>() {
                    msg
                } else if let Some(msg) = err.downcast_ref::<&str>() {
                    msg
                } else {
                    "Unknown blaze-rs exception"
                },
            )
            .unwrap();
        }
    }
    let duration = std::time::Instant::now().duration_since(start_time);
    info!(
        "blaze_call_native() time cost: {} sec",
        duration.as_secs_f64()
    );
}

pub fn blaze_call_native(
    env: &JNIEnv,
    task_definition: JByteBuffer,
    metric_node: JObject,
    ipc_record_batch_data_consumer: JObject,
) {
    let start_time = std::time::Instant::now();
    let _env_logger_init = env_logger::try_init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );
    info!("Blaze native computing started");

    info!("Initializing JavaClasses");
    JavaClasses::init(env).expect("Error initializing JavaClasses");
    let env = JavaClasses::get_thread_jnienv();
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
    datafusion_ext::set_job_id(&task_id.job_id);

    let plan = &task_definition.plan.expect("Missing task_definition.plan");
    let execution_plan: Arc<dyn ExecutionPlan> =
        plan.try_into().expect("Error converting to ExecutionPlan");
    info!(
        "Creating native execution plan succeeded: task_id={:?}",
        task_id
    );

    info!(
        "Executing plan:\n{}",
        datafusion::physical_plan::displayable(execution_plan.as_ref()).indent()
    );

    TOKIO_RUNTIME
        .try_get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(10) // TODO: we should set this through JNI param
                .thread_name("blaze")
                .build()
                .unwrap()
        })
        .unwrap()
        .block_on(async {
            // TODO: we can pass down shuffle dirs, max memory threshold and batch_size
            // by creating RuntimeEnv with specific RuntimeConfig.
            // use the default one here as placeholder now.
            let runtime = Arc::new(RuntimeEnv::default());

            let schema = execution_plan.schema();

            let result = execution_plan
                .execute(task_id.partition_id as usize, runtime)
                .await
                .unwrap();

            let record_batches: Vec<RecordBatch> = result
                .try_filter(|b| future::ready(b.num_rows() > 0))
                .try_collect::<Vec<_>>()
                .map_err(DataFusionError::from)
                .await
                .unwrap();

            info!("Executing plan finished");

            // update spark metrics
            update_spark_metric_node(&env, metric_node, execution_plan).unwrap();

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
                let mut arrow_writer =
                    StreamWriter::try_new(&mut buf_writer, &*schema).unwrap();

                info!("Writing IPC");
                let mut num_rows_total = 0;
                for record_batch in record_batches.iter() {
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
                update_extra_metrics(
                    &env,
                    metric_node,
                    start_time,
                    num_rows_total,
                    buf_writer.get_ref().len(),
                )
                .unwrap();

                info!("Invoking IPC data consumer");
                let byte_buffer = env
                    .new_direct_byte_buffer(buf_writer.get_mut())
                    .expect("Error creating ByteBuffer");
                jni_bridge_call_method!(
                    env,
                    JavaConsumer.accept,
                    ipc_record_batch_data_consumer,
                    JValue::Object(byte_buffer.into())
                )
                .expect("Error invoking IPC data consumer");
                info!("Invoking IPC data consumer succeeded");
            } else {
                update_extra_metrics(&env, metric_node, start_time, 0, 0).unwrap();

                info!("Invoking IPC data consumer (with null result)");
                jni_bridge_call_method!(
                    env,
                    JavaConsumer.accept,
                    ipc_record_batch_data_consumer,
                    JValue::Object(JObject::null())
                )
                .expect("Error invoking IPC data consumer");
                info!("Invoking IPC data consumer succeeded");
            }
        });

    info!("Blaze native computing finished");
}

fn update_spark_metric_node(
    env: &JNIEnv,
    metric_node: JObject,
    execution_plan: Arc<dyn ExecutionPlan>,
) -> JniResult<()> {
    // update current node
    update_metrics(
        env,
        metric_node,
        &execution_plan
            .metrics()
            .unwrap_or_default()
            .iter()
            .map(|m| (m.value().name(), m.value().as_usize() as i64))
            .collect::<Vec<_>>(),
    )?;

    // update children nodes
    for (i, child_plan) in execution_plan.children().iter().enumerate() {
        let child_metric_node = jni_bridge_call_method!(
            env,
            SparkMetricNode.getChild,
            metric_node,
            JValue::Int(i as i32)
        )?
        .l()?;
        update_spark_metric_node(env, child_metric_node, child_plan.clone())?;
    }
    Ok(())
}

fn update_extra_metrics(
    env: &JNIEnv,
    metric_node: JObject,
    start_time: Instant,
    num_ipc_rows: usize,
    num_ipc_bytes: usize,
) -> JniResult<()> {
    let duration_ns = Instant::now().duration_since(start_time).as_nanos();
    update_metrics(
        env,
        metric_node,
        &[
            ("blaze_output_ipc_rows", num_ipc_rows as i64),
            ("blaze_output_ipc_bytes", num_ipc_bytes as i64),
            ("blaze_exec_time", duration_ns as i64),
        ],
    )
}

fn update_metrics(
    env: &JNIEnv,
    metric_node: JObject,
    metric_values: &[(&str, i64)],
) -> JniResult<()> {
    for &(name, value) in metric_values {
        jni_bridge_call_method!(
            env,
            SparkMetricNode.add,
            metric_node,
            JValue::Object(env.new_string(name)?.into()),
            JValue::Long(value)
        )?;
    }
    Ok(())
}
