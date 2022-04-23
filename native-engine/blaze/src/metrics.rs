use std::sync::Arc;
use std::time::Instant;

use datafusion::physical_plan::ExecutionPlan;
use datafusion_ext::jni_bridge::JavaClasses;
use datafusion_ext::jni_bridge_call_method;
use jni::errors::Result as JniResult;
use jni::objects::JObject;
use jni::JNIEnv;

pub fn update_spark_metric_node(
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
            i as i32
        )?
        .l()?;
        update_spark_metric_node(env, child_metric_node, child_plan.clone())?;
    }
    Ok(())
}

#[allow(dead_code)]
pub fn update_extra_metrics(
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
        let jname = env.new_string(name)?;
        jni_bridge_call_method!(env, SparkMetricNode.add, metric_node, jname, value)?;
    }
    Ok(())
}
