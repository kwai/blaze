use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;
use jni::objects::JObject;
use jni::JNIEnv;

use datafusion_ext::jni_bridge::JavaClasses;
use datafusion_ext::{jni_bridge_call_method, jni_map_error};

const REPORTED_METRICS: &[&str] = &[
    "input_rows",
    "input_batches",
    "output_rows",
    "output_batches",
    "elapsed_compute",
    "join_time",
];

pub fn update_spark_metric_node(
    env: &JNIEnv,
    metric_node: JObject,
    execution_plan: Arc<dyn ExecutionPlan>,
) -> datafusion::error::Result<()> {
    // update current node
    update_metrics(
        env,
        metric_node,
        &execution_plan
            .metrics()
            .unwrap_or_default()
            .iter()
            .map(|m| m.value())
            .map(|m| (m.name(), m.as_usize() as i64))
            .collect::<Vec<_>>(),
    )?;

    // update children nodes
    for (i, child_plan) in execution_plan.children().iter().enumerate() {
        let child_metric_node = jni_bridge_call_method!(
            env,
            SparkMetricNode.getChild -> JObject,
            metric_node,
            i as i32
        )?;
        update_spark_metric_node(env, child_metric_node, child_plan.clone())?;
    }
    Ok(())
}

fn update_metrics(
    env: &JNIEnv,
    metric_node: JObject,
    metric_values: &[(&str, i64)],
) -> datafusion::error::Result<()> {
    for &(name, value) in metric_values {
        if REPORTED_METRICS.contains(&name) {
            let jname = jni_map_error!(env.new_string(name))?;
            jni_bridge_call_method!(env, SparkMetricNode.add -> (), metric_node, jname, value)?;
        }
    }
    Ok(())
}
