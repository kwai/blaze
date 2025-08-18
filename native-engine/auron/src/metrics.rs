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

use std::sync::Arc;

use auron_jni_bridge::{jni_call, jni_new_string};
use datafusion::{common::Result, physical_plan::ExecutionPlan};
use jni::objects::JObject;

pub fn update_spark_metric_node(
    metric_node: JObject,
    execution_plan: Arc<dyn ExecutionPlan>,
) -> Result<()> {
    if metric_node.is_null() {
        return Ok(());
    }

    // update current node
    update_metrics(
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
    for (i, &child_plan) in execution_plan.children().iter().enumerate() {
        let child_metric_node = jni_call!(
            SparkMetricNode(metric_node).getChild(i as i32) -> JObject
        )?;
        update_spark_metric_node(child_metric_node.as_obj(), child_plan.clone())?;
    }
    Ok(())
}

fn update_metrics(metric_node: JObject, metric_values: &[(&str, i64)]) -> Result<()> {
    for &(name, value) in metric_values {
        let jname = jni_new_string!(&name)?;
        jni_call!(SparkMetricNode(metric_node).add(jname.as_obj(), value) -> ())?;
    }
    Ok(())
}
