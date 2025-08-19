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

use datafusion::common::{DataFusionError, Result};

pub mod conf;
pub mod jni_bridge;

pub fn is_jni_bridge_inited() -> bool {
    jni_bridge::JavaClasses::inited()
}

pub fn ensure_jni_bridge_inited() -> Result<()> {
    if is_jni_bridge_inited() {
        Ok(())
    } else {
        Err(DataFusionError::Execution(
            "JNIEnv not initialized".to_string(),
        ))
    }
}

pub fn is_task_running() -> bool {
    fn is_task_running_impl() -> Result<bool> {
        if !jni_call_static!(JniBridge.isTaskRunning() -> bool).unwrap() {
            jni_exception_clear!()?;
            return Ok(false);
        }
        Ok(true)
    }
    if !is_jni_bridge_inited() {
        // only for testing
        return true;
    }
    is_task_running_impl().expect("calling JniBridge.isTaskRunning() error")
}
