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

use std::sync::Arc;

use blaze_jni_bridge::{
    conf::{DoubleConf, IntConf},
    jni_bridge::JavaClasses,
    *,
};
use blaze_serde::protobuf::TaskDefinition;
use datafusion::{
    common::Result,
    error::DataFusionError,
    execution::{
        disk_manager::DiskManagerConfig,
        runtime_env::{RuntimeConfig, RuntimeEnv},
    },
    physical_plan::{displayable, ExecutionPlan},
    prelude::{SessionConfig, SessionContext},
};
use datafusion_ext_commons::df_execution_err;
use datafusion_ext_plans::memmgr::MemManager;
use jni::{
    objects::{JClass, JObject},
    JNIEnv,
};
use once_cell::sync::OnceCell;
use prost::Message;

use crate::{handle_unwinded_scope, logging::init_logging, rt::NativeExecutionRuntime};

#[allow(non_snake_case)]
#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_blaze_JniBridge_callNative(
    env: JNIEnv,
    _: JClass,
    executor_memory_overhead: i64,
    native_wrapper: JObject,
) -> i64 {
    handle_unwinded_scope(|| -> Result<i64> {
        static SESSION: OnceCell<SessionContext> = OnceCell::new();
        static INIT: OnceCell<()> = OnceCell::new();

        INIT.get_or_try_init(|| {
            // logging is not initialized at this moment
            eprintln!("------ initializing blaze native environment ------");
            init_logging();

            // init jni java classes
            log::info!("initializing JNI bridge");
            JavaClasses::init(&env);

            // init datafusion session context
            log::info!("initializing datafusion session");
            SESSION.get_or_try_init(|| {
                let max_memory = executor_memory_overhead as usize;
                let memory_fraction = conf::MEMORY_FRACTION.value()?;
                let batch_size = conf::BATCH_SIZE.value()? as usize;
                MemManager::init((max_memory as f64 * memory_fraction) as usize);

                let session_config = SessionConfig::new().with_batch_size(batch_size);
                let runtime_config =
                    RuntimeConfig::new().with_disk_manager(DiskManagerConfig::Disabled);
                let runtime = Arc::new(RuntimeEnv::new(runtime_config)?);
                let session = SessionContext::new_with_config_rt(session_config, runtime);
                Ok::<_, DataFusionError>(session)
            })?;
            Ok::<_, DataFusionError>(())
        })?;
        let native_wrapper = jni_new_global_ref!(native_wrapper)?;

        // decode plan
        let raw_task_definition = jni_call!(
            BlazeCallNativeWrapper(native_wrapper.as_obj())
                .getRawTaskDefinition() -> JObject)?;
        let task_definition = TaskDefinition::decode(
            jni_convert_byte_array!(raw_task_definition.as_obj())?.as_slice(),
        )
        .or_else(|err| df_execution_err!("cannot decode execution plan: {err:?}"))?;

        let task_id = &task_definition.task_id.expect("task_id is empty");
        let plan = &task_definition.plan.expect("plan is empty");
        drop(raw_task_definition);

        // get execution plan
        let execution_plan: Arc<dyn ExecutionPlan> = plan
            .try_into()
            .or_else(|err| df_execution_err!("cannot create execution plan: {err:?}"))?;
        let execution_plan_displayable = displayable(execution_plan.as_ref())
            .indent(true)
            .to_string();
        log::info!("Creating native execution plan succeeded");
        log::info!("  task_id={task_id:?}");
        log::info!("  execution plan:\n{execution_plan_displayable}");

        // execute to stream
        let runtime = Box::new(NativeExecutionRuntime::start(
            native_wrapper,
            execution_plan,
            task_id.partition_id as usize,
            SESSION.get().unwrap().task_ctx(),
        )?);
        log::info!("Blaze native thread created");

        // returns runtime raw pointer
        Ok::<_, DataFusionError>(Box::into_raw(runtime) as usize as i64)
    })
}

#[allow(non_snake_case)]
#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_blaze_JniBridge_nextBatch(
    _: JNIEnv,
    _: JClass,
    raw_ptr: i64,
) -> bool {
    let runtime = unsafe { &*(raw_ptr as usize as *const NativeExecutionRuntime) };
    runtime.next_batch()
}

#[allow(non_snake_case)]
#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_blaze_JniBridge_finalizeNative(
    _: JNIEnv,
    _: JClass,
    raw_ptr: i64,
) {
    let runtime = unsafe { Box::from_raw(raw_ptr as usize as *mut NativeExecutionRuntime) };
    runtime.finalize();
}
