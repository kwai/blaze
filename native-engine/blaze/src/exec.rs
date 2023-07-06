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

use crate::rt::NativeExecutionRuntime;
use crate::{handle_unwinded_scope, SESSION};
use blaze_jni_bridge::jni_bridge::JavaClasses;
use blaze_jni_bridge::*;
use blaze_serde::protobuf::TaskDefinition;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_plan::{displayable, ExecutionPlan};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_ext_plans::common::memory_manager::MemManager;
use jni::objects::JClass;
use jni::objects::JObject;
use jni::JNIEnv;
use log::LevelFilter;
use once_cell::sync::OnceCell;
use prost::Message;
use simplelog::{ColorChoice, ConfigBuilder, TermLogger, TerminalMode, ThreadLogMode};
use std::sync::Arc;

fn init_logging() {
    static LOGGING_INIT: OnceCell<()> = OnceCell::new();
    LOGGING_INIT.get_or_init(|| {
        TermLogger::init(
            LevelFilter::Info,
            ConfigBuilder::new()
                .set_thread_mode(ThreadLogMode::Both)
                .build(),
            TerminalMode::Stderr,
            ColorChoice::Never,
        )
        .unwrap();
    });
}

#[allow(non_snake_case)]
#[allow(clippy::single_match)]
#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_blaze_JniBridge_initNative(
    env: JNIEnv,
    _: JClass,
    batch_size: i64,
    native_memory: i64,
    memory_fraction: f64,
) {
    handle_unwinded_scope(|| -> Result<()> {
        // init logging
        init_logging();

        // init jni java classes
        JavaClasses::init(&env);

        // init datafusion session context
        SESSION.get_or_init(|| {
            let max_memory = native_memory as usize;
            let batch_size = batch_size as usize;
            let runtime_config =
                RuntimeConfig::new().with_disk_manager(DiskManagerConfig::Disabled);

            MemManager::init((max_memory as f64 * memory_fraction) as usize);
            let runtime = Arc::new(RuntimeEnv::new(runtime_config).unwrap());
            let config = SessionConfig::new().with_batch_size(batch_size);
            SessionContext::with_config_rt(config, runtime)
        });
        datafusion::error::Result::Ok(())
    });
}

#[allow(non_snake_case)]
#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_blaze_JniBridge_callNative(
    _: JNIEnv,
    _: JClass,
    native_wrapper: JObject,
) -> i64 {
    handle_unwinded_scope(|| -> Result<i64> {
        log::info!("Entering blaze callNative()");
        let native_wrapper = jni_new_global_ref!(native_wrapper).unwrap();

        // decode plan
        let raw_task_definition = jni_call!(
            BlazeCallNativeWrapper(native_wrapper.as_obj())
                .getRawTaskDefinition() -> JObject)?;
        let task_definition = TaskDefinition::decode(
            jni_convert_byte_array!(raw_task_definition.as_obj())?.as_slice(),
        )
        .map_err(|err| DataFusionError::Plan(format!("cannot decode execution plan: {:?}", err)))?;

        let task_id = &task_definition.task_id.expect("task_id is empty");
        let plan = &task_definition.plan.expect("plan is empty");
        drop(raw_task_definition);

        // get execution plan
        let execution_plan: Arc<dyn ExecutionPlan> = plan.try_into().map_err(|err| {
            DataFusionError::Plan(format!("cannot create execution plan: {:?}", err))
        })?;
        let execution_plan_displayable = displayable(execution_plan.as_ref())
            .indent(true)
            .to_string();
        log::info!("Creating native execution plan succeeded");
        log::info!("  task_id={:?}", task_id);
        log::info!("  execution plan:\n{}", execution_plan_displayable);

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
pub extern "system" fn Java_org_apache_spark_sql_blaze_JniBridge_finalizeNative(
    _: JNIEnv,
    _: JClass,
    rtw_ptr: i64,
) {
    let runtime = unsafe { Box::from_raw(rtw_ptr as usize as *mut NativeExecutionRuntime) };
    runtime.finalize();
}
