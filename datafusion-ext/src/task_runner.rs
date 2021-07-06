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

use crate::shuffle_writer::ShuffleWriterExec;
use arrow::record_batch::RecordBatch;
use ballista_core::error::Result;
use ballista_core::utils;
use datafusion::physical_plan::ExecutionPlan;
use log::{debug, info};
use prost::Message;
use std::any::type_name;
use std::convert::TryInto;
use std::sync::Arc;

use ballista_core::{
    error,
    error::BallistaError,
    serde::protobuf::{
        task_status, CompletedTask, FailedTask, PartitionId, TaskDefinition, TaskStatus,
    },
};

pub async fn run_task(
    task: Vec<u8>,
    executor_id: String,
    work_dir: String,
    file_name: String,
) -> Result<Vec<u8>> {
    let task: TaskDefinition = decode_protobuf(&task).unwrap();
    let status = run_task_inner(task, executor_id, work_dir, file_name).await;
    encode_protobuf(&status)
}

async fn run_task_inner(
    task: TaskDefinition,
    executor_id: String,
    work_dir: String,
    file_name: String,
) -> TaskStatus {
    let task_id = task.task_id.unwrap();
    let task_id_log = format!(
        "{}/{}/{}",
        task_id.job_id, task_id.stage_id, task_id.partition_id
    );
    info!("Received task {}", task_id_log);
    let plan: Arc<dyn ExecutionPlan> = (&task.plan.unwrap()).try_into().unwrap();

    let execution_result = execute_partition(
        task_id.job_id.clone(),
        task_id.stage_id as usize,
        work_dir,
        file_name,
        task_id.partition_id as usize,
        plan,
    )
    .await;
    info!("Done with task {}", task_id_log);
    debug!("Statistics: {:?}", execution_result);
    as_task_status(execution_result.map(|_| ()), executor_id, task_id)
}

async fn execute_partition(
    job_id: String,
    stage_id: usize,
    work_dir: String,
    file_name: String,
    part: usize,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<RecordBatch> {
    let exec =
        ShuffleWriterExec::try_new(job_id, stage_id, plan, work_dir, file_name, None)?;
    let mut stream = exec.execute(part).await?;
    let batches = utils::collect_stream(&mut stream).await?;
    // the output should be a single batch containing metadata (path and statistics)
    assert!(batches.len() == 1);
    Ok(batches[0].clone())
}

fn as_task_status(
    execution_result: error::Result<()>,
    executor_id: String,
    task_id: PartitionId,
) -> TaskStatus {
    match execution_result {
        Ok(_) => {
            info!("Task {:?} finished", task_id);
            TaskStatus {
                partition_id: Some(task_id),
                status: Some(task_status::Status::Completed(CompletedTask {
                    executor_id,
                })),
            }
        }
        Err(e) => {
            let error_msg = e.to_string();
            info!("Task {:?} failed: {}", task_id, error_msg);

            TaskStatus {
                partition_id: Some(task_id),
                status: Some(task_status::Status::Failed(FailedTask {
                    error: format!("Task failed due to Tokio error: {}", error_msg),
                })),
            }
        }
    }
}

fn decode_protobuf<T: Message + Default>(bytes: &[u8]) -> Result<T> {
    T::decode(bytes).map_err(|e| {
        BallistaError::Internal(format!(
            "Could not deserialize {}: {}",
            type_name::<T>(),
            e
        ))
    })
}

fn encode_protobuf<T: Message + Default>(msg: &T) -> Result<Vec<u8>> {
    let mut value: Vec<u8> = Vec::with_capacity(msg.encoded_len());
    msg.encode(&mut value).map_err(|e| {
        BallistaError::Internal(format!(
            "Could not serialize {}: {}",
            type_name::<T>(),
            e
        ))
    })?;
    Ok(value)
}
