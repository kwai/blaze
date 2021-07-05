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

use crate::execution_plans::ShuffleWriterExec;
use crate::utils;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::ExecutionPlan;
use log::{debug, info};
use std::convert::TryInto;
use std::sync::Arc;

use crate::{
    error,
    error::BallistaError,
    serde::protobuf::{
        task_status, CompletedTask, FailedTask, PartitionId, TaskDefinition, TaskStatus,
    },
};

pub async fn run_task(
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

    let status = tokio::spawn(async move {
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
    });
    status.await.unwrap()
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

async fn execute_partition(
    job_id: String,
    stage_id: usize,
    work_dir: String,
    file_name: String,
    part: usize,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<RecordBatch, BallistaError> {
    let exec =
        ShuffleWriterExec::try_new(job_id, stage_id, plan, work_dir, file_name, None)?;
    let mut stream = exec.execute(part).await?;
    let batches = utils::collect_stream(&mut stream).await?;
    // the output should be a single batch containing metadata (path and statistics)
    assert!(batches.len() == 1);
    Ok(batches[0].clone())
}
