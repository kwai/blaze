use std::sync::Arc;

use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};

use tokio::runtime::Runtime;

mod exec;
mod metrics;

#[cfg(feature = "mm")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "sn")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

pub struct BlazeIter {
    pub stream: SendableRecordBatchStream,
    pub execution_plan: Arc<dyn ExecutionPlan>,
    pub runtime: Arc<Runtime>,
}
