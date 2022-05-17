use datafusion::arrow::datatypes::Schema;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::memory_manager::MemoryManagerConfig;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion::prelude::{SessionConfig, SessionContext};
use lazy_static::lazy_static;
use log::LevelFilter;
use simplelog::{ConfigBuilder, SimpleLogger, ThreadLogMode};

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
    pub renamed_schema: Arc<Schema>,
}

pub fn init_session_ctx(
    max_memory: usize,
    memory_fraction: f64,
    batch_size: usize,
    tmp_dirs: String,
) -> Arc<SessionContext> {
    lazy_static! {
        static ref SESSION_CTX: Mutex<Option<Arc<SessionContext>>> = Mutex::default();
    }
    let mut session_ctx = SESSION_CTX.lock().unwrap();
    if session_ctx.is_none() {
        let dirs = tmp_dirs.split(',').map(PathBuf::from).collect::<Vec<_>>();
        let runtime_config = RuntimeConfig::new()
            .with_memory_manager(MemoryManagerConfig::New {
                max_memory,
                memory_fraction,
            })
            .with_disk_manager(DiskManagerConfig::NewSpecified(dirs));
        let runtime = Arc::new(RuntimeEnv::new(runtime_config).unwrap());
        let config = SessionConfig::new().with_batch_size(batch_size);
        *session_ctx = Some(Arc::new(SessionContext::with_config_rt(config, runtime)));
    }
    return session_ctx.as_ref().unwrap().clone();
}

pub fn init_logging() {
    lazy_static! {
        static ref LOGGING_INIT: Mutex<Option<()>> = Mutex::default();
    }

    let mut logging_init = LOGGING_INIT.lock().unwrap();
    if logging_init.is_none() {
        let config = ConfigBuilder::new()
            .set_thread_mode(ThreadLogMode::Both)
            .build();
        SimpleLogger::init(LevelFilter::Info, config).unwrap();
        *logging_init = Some(());
    }
}
