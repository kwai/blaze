use std::path::PathBuf;
use std::sync::Arc;

use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::memory_manager::MemoryManagerConfig;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion::prelude::{SessionConfig, SessionContext};
use once_cell::sync::OnceCell;
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

pub fn session_ctx(
    max_memory: usize,
    memory_fraction: f64,
    batch_size: usize,
    tmp_dirs: String,
) -> &'static SessionContext {
    static SESSION_CONTEXT: OnceCell<SessionContext> = OnceCell::new();
    SESSION_CONTEXT.get_or_init(|| {
        let dirs = tmp_dirs.split(',').map(PathBuf::from).collect::<Vec<_>>();
        let runtime_config = RuntimeConfig::new()
            .with_memory_manager(MemoryManagerConfig::New {
                max_memory,
                memory_fraction,
            })
            .with_disk_manager(DiskManagerConfig::NewSpecified(dirs));
        let runtime = Arc::new(RuntimeEnv::new(runtime_config).unwrap());
        let config = SessionConfig::new().with_batch_size(batch_size);
        SessionContext::with_config_rt(config, runtime)
    })
}

pub fn setup_env_logger() {
    static ENV_LOGGER_INIT: OnceCell<()> = OnceCell::new();
    ENV_LOGGER_INIT.get_or_init(|| {
        let _ = env_logger::try_init_from_env(
            env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
        );
    });
}
