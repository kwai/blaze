#![feature(trait_upcasting)]

#[macro_use] extern crate log;
#[macro_use] extern crate timed;
extern crate async_trait;
extern crate ballista_core;
extern crate datafusion;
extern crate env_logger;
extern crate futures;
extern crate jni;
extern crate prost;
extern crate tokio;
extern crate tokio_stream;

pub use std::io::Error as IoError;
pub use std::io::ErrorKind as IoErrorKind;

pub use datafusion::error::Result as DFResult;

mod blaze;
mod blaze_shuffle_reader_exec;
mod hdfs_object_store;
mod jni_bridge;
mod util;
