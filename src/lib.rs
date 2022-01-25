#![feature(trait_upcasting)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate timed;

pub use std::io::Error as IoError;
pub use std::io::ErrorKind as IoErrorKind;

pub use datafusion::error::Result as DFResult;

mod batch_buffer;
mod blaze;
mod blaze_shuffle_reader_exec;
mod hdfs_object_store;
mod jni_bridge;
mod shuffle_writer_exec;
mod util;
