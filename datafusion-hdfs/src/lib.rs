
use std::sync::Arc;
use async_trait::async_trait;
use futures::{AsyncRead, Stream};

use datafusion::datasource::object_store;
use datafusion::datasource::object_store::{ObjectStore, FileMeta, FileMetaStream, ObjectReader};
use datafusion::error::{DataFusionError, Result};
use hdfs::{HdfsFs, HdfsFsCache};
use std::task::{Context, Poll};
use std::pin::Pin;
use std::rc::Rc;
use std::cell::RefCell;

#[derive(Debug)]
pub struct HdfsStore<'a> {
    fs: HdfsFs<'a>,
}

impl HdfsStore {
    pub fn try_new(namenode_uri: &str) -> Result<Self> {
        let cache = Rc::new(RefCell::new(HdfsFsCache::new()));
        let fs: HdfsFs = cache.borrow_mut().get(namenode_uri)
    }
}

#[async_trait]
impl ObjectStore for HdfsStore {
    async fn list(&self, prefix: &str) -> Result<FileMetaStream> {
        todo!()
    }

    async fn get_reader(&self, file: FileMeta) -> Result<Arc<dyn ObjectReader>> {
        Ok(Arc::new(HdfsFileReader::new(file)?))
    }
}

struct A {

}

impl AsyncRead for A {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        todo!()
    }
}

pub struct HdfsFileReader {
    file: FileMeta
}

impl HdfsFileReader {
    pub fn new(file: FileMeta) -> Result<Self> {
        Ok(Self {
            file
        })
    }
}

#[async_trait]
impl ObjectReader for HdfsFileReader {
    async fn get_reader(&self, start: u64, length: usize) -> Result<Arc<dyn AsyncRead>> {
        self.file.path
    }

    async fn length(&self) -> Result<u64> {
        match self.file.size {
            Some(size) => Ok(size),
            None => Ok(0u64),
        }    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
