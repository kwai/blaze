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

use arrow::error::{ArrowError, Result as ArrowResult};
use datafusion::error::{DataFusionError, Result};
use std::any::Any;
use std::cell::RefCell;
use std::fmt::{Debug, Formatter};
use std::fs::{self, File, Metadata};
use std::io::{BufReader, Cursor, Read, Seek, SeekFrom};

use arrow::datatypes::SchemaRef;
use arrow::io::ipc::read::{
    read_file_segment_metadata, FileMetadata as IPCMeta, FileReader as IPCReader,
    FileReader,
};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::physical_plan::common::AbortOnDropSingle;
use datafusion::physical_plan::{
    ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use futures::{Stream, StreamExt};
use log::info;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

pub trait ReadSeek: Read + Seek {}

impl ReadSeek for std::fs::File {}

impl<R: Read + Seek> ReadSeek for std::io::BufReader<R> {}

impl<R: AsRef<[u8]>> ReadSeek for std::io::Cursor<R> {}

pub type BatchIter = Box<dyn Iterator<Item = ArrowResult<RecordBatch>> + Send + Sync>;

#[derive(Debug)]
pub enum Content {
    File(String),
    // FIXME: raw pointer from JNI to avoid copy
    Mem(Vec<u8>),
}

#[derive(Debug)]
pub struct Block {
    pub content: Content,
    pub offset: usize,
    pub length: usize,
}

#[derive(Debug, Clone)]
pub struct ShuffleReaderExec {
    pub tx: Arc<Mutex<Option<Sender<Result<Block>>>>>,
    pub schema: SchemaRef,
}

impl ShuffleReaderExec {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            tx: Arc::new(Mutex::new(None)),
            schema,
        }
    }

    // execute(i) first to set up block sending channel, get sender here to insert shuffle blocks
    pub fn tx(&self) -> Sender<Result<Block>> {
        let tx = self.tx.lock().unwrap();
        tx.clone().unwrap()
    }
}

#[async_trait]
impl ExecutionPlan for ShuffleReaderExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "Blaze ShuffleReaderExec does not support with_new_children()".to_owned(),
        ))
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        info!("ShuffleReader for partition {}", partition);
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        let mut stx = self.tx.lock().unwrap();
        *stx = Some(tx);
        Ok(Box::pin(ShuffleReaderStream::new(rx, self.schema.clone())))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

struct ShuffleReaderStream {
    inner: ReceiverStream<Result<Block>>,
    schema: SchemaRef,
    batch_iter: BatchIter,
}

impl ShuffleReaderStream {
    fn new(rx: Receiver<Result<Block>>, schema: SchemaRef) -> Self {
        Self {
            inner: ReceiverStream::new(rx),
            schema,
            batch_iter: Box::new(std::iter::empty()),
        }
    }
}

impl Stream for ShuffleReaderStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let item = self.batch_iter.next();
        match item {
            None => {
                let poll = self.inner.poll_next_unpin(cx);
                match poll {
                    Poll::Ready(Some(Ok(block))) => {
                        let start = block.offset;
                        let end = block.offset + block.length;
                        match block.content {
                            Content::File(f) => {
                                let f = BufReader::new(File::open(f).unwrap());
                                let r = MultiIPCReader::new(f, start, end).unwrap();
                                self.as_mut().batch_iter = Box::new(r);
                            }
                            Content::Mem(m) => {
                                let c = Cursor::new(m);
                                let r = MultiIPCReader::new(c, start, end).unwrap();
                                self.as_mut().batch_iter = Box::new(r);
                            }
                        }
                        self.poll_next(cx)
                    }

                    Poll::Pending => Poll::Pending,
                    Poll::Ready(None) => Poll::Ready(None),
                    Poll::Ready(Some(Err(e))) => {
                        Poll::Ready(Some(Err(e.into_arrow_external_error())))
                    }
                }
            }
            Some(b) => Poll::Ready(Some(b)),
        }
    }
}

impl RecordBatchStream for ShuffleReaderStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn read_meta<R: ReadSeek>(reader: &mut R, end: usize) -> Result<(usize, IPCMeta)> {
    let ipc_end = end - 8;
    reader.seek(SeekFrom::Start(ipc_end as u64))?;
    let mut meta_buf = [0; 8];
    reader.read_exact(&mut meta_buf)?;
    let ipc_length = i64::from_le_bytes(meta_buf) as usize;
    let ipc_start = ipc_end - ipc_length;

    let ipc_meta = read_file_segment_metadata(reader, ipc_start as u64, ipc_end as u64)
        .map_err(DataFusionError::ArrowError)?;
    Ok((ipc_start, ipc_meta))
}

pub struct MultiIPCReader<R: ReadSeek> {
    start: usize,
    current_start: usize,
    reader: Option<IPCReader<R>>,
}

impl<R: ReadSeek> MultiIPCReader<R> {
    fn new(mut reader: R, start: usize, end: usize) -> Result<Self> {
        let (current_start, meta) = read_meta(&mut reader, end)?;
        Ok(Self {
            start,
            current_start,
            reader: Some(IPCReader::new(reader, meta, None)),
        })
    }
}

impl<R: ReadSeek> Iterator for MultiIPCReader<R> {
    type Item = ArrowResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.reader.as_mut().unwrap().next();
        match next {
            None => {
                if self.current_start == self.start {
                    None
                } else {
                    let mut r = self.reader.take().unwrap().into_inner();
                    let (new_start, meta) =
                        read_meta(&mut r, self.current_start).unwrap();
                    self.reader = Some(IPCReader::new(r, meta, None));
                    self.current_start = new_start;
                    self.next()
                }
            }
            item => item,
        }
    }
}
