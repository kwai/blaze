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

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::ipc;
use datafusion::arrow::ipc::reader::read_record_batch;
use datafusion::arrow::ipc::writer::write_message;
use datafusion::arrow::ipc::writer::DictionaryTracker;
use datafusion::arrow::ipc::writer::IpcDataGenerator;
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::record_batch::RecordBatchReader;
use std::collections::HashMap;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::io::{Seek, SeekFrom, Write};

pub fn write_ipc_compressed<W: Write + Seek>(
    batch: &RecordBatch,
    output: &mut W,
) -> ArrowResult<usize> {
    if batch.num_rows() == 0 {
        return Ok(0);
    }
    let start_pos = output.stream_position()?;

    // write ipc_length placeholder
    output.write_all(&[0u8; 8])?;

    // write ipc data
    let mut arrow_writer =
        HeadlessStreamWriter::new(zstd::Encoder::new(output, 1)?, &batch.schema());
    arrow_writer.write(batch)?;
    arrow_writer.finish()?;

    let zwriter = arrow_writer.into_inner()?;
    let output = zwriter.finish()?;
    let end_pos = output.stream_position()?;
    let ipc_length = end_pos - start_pos - 8;

    // fill ipc length
    output.seek(SeekFrom::Start(start_pos))?;
    output.write_all(&ipc_length.to_le_bytes()[..])?;

    output.seek(SeekFrom::Start(end_pos))?;
    Ok((end_pos - start_pos) as usize)
}

/// Simplified from arrow StreamReader
/// not reading schema from input because it is always available in execution context
pub struct HeadlessStreamReader<R: Read> {
    reader: BufReader<R>,
    schema: SchemaRef,
    finished: bool,
    empty_dictionaries: HashMap<i64, ArrayRef>,
}

impl<R: Read> HeadlessStreamReader<R> {
    pub fn new(reader: R, schema: SchemaRef) -> Self {
        Self {
            reader: BufReader::new(reader),
            schema,
            finished: false,
            empty_dictionaries: HashMap::new(),
        }
    }

    fn maybe_next(&mut self) -> ArrowResult<Option<RecordBatch>> {
        if self.finished {
            return Ok(None);
        }
        // determine metadata length
        let mut meta_size: [u8; 4] = [0; 4];

        match self.reader.read_exact(&mut meta_size) {
            Ok(()) => (),
            Err(e) => {
                return if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    self.finished = true;
                    Ok(None)
                } else {
                    Err(ArrowError::from(e))
                };
            }
        }

        let meta_len = {
            // If a continuation marker is encountered, skip over it and read
            // the size from the next four bytes.
            if meta_size == [0xff; 4] {
                self.reader.read_exact(&mut meta_size)?;
            }
            i32::from_le_bytes(meta_size)
        };

        if meta_len == 0 {
            // the stream has ended, mark the reader as finished
            self.finished = true;
            return Ok(None);
        }

        let mut meta_buffer = vec![0; meta_len as usize];
        self.reader.read_exact(&mut meta_buffer)?;

        let vecs = &meta_buffer.to_vec();
        let message = ipc::root_as_message(vecs).map_err(|err| {
            ArrowError::IoError(format!("Unable to get root as message: {:?}", err))
        })?;

        match message.header_type() {
            ipc::MessageHeader::RecordBatch => {
                let batch = message.header_as_record_batch().ok_or_else(|| {
                    ArrowError::IoError(
                        "Unable to read IPC message as record batch".to_string(),
                    )
                })?;
                // read the block that makes up the record batch into a buffer
                let mut buf = vec![0; message.bodyLength() as usize];
                self.reader.read_exact(&mut buf)?;

                read_record_batch(
                    &buf,
                    batch,
                    self.schema.clone(),
                    &self.empty_dictionaries,
                    None,
                    &message.version()
                ).map(Some)
            }
            ipc::MessageHeader::NONE => {
                Ok(None)
            }
            t => Err(ArrowError::IoError(
                format!("Reading types other than record batches not yet supported, unable to read {:?} ", t)
            )),
        }
    }
}

impl<R: Read> Iterator for HeadlessStreamReader<R> {
    type Item = ArrowResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.maybe_next().transpose()
    }
}

impl<R: Read> RecordBatchReader for HeadlessStreamReader<R> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Simplified from arrow StreamWriter
/// not writing schema from input because it is always available in execution context
pub struct HeadlessStreamWriter<W: Write> {
    writer: BufWriter<W>,
    write_options: IpcWriteOptions,
    finished: bool,
    dictionary_tracker: DictionaryTracker,
    data_gen: IpcDataGenerator,
}

impl<W: Write> HeadlessStreamWriter<W> {
    pub fn new(writer: W, _schema: &SchemaRef) -> Self {
        let write_options = IpcWriteOptions::default();
        let data_gen = IpcDataGenerator::default();
        let writer = BufWriter::new(writer);
        Self {
            writer,
            write_options,
            finished: false,
            dictionary_tracker: DictionaryTracker::new(false),
            data_gen,
        }
    }

    /// Write a record batch to the stream
    pub fn write(&mut self, batch: &RecordBatch) -> ArrowResult<()> {
        if self.finished {
            return Err(ArrowError::IoError(
                "Cannot write record batch to stream writer as it is closed".to_string(),
            ));
        }

        let (encoded_dictionaries, encoded_message) = self.data_gen.encoded_batch(
            batch,
            &mut self.dictionary_tracker,
            &self.write_options,
        )?;

        assert_eq!(
            encoded_dictionaries.len(),
            0,
            "Writing with dictionary is not yet supported"
        );
        write_message(&mut self.writer, encoded_message, &self.write_options)?;
        Ok(())
    }

    pub fn finish(&mut self) -> ArrowResult<()> {
        if self.finished {
            return Err(ArrowError::IoError(
                "Cannot write footer to stream writer as it is closed".to_string(),
            ));
        }

        // no need to write continuation bytes because we can always use EOF
        // to finish a HeadlessStreamReader
        self.finished = true;
        Ok(())
    }

    pub fn into_inner(mut self) -> ArrowResult<W> {
        if !self.finished {
            self.finish()?;
        }
        self.writer.into_inner().map_err(ArrowError::from)
    }
}
