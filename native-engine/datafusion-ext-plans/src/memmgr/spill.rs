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

use std::{
    any::Any,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Cursor, Read, Seek, Write},
    sync::Arc,
    time::Duration,
};

use blaze_jni_bridge::{
    is_jni_bridge_inited, jni_bridge::LocalRef, jni_call, jni_call_static, jni_get_string,
    jni_new_direct_byte_buffer, jni_new_global_ref,
};
use datafusion::{common::Result, parquet::file::reader::Length, physical_plan::metrics::Time};
use jni::{objects::GlobalRef, sys::jlong};

use crate::memmgr::metrics::SpillMetrics;

pub type SpillCompressedReader<'a> =
    lz4_flex::frame::FrameDecoder<BufReader<Box<dyn Read + Send + 'a>>>;
pub type SpillCompressedWriter<'a> =
    lz4_flex::frame::AutoFinishEncoder<BufWriter<Box<dyn Write + Send + 'a>>>;

pub trait Spill: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn get_buf_reader<'a>(&'a self) -> BufReader<Box<dyn Read + Send + 'a>>;
    fn get_buf_writer<'a>(&'a mut self) -> BufWriter<Box<dyn Write + Send + 'a>>;

    fn get_compressed_reader(&self) -> SpillCompressedReader<'_> {
        lz4_flex::frame::FrameDecoder::new(self.get_buf_reader())
    }

    fn get_compressed_writer(&mut self) -> SpillCompressedWriter<'_> {
        lz4_flex::frame::FrameEncoder::new(self.get_buf_writer()).auto_finish()
    }
}

impl Spill for Vec<u8> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_buf_reader<'a>(&'a self) -> BufReader<Box<dyn Read + Send + 'a>> {
        BufReader::new(Box::new(Cursor::new(self)))
    }

    fn get_buf_writer<'a>(&'a mut self) -> BufWriter<Box<dyn Write + Send + 'a>> {
        BufWriter::new(Box::new(self))
    }
}

pub fn try_new_spill(spill_metrics: &SpillMetrics) -> Result<Box<dyn Spill>> {
    if !is_jni_bridge_inited() || jni_call_static!(JniBridge.isDriverSide() -> bool)? {
        Ok(Box::new(FileSpill::try_new(spill_metrics)?))
    } else {
        // use on heap spill if on-heap memory is available, otherwise use file spill
        let hsm = jni_call_static!(JniBridge.getTaskOnHeapSpillManager() -> JObject)?;
        if jni_call!(BlazeOnHeapSpillManager(hsm.as_obj()).isOnHeapAvailable() -> bool)? {
            Ok(Box::new(OnHeapSpill::try_new(hsm, spill_metrics)?))
        } else {
            Ok(Box::new(FileSpill::try_new(spill_metrics)?))
        }
    }
}

/// A spill structure which write data to temporary files
/// used in driver side or executor side with on-heap memory is full
struct FileSpill(File, SpillMetrics);
impl FileSpill {
    fn try_new(spill_metrics: &SpillMetrics) -> Result<Self> {
        if is_jni_bridge_inited() {
            let file_name = jni_get_string!(
                jni_call_static!(JniBridge.getDirectWriteSpillToDiskFile() -> JObject)?
                    .as_obj()
                    .into()
            )?;
            let file = OpenOptions::new() // create file and open under rw mode
                .create(true)
                .truncate(true)
                .write(true)
                .read(true)
                .open(&file_name)?;
            Ok(Self(file, spill_metrics.clone()))
        } else {
            let file = tempfile::tempfile()?;
            Ok(Self(file, spill_metrics.clone()))
        }
    }
}

impl Spill for FileSpill {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_buf_reader<'a>(&'a self) -> BufReader<Box<dyn Read + Send + 'a>> {
        let mut file_cloned = self.0.try_clone().expect("File.try_clone() returns error");
        file_cloned.sync_data().expect("error synchronizing data");
        file_cloned.rewind().expect("error rewinding");
        BufReader::with_capacity(
            65536,
            Box::new(IoTimeReadWrapper(
                file_cloned,
                self.1.mem_spill_iotime.clone(),
            )),
        )
    }

    fn get_buf_writer<'a>(&'a mut self) -> BufWriter<Box<dyn Write + Send + 'a>> {
        let file_cloned = self.0.try_clone().expect("File.try_clone() returns error");
        BufWriter::with_capacity(
            65536,
            Box::new(IoTimeWriteWrapper(
                file_cloned,
                self.1.mem_spill_iotime.clone(),
            )),
        )
    }
}

impl Drop for FileSpill {
    fn drop(&mut self) {
        self.1.disk_spill_size.add(self.0.len() as usize);
        self.1
            .disk_spill_iotime
            .add_duration(Duration::from_nanos(self.1.mem_spill_iotime.value() as u64))
    }
}

/// A spill structure which cooperates with BlazeOnHeapSpillManager
/// used in executor side
struct OnHeapSpill(Arc<RawOnHeapSpill>, SpillMetrics);
impl OnHeapSpill {
    fn try_new(hsm: LocalRef, spill_metrics: &SpillMetrics) -> Result<Self> {
        let spill_id = jni_call!(BlazeOnHeapSpillManager(hsm.as_obj()).newSpill() -> i32)?;
        Ok(Self(
            Arc::new(RawOnHeapSpill {
                hsm: jni_new_global_ref!(hsm.as_obj())?,
                spill_id,
            }),
            spill_metrics.clone(),
        ))
    }

    fn get_disk_usage(&self) -> Result<u64> {
        let usage = jni_call!(BlazeOnHeapSpillManager(self.0.hsm.as_obj())
            .getSpillDiskUsage(self.0.spill_id) -> jlong)? as u64;
        Ok(usage)
    }

    fn get_disk_iotime(&self) -> Result<u64> {
        let iotime = jni_call!(BlazeOnHeapSpillManager(self.0.hsm.as_obj())
            .getSpillDiskIOTime(self.0.spill_id) -> jlong)? as u64;
        Ok(iotime)
    }
}

impl Spill for OnHeapSpill {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_buf_reader<'a>(&'a self) -> BufReader<Box<dyn Read + Send + 'a>> {
        let cloned = Self(self.0.clone(), self.1.clone());
        BufReader::with_capacity(65536, Box::new(cloned))
    }

    fn get_buf_writer<'a>(&'a mut self) -> BufWriter<Box<dyn Write + Send + 'a>> {
        let cloned = Self(self.0.clone(), self.1.clone());
        BufWriter::with_capacity(1048576, Box::new(cloned))
    }
}

impl Write for OnHeapSpill {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let _timer = self.1.mem_spill_iotime.timer();
        let write_len = buf.len();
        let buf = jni_new_direct_byte_buffer!(buf)?;

        jni_call!(BlazeOnHeapSpillManager(
            self.0.hsm.as_obj()).writeSpill(self.0.spill_id, buf.as_obj()) -> ()
        )?;
        self.1.mem_spill_size.add(write_len);
        Ok(write_len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Read for OnHeapSpill {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let _timer = self.1.mem_spill_iotime.timer();
        let buf = jni_new_direct_byte_buffer!(buf)?;
        let read_len = jni_call!(BlazeOnHeapSpillManager(
            self.0.hsm.as_obj()).readSpill(self.0.spill_id, buf.as_obj()) -> i32
        )?;
        Ok(read_len as usize)
    }
}

impl Drop for OnHeapSpill {
    fn drop(&mut self) {
        self.1.mem_spill_count.add(1);
        self.1
            .disk_spill_size
            .add(self.get_disk_usage().unwrap_or(0) as usize);
        self.1
            .disk_spill_iotime
            .add_duration(Duration::from_nanos(self.get_disk_iotime().unwrap_or(0)));
    }
}

struct RawOnHeapSpill {
    hsm: GlobalRef,
    spill_id: i32,
}

impl Drop for RawOnHeapSpill {
    fn drop(&mut self) {
        let _ = jni_call!(BlazeOnHeapSpillManager(self.hsm.as_obj())
            .releaseSpill(self.spill_id) -> ());
    }
}

struct IoTimeReadWrapper<R: Read>(R, Time);
struct IoTimeWriteWrapper<W: Write>(W, Time);

impl<R: Read> Read for IoTimeReadWrapper<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let _timer = self.1.timer();
        self.0.read(buf)
    }
}

impl<W: Write> Write for IoTimeWriteWrapper<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let _timer = self.1.timer();
        self.0.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let _timer = self.1.timer();
        self.0.flush()
    }
}
