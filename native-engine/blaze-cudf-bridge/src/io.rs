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
    fs::File,
    io::{Read, Seek},
};

use arrow::error::Result;
use parquet::file::reader::Length;

use crate::ffi::{FFI_CudfDataSource, FFI_USizeResult};

pub trait CudfDataSource {
    fn data_size(&self) -> usize;
    fn read(&mut self, offset: usize, buf: &mut [u8]) -> Result<usize>;
}

pub trait IntoFFICudfDataSource {
    fn into_ffi(self) -> FFI_CudfDataSource;
}

impl IntoFFICudfDataSource for Box<dyn CudfDataSource> {
    fn into_ffi(self) -> FFI_CudfDataSource {
        extern "C" fn dtor(this: *mut FFI_CudfDataSource) {
            unsafe {
                let this_mut = this.as_mut().expect("FFI_CudfDataSource is null");
                let raw_mut = this_mut
                    .raw
                    .as_mut()
                    .expect("FFI_CudfDataSource.raw is null");
                let _ = Box::from_raw(raw_mut as *mut Box<dyn CudfDataSource>);
            }
        }

        extern "C" fn data_size(this: *const FFI_CudfDataSource) -> usize {
            unsafe {
                let this_ref = this.as_ref().expect("FFI_CudfDataSource is null");
                let raw_ref = this_ref
                    .raw
                    .as_ref()
                    .expect("FFI_CudfDataSource.raw is null");
                raw_ref.data_size()
            }
        }

        extern "C" fn read(
            this: *mut FFI_CudfDataSource,
            offset: usize,
            buf: *mut u8,
            buf_size: usize,
        ) -> FFI_USizeResult {
            unsafe {
                let this_mut = this.as_mut().expect("FFI_CudfDataSource is null");
                let raw_mut = this_mut
                    .raw
                    .as_mut()
                    .expect("FFI_CudfDataSource.raw is null");
                let buf_mut = std::slice::from_raw_parts_mut(buf, buf_size);
                match raw_mut.read(offset, buf_mut) {
                    Ok(read_size) => FFI_USizeResult {
                        ok: true,
                        value: read_size,
                    },
                    Err(_err) => FFI_USizeResult {
                        ok: false,
                        value: 0,
                    },
                }
            }
        }

        FFI_CudfDataSource {
            dtor,
            data_size,
            read,
            raw: Box::into_raw(Box::new(self)),
        }
    }
}

pub struct CudfLocalFileDataSource {
    file_path: String,
    file: File,
}

impl CudfLocalFileDataSource {
    pub fn try_new(file_path: String) -> Result<Self> {
        let file = File::open(&file_path)?;
        Ok(Self { file_path, file })
    }
}

impl CudfDataSource for CudfLocalFileDataSource {
    fn data_size(&self) -> usize {
        self.file.len() as usize
    }

    fn read(&mut self, offset: usize, buf: &mut [u8]) -> Result<usize> {
        self.file.seek(std::io::SeekFrom::Start(offset as u64))?;
        let read_size = self.file.read(buf)?;
        Ok(read_size)
    }
}
