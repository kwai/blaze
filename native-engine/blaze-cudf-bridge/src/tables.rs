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

use arrow::{
    array::{make_array, AsArray, RecordBatch},
    datatypes::{DataType, SchemaRef},
    error::Result,
    ffi::{from_ffi_and_data_type, FFI_ArrowArray, FFI_ArrowSchema},
};

use crate::{ffi::*, AsRawPtrAddr};

pub struct CudfTableStream {
    table_stream: FFI_TableStreamPtr,
}

impl AsRawPtrAddr for CudfTableStream {
    fn as_raw_ptr_addr(&self) -> usize {
        self.table_stream.as_raw_ptr_addr()
    }
}

impl CudfTableStream {
    pub fn from_raw_table_stream(table_stream: FFI_TableStreamPtr) -> Self {
        Self { table_stream }
    }

    fn next_impl(&mut self) -> Result<Option<CudfTable>> {
        let has_next = unsafe {
            ffi_table_stream_has_next(self.as_raw_ptr_addr())
                .into_result(format!("error invoking table_stream.has_next()"))?
        };
        if has_next {
            let raw_table = unsafe {
                ffi_table_stream_next(self.as_raw_ptr_addr())
                    .into_result(format!("error invoking table_stream.next()"))?
            };
            return Ok(Some(CudfTable::from_raw_table(raw_table)));
        }
        Ok(None)
    }
}

impl Iterator for CudfTableStream {
    type Item = Result<CudfTable>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_impl().transpose()
    }
}

pub struct CudfTable {
    table: FFI_TablePtr,
}

impl AsRawPtrAddr for CudfTable {
    fn as_raw_ptr_addr(&self) -> usize {
        self.table.as_raw_ptr_addr()
    }
}

impl CudfTable {
    pub fn from_raw_table(table: FFI_TablePtr) -> Self {
        Self { table }
    }

    pub fn to_arrow_record_batch(&self, schema: SchemaRef) -> Result<RecordBatch> {
        let schema_type = DataType::Struct(schema.fields().clone());
        let mut ffi_schema = FFI_ArrowSchema::try_from(schema_type.clone())?;
        let mut ffi_array = FFI_ArrowArray::empty();
        let array_data = unsafe {
            ffi_table_to_arrow(self.as_raw_ptr_addr(), &mut ffi_schema, &mut ffi_array)
                .into_result(format!(
                    "error converting cudf::table to arrow record batch"
                ))?;
            from_ffi_and_data_type(ffi_array, schema_type)?
        };
        Ok(RecordBatch::from(
            make_array(array_data).as_struct().clone(),
        ))
    }
}
