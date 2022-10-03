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

//! Parquet format abstractions

use std::sync::Arc;
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use datafusion::parquet::file::footer::{decode_footer, decode_metadata};
use datafusion::parquet::file::metadata::ParquetMetaData;
use crate::file_format::ObjectMeta;
use crate::util::fs::FsDataInputStream;

pub(crate) async fn fetch_parquet_metadata(
    input: Arc<FsDataInputStream>,
    meta: &ObjectMeta,
) -> Result<ParquetMetaData> {
    if meta.size < 8 {
        return Err(DataFusionError::Execution(format!(
            "file size of {} is less than footer",
            meta.size
        )));
    }

    let footer_start = meta.size - 8;
    let mut footer = [0; 8];
    input.read_fully(footer_start as u64, &mut footer)?;

    let length = decode_footer(&footer)?;

    if meta.size < length + 8 {
        return Err(DataFusionError::Execution(format!(
            "file size of {} is less than footer + metadata {}",
            meta.size,
            length + 8
        )));
    }

    let metadata_start = meta.size - length - 8;
    let metadata_len = footer_start - metadata_start;
    let mut metadata = vec![0u8; metadata_len];
    input.read_fully(metadata_start as u64, &mut metadata)?;

    Ok(decode_metadata(metadata.as_ref())?)
}
