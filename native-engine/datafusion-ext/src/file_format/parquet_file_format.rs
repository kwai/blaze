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

use datafusion::common::DataFusionError;
use datafusion::error::Result;
use datafusion::parquet::file::footer::{decode_footer, decode_metadata};
use datafusion::parquet::file::metadata::ParquetMetaData;
use object_store::{ObjectMeta, ObjectStore};

pub(crate) async fn fetch_parquet_metadata(
    store: &dyn ObjectStore,
    meta: &ObjectMeta,
) -> Result<ParquetMetaData> {
    if meta.size < 8 {
        return Err(DataFusionError::Execution(format!(
            "file size of {} is less than footer",
            meta.size
        )));
    }

    let footer_start = meta.size - 8;
    let suffix = store
        .get_range(&meta.location, footer_start..meta.size)
        .await?;

    let mut footer = [0; 8];
    footer.copy_from_slice(suffix.as_ref());

    let length = decode_footer(&footer)?;

    if meta.size < length + 8 {
        return Err(DataFusionError::Execution(format!(
            "file size of {} is less than footer + metadata {}",
            meta.size,
            length + 8
        )));
    }

    let metadata_start = meta.size - length - 8;
    let metadata = store
        .get_range(&meta.location, metadata_start..footer_start)
        .await?;

    Ok(decode_metadata(metadata.as_ref())?)
}
