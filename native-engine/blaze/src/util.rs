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

use std::io::{Cursor, };
use blaze_commons::{jni_call, jni_new_direct_byte_buffer};
use datafusion::common::Result;
use datafusion::physical_plan::coalesce_batches::concat_batches;
use datafusion_ext_commons::io::write_one_batch;
use jni::JNIEnv;
use jni::objects::{JClass, JObject};
use jni::sys::{jboolean, JNI_TRUE};
use datafusion_ext_commons::streams::ipc_stream::get_channel_reader;
use crate::{handle_unwinded_scope, SESSION};

#[allow(non_snake_case)]
#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_blaze_JniBridge_mergeIpcs(
    _: JNIEnv,
    _: JClass,
    ipcBytesIter: JObject,
    mergedIpcBytesHandler: JObject,
) {
    handle_unwinded_scope(|| {
        merge_ipcs(ipcBytesIter, mergedIpcBytesHandler)?;
        Result::Ok(())
    });
}

fn merge_ipcs(
    ipc_channel_iter: JObject,
    merged_ipc_channel_handler: JObject,
) -> Result<()> {

    let mut staging_batches = vec![];
    let mut staging_rows = 0;
    let batch_size = SESSION.get().unwrap().copied_config().batch_size();

    macro_rules! flush_staging_batches {
        () => {{
            let mut buf = vec![];
            let mut cursor = Cursor::new(&mut buf);

            let batch = concat_batches(
                &staging_batches[0].schema(),
                &staging_batches,
                staging_rows)?;
            staging_batches.clear();
            staging_rows = 0;
            write_one_batch(&batch, &mut cursor, true)?;

            let merged_ipc_byte_buf = jni_new_direct_byte_buffer!(&mut buf)?;
            jni_call!(
                ScalaFunction1(merged_ipc_channel_handler).apply(merged_ipc_byte_buf) -> JObject
            )?;
            let _ = staging_rows; // suppress warnings
        }}
    }

    while jni_call!(ScalaIterator(ipc_channel_iter).hasNext() -> jboolean)? == JNI_TRUE {
        let ipc_channel = jni_call!(ScalaIterator(ipc_channel_iter).next() -> JObject)?;
        let mut channel_reader = get_channel_reader(None, ipc_channel, true)?;

        while let Some(batch) = channel_reader.next_batch()? {
            staging_rows += batch.num_rows();
            staging_batches.push(batch);
            if staging_rows >= batch_size {
                flush_staging_batches!();
            }
        }
    }
    if staging_rows > 0 {
        flush_staging_batches!();
    }
    Ok(())
}