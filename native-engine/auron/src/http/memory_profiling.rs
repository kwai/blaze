// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::{Error, ErrorKind};

use log::error;
use poem::{Request, RouteMethod, http::StatusCode};

use super::Handler;

#[cfg(feature = "jemalloc-pprof")]
#[cfg(not(target_env = "msvc"))]
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "malloc_conf")]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";

#[poem::handler]
async fn jemalloc_pprof_handler(_: &Request) -> poem::Result<Vec<u8>> {
    let pprof = dump_prof()
        .await
        .map_err(|e| poem::Error::from_string(e.to_string(), StatusCode::INTERNAL_SERVER_ERROR))?;
    Ok(pprof)
}

async fn dump_prof() -> Result<Vec<u8>, Error> {
    let prof_ctl = jemalloc_pprof::PROF_CTL
        .as_ref()
        .ok_or_else(|| Error::new(ErrorKind::Other, "JemallocProfCtl not initialized"))?;
    let mut prof_ctl = prof_ctl.lock().await;
    let pprof = prof_ctl.dump_pprof().map_err(|err| {
        error!("Errors on jemalloc profile. err: {:?}", &err);
        Error::new(ErrorKind::Other, err)
    })?;
    Ok(pprof)
}

pub struct MemoryProfileHandler {}

impl Default for MemoryProfileHandler {
    fn default() -> Self {
        MemoryProfileHandler {}
    }
}

impl Handler for MemoryProfileHandler {
    fn get_route_method(&self) -> RouteMethod {
        RouteMethod::new().get(jemalloc_pprof_handler)
    }

    fn get_route_path(&self) -> String {
        "/debug/memory/profile".to_string()
    }
}
