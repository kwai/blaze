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

use std::{fs, path::PathBuf};

fn main() -> Result<(), String> {
    // for use in docker build where file changes can be wonky
    println!("cargo:rerun-if-env-changed=FORCE_REBUILD");

    println!("cargo:rerun-if-changed=proto/auron.proto");

    let mut prost_build = tonic_build::Config::new();
    // protobuf-maven-plugin download the protoc executable in the target directory
    let protoc_dir_path = "../../dev/mvn-build-helper/proto/target/protoc-plugins";
    let mut protoc_file: Option<PathBuf> = None;
    if let Ok(entries) = fs::read_dir(protoc_dir_path) {
        for entry in entries.filter_map(Result::ok) {
            let path = entry.path();
            if path.is_file()
                && path
                    .file_name()
                    .map(|f| f.to_string_lossy().starts_with("protoc"))
                    .unwrap_or(false)
            {
                protoc_file = Some(path);
                break;
            }
        }
    }
    if let Some(path) = protoc_file {
        eprintln!("Using protoc executable: {:?}", path);
        prost_build.protoc_executable(path);
    }
    prost_build
        .compile_protos(&["proto/auron.proto"], &["proto"])
        .map_err(|e| format!("protobuf compilation failed: {}", e))
}
