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
    fs::{File, OpenOptions},
    io::Result,
    os::unix::fs::OpenOptionsExt,
    path::Path,
};

pub fn open_shuffle_file<P: AsRef<Path>>(path: P) -> Result<File> {
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        // Set the shuffle file permissions to 0644 to keep it consistent with the permissions of
        // the built-in shuffler manager in Spark.
        .mode(0o644)
        .open(path)
}
