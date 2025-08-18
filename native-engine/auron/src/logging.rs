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

use std::{cell::Cell, str::FromStr, time::Instant};

use chrono::Local;
use log::{Level, LevelFilter, Log, Metadata, Record};
use once_cell::sync::OnceCell;

thread_local! {
    pub static THREAD_TID: Cell<usize> = Cell::new(0);
    pub static THREAD_STAGE_ID: Cell<usize> = Cell::new(0);
    pub static THREAD_PARTITION_ID: Cell<usize> = Cell::new(0);
}

const DEFAULT_MAX_LEVEL: Level = Level::Info;

pub fn init_logging(level: &str) {
    let log_level = Level::from_str(level).unwrap_or_else(|_| DEFAULT_MAX_LEVEL);
    static LOGGER: OnceCell<SimpleLogger> = OnceCell::new();
    let logger = LOGGER.get_or_init(|| SimpleLogger {
        start_instant: Instant::now(),
        log_level,
    });

    log::set_logger(logger).expect("error setting logger");
    log::set_max_level(LevelFilter::Info);
}

#[derive(Clone, Copy)]
struct SimpleLogger {
    start_instant: Instant,
    log_level: Level,
}

impl Log for SimpleLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= self.log_level
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            let elapsed = Instant::now() - self.start_instant;
            let elapsed_sec = elapsed.as_secs_f64();
            let stage_id = THREAD_STAGE_ID.get();
            let partition_id = THREAD_PARTITION_ID.get();
            let tid = THREAD_TID.get();
            eprintln!(
                "{} (+{elapsed_sec:.3}s) [{}] [{}:{}] (stage: {stage_id}, partition: {partition_id}, tid: {tid}) - {}",
                Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
                record.level(),
                record.module_path().unwrap_or(""),
                record.line().unwrap_or(0),
                record.args()
            );
        }
    }

    fn flush(&self) {
        // do nothing
    }
}
