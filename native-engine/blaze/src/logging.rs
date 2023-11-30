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

use log::LevelFilter;
use log4rs::{
    append::console::{ConsoleAppender, Target},
    config::{Appender, Config, Root},
};
use once_cell::sync::OnceCell;

pub fn init_logging() {
    static LOGGING_INIT: OnceCell<()> = OnceCell::new();
    LOGGING_INIT.get_or_init(|| {
        let stderr = Box::new(ConsoleAppender::builder().target(Target::Stderr).build());
        let stderr_appender = Appender::builder().build("stderr", stderr);
        let config = Config::builder()
            .appender(stderr_appender)
            .build(Root::builder().appender("stderr").build(LevelFilter::Info))
            .expect("log4rs build config error");
        log4rs::init_config(config).expect("log4rs init config error");
    });
}
