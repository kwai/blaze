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

use std::{future::Future, time::Instant};

use datafusion::physical_plan::metrics::Time;
use futures::{future::BoxFuture, FutureExt};

pub trait TimerHelper {
    fn with_timer<T>(&self, f: impl FnOnce() -> T) -> T;
    fn with_timer_async<'a, T>(&'a self, f: impl Future<Output = T> + Send + 'a) -> BoxFuture<T>;
}

impl TimerHelper for Time {
    fn with_timer<T>(&self, f: impl FnOnce() -> T) -> T {
        let _timer = self.timer();
        f()
    }

    fn with_timer_async<'a, T>(&'a self, f: impl Future<Output = T> + Send + 'a) -> BoxFuture<T> {
        let time = self.clone();
        let start_time = Instant::now();
        f.inspect(move |_| time.add_duration(start_time.elapsed()))
            .boxed()
    }
}
