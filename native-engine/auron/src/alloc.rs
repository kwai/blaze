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

// #[global_allocator]
// static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

use std::{
    alloc::{GlobalAlloc, Layout},
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
};

#[cfg(any(feature = "jemalloc", feature = "jemalloc-pprof"))]
#[cfg(not(windows))]
#[cfg_attr(not(windows), global_allocator)]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// only used for debugging
//
// #[global_allocator]
// static GLOBAL: DebugAlloc<jemallocator::Jemalloc> =
// DebugAlloc::new(jemallocator::Jemalloc);

#[allow(unused)]
struct DebugAlloc<T: GlobalAlloc> {
    inner: T,
    last_updated: AtomicUsize,
    current: AtomicUsize,
    mutex: Mutex<()>,
}

#[allow(unused)]
impl<T: GlobalAlloc> DebugAlloc<T> {
    pub const fn new(inner: T) -> Self {
        Self {
            inner,
            last_updated: AtomicUsize::new(0),
            current: AtomicUsize::new(0),
            mutex: Mutex::new(()),
        }
    }

    fn update(&self) {
        let _lock = self.mutex.lock().unwrap();
        let current = self.current.load(SeqCst);
        let last_updated = self.last_updated.load(SeqCst);
        let delta = (current as isize - last_updated as isize).abs();
        if delta > 104857600 {
            eprintln!(" * ALLOC {} -> {}", last_updated, current);
            self.last_updated.store(current, SeqCst);
        }
    }
}

unsafe impl<T: GlobalAlloc> GlobalAlloc for DebugAlloc<T> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        unsafe {
            self.current.fetch_add(layout.size(), SeqCst);
            self.update();
            self.inner.alloc(layout)
        }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe {
            self.current.fetch_sub(layout.size(), SeqCst);
            self.update();
            self.inner.dealloc(ptr, layout)
        }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        unsafe {
            self.current.fetch_add(layout.size(), SeqCst);
            self.update();
            self.inner.alloc_zeroed(layout)
        }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        unsafe {
            self.current.fetch_add(new_size - layout.size(), SeqCst);
            self.update();
            self.inner.realloc(ptr, layout, new_size)
        }
    }
}
