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

// use std::sync::atomic::AtomicUsize;
use std::alloc::System;
// use std::sync::atomic::Ordering::{Relaxed, SeqCst};
// use std::alloc::Layout;
// use std::alloc::GlobalAlloc;

#[global_allocator]
static GLOBAL: System = System;

// #[global_allocator]
// static GLOBAL: CountedAlloc = CountedAlloc {
//     inner: System,
//     total_used: AtomicUsize::new(0),
// };
//
// #[derive(Default)]
// struct CountedAlloc {
//     inner: System,
//     total_used: AtomicUsize,
// }
//
// unsafe impl GlobalAlloc for CountedAlloc {
//     unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
//         self.total_used.fetch_add(layout.size(), SeqCst);
//         self.inner.alloc(layout)
//     }
//
//     unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
//         self.inner.dealloc(ptr, layout);
//         self.total_used.fetch_sub(layout.size(), SeqCst);
//     }
//
//     unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
//         self.total_used.fetch_add(layout.size(), SeqCst);
//         self.inner.alloc_zeroed(layout)
//     }
//
//     unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
//         self.total_used.fetch_add(new_size, SeqCst);
//         let ptr = self.inner.realloc(ptr, layout, new_size);
//
//         self.total_used.fetch_sub(layout.size(), SeqCst);
//         ptr
//
//     }
// }