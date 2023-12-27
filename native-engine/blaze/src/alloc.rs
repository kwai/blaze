// #[global_allocator]
// static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

use std::{
    alloc::{GlobalAlloc, Layout},
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Mutex,
    },
};

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

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
        self.current.fetch_add(layout.size(), SeqCst);
        self.update();
        self.inner.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.current.fetch_sub(layout.size(), SeqCst);
        self.update();
        self.inner.dealloc(ptr, layout)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        self.current.fetch_add(layout.size(), SeqCst);
        self.update();
        self.inner.alloc_zeroed(layout)
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        self.current.fetch_add(new_size - layout.size(), SeqCst);
        self.update();
        self.inner.realloc(ptr, layout, new_size)
    }
}
