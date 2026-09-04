//! The call an allocation profile. Exact counters behind the
//! global allocator for the table, dhat for the call-site dump.
//!
//! # Why the table is not read off dhat
//! Dhat knows a block by its pointer, and only the blocks it saw allocated. A
//! `realloc` of a block that predates its profiler is booked as a brand-new
//! allocation of the block's *whole* new size, and a free of such a block is ignored.
//! The optimizer grows arenas the frontend allocated, so every first push into
//! one of them added the entire arena to `retained` and `peak`. The counters here use
//! the `Layout` every allocator call carries, so a realloc is its exact delta and
//! a free is subtracted whichever side of the window the block was allocated on.
//!
//! `allocs` and `bytes` keep dhat's definitions so a row still reads against the
//! DHAT viewer's totals. Every `alloc` and every `realloc` is one allocation,
//! and a realloc contributes its new size.
//!
//! # The window
//! [`measure`] takes the call as two closures, `setup` and `unit`, and reads the
//! counters before `setup`, between the two, and after `unit` with its result still
//! alive:
//!
//!   * `allocs`, `bytes` — what the unit allocated.
//!   * `peak` — max live bytes during the unit, above the level it started at.
//!   * `retained` — live bytes after the unit minus live bytes before the setup.
//!     Negative if it freed more than it added.
//!
//! In `--dump` mode the dhat profiler opens around `unit` alone, so the file holds
//! the unit's call sites. dhat's own bookkeeping allocates through this allocator
//! too, so the counters of a dump run are not a table row and no example prints
//! them. One artifact of the pointer tracking stays in the viewer: the first growth
//! of a buffer allocated before the window is shown at the buffer's whole size, at
//! the site that grew it.

use std::alloc::{GlobalAlloc, Layout};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

/// `alloc` and `realloc` calls that succeeded.
static ALLOCS: AtomicU64 = AtomicU64::new(0);
/// Bytes those calls handed out. A realloc counts its new size.
static BYTES: AtomicU64 = AtomicU64::new(0);
/// Bytes currently allocated.
static LIVE: AtomicU64 = AtomicU64::new(0);
/// Max of `LIVE` since [`measure`] last reset it.
static PEAK: AtomicU64 = AtomicU64::new(0);

/// The counting layer over whichever allocator does the work -
/// `dhat::Alloc` in the examples, so the same binary can also write a DHAT dump.
pub struct CountingAlloc<A>(pub A);

fn grew(by: usize) {
    let live = LIVE.fetch_add(by as u64, Relaxed) + by as u64;
    PEAK.fetch_max(live, Relaxed);
}

fn shrank(by: usize) {
    LIVE.fetch_sub(by as u64, Relaxed);
}

unsafe impl<A: GlobalAlloc> GlobalAlloc for CountingAlloc<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.0.alloc(layout) };
        if !ptr.is_null() {
            ALLOCS.fetch_add(1, Relaxed);
            BYTES.fetch_add(layout.size() as u64, Relaxed);
            grew(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.0.alloc_zeroed(layout) };
        if !ptr.is_null() {
            ALLOCS.fetch_add(1, Relaxed);
            BYTES.fetch_add(layout.size() as u64, Relaxed);
            grew(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { self.0.dealloc(ptr, layout) };
        shrank(layout.size());
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { self.0.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            ALLOCS.fetch_add(1, Relaxed);
            BYTES.fetch_add(new_size as u64, Relaxed);
            let old_size = layout.size();
            if new_size >= old_size {
                grew(new_size - old_size);
            } else {
                shrank(old_size - new_size);
            }
        }
        new_ptr
    }
}

/// Which record [`measure`] keeps.
/// The counters, for a table row, or a DHAT file of the
/// given name for the viewer, written when the profiler drops.
pub enum Recorder {
    Stats,
    Dump(String),
}

/// One row of the report.
#[derive(Clone, Copy, Debug)]
pub struct Stats {
    pub allocs: u64,
    pub bytes: u64,
    pub peak: u64,
    pub retained: i64,
}

#[derive(Clone, Copy)]
struct Counters {
    allocs: u64,
    bytes: u64,
    live: u64,
}

fn counters() -> Counters {
    Counters {
        allocs: ALLOCS.load(Relaxed),
        bytes: BYTES.load(Relaxed),
        live: LIVE.load(Relaxed),
    }
}

/// Profile `unit` over the input `setup` builds.
pub fn measure<I, T>(
    recorder: Recorder,
    setup: impl FnOnce() -> I,
    unit: impl FnOnce(I) -> T,
) -> Stats {
    let before = counters();
    let input = setup();

    let profiler = match recorder {
        Recorder::Stats => None,
        Recorder::Dump(file) => Some(dhat::Profiler::builder().file_name(file).build()),
    };
    let start = counters();
    PEAK.store(start.live, Relaxed);

    let output = unit(input);

    let end = counters();
    let peak = PEAK.load(Relaxed);
    drop(black_box(output));
    drop(profiler);

    Stats {
        allocs: end.allocs - start.allocs,
        bytes: end.bytes - start.bytes,
        peak: peak - start.live,
        retained: end.live as i64 - before.live as i64,
    }
}
