use crate::scenarios::MemoryMeasurement;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

#[derive(Clone, Copy, Debug, Default)]
struct AllocationStats {
    current_bytes: usize,
    peak_bytes: usize,
}

thread_local! {
    static STATS: Cell<AllocationStats> = const {
        Cell::new(AllocationStats {
            current_bytes: 0,
            peak_bytes: 0,
        })
    };
}

struct TrackingAllocator;

#[global_allocator]
static GLOBAL_ALLOCATOR: TrackingAllocator = TrackingAllocator;

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record_resize(0, layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        record_resize(layout.size(), 0);
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record_resize(0, layout.size());
        }
        pointer
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_pointer = unsafe { System.realloc(pointer, layout, new_size) };
        if !new_pointer.is_null() {
            record_resize(layout.size(), new_size);
        }
        new_pointer
    }
}

fn record_resize(old_size: usize, new_size: usize) {
    let _ = STATS.try_with(|cell| {
        let mut stats = cell.get();
        stats.current_bytes = stats
            .current_bytes
            .saturating_sub(old_size)
            .saturating_add(new_size);
        stats.peak_bytes = stats.peak_bytes.max(stats.current_bytes);
        cell.set(stats);
    });
}

fn allocation_stats() -> AllocationStats {
    STATS.try_with(Cell::get).unwrap_or_default()
}

fn reset_peak() {
    let _ = STATS.try_with(|cell| {
        let mut stats = cell.get();
        stats.peak_bytes = stats.current_bytes;
        cell.set(stats);
    });
}

/// Measure heap bytes requested by allocations on the current thread.
///
/// `steady_bytes` is measured while the returned value is still alive. Allocations
/// made by other threads are intentionally not included.
pub fn measure<F, Output>(operation: F) -> MemoryMeasurement
where
    F: FnOnce() -> Output,
{
    let before = allocation_stats().current_bytes;
    reset_peak();

    let output = operation();
    std::hint::black_box(&output);

    let after = allocation_stats();
    let measurement = MemoryMeasurement {
        peak_bytes: after.peak_bytes.saturating_sub(before) as u64,
        steady_bytes: after.current_bytes.saturating_sub(before) as u64,
    };
    drop(output);
    measurement
}

#[cfg(test)]
mod tests {
    use super::measure;

    #[test]
    fn measures_peak_and_steady_allocations() {
        let measurement = measure(|| {
            let retained = Vec::<u8>::with_capacity(1_024);
            {
                let temporary = Vec::<u8>::with_capacity(2_048);
                std::hint::black_box(&temporary);
            }
            retained
        });

        assert!(measurement.peak_bytes >= 3_072, "{measurement:?}");
        assert!(measurement.steady_bytes >= 1_024, "{measurement:?}");
        assert!(measurement.steady_bytes < measurement.peak_bytes);
    }
}
