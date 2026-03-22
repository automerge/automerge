use divan::Bencher;
use hexane::*;
use std::time::Duration;

use rand::{Rng, SeedableRng};

fn main() {
    divan::main();
}

// ── Vec<u8> baseline ────────────────────────────────────────────────────────

/// Insert N spans of `span_len` bytes at random positions into a Vec<u8>.
fn vec_insert_spans(n: usize, span_len: usize) {
    let mut rng = rand::rngs::SmallRng::seed_from_u64(42);
    let mut buf = Vec::<u8>::new();
    let span: Vec<u8> = (0..span_len).map(|i| i as u8).collect();
    for _ in 0..n {
        let pos = if buf.is_empty() {
            0
        } else {
            rng.random_range(0..buf.len())
        };
        // Vec insert requires splicing bytes in
        buf.splice(pos..pos, span.iter().copied());
    }
    divan::black_box(&buf);
}

/// Read back all spans sequentially from Vec<u8>.
fn vec_read_spans(buf: &[u8], span_len: usize) {
    let mut pos = 0;
    while pos + span_len <= buf.len() {
        let slice = &buf[pos..pos + span_len];
        divan::black_box(slice);
        pos += span_len;
    }
}

// ── ColumnData<BigRawCursor> (slab-based, 160KB slabs) ───────────────────────

type BigRawCursor = RawCursorInternal<163840>;

/// Insert N spans of `span_len` bytes at random positions into a ColumnData<BigRawCursor>.
fn slab_insert_spans(n: usize, span_len: usize) -> ColumnData<BigRawCursor> {
    let mut rng = rand::rngs::SmallRng::seed_from_u64(42);
    let mut col: ColumnData<BigRawCursor> = ColumnData::new();
    let span: Vec<u8> = (0..span_len).map(|i| i as u8).collect();
    for _ in 0..n {
        let pos = if col.is_empty() {
            0
        } else {
            rng.random_range(0..col.len())
        };
        col.splice(pos, 0, vec![span.clone()]);
    }
    col
}

/// Read back all spans sequentially from ColumnData<BigRawCursor>.
fn slab_read_spans(col: &ColumnData<BigRawCursor>, span_len: usize) {
    let mut reader = col.raw_reader(0);
    let total = col.len();
    let mut pos = 0;
    while pos + span_len <= total {
        match reader.read_next(span_len) {
            Ok(slice) => {
                divan::black_box(slice);
            }
            Err(_) => {
                // CrossBoundary — re-seek (this is the cost of slab splits)
                reader.seek_to(pos);
                if let Ok(slice) = reader.read_next(span_len) {
                    divan::black_box(slice);
                }
            }
        }
        pos += span_len;
    }
}

// ── Benchmarks: Insert ──────────────────────────────────────────────────────

mod insert_100 {
    use super::*;
    const N: usize = 100;
    const SPAN: usize = 16;

    #[divan::bench(max_time = Duration::from_secs(3))]
    fn vec(bencher: Bencher) {
        bencher.bench(|| vec_insert_spans(N, SPAN));
    }

    #[divan::bench(max_time = Duration::from_secs(3))]
    fn slab(bencher: Bencher) {
        bencher.bench(|| slab_insert_spans(N, SPAN));
    }
}

mod insert_1k {
    use super::*;
    const N: usize = 1_000;
    const SPAN: usize = 16;

    #[divan::bench(max_time = Duration::from_secs(3))]
    fn vec(bencher: Bencher) {
        bencher.bench(|| vec_insert_spans(N, SPAN));
    }

    #[divan::bench(max_time = Duration::from_secs(3))]
    fn slab(bencher: Bencher) {
        bencher.bench(|| slab_insert_spans(N, SPAN));
    }
}

mod insert_10k {
    use super::*;
    const N: usize = 10_000;
    const SPAN: usize = 16;

    #[divan::bench(max_time = Duration::from_secs(3))]
    fn vec(bencher: Bencher) {
        bencher.bench(|| vec_insert_spans(N, SPAN));
    }

    #[divan::bench(max_time = Duration::from_secs(3))]
    fn slab(bencher: Bencher) {
        bencher.bench(|| slab_insert_spans(N, SPAN));
    }
}

mod insert_50k {
    use super::*;
    const N: usize = 50_000;
    const SPAN: usize = 16;

    #[divan::bench(max_time = Duration::from_secs(5))]
    fn vec(bencher: Bencher) {
        bencher.bench(|| vec_insert_spans(N, SPAN));
    }

    #[divan::bench(max_time = Duration::from_secs(5))]
    fn slab(bencher: Bencher) {
        bencher.bench(|| slab_insert_spans(N, SPAN));
    }
}

mod insert_100k {
    use super::*;
    const N: usize = 100_000;
    const SPAN: usize = 16;

    #[divan::bench(max_time = Duration::from_secs(10))]
    fn vec(bencher: Bencher) {
        bencher.bench(|| vec_insert_spans(N, SPAN));
    }

    #[divan::bench(max_time = Duration::from_secs(10))]
    fn slab(bencher: Bencher) {
        bencher.bench(|| slab_insert_spans(N, SPAN));
    }
}

// ── Benchmarks: Sequential Read ─────────────────────────────────────────────

mod read_10k {
    use super::*;
    const N: usize = 10_000;
    const SPAN: usize = 16;

    #[divan::bench(max_time = Duration::from_secs(3))]
    fn vec(bencher: Bencher) {
        let mut rng = rand::rngs::SmallRng::seed_from_u64(42);
        let span: Vec<u8> = (0..SPAN).map(|i| i as u8).collect();
        let mut buf = Vec::<u8>::new();
        for _ in 0..N {
            let pos = if buf.is_empty() {
                0
            } else {
                rng.random_range(0..buf.len())
            };
            buf.splice(pos..pos, span.iter().copied());
        }
        bencher.bench(|| vec_read_spans(&buf, SPAN));
    }

    #[divan::bench(max_time = Duration::from_secs(3))]
    fn slab(bencher: Bencher) {
        let col = slab_insert_spans(N, SPAN);
        bencher.bench(|| slab_read_spans(&col, SPAN));
    }
}

// ── Benchmarks: Larger spans ────────────────────────────────────────────────

mod insert_10k_span64 {
    use super::*;
    const N: usize = 10_000;
    const SPAN: usize = 64;

    #[divan::bench(max_time = Duration::from_secs(5))]
    fn vec(bencher: Bencher) {
        bencher.bench(|| vec_insert_spans(N, SPAN));
    }

    #[divan::bench(max_time = Duration::from_secs(5))]
    fn slab(bencher: Bencher) {
        bencher.bench(|| slab_insert_spans(N, SPAN));
    }
}

mod insert_10k_span256 {
    use super::*;
    const N: usize = 10_000;
    const SPAN: usize = 256;

    #[divan::bench(max_time = Duration::from_secs(5))]
    fn vec(bencher: Bencher) {
        bencher.bench(|| vec_insert_spans(N, SPAN));
    }

    #[divan::bench(max_time = Duration::from_secs(5))]
    fn slab(bencher: Bencher) {
        bencher.bench(|| slab_insert_spans(N, SPAN));
    }
}
