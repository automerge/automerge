//! Benchmarks for the change-graph fragmenter.
//!
//! Two implementations live side by side on this branch:
//!
//! - `Automerge::fragments()` — the existing eager, clock-based prototype.
//!   Runs `cache_fragment` per change inside `add_change`, so the work is
//!   amortised across `apply_changes` / commit; query is cheap.
//! - `Automerge::depth_fragments()` — spec-correct, depth-stratified BFS.
//!   Lazy: no eager work; full BFS on every query.
//!
//! Three bench groups:
//!
//! - `fragments/build` — total cost of building a doc, one tx per change.
//!   Measures the clock-based eager pipeline (depth has no eager cost).
//!   Reference only.
//! - `fragments/apply` — `apply_changes` of a pre-extracted Vec.
//!   Same caveat: the clock-based eager path is always on when calling
//!   `add_change`, so this also measures clock-based overhead.
//! - `fragments/query` — head-to-head: time `fragments()` (clock-based)
//!   vs `depth_fragments()` (spec) on the same prebuilt doc. This is the
//!   apples-to-apples comparison of pure query cost.
//!
//! Topologies:
//!
//! - `linear` — one actor, one transaction per commit.
//! - `concurrent2` — two actors making half the commits each on forks,
//!   then merging at the end.
//! - `wide8` — eight actors making concurrent commits on independent
//!   forks, then merging.

use automerge::{transaction::Transactable, Automerge, ChangeHash, ROOT};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;

/// Build function for a parameterised topology.
type TopologyBuilder = fn(u64) -> Automerge;

/// Named topology used by the parameterised bench groups.
type Topology = (&'static str, TopologyBuilder);

// ─── doc builders ──────────────────────────────────────────────────────────

fn build_linear(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    for i in 0..n {
        let mut tx = doc.transaction();
        tx.put(ROOT, i.to_string(), i as i64).unwrap();
        tx.commit();
    }
    doc
}

fn build_concurrent2(n: u64) -> Automerge {
    let half = n / 2;

    let mut a = Automerge::new();
    let mut b = a.fork();

    for i in 0..half {
        let mut tx = a.transaction();
        tx.put(ROOT, format!("a{i}"), i as i64).unwrap();
        tx.commit();
    }
    for i in 0..half {
        let mut tx = b.transaction();
        tx.put(ROOT, format!("b{i}"), i as i64).unwrap();
        tx.commit();
    }

    a.merge(&mut b).unwrap();
    a
}

fn build_wide8(n: u64) -> Automerge {
    build_wide(n, 8)
}

fn build_wide(n: u64, actors: u64) -> Automerge {
    let per = n / actors;

    let base = Automerge::new();
    let mut forks: Vec<Automerge> = (0..actors).map(|_| base.fork()).collect();

    for (idx, fork) in forks.iter_mut().enumerate() {
        for i in 0..per {
            let mut tx = fork.transaction();
            tx.put(ROOT, format!("a{idx}_{i}"), i as i64).unwrap();
            tx.commit();
        }
    }

    let mut acc = forks.remove(0);
    for mut other in forks {
        acc.merge(&mut other).unwrap();
    }
    acc
}

fn changes_topo(doc: &Automerge) -> Vec<automerge::Change> {
    doc.get_changes(&[] as &[ChangeHash])
}

const TOPOLOGIES: &[Topology] = &[
    ("linear", build_linear),
    ("concurrent2", build_concurrent2),
    ("wide8", build_wide8),
];

// ─── bench groups ──────────────────────────────────────────────────────────

fn bench_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("fragments/build");
    for &n in &[1_000u64, 10_000] {
        group.throughput(Throughput::Elements(n));
        for &(topo, builder) in TOPOLOGIES {
            group.bench_with_input(BenchmarkId::new(topo, n), &n, |b, &n| {
                b.iter(|| black_box(builder(n)))
            });
        }
    }
    group.finish();
}

/// Head-to-head: clock-based `fragments()` vs depth-stratified
/// `depth_fragments()`, on the same prebuilt doc. This is the only bench
/// group where the two strategies are doing work along the comparable
/// critical path.
fn bench_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("fragments/query");
    for &n in &[1_000u64, 10_000] {
        group.throughput(Throughput::Elements(n));
        for &(topo, builder) in TOPOLOGIES {
            let doc = builder(n);

            // Sanity: print fragment counts so a glance at bench output
            // confirms both strategies actually produced fragments and shows
            // their semantic divergence (clock-based emits one Fragment per
            // depth-0 commit above `fragment_top` plus one per cached
            // FragmentNode; depth emits one per commit with `level >= 1`).
            let frags_clock = doc.fragments();
            let frags_depth = doc.depth_fragments();
            eprintln!(
                "  [{topo}/{n}] clock={} depth={} (heads={})",
                frags_clock.len(),
                frags_depth.len(),
                doc.get_heads().len()
            );

            group.bench_with_input(
                BenchmarkId::new(format!("clock/{topo}"), n),
                &doc,
                |b, doc| b.iter(|| black_box(doc.fragments().len())),
            );
            group.bench_with_input(
                BenchmarkId::new(format!("depth/{topo}"), n),
                &doc,
                |b, doc| b.iter(|| black_box(doc.depth_fragments().len())),
            );
        }
    }
    group.finish();
}

fn bench_apply(c: &mut Criterion) {
    let mut group = c.benchmark_group("fragments/apply");
    for &n in &[1_000u64, 10_000] {
        group.throughput(Throughput::Elements(n));
        for &(topo, builder) in TOPOLOGIES {
            let src = builder(n);
            let changes = changes_topo(&src);
            group.bench_with_input(BenchmarkId::new(topo, n), &changes, |b, changes| {
                b.iter_batched(
                    || changes.clone(),
                    |changes| {
                        let mut dst = Automerge::new();
                        dst.apply_changes(changes).unwrap();
                        black_box(dst.fragments().len())
                    },
                    criterion::BatchSize::LargeInput,
                )
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_build, bench_query, bench_apply);
criterion_main!(benches);
