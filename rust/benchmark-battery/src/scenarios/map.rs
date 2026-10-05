use super::{Benchmark, SampledBenchmark};
use benchmark_battery::automerge::{
    transaction::Transactable, Author, Automerge, Change, ChangeHash, LoadOptions, ScalarValue,
    ROOT,
};
use benchmark_battery::{
    deep_history_doc, in_author_transactions, local_user, masked, policy_masked_at_heads,
    policy_masked_for_bytes,
};
use std::collections::HashMap;
use std::hint::black_box;

const SIZES: [u64; 3] = [100, 1_000, 10_000];

type Policy = HashMap<Author<'static>, Vec<ChangeHash>>;
pub fn benchmarks() -> Vec<Benchmark> {
    let mut benchmarks = Vec::new();
    for n in SIZES {
        add_case(
            &mut benchmarks,
            "repeated_increment",
            n,
            repeated_increment_doc,
        );
        add_case(&mut benchmarks, "repeated_put", n, repeated_put_doc);
        add_case(&mut benchmarks, "increasing_put", n, increasing_put_doc);
        add_case(&mut benchmarks, "decreasing_put", n, decreasing_put_doc);

        // A build, so unmasked for the reason given in build.rs.
        benchmarks.push(
            SampledBenchmark::no_setup("map", name("map", "deep_history", n), move || {
                build_setup(n, deep_history_doc)
            })
            .into(),
        );
    }
    benchmarks
}

type DocBuilder = fn(u64) -> Automerge;

fn add_case(benchmarks: &mut Vec<Benchmark>, operation: &'static str, n: u64, builder: DocBuilder) {
    benchmarks.extend([
        // A build, so unmasked for the reason given in build.rs.
        SampledBenchmark::no_setup("map", name("map", operation, n), move || {
            build_setup(n, builder)
        })
        .into(),
        SampledBenchmark::batched(
            "map",
            name("map/save", operation, n),
            move || masked(builder(n)),
            save_doc,
        )
        .into(),
        SampledBenchmark::batched(
            "map",
            name("map/load", operation, n),
            move || {
                let bytes = builder(n).save();
                let policy = policy_masked_for_bytes(&bytes);
                (bytes, policy)
            },
            load_doc,
        )
        .into(),
        SampledBenchmark::batched(
            "map",
            name("map/apply", operation, n),
            move || {
                let doc = builder(n);
                (owned_changes(&doc), policy_masked_at_heads(&doc))
            },
            apply_changes,
        )
        .into(),
    ]);
}

fn name(group: &str, operation: &str, n: u64) -> &'static str {
    Box::leak(format!("{group}/{operation}/{n}").into_boxed_str())
}

fn build_setup(n: u64, builder: DocBuilder) -> Box<dyn FnMut()> {
    Box::new(move || {
        black_box(builder(n));
    })
}

fn save_doc(doc: Automerge) -> (Automerge, Vec<u8>) {
    let bytes = doc.save();
    (doc, bytes)
}

fn load_doc((bytes, policy): (Vec<u8>, Policy)) -> (Vec<u8>, Policy, Automerge) {
    let options = LoadOptions::new().write_frontier(policy.clone());
    let doc = Automerge::load_with_options(&bytes, options).unwrap();
    (bytes, policy, doc)
}

/// Apply `changes` to an empty document that already carries the frontier
/// pinned at the source's heads, as a receiving peer would.
fn apply_changes((changes, policy): (Vec<Change>, Policy)) -> Automerge {
    let mut doc = Automerge::new().with_write_frontier(policy);
    doc.apply_changes(changes).unwrap();
    doc
}

fn owned_changes(doc: &Automerge) -> Vec<Change> {
    doc.get_changes(&[])
}

/// A counter on `ROOT` incremented `n` times across the author blocks. The
/// counter itself is the local user's own change so that it is never hidden.
fn repeated_increment_doc(n: u64) -> Automerge {
    let mut doc = Automerge::new().with_author(Some(local_user()));
    let mut tx = doc.transaction();
    tx.put(ROOT, "counter", ScalarValue::counter(0)).unwrap();
    tx.commit();
    in_author_transactions(&mut doc, n as usize, |tx, range| {
        for _ in range {
            tx.increment(ROOT, "counter", 1).unwrap();
        }
    });
    doc
}

/// `n` puts to the single key `"0"`, so every author block overwrites the last.
fn repeated_put_doc(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    in_author_transactions(&mut doc, n as usize, |tx, range| {
        for i in range {
            tx.put(ROOT, "0", i as u64).unwrap();
        }
    });
    doc
}

fn increasing_put_doc(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    in_author_transactions(&mut doc, n as usize, |tx, range| {
        for i in range {
            tx.put(ROOT, i.to_string(), i as u64).unwrap();
        }
    });
    doc
}

fn decreasing_put_doc(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    in_author_transactions(&mut doc, n as usize, |tx, range| {
        for i in range.rev() {
            tx.put(ROOT, i.to_string(), i as u64).unwrap();
        }
    });
    doc
}
