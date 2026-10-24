use super::{Benchmark, SampledBenchmark};
use benchmark_battery::automerge::{
    transaction::Transactable, transaction::Transaction, Author, Automerge, Change, ChangeHash,
    LoadOptions, ScalarValue, ROOT,
};
use benchmark_battery::{
    deep_history_doc, hidden, in_author_transactions, local_user, masked, pending_receiver,
    policy_masked_for_bytes, tail,
};
use std::collections::HashMap;
use std::hint::black_box;
use std::ops::Range;

const SIZES: [u64; 3] = [100, 1_000, 10_000];

type Policy = HashMap<Author<'static>, Vec<ChangeHash>>;

/// A map workload: how to build an `n`-op document and how to write `range`
/// more ops of the same shape as a post-frontier tail.
#[derive(Clone, Copy)]
struct Pattern {
    name: &'static str,
    build: fn(u64) -> Automerge,
    write: fn(&mut Transaction<'_>, Range<u64>),
}

const PATTERNS: [Pattern; 4] = [
    Pattern {
        name: "repeated_increment",
        build: repeated_increment_doc,
        write: write_increments,
    },
    Pattern {
        name: "repeated_put",
        build: repeated_put_doc,
        write: write_repeated_puts,
    },
    Pattern {
        name: "increasing_put",
        build: increasing_put_doc,
        write: write_increasing_puts,
    },
    Pattern {
        name: "decreasing_put",
        build: decreasing_put_doc,
        write: write_decreasing_puts,
    },
];

pub fn benchmarks() -> Vec<Benchmark> {
    let mut benchmarks = Vec::new();
    for n in SIZES {
        for pattern in PATTERNS {
            add_case(&mut benchmarks, pattern, n);
        }

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

fn add_case(benchmarks: &mut Vec<Benchmark>, pattern: Pattern, n: u64) {
    let Pattern {
        name: operation,
        build,
        ..
    } = pattern;
    benchmarks.extend([
        // A build, so unmasked for the reason given in build.rs.
        SampledBenchmark::no_setup("map", name("map", operation, n), move || {
            build_setup(n, build)
        })
        .into(),
        SampledBenchmark::batched(
            "map",
            name("map/save", operation, n),
            move || masked(build(n)),
            save_doc,
        )
        .into(),
        SampledBenchmark::batched(
            "map",
            name("map/load", operation, n),
            move || {
                let bytes = build(n).save();
                let policy = policy_masked_for_bytes(&bytes);
                (bytes, policy)
            },
            load_doc,
        )
        .into(),
        // A peer that holds the first `n` ops under the frontier receives `n`
        // more from the local user.
        SampledBenchmark::batched(
            "map",
            name("map/apply", operation, n),
            move || receiver_and_tail(pattern, n, masked),
            apply_changes,
        )
        .into(),
    ]);
    if operation == "repeated_increment" {
        benchmarks.extend([
            // The same, with half the authors' increments hidden: counter
            // totals must exclude them.
            SampledBenchmark::batched(
                "map",
                name("map/apply", operation, n).suffixed("_hidden"),
                move || receiver_and_tail(pattern, n, hidden),
                apply_changes,
            )
            .into(),
            // An empty peer that knows the frontier receives the whole
            // source; every boundary head is pending until it arrives.
            SampledBenchmark::batched(
                "map",
                name("map/apply", operation, n).suffixed("_pending"),
                move || {
                    let source = masked(build(n));
                    (pending_receiver(&source), source.get_changes(&[]))
                },
                apply_changes,
            )
            .into(),
        ]);
    }
}

fn name(group: &str, operation: &str, n: u64) -> &'static str {
    Box::leak(format!("{group}/{operation}/{n}").into_boxed_str())
}

trait Suffixed {
    fn suffixed(self, suffix: &str) -> &'static str;
}

impl Suffixed for &'static str {
    fn suffixed(self, suffix: &str) -> &'static str {
        Box::leak(format!("{self}{suffix}").into_boxed_str())
    }
}

fn build_setup(n: u64, builder: fn(u64) -> Automerge) -> Box<dyn FnMut()> {
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

/// Build the `n`-op source under `regime`, fork the receiver from it, then
/// write `n` more ops to the source as the tail the receiver will apply.
fn receiver_and_tail(
    pattern: Pattern,
    n: u64,
    regime: fn(Automerge) -> Automerge,
) -> (Automerge, Vec<Change>) {
    let mut source = regime((pattern.build)(n));
    let receiver = source.fork();
    let changes = tail(&mut source, |tx| (pattern.write)(tx, n..2 * n));
    (receiver, changes)
}

fn apply_changes((mut receiver, changes): (Automerge, Vec<Change>)) -> Automerge {
    receiver.apply_changes(changes).unwrap();
    receiver
}

/// A counter on `ROOT` incremented `n` times across the author blocks. The
/// counter itself is the local user's own change so that it is never hidden.
fn repeated_increment_doc(n: u64) -> Automerge {
    let mut doc = Automerge::new().with_author(Some(local_user()));
    let mut tx = doc.transaction();
    tx.put(ROOT, "counter", ScalarValue::counter(0)).unwrap();
    tx.commit();
    in_author_transactions(&mut doc, n as usize, |tx, range| {
        write_increments(tx, range.start as u64..range.end as u64)
    });
    doc
}

/// `n` puts to the single key `"0"`, so every author block overwrites the last.
fn repeated_put_doc(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    in_author_transactions(&mut doc, n as usize, |tx, range| {
        write_repeated_puts(tx, range.start as u64..range.end as u64)
    });
    doc
}

fn increasing_put_doc(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    in_author_transactions(&mut doc, n as usize, |tx, range| {
        write_increasing_puts(tx, range.start as u64..range.end as u64)
    });
    doc
}

fn decreasing_put_doc(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    in_author_transactions(&mut doc, n as usize, |tx, range| {
        write_decreasing_puts(tx, range.start as u64..range.end as u64)
    });
    doc
}

fn write_increments(tx: &mut Transaction<'_>, range: Range<u64>) {
    for _ in range {
        tx.increment(ROOT, "counter", 1).unwrap();
    }
}

fn write_repeated_puts(tx: &mut Transaction<'_>, range: Range<u64>) {
    for i in range {
        tx.put(ROOT, "0", i).unwrap();
    }
}

fn write_increasing_puts(tx: &mut Transaction<'_>, range: Range<u64>) {
    for i in range {
        tx.put(ROOT, i.to_string(), i).unwrap();
    }
}

fn write_decreasing_puts(tx: &mut Transaction<'_>, range: Range<u64>) {
    for i in range.rev() {
        tx.put(ROOT, i.to_string(), i).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::{apply_changes, benchmarks, receiver_and_tail, PATTERNS};
    use benchmark_battery::automerge::{ReadDoc, ScalarValue, ROOT};
    use benchmark_battery::{hidden, masked};

    fn counter(doc: &benchmark_battery::Automerge) -> i64 {
        match doc.get(ROOT, "counter").unwrap().unwrap().0 {
            benchmark_battery::automerge::Value::Scalar(s) => match s.as_ref() {
                ScalarValue::Counter(c) => c.into(),
                other => panic!("not a counter: {other:?}"),
            },
            other => panic!("not a scalar: {other:?}"),
        }
    }

    /// The rebuilt receiver holds the first `n` ops and applies `n` more.
    #[test]
    fn masked_receiver_applies_the_tail_onto_the_base() {
        let n = 80;
        let (receiver, changes) = receiver_and_tail(PATTERNS[0], n, masked);
        assert_eq!(counter(&receiver), n as i64, "base: all increments visible");
        let receiver = apply_changes((receiver, changes));
        assert_eq!(counter(&receiver), 2 * n as i64, "base + tail");
    }

    /// Under `hidden`, half the authors' base increments are excluded; the
    /// local user's tail is not.
    #[test]
    fn hidden_receiver_excludes_half_the_base_but_not_the_tail() {
        let n = 80;
        let (receiver, changes) = receiver_and_tail(PATTERNS[0], n, hidden);
        assert_eq!(counter(&receiver), n as i64 / 2, "base: half hidden");
        let receiver = apply_changes((receiver, changes));
        assert_eq!(
            counter(&receiver),
            n as i64 / 2 + n as i64,
            "tail fully visible"
        );
    }

    #[test]
    fn map_registers_the_apply_regimes() {
        let names: Vec<&'static str> = benchmarks().into_iter().map(|b| b.name()).collect();
        for name in [
            "map/apply/repeated_increment/1000",
            "map/apply/repeated_increment/1000_hidden",
            "map/apply/repeated_increment/1000_pending",
            "map/apply/repeated_put/1000",
        ] {
            assert!(names.contains(&name), "expected {name} to be registered");
        }
        assert!(
            !names.contains(&"map/apply/repeated_put/1000_hidden"),
            "only the counter pattern gets the extra regimes"
        );
    }
}
