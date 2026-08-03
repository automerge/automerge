use super::SampledBenchmark;
use benchmark_battery::automerge::{AutoCommit, LoadOptions};
use std::hint::black_box;

const FILES: [&str; 7] = [
    "S1.am", "S2.am", "S3.am", "A1.am", "A2.am", "C1.am", "C2.am",
];

/// Loading with and without audit mode, from three shapes of the same
/// history: a whole-document save, every fragment concatenated into one
/// file, and the fragments applied one at a time.
///
/// Audit mode rebuilds and verifies every change hash; the default trusts
/// the ones the document carries. The shapes differ in what the loader
/// has to trust — a document chunk carries hash columns, a change set
/// does not.
pub fn benchmarks() -> Vec<SampledBenchmark> {
    let mut benchmarks = Vec::new();
    for filename in FILES {
        let label = filename.trim_end_matches(".am");
        for audit in [false, true] {
            let mode = if audit { "audit" } else { "no_audit" };
            benchmarks.extend([
                SampledBenchmark::no_setup(
                    "audit_mode",
                    Box::leak(format!("audit_mode/load_doc_{mode}/{label}").into_boxed_str()),
                    move || load_all_at_once(full_save(filename), audit),
                ),
                SampledBenchmark::no_setup(
                    "audit_mode",
                    Box::leak(
                        format!("audit_mode/load_change_sets_{mode}/{label}").into_boxed_str(),
                    ),
                    move || load_all_at_once(change_sets(filename).concat(), audit),
                ),
                SampledBenchmark::no_setup(
                    "audit_mode",
                    Box::leak(
                        format!("audit_mode/load_incremental_{mode}/{label}").into_boxed_str(),
                    ),
                    move || load_incrementally(change_sets(filename), audit),
                ),
            ]);
        }
    }
    benchmarks
}

fn load_all_at_once(bytes: Vec<u8>, audit: bool) -> Box<dyn FnMut()> {
    Box::new(move || {
        let doc = AutoCommit::load_with_options(&bytes, options(audit)).unwrap();
        black_box(doc);
    })
}

/// Each fragment fed in on its own, in apply order.
fn load_incrementally(change_sets: Vec<Vec<u8>>, audit: bool) -> Box<dyn FnMut()> {
    Box::new(move || {
        // `load_with_options` on empty bytes drops the audit option, so the
        // starting document has to be switched over explicitly
        let mut doc = AutoCommit::new();
        if audit {
            doc.enable_audit_mode().unwrap();
        }
        for change_set in &change_sets {
            doc.load_incremental(change_set).unwrap();
        }
        black_box(doc);
    })
}

fn options(audit: bool) -> LoadOptions {
    if audit {
        LoadOptions::new().with_audit_mode()
    } else {
        LoadOptions::new()
    }
}

fn full_save(filename: &str) -> Vec<u8> {
    source_doc(filename).save()
}

fn change_sets(filename: &str) -> Vec<Vec<u8>> {
    let doc = source_doc(filename);
    let fragments = doc.fragments(..);
    doc.change_sets_for_fragments(fragments).unwrap()
}

/// The fixture in audit mode: fragments are only enumerable when the
/// hashes naming them are known.
fn source_doc(filename: &str) -> AutoCommit {
    let bytes = std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("egwalker-paper")
            .join(filename),
    )
    .unwrap();
    AutoCommit::load_with_options(&bytes, LoadOptions::new().with_audit_mode()).unwrap()
}
