use super::MemoryBenchmark;

#[cfg(feature = "memory")]
use super::MemoryMeasurement;
#[cfg(feature = "memory")]
use benchmark_battery::automerge::Automerge;
#[cfg(feature = "memory")]
use benchmark_battery::{
    big_paste_doc, build_hydrate_map, from_hydrate, poorly_simulated_typing_doc, text_splice_100,
};
#[cfg(feature = "memory")]
use std::path::Path;

#[cfg(feature = "memory")]
const N: u64 = 100_000;

#[derive(Clone, Copy)]
enum Workload {
    Typing,
    BigPaste,
    TextSplice100,
    Fixture(&'static str),
    /// `Autocommit::init_root_from_hydrate` applied to a document with the
    /// given number of N records.
    FromHydrate(u64),
}

const BENCHMARKS: [(&str, Workload); 12] = [
    ("memory/from/hydrate_map/1000", Workload::FromHydrate(1_000)),
    (
        "memory/from/hydrate_map/10000",
        Workload::FromHydrate(10_000),
    ),
    ("memory/load/typing", Workload::Typing),
    ("memory/load/big-paste", Workload::BigPaste),
    ("memory/load/text-splice-100", Workload::TextSplice100),
    (
        "memory/load/unsinkable",
        Workload::Fixture("unsinkable.amrg"),
    ),
    (
        "memory/load/moby-dick",
        Workload::Fixture("moby-dick.automerge"),
    ),
    ("memory/load/essay", Workload::Fixture("essay.amrg")),
    ("memory/load/stephen", Workload::Fixture("stephen.amrg")),
    (
        "memory/load/webstraits",
        Workload::Fixture("webstraits.amrg"),
    ),
    (
        "memory/load/monday-meeting-notes",
        Workload::Fixture("monday-meeting-notes.automerge"),
    ),
    (
        "memory/load/pathological",
        Workload::Fixture("pathological.amrg"),
    ),
];

pub fn benchmarks() -> Vec<MemoryBenchmark> {
    BENCHMARKS
        .into_iter()
        .map(|(name, workload)| {
            #[cfg(feature = "memory")]
            {
                MemoryBenchmark::new("memory", name, move || setup(workload))
            }
            #[cfg(not(feature = "memory"))]
            {
                match workload {
                    Workload::Fixture(filename) => {
                        let _ = filename;
                    }
                    Workload::FromHydrate(n) => {
                        let _ = n;
                    }
                    _ => {}
                }
                MemoryBenchmark::unavailable("memory", name)
            }
        })
        .collect()
}

#[cfg(feature = "memory")]
fn setup(workload: Workload) -> Box<dyn FnMut() -> MemoryMeasurement> {
    match workload {
        Workload::Typing => load_data(poorly_simulated_typing_doc(N).save()),
        Workload::BigPaste => load_data(big_paste_doc(N).save()),
        Workload::TextSplice100 => load_data(text_splice_100(N).save()),
        Workload::Fixture(filename) => load_fixture(filename),
        Workload::FromHydrate(n) => from_hydrate_measure(n),
    }
}

#[cfg(feature = "memory")]
fn from_hydrate_measure(n: u64) -> Box<dyn FnMut() -> MemoryMeasurement> {
    // Building the hydrate map is setup and excluded from the measurement; only
    // the `init_root_from_hydrate` expansion is measured.
    let map = build_hydrate_map(n);
    Box::new(move || crate::memory::measure(|| from_hydrate(&map)))
}

#[cfg(feature = "memory")]
fn load_fixture(filename: &str) -> Box<dyn FnMut() -> MemoryMeasurement> {
    let data = std::fs::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("data")
            .join(filename),
    )
    .unwrap();
    load_data(data)
}

#[cfg(feature = "memory")]
fn load_data(data: Vec<u8>) -> Box<dyn FnMut() -> MemoryMeasurement> {
    Box::new(move || crate::memory::measure(|| Automerge::load(&data).unwrap()))
}
