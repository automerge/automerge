use super::MemoryBenchmark;

#[cfg(feature = "memory")]
use super::MemoryMeasurement;
#[cfg(feature = "memory")]
use benchmark_battery::automerge::Automerge;
#[cfg(feature = "memory")]
use benchmark_battery::{big_paste_doc, poorly_simulated_typing_doc, text_splice_100};
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
}

const BENCHMARKS: [(&str, Workload); 10] = [
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
    let mut benchmarks: Vec<_> = BENCHMARKS
        .into_iter()
        .map(|(name, workload)| {
            #[cfg(feature = "memory")]
            {
                MemoryBenchmark::new("memory", name, move || setup(workload))
            }
            #[cfg(not(feature = "memory"))]
            {
                if let Workload::Fixture(filename) = workload {
                    let _ = filename;
                }
                MemoryBenchmark::unavailable("memory", name)
            }
        })
        .collect();
    for filename in super::egwalker_paper::FILES {
        let label = filename.trim_end_matches(".am");
        for operation in ["load", "clone"] {
            let name =
                Box::leak(format!("memory/{operation}/egwalker_paper/{label}").into_boxed_str());
            #[cfg(feature = "memory")]
            benchmarks.push(MemoryBenchmark::new("memory", name, move || {
                let bytes = super::egwalker_paper::read_file(filename);
                if operation == "load" {
                    Box::new(move || {
                        crate::memory::measure(|| {
                            benchmark_battery::automerge::AutoCommit::load(&bytes).unwrap()
                        })
                    })
                } else {
                    let doc = benchmark_battery::automerge::AutoCommit::load(&bytes).unwrap();
                    Box::new(move || crate::memory::measure(|| doc.clone()))
                }
            }));
            #[cfg(not(feature = "memory"))]
            benchmarks.push(MemoryBenchmark::unavailable("memory", name));
        }
    }
    benchmarks
}

#[cfg(feature = "memory")]
fn setup(workload: Workload) -> Box<dyn FnMut() -> MemoryMeasurement> {
    match workload {
        Workload::Typing => load_data(poorly_simulated_typing_doc(N).save()),
        Workload::BigPaste => load_data(big_paste_doc(N).save()),
        Workload::TextSplice100 => load_data(text_splice_100(N).save()),
        Workload::Fixture(filename) => load_fixture(filename),
    }
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
