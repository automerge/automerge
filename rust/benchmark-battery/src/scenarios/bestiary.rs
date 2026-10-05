use super::SampledBenchmark;
use benchmark_battery::automerge::{Automerge, LoadOptions, ReadDoc};
use benchmark_battery::{load_masked, policy_masked_for_bytes};
use std::hint::black_box;

const FILES: [(&str, &str); 2] = [
    (
        "godot-modeled",
        concat!(env!("CARGO_MANIFEST_DIR"), "/data/godot-modeled.amrg"),
    ),
    (
        "godot-thread",
        concat!(env!("CARGO_MANIFEST_DIR"), "/data/godot-thread.amrg"),
    ),
];

pub fn benchmarks() -> Vec<SampledBenchmark> {
    let mut benchmarks = Vec::new();
    for (label, filename) in FILES {
        benchmarks.extend([
            SampledBenchmark::no_setup(
                "bestiary",
                Box::leak(format!("bestiary/load/{label}").into_boxed_str()),
                move || load(filename),
            ),
            SampledBenchmark::no_setup(
                "bestiary",
                Box::leak(format!("bestiary/reload/{label}").into_boxed_str()),
                move || reload(filename),
            ),
            SampledBenchmark::no_setup(
                "bestiary",
                Box::leak(format!("bestiary/fork/{label}").into_boxed_str()),
                move || fork(filename),
            ),
            SampledBenchmark::no_setup(
                "bestiary",
                Box::leak(format!("bestiary/save/{label}").into_boxed_str()),
                move || save(filename),
            ),
        ]);
        benchmarks.push(SampledBenchmark::no_setup(
            "bestiary",
            Box::leak(format!("bestiary/iter/{label}").into_boxed_str()),
            move || iter(filename),
        ));
    }
    benchmarks
}
fn load(filename: &str) -> Box<dyn FnMut()> {
    let data = std::fs::read(filename).unwrap();
    let policy = policy_masked_for_bytes(&data);
    Box::new(move || {
        let options = LoadOptions::new().write_frontier(policy.clone());
        black_box(Automerge::load_with_options(data.as_slice(), options).unwrap());
    })
}

fn reload(filename: &str) -> Box<dyn FnMut()> {
    let data = std::fs::read(filename).unwrap();
    let saved = load_masked(&data).save();
    let policy = policy_masked_for_bytes(&saved);
    Box::new(move || {
        let options = LoadOptions::new().write_frontier(policy.clone());
        black_box(Automerge::load_with_options(saved.as_slice(), options).unwrap());
    })
}

fn fork(filename: &str) -> Box<dyn FnMut()> {
    let data = std::fs::read(filename).unwrap();
    let doc = load_masked(&data);
    Box::new(move || {
        let fork = doc.fork();
        black_box(fork.save());
    })
}

fn save(filename: &str) -> Box<dyn FnMut()> {
    let data = std::fs::read(filename).unwrap();
    let doc = load_masked(&data);
    Box::new(move || {
        black_box(doc.save());
    })
}

fn iter(filename: &str) -> Box<dyn FnMut()> {
    let data = std::fs::read(filename).unwrap();
    let doc = load_masked(&data);
    Box::new(move || {
        let items = doc.iter().collect::<Vec<_>>();
        black_box(items);
    })
}
