use super::SampledBenchmark;
use benchmark_battery::automerge::Automerge;
use benchmark_battery::rand;
use std::hint::black_box;

pub fn benchmarks() -> Vec<SampledBenchmark> {
    vec![SampledBenchmark::no_setup("diff", "diff/diff", diff)]
}

fn diff() -> Box<dyn FnMut()> {
    let data = std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/data/essay.amrg")).unwrap();
    let doc = Automerge::load(&data).unwrap();
    let history = doc
        .get_changes(&[])
        .unwrap()
        .iter()
        .map(|c| vec![c.id()])
        .collect::<Vec<_>>();
    Box::new(move || {
        let a = rand() % history.len();
        let b = rand() % history.len();
        black_box(doc.diff(&history[a], &history[b]).unwrap());
    })
}
