use super::SampledBenchmark;
use benchmark_battery::automerge::{AutoCommit, ReadDoc, ROOT};
use std::hint::black_box;

const FILES: [&str; 6] = ["S1.am", "S3.am", "A1.am", "A2.am", "C1.am", "C2.am"];

pub fn benchmarks() -> Vec<SampledBenchmark> {
    let mut benchmarks = Vec::new();
    for filename in FILES {
        let label = filename.trim_end_matches(".am");
        benchmarks.extend([
            SampledBenchmark::no_setup(
                "egwalker_paper",
                Box::leak(format!("egwalker_paper/load/{label}").into_boxed_str()),
                move || load(filename),
            ),
            SampledBenchmark::no_setup(
                "egwalker_paper",
                Box::leak(format!("egwalker_paper/get_text/{label}").into_boxed_str()),
                move || get_text(filename),
            ),
        ]);
    }
    benchmarks
}

fn load(filename: &str) -> Box<dyn FnMut()> {
    let bytes = read_file(filename);
    Box::new(move || {
        let doc = AutoCommit::load(&bytes).unwrap();
        black_box(doc);
    })
}

fn get_text(filename: &str) -> Box<dyn FnMut()> {
    let bytes = read_file(filename);
    let doc = AutoCommit::load(&bytes).unwrap();
    Box::new(move || {
        let (_, text_id) = doc.get(ROOT, "text").unwrap().unwrap();
        let result = doc.text(text_id).unwrap();
        black_box(result);
    })
}

fn read_file(filename: &str) -> Vec<u8> {
    std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("egwalker-paper")
            .join(filename),
    )
    .unwrap()
}
