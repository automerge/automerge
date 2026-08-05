use super::SampledBenchmark;
use benchmark_battery::{
    big_paste_doc, deep_history_doc, maps_in_maps_doc, poorly_simulated_typing_doc, text_splice_100,
};
use std::hint::black_box;

const N: u64 = 100_000;

pub fn benchmarks() -> Vec<SampledBenchmark> {
    vec![
        SampledBenchmark::no_setup("build", "build/build_typing", build_typing),
        SampledBenchmark::no_setup("build", "build/build_big_paste", build_big_paste),
        SampledBenchmark::no_setup(
            "build",
            "build/build_text_splice_100",
            build_text_splice_100,
        ),
        SampledBenchmark::no_setup(
            "build",
            "build/build_maps_in_maps_doc",
            build_maps_in_maps_doc,
        ),
        SampledBenchmark::no_setup(
            "build",
            "build/build_deep_history_doc",
            build_deep_history_doc,
        ),
    ]
}

fn build_typing() -> Box<dyn FnMut()> {
    Box::new(|| {
        black_box(poorly_simulated_typing_doc(N));
    })
}

fn build_big_paste() -> Box<dyn FnMut()> {
    Box::new(|| {
        black_box(big_paste_doc(N));
    })
}

fn build_text_splice_100() -> Box<dyn FnMut()> {
    Box::new(|| {
        black_box(text_splice_100(N));
    })
}

fn build_maps_in_maps_doc() -> Box<dyn FnMut()> {
    Box::new(|| {
        black_box(maps_in_maps_doc(N));
    })
}

fn build_deep_history_doc() -> Box<dyn FnMut()> {
    Box::new(|| {
        black_box(deep_history_doc(N));
    })
}
