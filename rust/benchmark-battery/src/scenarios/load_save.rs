use super::SampledBenchmark;
use benchmark_battery::automerge::Automerge;
use benchmark_battery::{
    big_paste_doc, big_random_doc, deep_history_doc, maps_in_maps_doc, poorly_simulated_typing_doc,
    text_splice_100,
};
use std::hint::black_box;

const N: u64 = 100_000;

pub fn benchmarks() -> Vec<SampledBenchmark> {
    vec![
        SampledBenchmark::no_setup("load_save", "load_save/load_typing", load_typing),
        SampledBenchmark::no_setup("load_save", "load_save/save_typing", save_typing),
        SampledBenchmark::no_setup("load_save", "load_save/load_big_paste", load_big_paste),
        SampledBenchmark::no_setup("load_save", "load_save/save_big_paste", save_big_paste),
        SampledBenchmark::no_setup(
            "load_save",
            "load_save/load_deep_history/1000",
            load_deep_history,
        ),
        SampledBenchmark::no_setup(
            "load_save",
            "load_save/save_deep_history/1000",
            save_deep_history,
        ),
        SampledBenchmark::no_setup(
            "load_save",
            "load_save/load_text_splice_100",
            load_text_splice_100,
        ),
        SampledBenchmark::no_setup(
            "load_save",
            "load_save/save_text_splice_100",
            save_text_splice_100,
        ),
        SampledBenchmark::no_setup(
            "load_save",
            "load_save/load_maps_in_maps",
            load_maps_in_maps,
        ),
        SampledBenchmark::no_setup(
            "load_save",
            "load_save/save_maps_in_maps",
            save_maps_in_maps,
        ),
        SampledBenchmark::no_setup("load_save", "load_save/load_big_random", load_big_random),
        SampledBenchmark::no_setup("load_save", "load_save/save_big_random", save_big_random),
    ]
}

fn load_typing() -> Box<dyn FnMut()> {
    let data = poorly_simulated_typing_doc(N).save();
    Box::new(move || {
        let doc = Automerge::load(&data).unwrap();
        black_box(doc);
    })
}

fn save_typing() -> Box<dyn FnMut()> {
    let doc = poorly_simulated_typing_doc(N);
    Box::new(move || {
        let data = doc.save();
        black_box(data);
    })
}

fn load_big_paste() -> Box<dyn FnMut()> {
    let data = big_paste_doc(N).save();
    Box::new(move || {
        let doc = Automerge::load(&data).unwrap();
        black_box(doc);
    })
}

fn save_big_paste() -> Box<dyn FnMut()> {
    let doc = big_paste_doc(N);
    Box::new(move || {
        let data = doc.save();
        black_box(data);
    })
}

fn load_text_splice_100() -> Box<dyn FnMut()> {
    let data = text_splice_100(N).save();
    Box::new(move || {
        let doc = Automerge::load(&data).unwrap();
        black_box(doc);
    })
}

fn save_text_splice_100() -> Box<dyn FnMut()> {
    let doc = text_splice_100(N);
    Box::new(move || {
        let data = doc.save();
        black_box(data);
    })
}

fn load_maps_in_maps() -> Box<dyn FnMut()> {
    let data = maps_in_maps_doc(N).save();
    Box::new(move || {
        let doc = Automerge::load(&data).unwrap();
        black_box(doc);
    })
}

fn save_maps_in_maps() -> Box<dyn FnMut()> {
    let doc = maps_in_maps_doc(N);
    Box::new(move || {
        let data = doc.save();
        black_box(data);
    })
}

fn load_big_random() -> Box<dyn FnMut()> {
    let data = big_random_doc(N).save();
    Box::new(move || {
        let doc = Automerge::load(&data).unwrap();
        black_box(doc);
    })
}

fn save_big_random() -> Box<dyn FnMut()> {
    let doc = big_random_doc(N);
    Box::new(move || {
        let data = doc.save();
        black_box(data);
    })
}

fn load_deep_history() -> Box<dyn FnMut()> {
    let data = deep_history_doc(N).save();
    Box::new(move || {
        let doc = Automerge::load(&data).unwrap();
        black_box(doc);
    })
}

fn save_deep_history() -> Box<dyn FnMut()> {
    let doc = deep_history_doc(N);
    Box::new(move || {
        let data = doc.save();
        black_box(data);
    })
}
