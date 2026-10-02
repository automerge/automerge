use super::SampledBenchmark;
use benchmark_battery::automerge::{Author, Automerge, ChangeHash, LoadOptions};
use benchmark_battery::Transactable;
use benchmark_battery::{
    big_paste_doc, big_random_doc, deep_history_doc, maps_in_maps_doc, poorly_simulated_typing_doc,
    text_splice_100,
};
use std::hint::black_box;

const N: u64 = 100_000;
const ACTOR_SIZES: [usize; 3] = [10, 100, 1000];

pub fn benchmarks() -> Vec<SampledBenchmark> {
    let mut benchmarks = vec![
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
    ];

    for n in ACTOR_SIZES {
        let bytes = saved_doc_with_actors(n);
        let heads = Automerge::load(&bytes).unwrap().get_heads();
        let policy = policy_masking_half(n, &heads);

        benchmarks.extend([
            SampledBenchmark::no_setup("load_save", name("load_actors/load", n), {
                let bytes = bytes.clone();
                move || {
                    let bytes = bytes.clone();
                    Box::new(move || {
                        std::hint::black_box(Automerge::load(&bytes).unwrap());
                    })
                }
            }),
            SampledBenchmark::no_setup("load_save", name("load_actors/load_masked_half", n), {
                let bytes = bytes.clone();
                let policy = policy.clone();
                move || {
                    let bytes = bytes.clone();
                    let policy = policy.clone();
                    Box::new(move || {
                        std::hint::black_box(
                            Automerge::load_with_options(
                                &bytes,
                                LoadOptions::new().write_frontier(policy.clone()),
                            )
                            .unwrap(),
                        );
                    })
                }
            }),
        ]);
    }

    benchmarks
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

fn name(operation: &str, n: usize) -> &'static str {
    Box::leak(format!("{operation}/{n}").into_boxed_str())
}

fn author(i: usize) -> Author<'static> {
    Author::try_from(format!("{i:08x}")).unwrap()
}

fn saved_doc_with_actors(n: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    for i in 0..n {
        let mut doc = benchmark_battery::automerge::AutoCommit::new().with_author(Some(author(i)));
        doc.put(benchmark_battery::ROOT, format!("k{i}"), i as i64)
            .unwrap();
        doc.commit();
        bytes.extend(doc.save());
    }

    let mut doc = benchmark_battery::automerge::AutoCommit::new();
    doc.load_incremental(&bytes).unwrap();
    doc.save()
}

fn policy_masking_half(
    n: usize,
    heads: &[ChangeHash],
) -> std::collections::HashMap<Author<'static>, Vec<ChangeHash>> {
    (0..n / 2).map(|i| (author(i), heads.to_vec())).collect()
}

#[cfg(test)]
mod tests {
    use super::benchmarks;

    #[test]
    fn load_save_registers_load_actors_benchmarks() {
        let names: Vec<&'static str> = benchmarks().into_iter().map(|b| b.name).collect();
        for name in [
            "load_actors/load/10",
            "load_actors/load/100",
            "load_actors/load/1000",
            "load_actors/load_masked_half/10",
            "load_actors/load_masked_half/100",
            "load_actors/load_masked_half/1000",
        ] {
            assert!(
                names.contains(&name),
                "expected benchmark {name} to be registered"
            );
        }
    }
}
