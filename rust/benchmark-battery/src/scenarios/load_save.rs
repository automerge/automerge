use super::SampledBenchmark;
use benchmark_battery::automerge::{Author, AutoCommit, Automerge, ChangeHash, LoadOptions};
use benchmark_battery::Transactable;
use benchmark_battery::{
    author, big_paste_doc, big_random_doc, deep_history_doc, maps_in_maps_doc, masked,
    policy_masked_for_bytes, poorly_simulated_typing_doc, text_splice_100, ROOT,
};
use std::collections::HashMap;
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

/// A measured load of the bytes of `doc`, under the frontier the bytes
/// produce. The policy is resolved in setup so that only the load is timed.
fn load_masked_doc(doc: Automerge) -> Box<dyn FnMut()> {
    let data = doc.save();
    let policy = policy_masked_for_bytes(&data);
    Box::new(move || {
        let options = LoadOptions::new().write_frontier(policy.clone());
        black_box(Automerge::load_with_options(&data, options).unwrap());
    })
}

/// A measured save of `doc` under the masked frontier.
fn save_masked_doc(doc: Automerge) -> Box<dyn FnMut()> {
    let doc = masked(doc);
    Box::new(move || {
        black_box(doc.save());
    })
}

fn load_typing() -> Box<dyn FnMut()> {
    load_masked_doc(poorly_simulated_typing_doc(N))
}

fn save_typing() -> Box<dyn FnMut()> {
    save_masked_doc(poorly_simulated_typing_doc(N))
}

fn load_big_paste() -> Box<dyn FnMut()> {
    load_masked_doc(big_paste_doc(N))
}

fn save_big_paste() -> Box<dyn FnMut()> {
    save_masked_doc(big_paste_doc(N))
}

fn load_text_splice_100() -> Box<dyn FnMut()> {
    load_masked_doc(text_splice_100(N))
}

fn save_text_splice_100() -> Box<dyn FnMut()> {
    save_masked_doc(text_splice_100(N))
}

fn load_maps_in_maps() -> Box<dyn FnMut()> {
    load_masked_doc(maps_in_maps_doc(N))
}

fn save_maps_in_maps() -> Box<dyn FnMut()> {
    save_masked_doc(maps_in_maps_doc(N))
}

fn load_big_random() -> Box<dyn FnMut()> {
    load_masked_doc(big_random_doc(N))
}

fn save_big_random() -> Box<dyn FnMut()> {
    save_masked_doc(big_random_doc(N))
}

fn load_deep_history() -> Box<dyn FnMut()> {
    load_masked_doc(deep_history_doc(N))
}

fn save_deep_history() -> Box<dyn FnMut()> {
    save_masked_doc(deep_history_doc(N))
}

fn name(operation: &str, n: usize) -> &'static str {
    Box::leak(format!("{operation}/{n}").into_boxed_str())
}

/// A document with `n` authors, each contributing one change from its own
/// actor.
fn saved_doc_with_actors(n: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    for i in 0..n {
        let mut doc = AutoCommit::new().with_author(Some(author(i)));
        doc.put(ROOT, format!("k{i}"), i as i64).unwrap();
        doc.commit();
        bytes.extend(doc.save());
    }

    let mut doc = AutoCommit::new();
    doc.load_incremental(&bytes).unwrap();
    doc.save()
}

/// Mask the first `n / 2` of the `n` authors in [`saved_doc_with_actors`] at
/// `heads`.
fn policy_masking_half(
    n: usize,
    heads: &[ChangeHash],
) -> HashMap<Author<'static>, Vec<ChangeHash>> {
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
