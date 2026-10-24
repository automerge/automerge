use super::SampledBenchmark;
use benchmark_battery::automerge::{
    Author, AutoCommit, Automerge, ChangeHash, LoadOptions, PatchLog,
};
use benchmark_battery::Transactable;
use benchmark_battery::{
    author, authors, big_paste_doc, big_random_doc, deep_history_doc, maps_in_maps_doc, masked,
    policy_hiding_half, policy_masked_for_bytes, poorly_simulated_typing_doc, text_splice_100,
    N_AUTHORS, ROOT,
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
        // A load where the frontier hides half the authors' ops: the indexes
        // built during load must be rebuilt under the mask.
        SampledBenchmark::no_setup(
            "load_save",
            "load_save/load_text_splice_100_hidden",
            load_text_splice_100_hidden,
        ),
        // Changing the frontier on a loaded document rebuilds its indexes.
        SampledBenchmark::batched(
            "load_save",
            "load_save/mask_author/text_splice_100",
            || masked(text_splice_100(N)),
            mask_last_author,
        ),
        SampledBenchmark::batched(
            "load_save",
            "load_save/reveal_author/text_splice_100",
            || hidden_doc(text_splice_100(N)),
            reveal_last_author,
        ),
        SampledBenchmark::batched(
            "load_save",
            "load_save/mask_author/deep_history/1000",
            || masked(deep_history_doc(1_000)),
            mask_last_author,
        ),
        SampledBenchmark::batched(
            "load_save",
            "load_save/reveal_author/deep_history/1000",
            || hidden_doc(deep_history_doc(1_000)),
            reveal_last_author,
        ),
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

/// A measured load of the bytes of `doc` under `policy_hiding_half`, so that
/// the load has to rebuild its indexes under the mask.
fn load_hidden_doc(doc: Automerge) -> Box<dyn FnMut()> {
    let data = doc.save();
    Box::new(move || {
        let options = LoadOptions::new().write_frontier(policy_hiding_half());
        black_box(Automerge::load_with_options(&data, options).unwrap());
    })
}

fn hidden_doc(doc: Automerge) -> Automerge {
    benchmark_battery::hidden(doc)
}

/// Hide the last author entirely on a `masked` document. The mask changes,
/// so the indexes are rebuilt.
fn mask_last_author(mut doc: Automerge) -> Automerge {
    doc.mask_author(author(N_AUTHORS - 1), &[], &mut PatchLog::inactive())
        .unwrap();
    doc
}

/// Reveal the last author on a `hidden` document. The mask changes, so the
/// indexes are rebuilt.
fn reveal_last_author(mut doc: Automerge) -> Automerge {
    let last = authors().pop().unwrap();
    doc.reveal_author(&last, &mut PatchLog::inactive()).unwrap();
    doc
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

fn load_text_splice_100_hidden() -> Box<dyn FnMut()> {
    load_hidden_doc(text_splice_100(N))
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
    use super::{benchmarks, hidden_doc, mask_last_author, reveal_last_author};
    use benchmark_battery::automerge::{ReadDoc, ROOT};
    use benchmark_battery::{masked, text_splice_100, N_AUTHORS};

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
            "load_save/load_text_splice_100_hidden",
            "load_save/mask_author/text_splice_100",
            "load_save/reveal_author/text_splice_100",
            "load_save/mask_author/deep_history/1000",
            "load_save/reveal_author/deep_history/1000",
        ] {
            assert!(
                names.contains(&name),
                "expected benchmark {name} to be registered"
            );
        }
    }

    fn content_len(doc: &benchmark_battery::Automerge) -> usize {
        let (_, text) = doc.get(ROOT, "content").unwrap().unwrap();
        doc.length(&text)
    }

    /// `mask_author` on a masked doc hides one of `N_AUTHORS` authors' blocks;
    /// `reveal_author` on a hidden doc restores one of the hidden half.
    #[test]
    fn mask_and_reveal_move_the_visible_content_by_one_author() {
        let n = 800;
        let per_author = n / N_AUTHORS;

        let doc = mask_last_author(masked(text_splice_100(n as u64)));
        assert_eq!(content_len(&doc), n - per_author);

        let doc = reveal_last_author(hidden_doc(text_splice_100(n as u64)));
        assert_eq!(content_len(&doc), n / 2 + per_author);
    }
}
