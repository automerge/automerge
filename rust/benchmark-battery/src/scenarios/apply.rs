use super::SeriesBenchmark;
use benchmark_battery::automerge::transaction::Transactable;
use benchmark_battery::automerge::{AutoCommit, ObjType, ReadDoc, ROOT};
use rand::distr::Alphanumeric;
use rand::rngs::StdRng;
use rand::{Rng, RngExt, SeedableRng};

const INITIAL_CHARS: u64 = 100_000;
const STEPS: usize = 1_000;

pub fn benchmarks() -> Vec<SeriesBenchmark> {
    vec![SeriesBenchmark {
        group: "apply",
        name: "apply/single_char_incremental_load",
        steps: STEPS,
        setup: apply_single,
    }]
}

fn apply_single() -> Box<dyn FnMut(usize)> {
    let mut rng = StdRng::seed_from_u64(1);
    let mut doc = gen_text_doc(INITIAL_CHARS, 1, &mut rng);
    let mut remote = doc.fork();
    let (_, text) = doc.get(ROOT, "content").unwrap().unwrap();

    Box::new(move |_| {
        let pos = (rng.next_u32() as u64 % INITIAL_CHARS) as usize;
        doc.splice_text(&text, pos, 0, ".").unwrap();
        remote.load_incremental(&doc.save_incremental()).unwrap();
        assert_eq!(doc.get_heads(), remote.get_heads());
    })
}

fn gen_text_doc(n: u64, chunk: u64, rng: &mut StdRng) -> AutoCommit {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "content", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, &random_string(chunk, rng))
        .unwrap();
    let mut len = chunk;
    for _ in 0..(n / chunk) {
        let pos = (rng.next_u32() as u64 % len) as usize;
        doc.splice_text(&text, pos, 0, &random_string(chunk, rng))
            .unwrap();
        len += chunk;
    }
    assert_eq!(doc.stats().num_ops, n + 1 + chunk);
    doc
}

fn random_string(n: u64, rng: &mut StdRng) -> String {
    rng.sample_iter(&Alphanumeric)
        .take(n as usize)
        .map(char::from)
        .collect()
}
