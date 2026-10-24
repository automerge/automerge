use super::SeriesBenchmark;
use benchmark_battery::automerge::transaction::Transactable;
use benchmark_battery::automerge::{ReadDoc, ROOT};
use benchmark_battery::{masked_autocommit, seeded_text_doc};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

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
    let mut doc = masked_autocommit(seeded_text_doc(INITIAL_CHARS, 1, &mut rng));
    let mut remote = doc.fork();
    let (_, text) = doc.get(ROOT, "content").unwrap().unwrap();

    Box::new(move |_| {
        let pos = (rng.next_u32() as u64 % INITIAL_CHARS) as usize;
        doc.splice_text(&text, pos, 0, ".").unwrap();
        remote.load_incremental(&doc.save_incremental()).unwrap();
        assert_eq!(doc.get_heads(), remote.get_heads());
    })
}
