use super::SeriesBenchmark;
use benchmark_battery::automerge::sync;
use benchmark_battery::automerge::sync::SyncDoc;
use benchmark_battery::automerge::transaction::Transactable;
use benchmark_battery::automerge::{AutoCommit, ReadDoc, ROOT};
use benchmark_battery::{masked_autocommit, seeded_text_doc};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const INITIAL_CHARS: u64 = 100_000;
const STEPS: usize = 1_000;
const BULK_STEPS: usize = 100;
const BULK_CHANGES: usize = 100;

pub fn benchmarks() -> Vec<SeriesBenchmark> {
    vec![
        SeriesBenchmark {
            group: "typing",
            name: "typing/single_char_sync",
            steps: STEPS,
            setup: single_char_sync,
        },
        SeriesBenchmark {
            group: "typing",
            name: "typing/single_char_apply_change",
            steps: STEPS,
            setup: single_char_apply_change,
        },
        SeriesBenchmark {
            group: "typing",
            name: "typing/single_char_100_bulk_incremental_load",
            steps: BULK_STEPS,
            setup: single_char_100_bulk_incremental_load,
        },
        SeriesBenchmark {
            group: "typing",
            name: "typing/single_char_100_incremental_loads",
            steps: BULK_STEPS,
            setup: single_char_100_incremental_loads,
        },
        SeriesBenchmark {
            group: "typing",
            name: "typing/single_char_merge",
            steps: STEPS,
            setup: single_char_merge,
        },
    ]
}

fn single_char_sync() -> Box<dyn FnMut(usize)> {
    let mut rng = StdRng::seed_from_u64(2);
    let mut doc = masked_autocommit(seeded_text_doc(INITIAL_CHARS, 1, &mut rng));
    let mut remote = doc.fork();
    let mut doc_state = sync::State::new();
    let mut remote_state = sync::State::new();
    let (_, text) = doc.get(ROOT, "content").unwrap().unwrap();

    sync_docs(&mut doc, &mut doc_state, &mut remote, &mut remote_state);

    Box::new(move |_| {
        let pos = (rng.next_u32() as u64 % INITIAL_CHARS) as usize;
        doc.splice_text(&text, pos, 0, ".").unwrap();
        doc.commit();
        sync_docs(&mut doc, &mut doc_state, &mut remote, &mut remote_state);
        assert_eq!(doc.get_heads(), remote.get_heads());
    })
}

fn single_char_apply_change() -> Box<dyn FnMut(usize)> {
    let mut rng = StdRng::seed_from_u64(3);
    let mut doc = masked_autocommit(seeded_text_doc(INITIAL_CHARS, 1, &mut rng));
    let mut remote = doc.fork();
    let (_, text) = doc.get(ROOT, "content").unwrap().unwrap();

    Box::new(move |_| {
        let pos = (rng.next_u32() as u64 % INITIAL_CHARS) as usize;
        doc.splice_text(&text, pos, 0, ".").unwrap();
        let change = doc.get_last_local_change().unwrap();
        remote.apply_changes(vec![change.clone()]).unwrap();
        assert_eq!(doc.get_heads(), remote.get_heads());
    })
}

fn single_char_100_bulk_incremental_load() -> Box<dyn FnMut(usize)> {
    let mut rng = StdRng::seed_from_u64(4);
    let mut doc = masked_autocommit(seeded_text_doc(INITIAL_CHARS, 1, &mut rng));
    let mut remote = doc.fork();
    let (_, text) = doc.get(ROOT, "content").unwrap().unwrap();

    Box::new(move |_| {
        for _ in 0..BULK_CHANGES {
            let pos = (rng.next_u32() as u64 % INITIAL_CHARS) as usize;
            doc.splice_text(&text, pos, 0, ".").unwrap();
        }
        remote.load_incremental(&doc.save_incremental()).unwrap();
        assert_eq!(doc.get_heads(), remote.get_heads());
    })
}

fn single_char_100_incremental_loads() -> Box<dyn FnMut(usize)> {
    let mut rng = StdRng::seed_from_u64(5);
    let mut doc = masked_autocommit(seeded_text_doc(INITIAL_CHARS, 1, &mut rng));
    let mut remote = doc.fork();
    let (_, text) = doc.get(ROOT, "content").unwrap().unwrap();

    Box::new(move |_| {
        for _ in 0..BULK_CHANGES {
            let pos = (rng.next_u32() as u64 % INITIAL_CHARS) as usize;
            doc.splice_text(&text, pos, 0, ".").unwrap();
            remote.load_incremental(&doc.save_incremental()).unwrap();
        }
        assert_eq!(doc.get_heads(), remote.get_heads());
    })
}

fn single_char_merge() -> Box<dyn FnMut(usize)> {
    let mut rng = StdRng::seed_from_u64(6);
    let mut doc = masked_autocommit(seeded_text_doc(INITIAL_CHARS, 1, &mut rng));
    let mut remote = doc.fork();
    let (_, text) = doc.get(ROOT, "content").unwrap().unwrap();

    Box::new(move |_| {
        let pos = (rng.next_u32() as u64 % INITIAL_CHARS) as usize;
        doc.splice_text(&text, pos, 0, ".").unwrap();
        remote.merge(&mut doc).unwrap();
        assert_eq!(doc.get_heads(), remote.get_heads());
    })
}

fn sync_docs(d1: &mut AutoCommit, s1: &mut sync::State, d2: &mut AutoCommit, s2: &mut sync::State) {
    while d1.get_heads() != d2.get_heads() {
        if let Some(msg) = d1.sync().generate_sync_message(s1) {
            d2.sync().receive_sync_message(s2, msg).unwrap();
        }
        if let Some(msg) = d2.sync().generate_sync_message(s2) {
            d1.sync().receive_sync_message(s1, msg).unwrap();
        }
    }
}
