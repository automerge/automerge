use super::{Benchmark, SampledBenchmark, SeriesBenchmark};
use benchmark_battery::automerge::{
    sync::{self, Message, SyncDoc},
    transaction::Transactable,
    Author, Automerge, ChangeHash, ReadDoc, ScalarValue, ROOT,
};
use benchmark_battery::{
    in_author_blocks, in_author_transactions, list_splice_100, masked, policy_masked_for, rand,
    text_splice_100,
};
use std::collections::HashMap;

const FULL_SYNC_SIZE: u64 = 10_000;
const TINY_SYNC_INITIAL_SIZE: u64 = 100_000;
const TINY_SYNC_STEPS: usize = 1_000;

type Policy = HashMap<Author<'static>, Vec<ChangeHash>>;

pub fn benchmarks() -> Vec<Benchmark> {
    vec![
        SampledBenchmark::no_setup("sync", "sync/full_many_tx", full_many_tx).into(),
        SampledBenchmark::batched(
            "sync",
            "sync/full_one_tx/100",
            || peers(one_tx_increasing_put(100)),
            run_full_sync,
        )
        .into(),
        SampledBenchmark::batched(
            "sync",
            "sync/full_one_tx/1000",
            || peers(one_tx_increasing_put(1_000)),
            run_full_sync,
        )
        .into(),
        SampledBenchmark::batched(
            "sync",
            "sync/full_one_tx",
            || peers(one_tx_increasing_put(10_000)),
            run_full_sync,
        )
        .into(),
        SampledBenchmark::no_setup("sync", "sync/every_change/100", every_change_100).into(),
        SampledBenchmark::no_setup("sync", "sync/every_change/1000", every_change_1000).into(),
        SampledBenchmark::no_setup("sync", "sync/every_change/10000", every_change_10000).into(),
        SeriesBenchmark {
            group: "sync",
            name: "sync/tiny_text",
            steps: TINY_SYNC_STEPS,
            setup: tiny_text_sync,
        }
        .into(),
        SeriesBenchmark {
            group: "sync",
            name: "sync/tiny_list",
            steps: TINY_SYNC_STEPS,
            setup: tiny_list_sync,
        }
        .into(),
        SampledBenchmark::no_setup(
            "sync",
            "sync/big_chunky_sync_message",
            big_chunky_sync_message,
        )
        .into(),
    ]
}

#[derive(Clone, Default)]
struct DocWithSync {
    doc: Automerge,
    peer_state: sync::State,
}

impl DocWithSync {
    fn sync(&mut self, other: &mut DocWithSync) {
        while let Some(message1) = self.doc.generate_sync_message(&mut self.peer_state) {
            other
                .doc
                .receive_sync_message(&mut other.peer_state, message1)
                .unwrap();
            if let Some(message2) = other.doc.generate_sync_message(&mut other.peer_state) {
                self.doc
                    .receive_sync_message(&mut self.peer_state, message2)
                    .unwrap()
            }
        }
    }

    /// An empty peer that already carries the frontier pinned at this
    /// document's heads, as a receiver that knows about the revocations would.
    fn empty_peer(&self) -> DocWithSync {
        Automerge::new()
            .with_write_frontier(policy_masked_for(&self.doc.get_heads()))
            .into()
    }
}

impl From<Automerge> for DocWithSync {
    fn from(doc: Automerge) -> Self {
        Self {
            doc,
            peer_state: sync::State::default(),
        }
    }
}

/// A masked origin and an empty peer carrying the same frontier.
fn peers(origin: Automerge) -> (DocWithSync, DocWithSync) {
    let origin: DocWithSync = masked(origin).into();
    let peer = origin.empty_peer();
    (origin, peer)
}

fn full_many_tx() -> Box<dyn FnMut()> {
    let (doc, peer) = peers(many_tx_increasing_put(FULL_SYNC_SIZE));
    Box::new(move || {
        let mut doc1 = doc.clone();
        let mut doc2 = peer.clone();
        doc1.sync(&mut doc2);
    })
}

fn run_full_sync((mut doc1, mut doc2): (DocWithSync, DocWithSync)) -> (DocWithSync, DocWithSync) {
    doc1.sync(&mut doc2);
    (doc1, doc2)
}

// Deliberately unmasked: both peers start empty and the origin is built
// inside the measured operation, so there are no heads to pin a frontier at.
// See build.rs.
fn every_change(n: u64) -> Box<dyn FnMut()> {
    Box::new(move || {
        let mut doc1 = DocWithSync::default();
        let mut doc2 = DocWithSync::default();
        for i in 0..n {
            let mut tx = doc1.doc.transaction();
            tx.put(ROOT, i.to_string(), i).unwrap();
            tx.commit();
            doc1.sync(&mut doc2);
        }
    })
}

fn every_change_100() -> Box<dyn FnMut()> {
    every_change(100)
}

fn every_change_1000() -> Box<dyn FnMut()> {
    every_change(1_000)
}

fn every_change_10000() -> Box<dyn FnMut()> {
    every_change(10_000)
}

fn tiny_text_sync() -> Box<dyn FnMut(usize)> {
    let (mut doc1, mut doc2) = peers(text_splice_100(TINY_SYNC_INITIAL_SIZE));
    let len = TINY_SYNC_INITIAL_SIZE as usize;
    doc1.sync(&mut doc2);
    let (_, text) = doc1.doc.get(ROOT, "content").unwrap().unwrap();
    Box::new(move |_| {
        let mut tx = doc1.doc.transaction();
        let pos = rand() % len;
        tx.splice_text(&text, pos, 1, "_").unwrap();
        tx.commit();
        doc1.sync(&mut doc2);
    })
}

fn tiny_list_sync() -> Box<dyn FnMut(usize)> {
    let (mut doc1, mut doc2) = peers(list_splice_100(TINY_SYNC_INITIAL_SIZE));
    let len = TINY_SYNC_INITIAL_SIZE as usize;
    doc1.sync(&mut doc2);
    let (_, list) = doc1.doc.get(ROOT, "content").unwrap().unwrap();
    Box::new(move |_| {
        let mut tx = doc1.doc.transaction();
        let pos = rand() % len;
        tx.splice(&list, pos, 0, vec![ScalarValue::from("_")])
            .unwrap();
        tx.commit();
        doc1.sync(&mut doc2);
    })
}

fn big_chunky_sync_message() -> Box<dyn FnMut()> {
    let data = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/data/slowSyncMessage.amrgsync"
    ))
    .unwrap();
    // The receiver's frontier is pinned at the heads the message produces,
    // which are only known after receiving it once.
    let policy: Policy = {
        let mut peer_state = sync::State::default();
        let mut doc = Automerge::new();
        let message = Message::decode(&data).unwrap();
        doc.receive_sync_message(&mut peer_state, message).unwrap();
        policy_masked_for(&doc.get_heads())
    };
    Box::new(move || {
        let mut peer_state = sync::State::default();
        let mut doc = Automerge::new().with_write_frontier(policy.clone());
        let message = Message::decode(&data).unwrap();
        doc.receive_sync_message(&mut peer_state, message).unwrap();
    })
}

fn one_tx_increasing_put(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    in_author_transactions(&mut doc, n as usize, |tx, range| {
        for i in range {
            tx.put(ROOT, i.to_string(), i as u64).unwrap();
        }
    });
    doc
}

fn many_tx_increasing_put(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    in_author_blocks(&mut doc, n as usize, |doc, range| {
        for i in range {
            let mut tx = doc.transaction();
            tx.put(ROOT, i.to_string(), i as u64).unwrap();
            tx.commit();
        }
    });
    doc
}
