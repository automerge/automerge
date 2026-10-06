use super::{Benchmark, SampledBenchmark, SeriesBenchmark};
use benchmark_battery::automerge::{
    sync::{self, Message, SyncDoc},
    transaction::Transactable,
    Author, Automerge, ChangeHash, ReadDoc, ScalarValue, ROOT,
};
use benchmark_battery::{
    hidden, in_author_blocks, in_author_transactions, list_splice_100, masked, pending_receiver,
    policy_masked_for, rand, tail, text_splice_100,
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
            || peers_with_tail(one_tx_increasing_put(100), 100),
            run_full_sync,
        )
        .into(),
        SampledBenchmark::batched(
            "sync",
            "sync/full_one_tx/1000",
            || peers_with_tail(one_tx_increasing_put(1_000), 1_000),
            run_full_sync,
        )
        .into(),
        SampledBenchmark::batched(
            "sync",
            "sync/full_one_tx",
            || peers_with_tail(one_tx_increasing_put(10_000), 10_000),
            run_full_sync,
        )
        .into(),
        // The same, with half the authors' puts hidden on both peers.
        SampledBenchmark::batched(
            "sync",
            "sync/full_one_tx_hidden",
            || peers_with_tail_under(one_tx_increasing_put(10_000), 10_000, hidden),
            run_full_sync,
        )
        .into(),
        // An empty peer that knows the frontier receives the whole origin;
        // every boundary head is pending until it arrives.
        SampledBenchmark::batched(
            "sync",
            "sync/full_one_tx_pending",
            || {
                let origin = masked(one_tx_increasing_put(10_000));
                let peer = pending_receiver(&origin).into();
                (origin.into(), peer)
            },
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
}

impl From<Automerge> for DocWithSync {
    fn from(doc: Automerge) -> Self {
        Self {
            doc,
            peer_state: sync::State::default(),
        }
    }
}

/// Two peers that both hold an `n`-key `origin` under the frontier, after
/// which the origin writes `n` more keys for the sync to carry across. This
/// is a peer that knows the frontier receiving new work.
fn peers_with_tail(origin: Automerge, n: u64) -> (DocWithSync, DocWithSync) {
    peers_with_tail_under(origin, n, masked)
}

fn peers_with_tail_under(
    origin: Automerge,
    n: u64,
    regime: fn(Automerge) -> Automerge,
) -> (DocWithSync, DocWithSync) {
    let mut origin = regime(origin);
    let peer = origin.fork();
    // Keys `0..n` exist (some possibly hidden); the tail is `n..2n`.
    tail(&mut origin, |tx| {
        for i in n..2 * n {
            tx.put(ROOT, i.to_string(), i).unwrap();
        }
    });
    (origin.into(), peer.into())
}

/// Two peers that both hold `origin` under the frontier. The series
/// benchmarks write their own tail, one edit per step.
fn forked_peers(origin: Automerge) -> (DocWithSync, DocWithSync) {
    let origin = masked(origin);
    let peer = origin.fork();
    (origin.into(), peer.into())
}

fn full_many_tx() -> Box<dyn FnMut()> {
    let (doc, peer) = peers_with_tail(many_tx_increasing_put(FULL_SYNC_SIZE), FULL_SYNC_SIZE);
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
    let (mut doc1, mut doc2) = forked_peers(text_splice_100(TINY_SYNC_INITIAL_SIZE));
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
    let (mut doc1, mut doc2) = forked_peers(list_splice_100(TINY_SYNC_INITIAL_SIZE));
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

#[cfg(test)]
mod tests {
    use super::{benchmarks, one_tx_increasing_put, peers_with_tail, peers_with_tail_under};
    use benchmark_battery::automerge::{ReadDoc, ROOT};
    use benchmark_battery::hidden;

    #[test]
    fn sync_registers_the_full_one_tx_regimes() {
        let names: Vec<&'static str> = benchmarks().into_iter().map(|b| b.name()).collect();
        for name in [
            "sync/full_one_tx",
            "sync/full_one_tx_hidden",
            "sync/full_one_tx_pending",
        ] {
            assert!(names.contains(&name), "expected {name} to be registered");
        }
    }

    /// Both peers start from the same base; the sync carries only the tail.
    #[test]
    fn peers_share_the_base_and_sync_carries_the_tail() {
        let (mut origin, mut peer) = peers_with_tail(one_tx_increasing_put(80), 80);
        assert_eq!(peer.doc.length(ROOT), 80, "peer holds the base");
        assert_eq!(origin.doc.length(ROOT), 160, "origin holds base + tail");
        assert_eq!(
            peer.doc.get_write_frontier(),
            origin.doc.get_write_frontier()
        );

        origin.sync(&mut peer);

        assert_eq!(peer.doc.length(ROOT), 160);
        assert_eq!(peer.doc.get_heads(), origin.doc.get_heads());
    }

    /// Under `hidden`, half the base keys are invisible on both sides; the
    /// tail is the local user's and stays visible.
    #[test]
    fn hidden_peers_hide_half_the_base_but_not_the_tail() {
        let (mut origin, mut peer) = peers_with_tail_under(one_tx_increasing_put(80), 80, hidden);
        assert_eq!(peer.doc.length(ROOT), 40);

        origin.sync(&mut peer);

        assert_eq!(peer.doc.length(ROOT), 40 + 80);
        assert_eq!(peer.doc.length(ROOT), origin.doc.length(ROOT));
    }
}
