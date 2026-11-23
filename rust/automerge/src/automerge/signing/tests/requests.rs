//! Local draft provenance, signing request planning and publication status.

use std::collections::BTreeSet;

use crate::{AutoCommit, Signature, SignatureBatchOutcome, SigningRequests};

use super::*;

mod properties;

fn requested(doc: &mut Automerge) -> Vec<ChangeHash> {
    doc.signing_requests()
        .unwrap()
        .iter()
        .map(|request| request.hash())
        .collect()
}

fn sign_requests(doc: &mut Automerge, requests: &SigningRequests, key: &SigningKey) {
    let inputs: Vec<_> = requests
        .iter()
        .map(|request| {
            assert_eq!(request.message(), signing_message(&request.hash()));
            let signature = ed25519::Signature::from_bytes(key.sign(request.message()).to_bytes());
            (request.hash(), signature)
        })
        .collect();
    assert!(matches!(
        doc.insert_signatures(inputs),
        SignatureBatchOutcome::Completed { .. }
    ));
}

/// Push `change` straight into applied history without proof: a deliberate
/// ingress violation that must not grant local provenance.
fn inject_applied_change(doc: &mut Automerge, change: &Change, actor: ActorId) {
    doc.put_actor(actor);
    let _ = crate::automerge::history::History::new(
        &mut doc.change_graph,
        &mut doc.deps,
        &mut doc.authors,
        &doc.ops.actors,
        &doc.visibility,
        &mut doc.signing,
    )
    .update(change);
}

#[test]
fn unsigned_documents_never_report_unpublished_changes() {
    let mut doc = Automerge::new().with_actor(ActorId::from([1])).unwrap();
    append_at_level(&mut doc, None, 1, 0);
    assert!(!doc.has_unpublished_changes());
}

#[test]
fn status_is_true_from_first_local_commit_until_full_publication() {
    let k = key(7);
    let mut doc = signing_doc(&k, 1);
    assert!(!doc.has_unpublished_changes(), "empty document");
    let a = append_at_level(&mut doc, None, 1, 0);
    assert!(doc.has_unpublished_changes());
    doc.insert_signatures([(a, sig(&k, &a))]);
    assert!(!doc.has_unpublished_changes(), "fully published");
}

#[test]
fn pending_incoming_groups_do_not_count_as_unpublished_changes() {
    let k = key(7);
    let k2 = key(2);
    let mut doc = signing_doc(&k, 1);
    let a = append_at_level(&mut doc, None, 1, 0);
    doc.insert_signatures([(a, sig(&k, &a))]);
    assert!(!doc.has_unpublished_changes());

    // `c` arrives signed but depends on an unseen `b`, so it stays pending.
    let mut remote = replica(&doc, author(&k2), ActorId::from([2]));
    let b = append_at_level(&mut remote, Some(&[a]), 2, 0);
    let c = append_at_level(&mut remote, Some(&[b]), 3, 0);
    let c_bytes = attach(
        remote.get_change_by_hash(&c).unwrap().raw_bytes(),
        [(c, Signature::ed25519(sig(&k2, &c)))],
    );
    assert_eq!(doc.load_incremental(&c_bytes).unwrap(), 0);
    assert!(doc.signing.has_pending_groups());
    assert!(
        !doc.has_unpublished_changes(),
        "pending groups are not applied"
    );
}

#[test]
fn an_injected_non_local_change_is_neither_a_draft_nor_requested() {
    let k = key(7);
    let k2 = key(2);
    let mut doc = signing_doc(&k, 1);
    let a = append_at_level(&mut doc, None, 1, 0);
    doc.insert_signatures([(a, sig(&k, &a))]);

    let mut remote = replica(&doc, author(&k2), ActorId::from([2]));
    let b = append_at_level(&mut remote, Some(&[a]), 2, 0);
    let b_change = remote.get_change_by_hash(&b).unwrap();
    inject_applied_change(&mut doc, &b_change, ActorId::from([2]));

    assert!(drafts(&mut doc).is_empty());
    assert!(
        !doc.has_unpublished_changes(),
        "status trusts the local-draft ingress invariant"
    );
    assert!(
        requested(&mut doc).is_empty(),
        "imports are never requested"
    );
}

#[test]
fn unsigned_autocommit_status_is_false_with_pending_ops() {
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "v", 1).unwrap();
    assert!(!doc.has_unpublished_changes());
    assert_eq!(doc.pending_ops(), 1);
}

#[test]
fn autocommit_status_does_not_close_the_open_transaction() {
    let k = key(7);
    let mut doc = AutoCommit::from_config(crate::Config::default().signing(author(&k)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    doc.put(ROOT, "v", 1).unwrap();
    assert!(!doc.has_unpublished_changes());
    assert_eq!(
        doc.pending_ops(),
        1,
        "status does not close the transaction"
    );
    let a = doc.commit().unwrap();
    assert!(doc.has_unpublished_changes());
    doc.insert_signatures([(a, sig(&k, &a))]);
    assert!(!doc.has_unpublished_changes());
}

#[test]
fn unsigned_autocommit_signing_requests_is_an_error_and_keeps_pending_ops() {
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "v", 1).unwrap();
    assert!(doc.signing_requests().is_err());
    assert_eq!(doc.pending_ops(), 1);
}

#[test]
fn autocommit_signing_requests_commits_the_open_transaction_first() {
    let k = key(7);
    let mut doc = AutoCommit::from_config(crate::Config::default().signing(author(&k)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    doc.put(ROOT, "v", 1).unwrap();
    let requests = doc.signing_requests().unwrap();
    assert_eq!(doc.pending_ops(), 0);
    assert_eq!(requests.len(), 1);
    assert_eq!(requests.iter().next().unwrap().hash(), doc.get_heads()[0]);
}

#[test]
fn local_commits_add_one_draft_each() {
    let k = key(7);
    let mut doc = signing_doc(&k, 1);
    let a = append_at_level(&mut doc, None, 1, 0);
    let b = append_at_level(&mut doc, None, 2, 0);
    assert_eq!(drafts(&mut doc), [a, b]);
    assert_eq!(requested(&mut doc), [a, b]);
}

#[test]
fn signature_without_publication_keeps_the_draft_and_drops_the_request() {
    let k = key(7);
    let mut doc = signing_doc(&k, 1);
    let a = append_at_level(&mut doc, None, 1, 0);
    let b = append_at_level(&mut doc, None, 2, 0);

    // `b` cannot publish while the earlier `a` is unsigned.
    doc.insert_signatures([(b, sig(&k, &b))]);
    assert!(!is_published(&mut doc, &b));
    assert_eq!(drafts(&mut doc), [a, b]);
    assert_eq!(requested(&mut doc), [a]);
    assert!(doc.has_unpublished_changes());
}

#[test]
fn request_snapshots_are_owned_and_unaffected_by_later_commits() {
    let k = key(7);
    let mut doc = signing_doc(&k, 1);
    let a = append_at_level(&mut doc, None, 1, 0);
    let b = append_at_level(&mut doc, None, 2, 0);
    let snapshot = doc.signing_requests().unwrap();

    let c = append_at_level(&mut doc, None, 3, 0);
    assert_eq!(snapshot.len(), 2);
    for request in &snapshot {
        assert!(request.hash() == a || request.hash() == b);
    }
    assert_eq!(drafts(&mut doc), [a, b, c]);
}

#[test]
fn delayed_and_duplicate_signatures_publish_and_retire_their_drafts() {
    let k = key(7);
    let mut doc = signing_doc(&k, 1);
    let a = append_at_level(&mut doc, None, 1, 0);
    let b = append_at_level(&mut doc, None, 2, 0);
    doc.insert_signatures([(b, sig(&k, &b))]);
    let c = append_at_level(&mut doc, None, 3, 0);

    // A delayed response for the historical request on `a` still publishes,
    // and the duplicate of `b` is reported without harm.
    let outcome = doc.insert_signatures([(a, sig(&k, &a)), (b, sig(&k, &b))]);
    assert!(matches!(outcome, SignatureBatchOutcome::Completed { .. }));
    assert!(is_published(&mut doc, &a));
    assert!(is_published(&mut doc, &b));
    assert_eq!(drafts(&mut doc), [c]);
    assert_eq!(requested(&mut doc), [c]);
}

#[test]
fn cloned_documents_track_drafts_independently() {
    let k = key(7);
    let mut doc = signing_doc(&k, 1);
    let a = append_at_level(&mut doc, None, 1, 0);

    let mut cloned = doc.clone();
    let cloned_requests = cloned.signing_requests().unwrap();
    sign_requests(&mut cloned, &cloned_requests, &k);
    assert!(drafts(&mut cloned).is_empty());
    assert!(!cloned.has_unpublished_changes());
    assert_eq!(drafts(&mut doc), [a]);
    assert!(doc.has_unpublished_changes());
}

#[test]
fn loading_signed_history_creates_no_drafts() {
    let k = key(7);
    let mut source = signing_doc(&k, 1);
    let a = append_at_level(&mut source, None, 1, 0);
    source.insert_signatures([(a, sig(&k, &a))]);
    let wire = source.bundle_fragments(source.fragments(..)).concat();

    // Same author as the source, but imported: nothing to sign locally.
    let mut peer = Automerge::load_with_options(
        &wire,
        crate::LoadOptions::new().signing(author(&k)).unwrap(),
    )
    .unwrap();
    assert!(drafts(&mut peer).is_empty());
    assert!(requested(&mut peer).is_empty());
    assert!(!peer.has_unpublished_changes());
}

#[test]
fn every_unsigned_local_draft_is_requested_even_inside_a_hard_fragment() {
    let k = key(7);
    let mut doc = signing_doc(&k, 1);
    let a1 = append_at_level(&mut doc, None, 1, 0);
    let a2 = append_at_level(&mut doc, None, 2, 0);
    let a3 = append_at_level(&mut doc, None, 3, 1);
    let fragment = doc.change_graph.get_historical_fragment(a3).unwrap();
    assert_eq!(
        fragment.members.iter().copied().collect::<BTreeSet<_>>(),
        [a1, a2, a3].into_iter().collect()
    );
    assert_eq!(drafts(&mut doc), [a1, a2, a3]);

    let requests = doc.signing_requests().unwrap();
    assert_eq!(
        requests.iter().map(|r| r.hash()).collect::<Vec<_>>(),
        sorted(&[a1, a2, a3]),
        "request local signatures without planning fragment coverage"
    );
    sign_requests(&mut doc, &requests, &k);
    for hash in [a1, a2, a3] {
        assert!(is_published(&mut doc, &hash));
    }
    assert!(drafts(&mut doc).is_empty());
    assert!(requested(&mut doc).is_empty());
    assert!(!doc.has_unpublished_changes());
}

/// Published loose `a`, foreign unpublished hard `b`, local hard `c` at
/// `[a, b]` so `G = {a, c}` has boundary `{b}`. Then a loose local `f` at
/// `[a]` is the useful fallback: `F = {f}` can publish while `G` waits.
fn blocked_cover_with_fallback() -> (Automerge, SigningKey, SigningKey, [ChangeHash; 4]) {
    let ka = key(7);
    let kb = key(8);
    let mut doc = signing_doc(&ka, 1);
    let a = append_at_level(&mut doc, None, 1, 0);
    doc.insert_signatures([(a, sig(&ka, &a))]);
    assert!(is_published(&mut doc, &a));

    // `b` is authored by another author on a clone and injected as applied
    // history without proof: it is unpublished and not locally signable.
    let mut remote = replica(&doc, author(&kb), ActorId::from([2]));
    let b = append_at_level(&mut remote, Some(&[]), 2, 1);
    let b_change = remote.get_change_by_hash(&b).unwrap();
    inject_applied_change(&mut doc, &b_change, ActorId::from([2]));
    assert!(!is_published(&mut doc, &b));

    let c = append_at_level(&mut doc, Some(&[a, b]), 3, 1);
    let g = doc.change_graph.get_historical_fragment(c).unwrap();
    assert_eq!(g.boundary, [b]);
    assert_eq!(
        g.members.iter().copied().collect::<BTreeSet<_>>(),
        [a, c].into_iter().collect()
    );
    (doc, ka, kb, [a, b, c, c])
}

#[test]
fn blocked_local_change_is_requested_but_signature_does_not_publish_it() {
    let (mut doc, ka, kb, [_a, b, c, _]) = blocked_cover_with_fallback();
    assert_eq!(drafts(&mut doc), [c]);
    // Request local c immediately, but never request imported boundary b.
    assert_eq!(requested(&mut doc), [c]);
    assert!(doc.has_unpublished_changes());

    // Signing c anyway (e.g. a stale response) never drops the draft.
    doc.insert_signatures([(c, sig(&ka, &c))]);
    assert!(!is_published(&mut doc, &c));
    assert_eq!(drafts(&mut doc), [c]);
    assert!(requested(&mut doc).is_empty());

    // Boundary progress from the other author publishes G and retires c.
    doc.insert_signatures([(b, sig(&kb, &b))]);
    assert!(is_published(&mut doc, &c));
    assert!(drafts(&mut doc).is_empty());
    assert!(!doc.has_unpublished_changes());
}

#[test]
fn fallback_is_preserved_when_covering_fragment_is_blocked() {
    let (mut doc, ka, kb, [a, b, c, _]) = blocked_cover_with_fallback();
    // Local loose `f` at `[a]` is feasible on its own (F = {f}). Hard `h` at
    // `[c, f]` forms H = {f, h} with boundary {c}; c belongs to G, which is
    // blocked by b, so H is transitively blocked. F must stay requested.
    let f = append_at_level(&mut doc, Some(&[a]), 4, 0);
    let h = append_at_level(&mut doc, Some(&[c, f]), 5, 1);
    let big = doc.change_graph.get_historical_fragment(h).unwrap();
    assert!(big.members.contains(&f));
    assert_eq!(big.boundary, [c]);

    assert_eq!(requested(&mut doc), sorted(&[c, f, h]));

    let snapshot = doc.signing_requests().unwrap();
    sign_requests(&mut doc, &snapshot, &ka);
    assert!(is_published(&mut doc, &f));
    assert!(!is_published(&mut doc, &h));
    assert!(!is_published(&mut doc, &c));
    assert!(doc.has_unpublished_changes());
    assert!(requested(&mut doc).is_empty());
    assert_eq!(drafts(&mut doc), [c, h]);

    // All local proof is already retained. Boundary publication unlocks the
    // rest without generating another request or resubmitting local signatures.
    doc.insert_signatures([(b, sig(&kb, &b))]);
    assert!(is_published(&mut doc, &b));
    assert!(requested(&mut doc).is_empty());
    assert!(is_published(&mut doc, &c));
    assert!(is_published(&mut doc, &h));
    assert!(drafts(&mut doc).is_empty());
    assert!(!doc.has_unpublished_changes());
}

fn commit_at_level_two(doc: &mut Automerge, heads: &[ChangeHash], value: i64) -> ChangeHash {
    for time in 0..400_000 {
        let mut next = doc.clone();
        let mut tx = next.transaction_at(PatchLog::inactive(), heads).unwrap();
        tx.put(ROOT, "value", value).unwrap();
        let (h, _) = tx.commit_with(CommitOptions::default().with_time(time));
        let h = h.unwrap();
        if h.fragment_level() >= 2 {
            *doc = next;
            return h;
        }
    }
    panic!("level-2 fixture search exhausted")
}

#[test]
fn blocked_level_two_cover_still_requests_its_unsigned_local_members() {
    // A level-2 cover whose boundary is a foreign unpublished change cannot
    // publish, yet every local member it swallows stays requested and the
    // publishable ones are published on their own proof.
    let ka = key(7);
    let kb = key(8);
    let mut doc = signing_doc(&ka, 1);
    let x0 = append_at_level(&mut doc, None, 1, 0);
    let x1 = append_at_level(&mut doc, None, 2, 1);
    assert_eq!(requested(&mut doc), sorted(&[x0, x1]));

    let mut remote = replica(&doc, author(&kb), ActorId::from([2]));
    let b = commit_at_level_two(&mut remote, &[], 4);
    let b_change = remote.get_change_by_hash(&b).unwrap();
    inject_applied_change(&mut doc, &b_change, ActorId::from([2]));
    let x2 = commit_at_level_two(&mut doc, &[x1, b], 5);
    let x2_fragment = doc.change_graph.get_historical_fragment(x2).unwrap();
    assert_eq!(x2_fragment.boundary, [b]);
    assert!(x2_fragment.members.contains(&x1));
    assert!(x2_fragment.members.contains(&x0));

    assert_eq!(requested(&mut doc), sorted(&[x0, x1, x2]));
    let snapshot = doc.signing_requests().unwrap();
    sign_requests(&mut doc, &snapshot, &ka);
    assert!(is_published(&mut doc, &x1));
    assert!(!is_published(&mut doc, &x2));
    assert!(requested(&mut doc).is_empty());
    assert!(doc.has_unpublished_changes());
    assert_eq!(drafts(&mut doc), [x2]);
}

#[test]
fn requests_are_deterministic_hash_ordered_and_deduplicated() {
    let k = key(7);
    let mut doc = signing_doc(&k, 1);
    let mut hashes = Vec::new();
    for value in 1..=6 {
        hashes.push(append_at_level(&mut doc, None, value, 0));
    }
    let requests = requested(&mut doc);
    assert_eq!(requests, sorted(&hashes));
    assert_eq!(requested(&mut doc), sorted(&hashes));
    let unique: BTreeSet<_> = requests.iter().copied().collect();
    assert_eq!(unique.len(), requests.len());
}

/// Members, boundary and frontier of the fragment headed by `head`.
fn fragment_shape(
    doc: &Automerge,
    head: ChangeHash,
) -> (BTreeSet<ChangeHash>, Vec<ChangeHash>, Vec<ChangeHash>) {
    let fragment = doc.change_graph.get_historical_fragment(head).unwrap();
    let members: BTreeSet<_> = fragment.members.iter().copied().collect();
    let frontier = sorted(
        &doc.change_graph
            .fragment_requirements(head)
            .unwrap()
            .hashes()
            .copied()
            .collect::<Vec<_>>(),
    );
    (members, sorted(&fragment.boundary), frontier)
}

/// Both actors sign as the same fixed author. Actor 1 commits loose `a`;
/// actor 2 commits loose `b` and hard `c` on top, so C = {a, b, c} spans two
/// actors and has two frontier changes, `a` and `c`. Actor 1 concurrently
/// commits loose `d` and hard `a1` at `[d]`, so A1 = {a, d, a1} has frontier
/// `a1` only, but overlaps C in `a`. Actor 2 then commits hard `e` at
/// `[c, a1]`: E = {e} depends on both through its boundary.
///
/// Returns the document and `[a, b, c, d, a1, e]`.
#[allow(deprecated)]
fn overlapping_two_actor_history(k: &SigningKey) -> (Automerge, [ChangeHash; 6]) {
    let mut doc = signing_doc(k, 1);
    let a = append_at_level(&mut doc, None, 1, 0);

    doc.set_actor(ActorId::from([2])).unwrap();
    let b = append_at_level(&mut doc, Some(&[a]), 2, 0);
    let c = append_at_level(&mut doc, Some(&[b]), 3, 1);

    doc.set_actor(ActorId::from([1])).unwrap();
    let d = append_at_level(&mut doc, Some(&[a]), 4, 0);
    let a1 = append_at_level(&mut doc, Some(&[d]), 5, 1);

    doc.set_actor(ActorId::from([2])).unwrap();
    let e = append_at_level(&mut doc, Some(&[c, a1]), 6, 1);
    (doc, [a, b, c, d, a1, e])
}

#[test]
fn one_author_two_actors_overlapping_fragments_with_local_boundary() {
    // Every frontier is requested exactly once, `a` being shared between the
    // overlapping fragments, and publication progresses one fragment at a time.
    let k = key(7);
    let (mut doc, [a, b, c, d, a1, e]) = overlapping_two_actor_history(&k);
    let actor1 = ActorId::from([1]);
    let actor2 = ActorId::from([2]);

    assert_eq!(
        fragment_shape(&doc, c),
        ([a, b, c].into(), vec![], sorted(&[a, c]))
    );
    assert_eq!(
        fragment_shape(&doc, a1),
        ([a, d, a1].into(), vec![], vec![a1])
    );
    assert_eq!(
        fragment_shape(&doc, e),
        ([e].into(), sorted(&[c, a1]), vec![e])
    );
    for hash in [a, d, a1] {
        assert_eq!(doc.get_change_by_hash(&hash).unwrap().actor_id(), &actor1);
    }
    for hash in [b, c, e] {
        assert_eq!(doc.get_change_by_hash(&hash).unwrap().actor_id(), &actor2);
    }
    assert_eq!(drafts(&mut doc), [a, b, c, d, a1, e]);

    // Round 1: discovery requests every local draft once, including interiors.
    let requests = doc.signing_requests().unwrap();
    assert_eq!(
        requests.iter().map(|r| r.hash()).collect::<Vec<_>>(),
        sorted(&[a, b, c, d, a1, e])
    );

    // Round 2: C's own frontier `c` alone does not prove C because `a` is
    // still unsigned; `a` and `c` together publish C.
    doc.insert_signatures([(c, sig(&k, &c))]);
    assert!(!is_published(&mut doc, &c));
    assert_eq!(requested(&mut doc), sorted(&[a, b, d, a1, e]));
    doc.insert_signatures([(a, sig(&k, &a))]);
    for hash in [a, b, c] {
        assert!(is_published(&mut doc, &hash));
    }
    for hash in [d, a1, e] {
        assert!(!is_published(&mut doc, &hash));
    }
    assert_eq!(drafts(&mut doc), [d, a1, e]);
    assert_eq!(requested(&mut doc), sorted(&[d, a1, e]));
    assert!(doc.has_unpublished_changes());

    // Round 3: signing E alone cannot publish it while A1 is unpublished.
    doc.insert_signatures([(e, sig(&k, &e))]);
    assert!(!is_published(&mut doc, &e));
    assert_eq!(requested(&mut doc), sorted(&[d, a1]));

    // Round 4: A1 completes from the original snapshot; E follows at once.
    doc.insert_signatures([(a1, sig(&k, &a1))]);
    for hash in [d, a1, e] {
        assert!(is_published(&mut doc, &hash));
    }
    assert!(drafts(&mut doc).is_empty());
    assert!(requested(&mut doc).is_empty());
    assert!(!doc.has_unpublished_changes());
}

#[test]
fn masking_the_local_author_leaves_status_and_requests_unchanged() {
    // Visibility masks hide ops; they are not publication. Hiding our own
    // unpublished drafts leaves status true and requests unchanged, and the
    // queries themselves mutate neither drafts nor visibility.
    let k = key(7);
    let mut doc = signing_doc(&k, 1);
    let a = append_at_level(&mut doc, None, 1, 0);
    let a1 = append_at_level(&mut doc, None, 2, 1);
    assert_eq!(doc.get(ROOT, "value").unwrap().unwrap().0.as_i64(), Some(2));
    let before = doc.signing_requests().unwrap();
    assert_eq!(
        before.iter().map(|r| r.hash()).collect::<Vec<_>>(),
        sorted(&[a, a1])
    );

    doc.mask_author(author(&k), &[], &mut PatchLog::inactive())
        .unwrap();
    assert!(doc.get(ROOT, "value").unwrap().is_none(), "ops hidden");
    let mask = format!("{:?}", doc.visibility.mask());

    assert!(doc.has_unpublished_changes());
    assert_eq!(doc.signing_requests().unwrap(), before);
    assert_eq!(doc.signing_requests().unwrap(), before);
    assert_eq!(drafts(&mut doc), [a, a1]);
    assert_eq!(
        format!("{:?}", doc.visibility.mask()),
        mask,
        "queries do not republish"
    );
    assert!(doc.get(ROOT, "value").unwrap().is_none());
}

#[test]
fn signing_requests_publish_masked_drafts_without_revealing_them() {
    let k = key(7);
    let mut doc = signing_doc(&k, 1);
    append_at_level(&mut doc, None, 1, 0);
    append_at_level(&mut doc, None, 2, 1);
    let requests = doc.signing_requests().unwrap();
    doc.mask_author(author(&k), &[], &mut PatchLog::inactive())
        .unwrap();

    sign_requests(&mut doc, &requests, &k);
    assert!(!doc.has_unpublished_changes());
    assert!(drafts(&mut doc).is_empty());
    assert!(doc.get(ROOT, "value").unwrap().is_none(), "still masked");

    doc.reveal_author(&author(&k), &mut PatchLog::inactive())
        .unwrap();
    assert_eq!(doc.get(ROOT, "value").unwrap().unwrap().0.as_i64(), Some(2));
    assert!(!doc.has_unpublished_changes());
}
