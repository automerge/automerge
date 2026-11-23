use automerge::transaction::Transactable;
use automerge::{ActorId, Author, AutoCommit, Automerge, Config, ForkOptions, ROOT};
use ed25519_dalek::SigningKey;

fn author(seed: u8) -> Author<'static> {
    Author::from(
        SigningKey::from_bytes(&[seed; 32])
            .verifying_key()
            .to_bytes()
            .to_vec(),
    )
}

#[test]
#[allow(deprecated)] // Deterministic identity fixtures and legacy-option regression coverage.
fn all_fork_choices_are_rejected_before_source_transaction_closes() {
    let mut source = AutoCommit::from_config(Config::default().author(Some(author(1))));
    source.set_actor(ActorId::from([1])).unwrap();
    source.put(ROOT, "a", 1).unwrap();
    source.commit();
    let mut other = source.fork_with_author(Some(author(2))).unwrap();
    other.put(ROOT, "pending", 1).unwrap();
    let before = other.get_actor().clone();
    let options = ForkOptions::default()
        .author(Some(author(2)))
        .actor(ActorId::from([1]));
    assert!(other.fork_with_options(options).is_err());
    assert_eq!(other.pending_ops(), 1);
    assert_eq!(other.get_actor(), &before);
}

#[test]
fn same_author_pending_fork_keeps_newly_committed_draft_state() {
    let local = author(1);
    let mut source = AutoCommit::from_config(Config::default().signing(local.clone()).unwrap());
    source.put(ROOT, "pending", 1).unwrap();
    let mut fork = source
        .fork_with_options(ForkOptions::default().author(Some(local)))
        .unwrap();
    assert_eq!(source.pending_ops(), 0);
    assert_eq!(
        source.signing_requests().unwrap(),
        fork.signing_requests().unwrap()
    );
    assert_eq!(fork.signing_requests().unwrap().len(), 1);
    assert!(fork.has_unpublished_changes());
}

#[test]
fn changed_unsigned_author_cannot_take_the_actor_of_pending_source_edits() {
    let mut source = AutoCommit::from_config(Config::default().author(Some(author(1))));
    let actor = source.get_actor().clone();
    source.put(ROOT, "pending", 1).unwrap();
    assert!(source
        .fork_with_options(ForkOptions::default().author(Some(author(2))).actor(actor))
        .is_err());
    assert_eq!(source.pending_ops(), 1);
}

#[test]
#[allow(deprecated)] // Deterministic identity fixtures and legacy-option regression coverage.
fn changed_author_fork_after_empty_transaction_keeps_aligned_publication() {
    use automerge::signature::ed25519;
    use automerge::LoadOptions;
    use ed25519_dalek::Signer;
    let key = SigningKey::from_bytes(&[1; 32]);
    let mut peer = AutoCommit::from_config(Config::default().signing(author(1)).unwrap());
    peer.set_actor(ActorId::from([0xff; 16])).unwrap();
    peer.put(ROOT, "peer", 1).unwrap();
    let requests = peer.signing_requests().unwrap();
    peer.insert_signatures(requests.iter().map(|r| {
        (
            r.hash(),
            ed25519::Signature::from_bytes(key.sign(r.message()).to_bytes()),
        )
    }));
    let bytes = peer.save();
    let mut source = AutoCommit::from_config(Config::default().signing(author(2)).unwrap());
    source.set_actor(ActorId::from([0; 16])).unwrap();
    source.load_incremental(&bytes).unwrap();
    let _ = source.delete(ROOT, "missing");
    assert_eq!(source.pending_ops(), 0);
    let mut fork = source
        .fork_with_options(ForkOptions::default().author(Some(author(3))))
        .unwrap();
    let loaded =
        Automerge::load_with_options(&fork.save(), LoadOptions::new().signing(author(4)).unwrap())
            .unwrap();
    assert_eq!(
        loaded.get_changes(&[]).len(),
        1,
        "publication clock stays aligned after actor removal"
    );
    assert!(!fork.has_unpublished_changes());
}

#[test]
#[allow(deprecated)] // Deterministic identity fixtures and legacy-option regression coverage.
fn changed_author_cannot_take_isolated_pending_actor() {
    let mut source = AutoCommit::from_config(Config::default().author(Some(author(1))));
    source.set_actor(ActorId::from([1])).unwrap();
    source.put(ROOT, "v", 1).unwrap();
    let first = source.commit().unwrap();
    source.put(ROOT, "v", 2).unwrap();
    source.commit();
    // Discover the deterministic isolation actor through an independent probe.
    let mut probe = source.clone();
    probe.isolate(&[first]);
    probe.put(ROOT, "branch", 1).unwrap();
    let isolated = probe.commit().unwrap();
    let actor = probe
        .get_change_by_hash(&isolated)
        .unwrap()
        .actor_id()
        .clone();
    source.isolate(&[first]);
    source.put(ROOT, "pending", 1).unwrap();
    assert!(source
        .fork_with_options(ForkOptions::default().author(Some(author(2))).actor(actor))
        .is_err());
    assert_eq!(source.pending_ops(), 1);
}

#[test]
fn fork_options_inherit_author_unless_explicitly_replaced() {
    let source = Automerge::from_config(Config::default().author(Some(author(1))));
    let inherited = source.fork_with_options(ForkOptions::default()).unwrap();
    assert_eq!(source.get_author(), inherited.get_author());
    assert_ne!(source.get_actor(), inherited.get_actor());
    let anonymous = source
        .fork_with_options(ForkOptions::default().author(None))
        .unwrap();
    assert!(anonymous.get_author().is_none());
    assert!(source.get_author().is_some());
}
