use automerge::sync::{State, SyncDoc};
use automerge::transaction::Transactable;
use automerge::{AutoCommit, Automerge, ROOT};

#[test]
fn unattested_sync_handle_is_public_and_closes_the_pending_transaction() {
    let mut doc = Automerge::new();
    let mut state = State::new();
    // The `SyncDoc` handle returned by the public `sync()` must type-check from
    // outside the crate, including the `Option<Message>` it generates.
    let _: Option<automerge::sync::Message> = doc.sync().unwrap().generate_sync_message(&mut state);
    let mut auto = AutoCommit::new();
    auto.put(ROOT, "pending", 1).unwrap();
    let _ = auto.sync().unwrap();
    assert_eq!(auto.pending_ops(), 0);
}
