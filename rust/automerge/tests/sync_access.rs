use automerge::sync::{State, SyncDoc};
use automerge::transaction::Transactable;
use automerge::{Author, AutoCommit, Automerge, Config, ROOT};
use ed25519_dalek::SigningKey;

fn author() -> Author<'static> {
    Author::from(
        SigningKey::from_bytes(&[7; 32])
            .verifying_key()
            .to_bytes()
            .to_vec(),
    )
}

#[test]
fn signed_documents_reject_sync_access_before_transaction_closure() {
    let mut doc = Automerge::from_config(Config::default().signing(author()).unwrap());
    assert!(doc.sync().is_err());
    let mut auto = AutoCommit::from_config(Config::default().signing(author()).unwrap());
    auto.put(ROOT, "pending", 1).unwrap();
    assert!(auto.sync().is_err());
    assert_eq!(auto.pending_ops(), 1);
}

#[test]
fn unsigned_handle_retains_option_generation_and_closes_pending_transaction() {
    let mut doc = Automerge::new();
    let mut state = State::new();
    let _: Option<automerge::sync::Message> = doc.sync().unwrap().generate_sync_message(&mut state);
    let mut auto = AutoCommit::new();
    auto.put(ROOT, "pending", 1).unwrap();
    let _ = auto.sync().unwrap();
    assert_eq!(auto.pending_ops(), 0);
}
