//! Sync is explicitly unsupported in required signing mode.

use crate::signature::error::SigningError;
use crate::sync::{self, SyncDoc};
use crate::{AutoCommit, AutomergeError};

use super::*;

#[test]
fn required_sync_is_a_checked_error_even_with_nothing_to_send() {
    let k = key(1);
    let empty = Automerge::signing(author(&k)).unwrap();
    let mut state = sync::State::new();
    assert!(matches!(
        empty.generate_sync_message(&mut state),
        Err(AutomergeError::Signing(SigningError::SyncNotSupported))
    ));
    assert!(!state.in_flight);

    let mut unsigned_peer = Automerge::new().with_actor(ActorId::from([9]));
    append_at_level(&mut unsigned_peer, None, 1, 0);
    let mut peer_state = sync::State::new();
    let msg = unsigned_peer
        .generate_sync_message(&mut peer_state)
        .unwrap()
        .unwrap();

    let mut doc = Automerge::signing(author(&k))
        .unwrap()
        .with_actor(ActorId::from([1]));
    let a = append_at_level(&mut doc, None, 1, 0);
    sign_doc(&mut doc, a, &k);
    let mut log = PatchLog::active();
    assert!(matches!(
        doc.receive_sync_message_log_patches(&mut state, msg.clone(), &mut log),
        Err(AutomergeError::Signing(SigningError::SyncNotSupported))
    ));
    assert!(doc.make_patches(&mut log).is_empty());
    assert_eq!(doc.get_heads(), [a]);
    assert!(state.their_heads.is_none());

    // AutoCommit's wrapper reports the same error. `sync()` closes the open
    // transaction as documented (for every mode), which is an ordinary local
    // commit: the result is a draft, never a sync-driven mutation.
    let mut auto = AutoCommit::signing(author(&k)).unwrap();
    auto.put(ROOT, "x", 1).unwrap();
    let mut auto_state = sync::State::new();
    assert!(matches!(
        auto.sync().generate_sync_message(&mut auto_state),
        Err(AutomergeError::Signing(SigningError::SyncNotSupported))
    ));
    assert!(matches!(
        auto.sync().receive_sync_message(&mut auto_state, msg),
        Err(AutomergeError::Signing(SigningError::SyncNotSupported))
    ));
    assert_eq!(auto.pending_ops(), 0);
    assert_eq!(auto.get_heads().len(), 1);
    assert!(auto.has_unpublished_changes());
    assert!(!auto_state.in_flight && auto_state.their_heads.is_none());
}

#[test]
fn unsigned_sync_behaviour_is_unchanged_behind_result() {
    let mut a = Automerge::new().with_actor(ActorId::from([1]));
    let mut b = Automerge::new().with_actor(ActorId::from([2]));
    let ha = append_at_level(&mut a, None, 1, 0);
    let hb = append_at_level(&mut b, None, 2, 0);
    let mut sa = sync::State::new();
    let mut sb = sync::State::new();
    let mut rounds = 0;
    loop {
        let mut progressed = false;
        if let Some(msg) = a.generate_sync_message(&mut sa).unwrap() {
            b.receive_sync_message(&mut sb, msg).unwrap();
            progressed = true;
        }
        if let Some(msg) = b.generate_sync_message(&mut sb).unwrap() {
            a.receive_sync_message(&mut sa, msg).unwrap();
            progressed = true;
        }
        rounds += 1;
        if !progressed || rounds > 20 {
            break;
        }
    }
    let mut expected = vec![ha, hb];
    expected.sort();
    assert_eq!(a.get_heads(), expected);
    assert_eq!(b.get_heads(), expected);
    // Quiescent peers produce no message, as `None`, not an error.
    assert!(a.generate_sync_message(&mut sa).unwrap().is_none());
    assert!(b.generate_sync_message(&mut sb).unwrap().is_none());
}
