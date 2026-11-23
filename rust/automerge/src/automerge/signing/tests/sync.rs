//! Sync eligibility is checked before acquiring protocol access.

use crate::signature::error::SigningError;
use crate::sync;
use crate::sync::SyncDoc;
use crate::{AutoCommit, AutomergeError};

use super::*;

/// Exchange sync messages until neither side has anything left to send.
fn sync_until_quiescent(
    a: &mut Automerge,
    b: &mut Automerge,
    sa: &mut sync::State,
    sb: &mut sync::State,
) {
    for _ in 0..20 {
        let ab = a.sync().unwrap().generate_sync_message(sa);
        let ba = b.sync().unwrap().generate_sync_message(sb);
        if ab.is_none() && ba.is_none() {
            break;
        }
        if let Some(msg) = ab {
            b.sync().unwrap().receive_sync_message(sb, msg).unwrap();
        }
        if let Some(msg) = ba {
            a.sync().unwrap().receive_sync_message(sa, msg).unwrap();
        }
    }
}

#[test]
fn required_mode_automerge_sync_is_unsupported() {
    let mut doc =
        Automerge::from_config(crate::Config::default().signing(author(&key(1))).unwrap());
    assert!(matches!(
        doc.sync(),
        Err(AutomergeError::Signing(SigningError::SyncNotSupported))
    ));
}

#[test]
fn required_mode_autocommit_sync_is_unsupported_and_keeps_the_transaction_open() {
    let mut auto =
        AutoCommit::from_config(crate::Config::default().signing(author(&key(1))).unwrap());
    auto.put(ROOT, "pending", 1).unwrap();
    assert!(matches!(
        auto.sync(),
        Err(AutomergeError::Signing(SigningError::SyncNotSupported))
    ));
    assert_eq!(auto.pending_ops(), 1);
    assert!(!auto.has_unpublished_changes());
}

#[test]
fn unsigned_autocommit_sync_closes_the_transaction_and_generates_a_message() {
    let mut unsigned = AutoCommit::new();
    unsigned.put(ROOT, "x", 1).unwrap();
    let mut state = sync::State::new();
    assert!(unsigned
        .sync()
        .unwrap()
        .generate_sync_message(&mut state)
        .is_some());
    assert_eq!(unsigned.pending_ops(), 0);
}

#[test]
fn unsigned_sync_messages_and_quiescence_are_unchanged() {
    let mut a = Automerge::new().with_actor(ActorId::from([1])).unwrap();
    let mut b = Automerge::new().with_actor(ActorId::from([2])).unwrap();
    let ha = append_at_level(&mut a, None, 1, 0);
    let hb = append_at_level(&mut b, None, 2, 0);
    let mut sa = sync::State::new();
    let mut sb = sync::State::new();

    sync_until_quiescent(&mut a, &mut b, &mut sa, &mut sb);

    let mut expected = vec![ha, hb];
    expected.sort();
    assert_eq!(a.get_heads(), expected);
    assert_eq!(b.get_heads(), expected);
    assert!(a.sync().unwrap().generate_sync_message(&mut sa).is_none());
    assert!(b.sync().unwrap().generate_sync_message(&mut sb).is_none());
}
