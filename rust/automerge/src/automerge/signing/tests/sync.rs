//! Sync eligibility is checked before acquiring protocol access.

use crate::signature::error::SigningError;
use crate::sync;
use crate::sync::SyncDoc;
use crate::{AutoCommit, AutomergeError};

use super::*;

#[test]
fn required_sync_rejects_access_without_committing() {
    let mut doc =
        Automerge::from_config(crate::Config::default().signing(author(&key(1))).unwrap());
    assert!(matches!(
        doc.sync(),
        Err(AutomergeError::Signing(SigningError::SyncNotSupported))
    ));
    let mut auto =
        AutoCommit::from_config(crate::Config::default().signing(author(&key(1))).unwrap());
    auto.put(ROOT, "pending", 1).unwrap();
    assert!(matches!(
        auto.sync(),
        Err(AutomergeError::Signing(SigningError::SyncNotSupported))
    ));
    assert_eq!(auto.pending_ops(), 1);
    assert!(!auto.has_unpublished_changes());

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
    for _ in 0..20 {
        let ab = a.sync().unwrap().generate_sync_message(&mut sa);
        let ba = b.sync().unwrap().generate_sync_message(&mut sb);
        if ab.is_none() && ba.is_none() {
            break;
        }
        if let Some(msg) = ab {
            b.sync()
                .unwrap()
                .receive_sync_message(&mut sb, msg)
                .unwrap();
        }
        if let Some(msg) = ba {
            a.sync()
                .unwrap()
                .receive_sync_message(&mut sa, msg)
                .unwrap();
        }
    }
    let mut expected = vec![ha, hb];
    expected.sort();
    assert_eq!(a.get_heads(), expected);
    assert_eq!(b.get_heads(), expected);
    assert!(a.sync().unwrap().generate_sync_message(&mut sa).is_none());
    assert!(b.sync().unwrap().generate_sync_message(&mut sb).is_none());
}
