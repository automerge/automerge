//! The README's example, kept compiling and correct.
//!
//! If you change one, change the other.

use automerge::{transaction::Transactable, AutoCommit, ReadDoc, ROOT};
use automerge_sync::{AutoCommitSync, State, SyncDoc};

#[test]
fn readme_example() -> Result<(), automerge::AutomergeError> {
    let mut peer1 = AutoCommit::new();
    peer1.put(ROOT, "key", "value")?;

    let mut peer2 = AutoCommit::new();

    // one State per peer you are talking to
    let mut peer1_state = State::new();
    let mut peer2_state = State::new();

    loop {
        let one_to_two = peer1.sync().generate_sync_message(&mut peer1_state);
        if let Some(message) = one_to_two.clone() {
            peer2
                .sync()
                .receive_sync_message(&mut peer2_state, message)?;
        }
        let two_to_one = peer2.sync().generate_sync_message(&mut peer2_state);
        if let Some(message) = two_to_one.clone() {
            peer1
                .sync()
                .receive_sync_message(&mut peer1_state, message)?;
        }
        if one_to_two.is_none() && two_to_one.is_none() {
            break;
        }
    }

    assert_eq!(peer2.get(ROOT, "key")?.unwrap().0.as_str(), Some("value"));
    Ok(())
}

/// The README's claim that a `State` round-trips through bytes, and that
/// a peer which loses one still converges (it just re-syncs from scratch).
#[test]
fn sync_state_round_trips_and_loss_is_recoverable() -> Result<(), automerge::AutomergeError> {
    let mut peer1 = AutoCommit::new();
    peer1.put(ROOT, "key", "value")?;

    let mut peer2 = AutoCommit::new();

    let mut peer1_state = State::new();
    let mut peer2_state = State::new();
    sync(&mut peer1, &mut peer1_state, &mut peer2, &mut peer2_state);
    assert_eq!(peer2.get(ROOT, "key")?.unwrap().0.as_str(), Some("value"));

    // encode/decode is lossless
    let encoded = peer1_state.encode();
    let decoded = State::decode(&encoded).unwrap();
    assert_eq!(decoded.shared_heads, peer1_state.shared_heads);

    // and a peer that drops its state still converges on the next change
    peer1.put(ROOT, "key2", "value2")?;
    peer1.commit();
    let mut fresh = State::new();
    sync(&mut peer1, &mut fresh, &mut peer2, &mut State::new());
    assert_eq!(peer2.get(ROOT, "key2")?.unwrap().0.as_str(), Some("value2"));
    Ok(())
}

fn sync(a: &mut AutoCommit, a_state: &mut State, b: &mut AutoCommit, b_state: &mut State) {
    loop {
        let a_to_b = a.sync().generate_sync_message(a_state);
        if let Some(m) = a_to_b.clone() {
            b.sync().receive_sync_message(b_state, m).unwrap();
        }
        let b_to_a = b.sync().generate_sync_message(b_state);
        if let Some(m) = b_to_a.clone() {
            a.sync().receive_sync_message(a_state, m).unwrap();
        }
        if a_to_b.is_none() && b_to_a.is_none() {
            return;
        }
    }
}
