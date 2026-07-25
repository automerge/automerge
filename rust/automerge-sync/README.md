# automerge-sync

**Replicate an automerge document between two peers, sending only what the
other side is missing.**

Two peers each keep a small [`State`] describing what they believe the other
has. From that they exchange messages until both hold the same set of
changes. Neither needs to know how the other's history is shaped, and
neither sends a change the other already has — the protocol works that out
from a bloom filter of the sender's hashes plus the receiver's advertised
heads.

The algorithm is the one described in [*Byzantine Eventual Consistency and
the Fundamental Limits of Peer-to-Peer Databases*][paper]. It assumes a
reliable, in-order stream between the two peers; it does not assume
anything about how many peers there are, or that they ever agree on who is
the server.

[paper]: https://arxiv.org/abs/2012.00472

```rust
use automerge::{transaction::Transactable, AutoCommit, ReadDoc, ROOT};
use automerge_sync::{State, Sync};

# fn main() -> Result<(), automerge::AutomergeError> {
let mut peer1 = AutoCommit::new();
peer1.enable_audit_mode()?;
peer1.put(ROOT, "key", "value")?;

let mut peer2 = AutoCommit::new();
peer2.enable_audit_mode()?;

// one State per peer you are talking to
let mut peer1_state = State::new();
let mut peer2_state = State::new();

loop {
    let one_to_two = Sync::generate_sync_message(peer1.document(), &mut peer1_state)?;
    if let Some(message) = one_to_two.clone() {
        Sync::receive_sync_message(peer2.document_mut(), &mut peer2_state, message)?;
    }
    let two_to_one = Sync::generate_sync_message(peer2.document(), &mut peer2_state)?;
    if let Some(message) = two_to_one.clone() {
        Sync::receive_sync_message(peer1.document_mut(), &mut peer1_state, message)?;
    }
    if one_to_two.is_none() && two_to_one.is_none() {
        break;
    }
}

assert_eq!(peer2.get(ROOT, "key")?.unwrap().0.to_str(), Some("value"));
# Ok(())
# }
```

`generate_sync_message` returning `None` means there is nothing to send —
either the peers agree, or you are waiting on a reply to a message already
in flight. That is the loop's termination condition, not an error.

## Two things to know before wiring this up

**Documents must be in audit mode.** The protocol identifies changes by
hash from end to end, so it needs a document which retains every change
hash. Both entry points fail with `AutomergeError::AuditModeRequired`
otherwise. The check is deliberately unconditional — a small document whose
retained hashes would happen to suffice still refuses, so replication never
works "sometimes".

**Sync state is per peer, and worth persisting.** A `State` is what lets
the second sync with a peer be cheap instead of re-deriving everything.
[`State::encode`] / [`State::decode`] round-trip it to bytes. Losing one is
safe — you just pay a full re-sync.

## Working with an `AutoCommit`

`AutoCommit` keeps an implicit transaction open, and ops in it are not yet
under the document's heads. `document()` and `document_mut()` settle it
first, which is why the example above goes through them rather than passing
the `AutoCommit` directly.

## Message and state encodings

Messages and sync states are versioned wire formats, not just Rust types —
[`Message::encode`] / [`Message::decode`] and [`State::encode`] /
[`State::decode`] are the boundary. Two message versions exist:

| Version | Carries | Sent when |
|---------|---------|-----------|
| V1 | individual change chunks | the peer has not advertised V2 support |
| V2 | one whole-document chunk | the peer supports it *and* more than a third of the document would otherwise be sent |

Peers negotiate through a flags byte; a peer that sends flags at all is
known to understand V2. The V1 encoding is still produced for peers that
predate the negotiation, and `v1_compat_test` drives a frozen copy of the
old implementation against the current one to keep that honest.

## Read-only peers

A peer that will never contribute changes — an observer, a backup — can say
so with [`State::new_read_only`], and the other side then skips computing
and sending changes to it entirely. Switching a peer back to read-write
resets the far side's idea of what it has already sent, so nothing is lost.

## What this needs from automerge

Everything the protocol asks of a document is on `Automerge` itself, under
the "Replication" heading: `change_hashes`, `num_changes`, `change_deps`,
`has_change`, `changes_by_hash`, `remove_ancestors`, `missing_deps`,
`missing_deps_with_queued`, plus `save` and `load_incremental`. Nothing
here reaches into automerge's internals, and the message parser is this
crate's own — the sync wire format and the document format are independent,
and share only the 32-byte change hash.

That is deliberate: the protocol is versioned separately from the document
format, and a different replication strategy can be built on the same
surface without changing automerge.

[`State`]: https://docs.rs/automerge-sync/latest/automerge_sync/struct.State.html
[`State::encode`]: https://docs.rs/automerge-sync/latest/automerge_sync/struct.State.html#method.encode
[`State::decode`]: https://docs.rs/automerge-sync/latest/automerge_sync/struct.State.html#method.decode
[`State::new_read_only`]: https://docs.rs/automerge-sync/latest/automerge_sync/struct.State.html#method.new_read_only
[`Message::encode`]: https://docs.rs/automerge-sync/latest/automerge_sync/struct.Message.html#method.encode
[`Message::decode`]: https://docs.rs/automerge-sync/latest/automerge_sync/struct.Message.html#method.decode
