//! # Sync Protocol
//!
//! The sync protocol is based on this paper:
//! <https://arxiv.org/abs/2012.00472>, it assumes a reliable in-order stream
//! between two peers who are synchronizing a document.
//!
//! Each peer maintains a [`State`] for each peer they are synchronizing with.
//! This state tracks things like what the heads of the other peer are and
//! whether there are in-flight messages. Anything which implements [`SyncDoc`]
//! can take part in the sync protocol. The flow goes something like this:
//!
//! * The initiating peer creates an empty [`State`] and then calls
//!   [`SyncDoc::generate_sync_message`] to generate new sync message and sends
//!   it to the receiving peer.
//! * The receiving peer receives a message from the initiator, creates a new
//!   [`State`], and calls [`SyncDoc::receive_sync_message`] on it's view of the
//!   document
//! * The receiving peer then calls [`SyncDoc::generate_sync_message`] to generate
//!   a new sync message and send it back to the initiator
//! * From this point on each peer operates in a loop, receiving a sync message
//!   from the other peer and then generating a new message to send back.
//!
//! ## Example
//!
//! ```
//! use automerge::{transaction::Transactable, sync::{self, SyncDoc}, ReadDoc};
//! # fn main() -> Result<(), automerge::AutomergeError> {
//! // Create a document on peer1
//! let mut peer1 = automerge::AutoCommit::new();
//! peer1.put(automerge::ROOT, "key", "value")?;
//!
//! // Create a state to track our sync with peer2
//! let mut peer1_state = sync::State::new();
//! // Generate the initial message to send to peer2, unwrap for brevity
//! let message1to2 = peer1.sync().generate_sync_message(&mut peer1_state).unwrap();
//!
//! // We receive the message on peer2. We don't have a document at all yet
//! // so we create one
//! let mut peer2 = automerge::AutoCommit::new();
//! // We don't have a state for peer1 (it's a new connection), so we create one
//! let mut peer2_state = sync::State::new();
//! // Now receive the message from peer 1
//! peer2.sync().receive_sync_message(&mut peer2_state, message1to2)?;
//!
//! // Now we loop, sending messages from one to two and two to one until
//! // neither has anything new to send
//!
//! loop {
//!     let two_to_one = peer2.sync().generate_sync_message(&mut peer2_state);
//!     if let Some(message) = two_to_one.as_ref() {
//!         println!("two to one");
//!         peer1.sync().receive_sync_message(&mut peer1_state, message.clone())?;
//!     }
//!     let one_to_two = peer1.sync().generate_sync_message(&mut peer1_state);
//!     if let Some(message) = one_to_two.as_ref() {
//!         println!("one to two");
//!         peer2.sync().receive_sync_message(&mut peer2_state, message.clone())?;
//!     }
//!     if two_to_one.is_none() && one_to_two.is_none() {
//!         break;
//!     }
//! }
//!
//! assert_eq!(peer2.get(automerge::ROOT, "key")?.unwrap().0.to_str(), Some("value"));
//!
//! # Ok(())
//! # }
//! ```

use itertools::Itertools;
use serde::ser::SerializeMap;
use std::collections::{HashMap, HashSet};

use crate::{
    patches::PatchLog,
    storage::{parse, Change as StoredChange, ReadChangeOpError},
    Automerge, AutomergeError, Change, ChangeHash, ReadDoc,
};

mod bloom;
mod state;

pub use bloom::{BloomFilter, DecodeError as DecodeBloomError};
pub use state::DecodeError as DecodeStateError;
pub use state::{Have, State};

/// A document which can take part in the sync protocol
///
/// See the [module level documentation](crate::sync) for more details.
pub trait SyncDoc {
    /// Generate a sync message for the remote peer represented by `sync_state`
    ///
    /// If this returns `None` then there are no new messages to send, either because we are
    /// waiting for an acknolwedgement of an in-flight message, or because the remote is up to
    /// date.
    ///
    /// * `sync_state` - The [`State`] for this document and the remote peer
    /// * `message` - The [`Message`] to receive
    /// * `patch_log` - A [`PatchLog`] which will be updated with any changes that are made to the
    ///                 current state of the document due to the received sync message
    fn generate_sync_message(&self, sync_state: &mut State) -> Option<Message>;

    /// Apply a received sync message to this document and `sync_state`
    fn receive_sync_message(
        &mut self,
        sync_state: &mut State,
        message: Message,
    ) -> Result<(), AutomergeError>;

    /// Apply a received sync message to this document and `sync_state`, logging any changes that
    /// are made to `patch_log`
    ///
    /// If this returns `None` then there are no new messages to send, either because we are
    /// waiting for an acknolwedgement of an in-flight message, or because the remote is up to
    /// date.
    ///
    /// # Arguments
    ///
    /// * `sync_state` - The [`State`] for this document and the remote peer
    /// * `message` - The [`Message`] to receive
    /// * `patch_log` - A [`PatchLog`] which will be updated with any changes that are made to the
    ///                 current state of the document due to the received sync message
    fn receive_sync_message_log_patches(
        &mut self,
        sync_state: &mut State,
        message: Message,
        patch_log: &mut PatchLog,
    ) -> Result<(), AutomergeError>;
}

const MESSAGE_TYPE_SYNC: u8 = 0x42; // first byte of a sync message, for identification

impl SyncDoc for Automerge {
    fn generate_sync_message(&self, sync_state: &mut State) -> Option<Message> {
        let our_heads = self.get_heads();

        let our_need = self.get_missing_deps(sync_state.their_heads.as_ref().unwrap_or(&vec![]));

        let their_heads_set = if let Some(ref heads) = sync_state.their_heads {
            heads.iter().collect::<HashSet<_>>()
        } else {
            HashSet::new()
        };
        let our_have = if our_need.iter().all(|hash| their_heads_set.contains(hash)) {
            vec![self.make_bloom_filter(sync_state.shared_heads.clone())]
        } else {
            Vec::new()
        };

        if let Some(ref their_have) = sync_state.their_have {
            if let Some(first_have) = their_have.first().as_ref() {
                if !first_have
                    .last_sync
                    .iter()
                    .all(|hash| self.get_change_by_hash(hash).is_some())
                {
                    let reset_msg = Message {
                        heads: our_heads,
                        need: Vec::new(),
                        have: vec![Have::default()],
                        changes: Vec::new(),
                    };
                    return Some(reset_msg);
                }
            }
        }

        let changes_to_send = if let (Some(their_have), Some(their_need)) = (
            sync_state.their_have.as_ref(),
            sync_state.their_need.as_ref(),
        ) {
            self.get_changes_to_send(their_have, their_need)
                .expect("Should have only used hashes that are in the document")
        } else {
            Vec::new()
        };

        let heads_unchanged = sync_state.last_sent_heads == our_heads;

        let heads_equal = if let Some(their_heads) = sync_state.their_heads.as_ref() {
            their_heads == &our_heads
        } else {
            false
        };

        // deduplicate the changes to send with those we have already sent and clone it now
        let changes_to_send = changes_to_send
            .into_iter()
            .filter_map(|change| {
                if !sync_state.sent_hashes.contains(&change.hash()) {
                    Some(change.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if heads_unchanged {
            if heads_equal && changes_to_send.is_empty() {
                return None;
            }
            if sync_state.in_flight {
                return None;
            }
        }

        sync_state.last_sent_heads = our_heads.clone();
        sync_state
            .sent_hashes
            .extend(changes_to_send.iter().map(|c| c.hash()));

        let sync_message = Message {
            heads: our_heads,
            have: our_have,
            need: our_need,
            changes: changes_to_send,
        };

        sync_state.in_flight = true;
        Some(sync_message)
    }

    fn receive_sync_message(
        &mut self,
        sync_state: &mut State,
        message: Message,
    ) -> Result<(), AutomergeError> {
        let mut patch_log = PatchLog::inactive();
        self.receive_sync_message_inner(sync_state, message, &mut patch_log)
    }

    fn receive_sync_message_log_patches(
        &mut self,
        sync_state: &mut State,
        message: Message,
        patch_log: &mut PatchLog,
    ) -> Result<(), AutomergeError> {
        self.receive_sync_message_inner(sync_state, message, patch_log)
    }
}

impl Automerge {
    fn make_bloom_filter(&self, last_sync: Vec<ChangeHash>) -> Have {
        let new_changes = self.get_changes(&last_sync);
        let hashes = new_changes.iter().map(|change| change.hash());
        Have {
            last_sync,
            bloom: BloomFilter::from_hashes(hashes),
        }
    }

    fn get_changes_to_send(
        &self,
        have: &[Have],
        need: &[ChangeHash],
    ) -> Result<Vec<&Change>, AutomergeError> {
        if have.is_empty() {
            Ok(need
                .iter()
                .filter_map(|hash| self.get_change_by_hash(hash))
                .collect())
        } else {
            let mut last_sync_hashes = HashSet::new();
            let mut bloom_filters = Vec::with_capacity(have.len());

            for h in have {
                let Have { last_sync, bloom } = h;
                last_sync_hashes.extend(last_sync);
                bloom_filters.push(bloom);
            }
            let last_sync_hashes = last_sync_hashes.into_iter().copied().collect::<Vec<_>>();

            let changes = self.get_changes(&last_sync_hashes);

            let mut change_hashes = HashSet::with_capacity(changes.len());
            let mut dependents: HashMap<ChangeHash, Vec<ChangeHash>> = HashMap::new();
            let mut hashes_to_send = HashSet::new();

            for change in &changes {
                change_hashes.insert(change.hash());

                for dep in change.deps() {
                    dependents.entry(*dep).or_default().push(change.hash());
                }

                if bloom_filters
                    .iter()
                    .all(|bloom| !bloom.contains_hash(&change.hash()))
                {
                    hashes_to_send.insert(change.hash());
                }
            }

            let mut stack = hashes_to_send.iter().copied().collect::<Vec<_>>();
            while let Some(hash) = stack.pop() {
                if let Some(deps) = dependents.get(&hash) {
                    for dep in deps {
                        if hashes_to_send.insert(*dep) {
                            stack.push(*dep);
                        }
                    }
                }
            }

            let mut changes_to_send = Vec::new();
            for hash in need {
                if !hashes_to_send.contains(hash) {
                    if let Some(change) = self.get_change_by_hash(hash) {
                        changes_to_send.push(change);
                    }
                }
            }

            for change in changes {
                if hashes_to_send.contains(&change.hash()) {
                    changes_to_send.push(change);
                }
            }
            Ok(changes_to_send)
        }
    }

    pub(crate) fn receive_sync_message_inner(
        &mut self,
        sync_state: &mut State,
        message: Message,
        patch_log: &mut PatchLog,
    ) -> Result<(), AutomergeError> {
        let before_heads = self.get_heads();

        let Message {
            heads: message_heads,
            changes: message_changes,
            need: message_need,
            have: message_have,
        } = message;

        let changes_is_empty = message_changes.is_empty();
        if !changes_is_empty {
            self.apply_changes_log_patches(message_changes, patch_log)?;
            sync_state.shared_heads = advance_heads(
                &before_heads.iter().collect(),
                &self.get_heads().into_iter().collect(),
                &sync_state.shared_heads,
            );
        }

        // trim down the sent hashes to those that we know they haven't seen
        self.filter_changes(&message_heads, &mut sync_state.sent_hashes)?;

        if changes_is_empty && message_heads == before_heads {
            sync_state.last_sent_heads = message_heads.clone();
        }

        if sync_state.sent_hashes.is_empty() {
            sync_state.in_flight = false;
        }

        let known_heads = message_heads
            .iter()
            .filter(|head| self.get_change_by_hash(head).is_some())
            .collect::<Vec<_>>();
        if known_heads.len() == message_heads.len() {
            sync_state.shared_heads = message_heads.clone();
            sync_state.in_flight = false;
            // If the remote peer has lost all its data, reset our state to perform a full resync
            if message_heads.is_empty() {
                sync_state.last_sent_heads = Default::default();
                sync_state.sent_hashes = Default::default();
            }
        } else {
            sync_state.shared_heads = sync_state
                .shared_heads
                .iter()
                .chain(known_heads)
                .copied()
                .unique()
                .sorted()
                .collect::<Vec<_>>();
        }

        sync_state.their_have = Some(message_have);
        sync_state.their_heads = Some(message_heads);
        sync_state.their_need = Some(message_need);

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReadMessageError {
    #[error("expected {expected_one_of:?} but found {found}")]
    WrongType { expected_one_of: Vec<u8>, found: u8 },
    #[error("{0}")]
    Parse(String),
    #[error(transparent)]
    ReadChangeOps(#[from] ReadChangeOpError),
    #[error("not enough input")]
    NotEnoughInput,
}

impl From<parse::leb128::Error> for ReadMessageError {
    fn from(e: parse::leb128::Error) -> Self {
        ReadMessageError::Parse(e.to_string())
    }
}

impl From<bloom::ParseError> for ReadMessageError {
    fn from(e: bloom::ParseError) -> Self {
        ReadMessageError::Parse(e.to_string())
    }
}

impl From<crate::storage::change::ParseError> for ReadMessageError {
    fn from(e: crate::storage::change::ParseError) -> Self {
        ReadMessageError::Parse(format!("error parsing changes: {}", e))
    }
}

impl From<ReadMessageError> for parse::ParseError<ReadMessageError> {
    fn from(e: ReadMessageError) -> Self {
        parse::ParseError::Error(e)
    }
}

impl From<parse::ParseError<ReadMessageError>> for ReadMessageError {
    fn from(p: parse::ParseError<ReadMessageError>) -> Self {
        match p {
            parse::ParseError::Error(e) => e,
            parse::ParseError::Incomplete(..) => Self::NotEnoughInput,
        }
    }
}

/// The sync message to be sent.
#[derive(Clone, Debug, PartialEq)]
pub struct Message {
    /// The heads of the sender.
    pub heads: Vec<ChangeHash>,
    /// The hashes of any changes that are being explicitly requested from the recipient.
    pub need: Vec<ChangeHash>,
    /// A summary of the changes that the sender already has.
    pub have: Vec<Have>,
    /// The changes for the recipient to apply.
    pub changes: Vec<Change>,
}

impl serde::Serialize for Message {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(Some(4))?;
        map.serialize_entry("heads", &self.heads)?;
        map.serialize_entry("need", &self.need)?;
        map.serialize_entry("have", &self.have)?;
        map.serialize_entry(
            "changes",
            &self
                .changes
                .iter()
                .map(crate::ExpandedChange::from)
                .collect::<Vec<_>>(),
        )?;
        map.end()
    }
}

fn parse_have(input: parse::Input<'_>) -> parse::ParseResult<'_, Have, ReadMessageError> {
    let (i, last_sync) = parse::length_prefixed(parse::change_hash)(input)?;
    let (i, bloom_bytes) = parse::length_prefixed_bytes(i)?;
    let (_, bloom) = BloomFilter::parse(parse::Input::new(bloom_bytes)).map_err(|e| e.lift())?;
    Ok((i, Have { last_sync, bloom }))
}

impl Message {
    pub fn decode(input: &[u8]) -> Result<Self, ReadMessageError> {
        let input = parse::Input::new(input);
        match Self::parse(input) {
            Ok((_, msg)) => Ok(msg),
            Err(parse::ParseError::Error(e)) => Err(e),
            Err(parse::ParseError::Incomplete(_)) => Err(ReadMessageError::NotEnoughInput),
        }
    }

    pub(crate) fn parse(input: parse::Input<'_>) -> parse::ParseResult<'_, Self, ReadMessageError> {
        let (i, message_type) = parse::take1(input)?;
        if message_type != MESSAGE_TYPE_SYNC {
            return Err(parse::ParseError::Error(ReadMessageError::WrongType {
                expected_one_of: vec![MESSAGE_TYPE_SYNC],
                found: message_type,
            }));
        }

        let (i, heads) = parse::length_prefixed(parse::change_hash)(i)?;
        let (i, need) = parse::length_prefixed(parse::change_hash)(i)?;
        let (i, have) = parse::length_prefixed(parse_have)(i)?;

        let change_parser = |i| {
            let (i, bytes) = parse::length_prefixed_bytes(i)?;
            let (_, change) =
                StoredChange::parse(parse::Input::new(bytes)).map_err(|e| e.lift())?;
            Ok((i, change))
        };
        let (i, stored_changes) = parse::length_prefixed(change_parser)(i)?;
        let changes_len = stored_changes.len();
        let changes: Vec<Change> = stored_changes
            .into_iter()
            .try_fold::<_, _, Result<_, ReadMessageError>>(
                Vec::with_capacity(changes_len),
                |mut acc, stored| {
                    let change = Change::new_from_unverified(stored.into_owned(), None)
                        .map_err(ReadMessageError::ReadChangeOps)?;
                    acc.push(change);
                    Ok(acc)
                },
            )?;

        Ok((
            i,
            Message {
                heads,
                need,
                have,
                changes,
            },
        ))
    }

    pub fn encode(mut self) -> Vec<u8> {
        let mut buf = vec![MESSAGE_TYPE_SYNC];

        encode_hashes(&mut buf, &self.heads);
        encode_hashes(&mut buf, &self.need);
        encode_many(&mut buf, self.have.iter(), |buf, h| {
            encode_hashes(buf, &h.last_sync);
            leb128::write::unsigned(buf, h.bloom.to_bytes().len() as u64).unwrap();
            buf.extend(h.bloom.to_bytes());
        });

        encode_many(&mut buf, self.changes.iter_mut(), |buf, change| {
            leb128::write::unsigned(buf, change.raw_bytes().len() as u64).unwrap();
            buf.extend::<&[u8]>(change.raw_bytes().as_ref())
        });

        buf
    }
}

fn encode_many<'a, I, It, F>(out: &mut Vec<u8>, data: I, f: F)
where
    I: Iterator<Item = It> + ExactSizeIterator + 'a,
    F: Fn(&mut Vec<u8>, It),
{
    leb128::write::unsigned(out, data.len() as u64).unwrap();
    for datum in data {
        f(out, datum)
    }
}

fn encode_hashes(buf: &mut Vec<u8>, hashes: &[ChangeHash]) {
    debug_assert!(
        hashes.windows(2).all(|h| h[0] <= h[1]),
        "hashes were not sorted"
    );
    encode_many(buf, hashes.iter(), |buf, hash| buf.extend(hash.as_bytes()))
}

fn advance_heads(
    my_old_heads: &HashSet<&ChangeHash>,
    my_new_heads: &HashSet<ChangeHash>,
    our_old_shared_heads: &[ChangeHash],
) -> Vec<ChangeHash> {
    let new_heads = my_new_heads
        .iter()
        .filter(|head| !my_old_heads.contains(head))
        .copied()
        .collect::<Vec<_>>();

    let common_heads = our_old_shared_heads
        .iter()
        .filter(|head| my_new_heads.contains(head))
        .copied()
        .collect::<Vec<_>>();

    let mut advanced_heads = HashSet::with_capacity(new_heads.len() + common_heads.len());
    for head in new_heads.into_iter().chain(common_heads) {
        advanced_heads.insert(head);
    }
    let mut advanced_heads = advanced_heads.into_iter().collect::<Vec<_>>();
    advanced_heads.sort();
    advanced_heads
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change::gen::gen_change;
    use crate::storage::parse::Input;
    use crate::transaction::Transactable;
    use crate::types::gen::gen_hash;
    use crate::ActorId;
    use proptest::prelude::*;

    prop_compose! {
        fn gen_bloom()(hashes in gen_sorted_hashes(0..10)) -> BloomFilter {
            BloomFilter::from_hashes(hashes.into_iter())
        }
    }

    prop_compose! {
        fn gen_have()(bloom in gen_bloom(), last_sync in gen_sorted_hashes(0..10))  -> Have {
            Have {
                bloom,
                last_sync,
            }
        }
    }

    fn gen_sorted_hashes(size: std::ops::Range<usize>) -> impl Strategy<Value = Vec<ChangeHash>> {
        proptest::collection::vec(gen_hash(), size).prop_map(|mut h| {
            h.sort();
            h
        })
    }

    prop_compose! {
        fn gen_sync_message()(
            heads in gen_sorted_hashes(0..10),
            need in gen_sorted_hashes(0..10),
            have in proptest::collection::vec(gen_have(), 0..10),
            changes in proptest::collection::vec(gen_change(), 0..10),
        ) -> Message {
            Message {
                heads,
                need,
                have,
                changes,
            }
        }

    }

    #[test]
    fn encode_decode_empty_message() {
        let msg = Message {
            heads: vec![],
            need: vec![],
            have: vec![],
            changes: vec![],
        };
        let encoded = msg.encode();
        Message::parse(Input::new(&encoded)).unwrap();
    }

    proptest! {
        #[test]
        fn encode_decode_message(msg in gen_sync_message()) {
            let encoded = msg.clone().encode();
            let (i, decoded) = Message::parse(Input::new(&encoded)).unwrap();
            assert!(i.is_empty());
            assert_eq!(msg, decoded);
        }
    }

    #[test]
    fn generate_sync_message_twice_does_nothing() {
        let mut doc = crate::AutoCommit::new();
        doc.put(crate::ROOT, "key", "value").unwrap();
        let mut sync_state = State::new();

        assert!(doc.sync().generate_sync_message(&mut sync_state).is_some());
        assert!(doc.sync().generate_sync_message(&mut sync_state).is_none());
    }

    #[test]
    fn should_not_reply_if_we_have_no_data() {
        let mut doc1 = crate::AutoCommit::new();
        let mut doc2 = crate::AutoCommit::new();
        let mut s1 = State::new();
        let mut s2 = State::new();
        let m1 = doc1
            .sync()
            .generate_sync_message(&mut s1)
            .expect("message was none");

        doc2.sync().receive_sync_message(&mut s2, m1).unwrap();
        let m2 = doc2.sync().generate_sync_message(&mut s2);
        assert!(m2.is_none());
    }

    #[test]
    fn should_allow_simultaneous_messages_during_synchronisation() {
        // create & synchronize two nodes
        let mut doc1 = crate::AutoCommit::new().with_actor(ActorId::try_from("abc123").unwrap());
        let mut doc2 = crate::AutoCommit::new().with_actor(ActorId::try_from("def456").unwrap());
        let mut s1 = State::new();
        let mut s2 = State::new();

        for i in 0..5 {
            doc1.put(&crate::ROOT, "x", i).unwrap();
            doc1.commit();
            doc2.put(&crate::ROOT, "y", i).unwrap();
            doc2.commit();
        }

        let head1 = doc1.get_heads()[0];
        let head2 = doc2.get_heads()[0];

        //// both sides report what they have but have no shared peer state
        let msg1to2 = doc1
            .sync()
            .generate_sync_message(&mut s1)
            .expect("initial sync from 1 to 2 was None");
        let msg2to1 = doc2
            .sync()
            .generate_sync_message(&mut s2)
            .expect("initial sync message from 2 to 1 was None");
        assert_eq!(msg1to2.changes.len(), 0);
        assert_eq!(msg1to2.have[0].last_sync.len(), 0);
        assert_eq!(msg2to1.changes.len(), 0);
        assert_eq!(msg2to1.have[0].last_sync.len(), 0);

        //// doc1 and doc2 receive that message and update sync state
        doc1.sync().receive_sync_message(&mut s1, msg2to1).unwrap();
        doc2.sync().receive_sync_message(&mut s2, msg1to2).unwrap();

        //// now both reply with their local changes the other lacks
        //// (standard warning that 1% of the time this will result in a "need" message)
        let msg1to2 = doc1
            .sync()
            .generate_sync_message(&mut s1)
            .expect("first reply from 1 to 2 was None");
        assert_eq!(msg1to2.changes.len(), 5);

        let msg2to1 = doc2
            .sync()
            .generate_sync_message(&mut s2)
            .expect("first reply from 2 to 1 was None");
        assert_eq!(msg2to1.changes.len(), 5);

        //// both should now apply the changes
        doc1.sync().receive_sync_message(&mut s1, msg2to1).unwrap();
        assert_eq!(doc1.get_missing_deps(&[]), Vec::new());

        doc2.sync().receive_sync_message(&mut s2, msg1to2).unwrap();
        assert_eq!(doc2.get_missing_deps(&[]), Vec::new());

        //// The response acknowledges the changes received and sends no further changes
        let msg1to2 = doc1
            .sync()
            .generate_sync_message(&mut s1)
            .expect("second reply from 1 to 2 was None");
        assert_eq!(msg1to2.changes.len(), 0);
        let msg2to1 = doc2
            .sync()
            .generate_sync_message(&mut s2)
            .expect("second reply from 2 to 1 was None");
        assert_eq!(msg2to1.changes.len(), 0);

        //// After receiving acknowledgements, their shared heads should be equal
        doc1.sync().receive_sync_message(&mut s1, msg2to1).unwrap();
        doc2.sync().receive_sync_message(&mut s2, msg1to2).unwrap();

        assert_eq!(s1.shared_heads, s2.shared_heads);

        //// We're in sync, no more messages required
        assert!(doc1.sync().generate_sync_message(&mut s1).is_none());
        assert!(doc2.sync().generate_sync_message(&mut s2).is_none());

        //// If we make one more change and start another sync then its lastSync should be updated
        doc1.put(crate::ROOT, "x", 5).unwrap();
        doc1.commit();
        let msg1to2 = doc1
            .sync()
            .generate_sync_message(&mut s1)
            .expect("third reply from 1 to 2 was None");
        let mut expected_heads = vec![head1, head2];
        expected_heads.sort();
        let mut actual_heads = msg1to2.have[0].last_sync.clone();
        actual_heads.sort();
        assert_eq!(actual_heads, expected_heads);
    }

    #[test]
    fn should_handle_false_positive_head() {
        // Scenario:                                                            ,-- n1
        // c0 <-- c1 <-- c2 <-- c3 <-- c4 <-- c5 <-- c6 <-- c7 <-- c8 <-- c9 <-+
        //                                                                      `-- n2
        // where n2 is a false positive in the Bloom filter containing {n1}.
        // lastSync is c9.

        let mut doc1 = crate::AutoCommit::new().with_actor(ActorId::try_from("abc123").unwrap());
        let mut doc2 = crate::AutoCommit::new().with_actor(ActorId::try_from("def456").unwrap());
        let mut s1 = State::new();
        let mut s2 = State::new();

        for i in 0..10 {
            doc1.put(crate::ROOT, "x", i).unwrap();
            doc1.commit();
        }

        sync(&mut doc1, &mut doc2, &mut s1, &mut s2);

        // search for false positive; see comment above
        let mut i = 0;
        let (mut doc1, mut doc2) = loop {
            let mut doc1copy = doc1
                .clone()
                .with_actor(ActorId::try_from("01234567").unwrap());
            let val1 = format!("{} @ n1", i);
            doc1copy.put(crate::ROOT, "x", val1).unwrap();
            doc1copy.commit();

            let mut doc2copy = doc1
                .clone()
                .with_actor(ActorId::try_from("89abcdef").unwrap());
            let val2 = format!("{} @ n2", i);
            doc2copy.put(crate::ROOT, "x", val2).unwrap();
            doc2copy.commit();

            let n1_bloom = BloomFilter::from_hashes(doc1copy.get_heads().into_iter());
            if n1_bloom.contains_hash(&doc2copy.get_heads()[0]) {
                break (doc1copy, doc2copy);
            }
            i += 1;
        };

        let mut all_heads = doc1.get_heads();
        all_heads.extend(doc2.get_heads());
        all_heads.sort();

        // reset sync states
        let (_, mut s1) = State::parse(Input::new(s1.encode().as_slice())).unwrap();
        let (_, mut s2) = State::parse(Input::new(s2.encode().as_slice())).unwrap();
        sync(&mut doc1, &mut doc2, &mut s1, &mut s2);
        assert_eq!(doc1.get_heads(), all_heads);
        assert_eq!(doc2.get_heads(), all_heads);
    }

    #[test]
    fn should_handle_chains_of_false_positives() {
        //// Scenario:                         ,-- c5
        //// c0 <-- c1 <-- c2 <-- c3 <-- c4 <-+
        ////                                   `-- n2c1 <-- n2c2 <-- n2c3
        //// where n2c1 and n2c2 are both false positives in the Bloom filter containing {c5}.
        //// lastSync is c4.
        let mut doc1 = crate::AutoCommit::new().with_actor(ActorId::try_from("abc123").unwrap());
        let mut doc2 = crate::AutoCommit::new().with_actor(ActorId::try_from("def456").unwrap());
        let mut s1 = State::new();
        let mut s2 = State::new();

        for i in 0..10 {
            doc1.put(crate::ROOT, "x", i).unwrap();
            doc1.commit();
        }

        sync(&mut doc1, &mut doc2, &mut s1, &mut s2);

        doc1.put(crate::ROOT, "x", 5).unwrap();
        doc1.commit();
        let bloom = BloomFilter::from_hashes(doc1.get_heads().into_iter());

        // search for false positive; see comment above
        let mut i = 0;
        let mut doc2 = loop {
            let mut doc = doc2
                .fork()
                .with_actor(ActorId::try_from("89abcdef").unwrap());
            doc.put(crate::ROOT, "x", format!("{} at 89abdef", i))
                .unwrap();
            doc.commit();
            if bloom.contains_hash(&doc.get_heads()[0]) {
                break doc;
            }
            i += 1;
        };

        // find another false positive building on the first
        i = 0;
        let mut doc2 = loop {
            let mut doc = doc2
                .fork()
                .with_actor(ActorId::try_from("89abcdef").unwrap());
            doc.put(crate::ROOT, "x", format!("{} again", i)).unwrap();
            doc.commit();
            if bloom.contains_hash(&doc.get_heads()[0]) {
                break doc;
            }
            i += 1;
        };

        doc2.put(crate::ROOT, "x", "final @ 89abcdef").unwrap();

        let mut all_heads = doc1.get_heads();
        all_heads.extend(doc2.get_heads());
        all_heads.sort();

        let (_, mut s1) = State::parse(Input::new(s1.encode().as_slice())).unwrap();
        let (_, mut s2) = State::parse(Input::new(s2.encode().as_slice())).unwrap();
        sync(&mut doc1, &mut doc2, &mut s1, &mut s2);
        assert_eq!(doc1.get_heads(), all_heads);
        assert_eq!(doc2.get_heads(), all_heads);
    }

    #[test]
    fn should_handle_lots_of_branching_and_merging() {
        let mut doc1 = crate::AutoCommit::new().with_actor(ActorId::try_from("01234567").unwrap());
        let mut doc2 = crate::AutoCommit::new().with_actor(ActorId::try_from("89abcdef").unwrap());
        let mut doc3 = crate::AutoCommit::new().with_actor(ActorId::try_from("fedcba98").unwrap());
        let mut s1 = State::new();
        let mut s2 = State::new();

        doc1.put(crate::ROOT, "x", 0).unwrap();
        let change1 = doc1.get_last_local_change().unwrap().clone();

        doc2.apply_changes([change1.clone()]).unwrap();
        doc3.apply_changes([change1]).unwrap();

        doc3.put(crate::ROOT, "x", 1).unwrap();

        ////        - n1c1 <------ n1c2 <------ n1c3 <-- etc. <-- n1c20 <------ n1c21
        ////       /          \/           \/                              \/
        ////      /           /\           /\                              /\
        //// c0 <---- n2c1 <------ n2c2 <------ n2c3 <-- etc. <-- n2c20 <------ n2c21
        ////      \                                                          /
        ////       ---------------------------------------------- n3c1 <-----
        for i in 1..20 {
            doc1.put(crate::ROOT, "n1", i).unwrap();
            doc2.put(crate::ROOT, "n2", i).unwrap();
            let change1 = doc1.get_last_local_change().unwrap().clone();
            let change2 = doc2.get_last_local_change().unwrap().clone();
            doc1.apply_changes([change2.clone()]).unwrap();
            doc2.apply_changes([change1]).unwrap();
        }

        sync(&mut doc1, &mut doc2, &mut s1, &mut s2);

        //// Having n3's last change concurrent to the last sync heads forces us into the slower code path
        let change3 = doc3.get_last_local_change().unwrap().clone();
        doc2.apply_changes([change3]).unwrap();

        doc1.put(crate::ROOT, "n1", "final").unwrap();
        doc2.put(crate::ROOT, "n1", "final").unwrap();

        sync(&mut doc1, &mut doc2, &mut s1, &mut s2);

        assert_eq!(doc1.get_heads(), doc2.get_heads());
    }

    fn sync(
        a: &mut crate::AutoCommit,
        b: &mut crate::AutoCommit,
        a_sync_state: &mut State,
        b_sync_state: &mut State,
    ) {
        //function sync(a: Automerge, b: Automerge, aSyncState = initSyncState(), bSyncState = initSyncState()) {
        const MAX_ITER: usize = 10;
        let mut iterations = 0;

        loop {
            let a_to_b = a.sync().generate_sync_message(a_sync_state);
            let b_to_a = b.sync().generate_sync_message(b_sync_state);
            if a_to_b.is_none() && b_to_a.is_none() {
                break;
            }
            if iterations > MAX_ITER {
                panic!("failed to sync in {} iterations", MAX_ITER);
            }
            if let Some(msg) = a_to_b {
                b.sync().receive_sync_message(b_sync_state, msg).unwrap()
            }
            if let Some(msg) = b_to_a {
                a.sync().receive_sync_message(a_sync_state, msg).unwrap()
            }
            iterations += 1;
        }
    }
}
