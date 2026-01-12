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
//!   [`SyncDoc::generate_sync_message()`] to generate new sync message and sends
//!   it to the receiving peer.
//! * The receiving peer receives a message from the initiator, creates a new
//!   [`State`], and calls [`SyncDoc::receive_sync_message()`] on it's view of the
//!   document
//! * The receiving peer then calls [`SyncDoc::generate_sync_message()`] to generate
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
    storage::{parse, ReadChangeOpError},
    Automerge, AutomergeError, ChangeHash, ReadDoc,
};

mod bloom;
mod message_builder;
mod state;
use message_builder::MessageBuilder;

#[cfg(test)]
mod v1_compat_test;

pub use bloom::{BloomFilter, DecodeError as DecodeBloomError};
pub use state::DecodeError as DecodeStateError;
pub use state::{Have, State};

/// A document which can take part in the sync protocol
///
/// See the [module level documentation](crate::sync) for more details.
pub trait SyncDoc {
    /// Generate a sync message for the remote peer represented by `sync_state`
    ///
    /// If this returns [`None`] then there are no new messages to send, either because we are
    /// waiting for an acknolwedgement of an in-flight message, or because the remote is up to
    /// date.
    ///
    /// * `sync_state` - The [`State`] for this document and the remote peer
    /// * `message` - The [`Message`] to receive
    /// * `patch_log` - A [`PatchLog`] which will be updated with any changes that are made to the current state of the document due to the received sync message
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
    /// If this returns [`None`] then there are no new messages to send, either because we are
    /// waiting for an acknolwedgement of an in-flight message, or because the remote is up to
    /// date.
    ///
    /// # Arguments
    ///
    /// * `sync_state` - The [`State`] for this document and the remote peer
    /// * `message` - The [`Message`] to receive
    /// * `patch_log` - A [`PatchLog`] which will be updated with any changes that are made to the current state of the document due to the received sync message
    fn receive_sync_message_log_patches(
        &mut self,
        sync_state: &mut State,
        message: Message,
        patch_log: &mut PatchLog,
    ) -> Result<(), AutomergeError>;
}

const MESSAGE_TYPE_SYNC: u8 = 0x42; // first byte of a sync message, for identification
const MESSAGE_TYPE_SYNC_V2: u8 = 0x43; // first byte of a sync message, for identification

#[derive(Clone, Debug, PartialEq)]
pub enum MessageVersion {
    V1,
    V2,
}

impl MessageVersion {
    fn parse(input: parse::Input<'_>) -> parse::ParseResult<'_, Self, ReadMessageError> {
        let (i, first_byte) = parse::take1(input)?;
        match first_byte {
            MESSAGE_TYPE_SYNC => Ok((i, Self::V1)),
            MESSAGE_TYPE_SYNC_V2 => Ok((i, Self::V2)),
            _ => Err(parse::ParseError::Error(ReadMessageError::WrongType {
                expected_one_of: vec![MESSAGE_TYPE_SYNC, MESSAGE_TYPE_SYNC_V2],
                found: first_byte,
            })),
        }
    }

    fn encode(&self) -> u8 {
        match self {
            Self::V1 => MESSAGE_TYPE_SYNC,
            Self::V2 => MESSAGE_TYPE_SYNC_V2,
        }
    }
}

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
                    .all(|hash| self.has_change(hash))
                {
                    return Some(Message::reset(our_heads));
                }
            }
        }

        let message_builder = if let Some((their_have, their_need)) = sync_state.their() {
            if sync_state.send_doc() {
                let hashes = self.change_graph.get_hashes(&[]);
                MessageBuilder::new_v2(self.save(), hashes)
            } else {
                let all_hashes = self
                    .get_hashes_to_send(their_have, their_need)
                    .expect("Should have only used hashes that are in the document");
                // deduplicate the changes to send with those we have already sent and clone it now
                let hashes: Vec<_> = all_hashes
                    .into_iter()
                    .filter(|hash| !sync_state.sent_hashes.contains(hash))
                    .collect();
                if hashes.len() > self.change_graph.len() / 3 && sync_state.supports_v2_messages() {
                    // sending more than a 1/3 of the document?  send everything
                    let all_hashes = self.change_graph.get_hashes(&[]);
                    MessageBuilder::new_v2(self.save(), all_hashes)
                } else {
                    let changes = self.get_changes_by_hashes(hashes.iter().copied()).ok()?;
                    MessageBuilder::new(changes, sync_state)
                }
            }
        } else {
            MessageBuilder::new(vec![], sync_state)
        };

        let heads_unchanged = sync_state.last_sent_heads == our_heads;

        let heads_equal = sync_state.their_heads.as_ref() == Some(&our_heads);

        if heads_unchanged && sync_state.have_responded {
            if heads_equal && message_builder.is_empty() {
                return None;
            }
            if sync_state.in_flight {
                return None;
            }
        }

        sync_state.have_responded = true;
        sync_state.last_sent_heads.clone_from(&our_heads);
        sync_state.sent_hashes.extend(message_builder.hashes());

        let sync_message = message_builder
            .heads(our_heads)
            .have(our_have)
            .need(our_need)
            .supported_capabilities(Some(vec![Capability::MessageV1, Capability::MessageV2]))
            .build();

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
    #[inline(never)]
    fn make_bloom_filter(&self, last_sync: Vec<ChangeHash>) -> Have {
        let hashes = self.change_graph.get_hashes(&last_sync);
        Have {
            last_sync,
            bloom: BloomFilter::from_hashes(hashes.iter()),
        }
    }

    #[inline(never)]
    fn get_hashes_to_send(
        &self,
        have: &[Have],
        need: &[ChangeHash],
    ) -> Result<Vec<ChangeHash>, AutomergeError> {
        if have.is_empty() {
            Ok(need.to_vec())
        } else {
            let mut last_sync_hashes = HashSet::new();
            let mut bloom_filters = Vec::with_capacity(have.len());

            for h in have {
                let Have { last_sync, bloom } = h;
                last_sync_hashes.extend(last_sync);
                bloom_filters.push(bloom);
            }
            let last_sync_hashes = last_sync_hashes.into_iter().copied().collect::<Vec<_>>();

            let hashes = self.change_graph.get_hashes(&last_sync_hashes);

            let mut change_hashes = HashSet::with_capacity(hashes.len());
            let mut dependents: HashMap<ChangeHash, Vec<ChangeHash>> = HashMap::new();
            let mut hashes_to_send = HashSet::new();

            for hash in &*hashes {
                change_hashes.insert(*hash);

                for dep in self.change_graph.deps(hash) {
                    dependents.entry(dep).or_default().push(*hash);
                }

                if bloom_filters.iter().all(|bloom| !bloom.contains_hash(hash)) {
                    hashes_to_send.insert(*hash);
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

            let mut final_hashes = Vec::with_capacity(hashes_to_send.len() + need.len());
            for hash in need {
                if !hashes_to_send.contains(hash) {
                    final_hashes.push(*hash);
                }
            }

            for hash in &*hashes {
                if hashes_to_send.contains(hash) {
                    final_hashes.push(*hash);
                }
            }
            Ok(final_hashes)
        }
    }

    #[inline(never)]
    pub(crate) fn receive_sync_message_inner(
        &mut self,
        sync_state: &mut State,
        message: Message,
        patch_log: &mut PatchLog,
    ) -> Result<(), AutomergeError> {
        sync_state.in_flight = false;
        let before_heads = self.get_heads();

        let Message {
            heads: message_heads,
            changes: message_changes,
            need: message_need,
            have: message_have,
            supported_capabilities,
            ..
        } = message;

        if let Some(caps) = supported_capabilities {
            sync_state.their_capabilities = Some(caps);
        }

        let changes_is_empty = message_changes.is_empty();
        if !changes_is_empty {
            self.load_incremental_log_patches(&message_changes.join(), patch_log)?;
            sync_state.shared_heads = advance_heads(
                &before_heads.iter().collect(),
                &self.get_heads().into_iter().collect(),
                &sync_state.shared_heads,
            );
        }

        // trim down the sent hashes to those that we know they haven't seen
        self.filter_changes(&message_heads, &mut sync_state.sent_hashes)?;

        if changes_is_empty && message_heads == before_heads {
            sync_state.last_sent_heads.clone_from(&message_heads);
        }

        let known_heads = message_heads
            .iter()
            .filter(|head| self.has_change(head))
            .collect::<Vec<_>>();
        if known_heads.len() == message_heads.len() {
            sync_state.shared_heads.clone_from(&message_heads);
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
///
/// ## Notes about encoding
///
/// There are two versions of the sync message, V1 and V2. The V1 message is the original message
/// which automerge shipped with and V2 is an extension which allows for encoding the changes as
/// either a list of changes or as a compressed document format. This makes syncing up for the
/// first time faster.
///
/// Encoding this in a backwards compatible way is a bit tricky. The wire format of the v1 message
/// didn't allow for any forwards compatible changes. In order to accomodate this the first message
/// a peer sends is a V1 message with a length previxed `Vec<Capability>` appended to it. For old
/// implementations this appended data is just ignored but new implementations read it and store
/// the advertised capabilities on the sync state. This allows new implementations to discover if
/// the remote peer supports the V2 message format (the `Capability::MessageV2` capability) and if
/// so send a V2 message.
#[derive(Clone, Debug, PartialEq)]
pub struct Message {
    /// The heads of the sender.
    pub heads: Vec<ChangeHash>,
    /// The hashes of any changes that are being explicitly requested from the recipient.
    pub need: Vec<ChangeHash>,
    /// A summary of the changes that the sender already has.
    pub have: Vec<Have>,
    /// The changes for the recipient to apply.
    ///
    /// This is a Vec of bytes which should be passed to `Automerge::load_incremental`. The reason
    /// it is a `Vec<Vec<u8>>` and not a `Vec<u8>` is that the V1 message format is a sequence of
    /// change chunks, each of which is length delimited. The V2 message format is a single length
    /// delimited chunk but we nest it inside a Vec for backwards compatibility.
    pub changes: ChunkList,
    /// The capabilities the sender supports
    pub supported_capabilities: Option<Vec<Capability>>,
    /// What version to encode this message as
    pub version: MessageVersion,
}

/// An array of changes, each of which should be passed to [`Automerge::load_incremental()`]
#[derive(Clone, Debug, PartialEq)]
pub struct ChunkList(Vec<Vec<u8>>);

impl From<Vec<Vec<u8>>> for ChunkList {
    fn from(v: Vec<Vec<u8>>) -> Self {
        Self(v)
    }
}

impl From<Vec<u8>> for ChunkList {
    fn from(v: Vec<u8>) -> Self {
        Self(vec![v])
    }
}

impl ChunkList {
    fn parse(i: parse::Input<'_>) -> parse::ParseResult<'_, Self, ReadMessageError> {
        let change_parser = |i| {
            let (i, bytes) = parse::length_prefixed_bytes(i)?;
            Ok((i, bytes.to_vec()))
        };
        let (i, stored_changes) = parse::length_prefixed(change_parser)(i)?;
        Ok((i, Self(stored_changes)))
    }

    pub fn empty() -> Self {
        Self(Vec::new())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &[u8]> {
        self.0.iter().map(|v| v.as_slice())
    }

    pub(crate) fn join(&self) -> Vec<u8> {
        let total: usize = self.0.iter().map(Vec::len).sum();
        let mut result = Vec::with_capacity(total);

        for v in &self.0 {
            result.extend(v);
        }

        result
    }
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
        map.serialize_entry("changes", &self.changes.0)?;
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
    pub(crate) fn reset(our_heads: Vec<ChangeHash>) -> Message {
        Message {
            heads: our_heads,
            need: Vec::new(),
            have: vec![Have::default()],
            changes: ChunkList::empty(),
            supported_capabilities: Some(vec![Capability::MessageV1, Capability::MessageV2]),
            version: MessageVersion::V1,
        }
    }

    pub fn decode(input: &[u8]) -> Result<Self, ReadMessageError> {
        let input = parse::Input::new(input);
        match Self::parse(input) {
            Ok((_, msg)) => Ok(msg),
            Err(parse::ParseError::Error(e)) => Err(e),
            Err(parse::ParseError::Incomplete(_)) => Err(ReadMessageError::NotEnoughInput),
        }
    }

    pub(crate) fn parse(input: parse::Input<'_>) -> parse::ParseResult<'_, Self, ReadMessageError> {
        let (i, message_version) = MessageVersion::parse(input)?;

        let (i, heads) = parse::length_prefixed(parse::change_hash)(i)?;
        let (i, need) = parse::length_prefixed(parse::change_hash)(i)?;
        let (i, have) = parse::length_prefixed(parse_have)(i)?;

        let (i, changes) = ChunkList::parse(i)?;
        let (i, supported_capabilities) = if !i.is_empty() {
            let (i, caps) = parse::length_prefixed(Capability::parse)(i)?;
            (i, Some(caps))
        } else {
            (i, None)
        };
        Ok((
            i,
            Message {
                heads,
                need,
                have,
                changes,
                supported_capabilities,
                version: message_version,
            },
        ))
    }

    pub fn encode(self) -> Vec<u8> {
        let mut buf = vec![self.version.encode()];

        encode_hashes(&mut buf, &self.heads);
        encode_hashes(&mut buf, &self.need);
        encode_many(&mut buf, self.have.iter(), |buf, h| {
            encode_hashes(buf, &h.last_sync);
            leb128::write::unsigned(buf, h.bloom.to_bytes().len() as u64).unwrap();
            buf.extend(h.bloom.to_bytes());
        });

        encode_many(&mut buf, self.changes.iter(), |buf, change| {
            leb128::write::unsigned(buf, change.len() as u64).unwrap();
            buf.extend::<&[u8]>(change.as_ref())
        });

        if let Some(supported_capabilities) = self.supported_capabilities {
            encode_many(&mut buf, supported_capabilities.iter(), |buf, cap| {
                cap.encode(buf);
            });
        }

        buf
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Capability {
    MessageV1,
    MessageV2,
    Unknown(u8),
}

impl Capability {
    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Capability::MessageV1 => out.push(0x01),
            Capability::MessageV2 => out.push(0x02),
            Capability::Unknown(v) => out.push(*v),
        }
    }

    fn parse(input: parse::Input<'_>) -> parse::ParseResult<'_, Self, ReadMessageError> {
        let (i, v) = parse::take1(input)?;
        match v {
            0x01 => Ok((i, Self::MessageV1)),
            0x02 => Ok((i, Self::MessageV2)),
            _ => Ok((i, Self::Unknown(v))),
        }
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
    //use crate::change::gen::gen_change;
    use crate::storage::parse::Input;
    use crate::storage::Chunk;
    use crate::transaction::Transactable;
    //use crate::types::gen::gen_hash;
    use crate::ActorId;
    //use proptest::prelude::*;

    /*
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
            fn gen_sync_message_v1()(
                heads in gen_sorted_hashes(0..10),
                need in gen_sorted_hashes(0..10),
                have in proptest::collection::vec(gen_have(), 0..10),
                changes in proptest::collection::vec(gen_change(), 0..10),
                supported_capabilities in prop_oneof![
                    Just(None),
                    Just(Some(vec![Capability::MessageV1])),
                    Just(Some(vec![Capability::MessageV2])),
                    Just(Some(vec![Capability::MessageV1, Capability::MessageV2])),
                ],
            ) -> Message {
                Message {
                    heads,
                    need,
                    have,
                    changes: changes.into_iter().map(|c| c.raw_bytes().to_vec()).collect::<Vec<Vec<u8>>>().into(),
                    supported_capabilities,
                    version: MessageVersion::V1,
                }
            }
        }
    */

    /*
        prop_compose! {
            fn gen_sync_message_v2()(
                heads in gen_sorted_hashes(0..10),
                need in gen_sorted_hashes(0..10),
                have in proptest::collection::vec(gen_have(), 0..10),
                raw in proptest::collection::vec(any::<u8>(), 0..100),
                supported_capabilities in prop_oneof![
                    Just(None),
                    Just(Some(vec![Capability::MessageV1])),
                    Just(Some(vec![Capability::MessageV2])),
                    Just(Some(vec![Capability::MessageV1, Capability::MessageV2])),
                ],
            ) -> Message {
                Message {
                    heads,
                    need,
                    have,
                    changes: ChunkList::from(raw),
                    supported_capabilities,
                    version: MessageVersion::V2,
                }
            }
        }
    */

    /*
        fn gen_sync_message() -> impl Strategy<Value = Message> {
            prop_oneof![gen_sync_message_v1(), gen_sync_message_v2(),].boxed()
        }
    */

    #[test]
    fn encode_decode_empty_message() {
        let msg = Message {
            heads: vec![],
            need: vec![],
            have: vec![],
            changes: ChunkList::empty(),
            supported_capabilities: None,
            version: MessageVersion::V2,
        };
        let encoded = msg.encode();
        Message::parse(Input::new(&encoded)).unwrap();
    }

    /*
        proptest! {
            #[test]
            fn encode_decode_message(msg in gen_sync_message()) {
                let encoded = msg.clone().encode();
                let (i, decoded) = Message::parse(Input::new(&encoded)).unwrap();
                assert!(i.is_empty());
                assert_eq!(msg, decoded);
            }
        }
    */

    #[test]
    fn generate_sync_message_twice_does_nothing() {
        let mut doc = crate::AutoCommit::new();
        doc.put(crate::ROOT, "key", "value").unwrap();
        let mut sync_state = State::new();

        assert!(doc.sync().generate_sync_message(&mut sync_state).is_some());
        assert!(doc.sync().generate_sync_message(&mut sync_state).is_none());
    }

    #[test]
    fn first_response_is_some_even_if_no_changes() {
        // The first time we generate a sync message for a given peer we should always send a
        // response so that they know what our heads are, even if we are at the same heads as them

        let mut doc1 = crate::AutoCommit::new();
        doc1.put(crate::ROOT, "key", "value").unwrap();
        let mut doc2 = doc1.fork();

        let mut s1 = State::new();
        let mut s2 = State::new();

        let m1 = doc1
            .sync()
            .generate_sync_message(&mut s1)
            .expect("message was none");

        doc2.sync().receive_sync_message(&mut s2, m1).unwrap();

        let _m2 = doc2
            .sync()
            .generate_sync_message(&mut s2)
            .expect("response was none");
    }

    #[test]
    fn should_not_reply_if_we_have_no_data_after_first_round() {
        let mut doc1 = crate::AutoCommit::new();
        let mut doc2 = crate::AutoCommit::new();
        let mut s1 = State::new();
        let mut s2 = State::new();
        let m1 = doc1
            .sync()
            .generate_sync_message(&mut s1)
            .expect("message was none");

        doc2.sync().receive_sync_message(&mut s2, m1).unwrap();
        let _m2 = doc2
            .sync()
            .generate_sync_message(&mut s2)
            .expect("first round message was none");

        let m1 = doc1.sync().generate_sync_message(&mut s1);
        assert!(m1.is_none());

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
        let Message {
            changes: changes1to2,
            ..
        } = &msg1to2;
        assert_eq!(changes1to2.len(), 0);
        assert_eq!(msg1to2.have[0].last_sync.len(), 0);
        let Message {
            changes: changes2to1,
            ..
        } = &msg2to1;
        assert_eq!(changes2to1.len(), 0);
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
        let Message {
            changes: changes1to2,
            ..
        } = &msg1to2;
        assert!(!changes1to2.is_empty());

        let msg2to1 = doc2
            .sync()
            .generate_sync_message(&mut s2)
            .expect("first reply from 2 to 1 was None");
        let Message {
            changes: changes2to1,
            ..
        } = &msg2to1;
        assert!(!changes2to1.is_empty());

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
        let Message {
            changes: changes1to2,
            ..
        } = &msg1to2;
        assert_eq!(changes1to2.len(), 0);
        let msg2to1 = doc2
            .sync()
            .generate_sync_message(&mut s2)
            .expect("second reply from 2 to 1 was None");
        let Message {
            changes: changes2to1,
            ..
        } = &msg2to1;
        assert_eq!(changes2to1.len(), 0);

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

    #[test]
    fn in_flight_logic_should_not_sabotage_concurrent_changes() {
        // This reproduces issue https://github.com/automerge/automerge/issues/702
        //
        // This problem manifested as a situation where the sync states of two
        // ends of a connection both return None from `generate_sync_message` -
        // indicating that there is nothing to send - yet the documents were
        // different at either end.

        // Because this logic depends on bloom filter false positives we have to
        // run the test many times, hence this loop
        for _ in 0..300 {
            // create two documents
            let mut doc1 = crate::AutoCommit::new();
            let mut doc2 = crate::AutoCommit::new();
            let mut s1 = State::new();
            let mut s2 = State::new();

            // get them in sync
            sync(&mut doc1, &mut doc2, &mut s1, &mut s2);

            // make a change on doc2
            doc2.put(crate::ROOT, "x", 0).unwrap();

            // generate a sync message containing the change (this should
            // alwasy be Some because we have generated new local changes)
            let msg = doc2.sync().generate_sync_message(&mut s2).unwrap();
            // Receive that sync message on doc1
            doc1.sync().receive_sync_message(&mut s1, msg).unwrap();

            // now before sending any messages back to doc2, make a change on
            // doc1
            doc1.put(crate::ROOT, "x", 1).unwrap();

            // now synchronize
            sync(&mut doc1, &mut doc2, &mut s1, &mut s2);

            // At this point both documents should be equal
            assert_eq!(doc1.get_heads(), doc2.get_heads());
        }
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

    #[test]
    fn if_first_message_has_no_heads_and_supports_v2_message_send_whole_doc() {
        let mut doc1 = crate::AutoCommit::new();
        let mut doc2 = crate::AutoCommit::new();
        doc2.put(crate::ROOT, "foo", "bar").unwrap();

        let mut s1 = State::new();
        let mut s2 = State::new();

        let outgoing = doc1
            .sync()
            .generate_sync_message(&mut s1)
            .expect("message was none");

        doc2.sync().receive_sync_message(&mut s2, outgoing).unwrap();

        let response = doc2
            .sync()
            .generate_sync_message(&mut s2)
            .expect("response was none");

        let Message { changes, .. } = response;

        let (_, chunk) = Chunk::parse(Input::new(&changes.0[0])).unwrap();
        assert!(matches!(chunk, Chunk::Document(_)));
    }
}
