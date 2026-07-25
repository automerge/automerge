//! This entire module is a copy of the sync module before we introduced the v2 message type and
//! only exists in order to be able to write tests against that old implementation.

use itertools::Itertools;
use serde::ser::SerializeMap;
use std::collections::{HashMap, HashSet};

use crate::parse::{self, Input};
use crate::{State as V2State, Sync};
use automerge::{
    transaction::Transactable, AutoCommit, Automerge, AutomergeError, Change, ChangeHash, ReadDoc,
    ROOT,
};

mod bloom;
mod state;

pub(crate) use bloom::BloomFilter;
use state::{Have, State};

use test_log::test;

const MESSAGE_TYPE_SYNC: u8 = 0x42; // first byte of a sync message, for identification

/// The pre-v2 protocol, frozen so the wire format can be tested
/// against the current implementation.
struct SyncV1;

impl SyncV1 {
    fn generate_sync_message_v1(doc: &Automerge, sync_state: &mut State) -> Option<Message> {
        let our_heads = doc.get_head_hashes();

        let our_need = doc
            .missing_deps_with_queued(sync_state.their_heads.as_deref().unwrap_or(&[]))
            .unwrap();

        let their_heads_set = if let Some(ref heads) = sync_state.their_heads {
            heads.iter().collect::<HashSet<_>>()
        } else {
            HashSet::new()
        };
        let our_have = if our_need.iter().all(|hash| their_heads_set.contains(hash)) {
            vec![Self::make_bloom_filter_v1(
                doc,
                sync_state.shared_heads.clone(),
            )]
        } else {
            Vec::new()
        };

        if let Some(ref their_have) = sync_state.their_have {
            if let Some(first_have) = their_have.first().as_ref() {
                if !first_have
                    .last_sync
                    .iter()
                    .all(|hash| doc.get_change_by_hash(hash).unwrap().is_some())
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
            Self::get_changes_to_send_v1(doc, their_have, their_need)
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

        if heads_unchanged && sync_state.have_responded {
            if heads_equal && changes_to_send.is_empty() {
                return None;
            }
            if sync_state.in_flight {
                return None;
            }
        }

        sync_state.have_responded = true;
        sync_state.last_sent_heads.clone_from(&our_heads);
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

    fn receive_sync_message_v1(
        doc: &mut Automerge,
        sync_state: &mut State,
        message: Message,
    ) -> Result<(), AutomergeError> {
        Self::receive_sync_message_inner_v1(doc, sync_state, message)
    }

    fn make_bloom_filter_v1(doc: &Automerge, last_sync: Vec<ChangeHash>) -> Have {
        let last_sync_ids = doc.hashes_to_change_ids(&last_sync).unwrap();
        let new_changes = doc.get_changes(&last_sync_ids).unwrap();
        let hashes = new_changes.iter().map(|change| change.hash());
        Have {
            last_sync,
            bloom: BloomFilter::from_hashes(hashes),
        }
    }

    fn get_changes_to_send_v1(
        doc: &Automerge,
        have: &[Have],
        need: &[ChangeHash],
    ) -> Result<Vec<Change>, AutomergeError> {
        if have.is_empty() {
            Ok(need
                .iter()
                .filter_map(|hash| doc.get_change_by_hash(hash).unwrap())
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

            let last_sync_ids = doc.hashes_to_change_ids(&last_sync_hashes).unwrap();
            let changes = doc.get_changes(&last_sync_ids).unwrap();

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
                    if let Some(change) = doc.get_change_by_hash(hash).unwrap() {
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

    fn receive_sync_message_inner_v1(
        doc: &mut Automerge,
        sync_state: &mut State,
        message: Message,
    ) -> Result<(), AutomergeError> {
        sync_state.in_flight = false;
        let before_heads = doc.get_head_hashes();

        let Message {
            heads: message_heads,
            changes: message_changes,
            need: message_need,
            have: message_have,
        } = message;

        let changes_is_empty = message_changes.is_empty();
        if !changes_is_empty {
            doc.apply_changes(message_changes)?;
            sync_state.shared_heads = advance_heads(
                &before_heads.iter().collect(),
                &doc.get_head_hashes().into_iter().collect(),
                &sync_state.shared_heads,
            );
        }

        // trim down the sent hashes to those that we know they haven't seen
        doc.remove_ancestors(&message_heads, &mut sync_state.sent_hashes)?;

        if changes_is_empty && message_heads == before_heads {
            sync_state.last_sent_heads.clone_from(&message_heads);
        }

        let known_heads = message_heads
            .iter()
            .filter(|head| doc.get_change_by_hash(head).unwrap().is_some())
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
enum ReadMessageError {
    #[error("expected {expected_one_of:?} but found {found}")]
    WrongType { expected_one_of: Vec<u8>, found: u8 },
    #[error("{0}")]
    Parse(String),
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
struct Message {
    /// The heads of the sender.
    heads: Vec<ChangeHash>,
    /// The hashes of any changes that are being explicitly requested from the recipient.
    need: Vec<ChangeHash>,
    /// A summary of the changes that the sender already has.
    have: Vec<Have>,
    /// The changes for the recipient to apply.
    changes: Vec<Change>,
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
                .map(automerge::ExpandedChange::from)
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
    fn decode(input: &[u8]) -> Result<Self, ReadMessageError> {
        let input = parse::Input::new(input);
        match Self::parse(input) {
            Ok((_, msg)) => Ok(msg),
            Err(parse::ParseError::Error(e)) => Err(e),
            Err(parse::ParseError::Incomplete(_)) => Err(ReadMessageError::NotEnoughInput),
        }
    }

    fn parse(input: parse::Input<'_>) -> parse::ParseResult<'_, Self, ReadMessageError> {
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
            let change = Change::try_from(bytes)
                .map_err(|e| parse::ParseError::Error(ReadMessageError::Parse(e.to_string())))?;
            Ok((i, change))
        };
        let (i, changes) = parse::length_prefixed(change_parser)(i)?;

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

    fn encode(mut self) -> Vec<u8> {
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

#[test]
fn sync_from_v1_to_v2() {
    let mut doc1 = AutoCommit::new();
    doc1.enable_audit_mode().unwrap();
    let mut doc2 = AutoCommit::new();
    doc2.enable_audit_mode().unwrap();

    doc1.put(ROOT, "foo", "bar").unwrap();
    doc2.put(ROOT, "baz", "quux").unwrap();
    doc1.commit().unwrap();
    doc2.commit().unwrap();

    let mut sync_state1 = State::new();
    let mut sync_state2 = V2State::new();

    sync_v1_to_v2(
        doc1.document_mut(),
        doc2.document_mut(),
        &mut sync_state1,
        &mut sync_state2,
    );

    assert_eq!(doc1.get_head_hashes(), doc2.get_head_hashes());
}

#[test]
fn sync_from_v2_to_v1() {
    let mut doc1 = AutoCommit::new();
    doc1.enable_audit_mode().unwrap();
    let mut doc2 = AutoCommit::new();
    doc2.enable_audit_mode().unwrap();

    doc1.put(ROOT, "foo", "bar").unwrap();
    doc2.put(ROOT, "baz", "quux").unwrap();
    doc1.commit().unwrap();
    doc2.commit().unwrap();

    let mut sync_state2 = V2State::new();
    let mut sync_state1 = State::new();

    sync_v2_to_v1(
        doc1.document_mut(),
        doc2.document_mut(),
        &mut sync_state1,
        &mut sync_state2,
    );

    assert_eq!(doc1.get_head_hashes(), doc2.get_head_hashes());
}

#[test]
fn sync_v1_to_v2_with_compressed_change() {
    // Reproduce an issue where the v2 peer was sending changes as compressed bytes rather than
    // uncompressed, which the old implementation couldn't handle.
    let mut doc1 = AutoCommit::new();
    doc1.enable_audit_mode().unwrap();
    let list = doc1
        .put_object(ROOT, "list", automerge::ObjType::List)
        .unwrap();
    for index in 0..1000 {
        doc1.insert(&list, index, index as i64).unwrap();
    }
    doc1.commit().unwrap();

    let mut doc2 = AutoCommit::new();
    doc2.enable_audit_mode().unwrap();

    let mut sync_state2 = V2State::new();
    let mut sync_state1 = State::new();

    sync_v1_to_v2(
        doc2.document_mut(),
        doc1.document_mut(),
        &mut sync_state1,
        &mut sync_state2,
    );

    assert_eq!(doc1.get_head_hashes(), doc2.get_head_hashes());

    doc1.put(ROOT, "foo", "bar").unwrap();
    doc2.put(ROOT, "baz", "quux").unwrap();
    doc1.commit().unwrap();
    doc2.commit().unwrap();
}

/// Run the sync protocol with the v1 peer starting first
fn sync_v1_to_v2(
    v1: &mut Automerge,
    v2: &mut Automerge,
    a_sync_state: &mut State,
    b_sync_state: &mut V2State,
) {
    const MAX_ITER: usize = 10;
    let mut iterations = 0;

    loop {
        let a_to_b = SyncV1::generate_sync_message_v1(v1, a_sync_state);
        let a_to_b_is_none = a_to_b.is_none();
        if iterations > MAX_ITER {
            panic!("failed to sync in {} iterations", MAX_ITER);
        }
        if let Some(msg) = a_to_b {
            tracing::debug!(msg=?msg, "sending message from v1 to v2");
            let encoded = msg.encode();
            let (_, decoded) = crate::Message::parse(Input::new(&encoded))
                .expect("v1 message should decode as a v2 message");
            tracing::debug!(decoded=?decoded, "receiving decoded message on v2");
            Sync::receive_sync_message(v2, b_sync_state, decoded).unwrap()
        }
        let b_to_a = Sync::generate_sync_message(v2, b_sync_state).unwrap();
        let b_to_a_is_none = b_to_a.is_none();
        if let Some(msg) = b_to_a {
            tracing::debug!(msg=?msg, "sending message from v2 to v1");
            let encoded = msg.encode();
            let (_, decoded) = Message::parse(Input::new(&encoded))
                .expect("v1 message should decode as a v2 message");
            tracing::debug!(decoded=?decoded, "receiving decoded message on v1");
            SyncV1::receive_sync_message_v1(v1, a_sync_state, decoded).unwrap()
        }
        if a_to_b_is_none && b_to_a_is_none {
            break;
        }
        iterations += 1;
    }
}

/// Run the sync protocol with the v2 peer starting first
fn sync_v2_to_v1(
    v1: &mut Automerge,
    v2: &mut Automerge,
    v1_sync_state: &mut State,
    v2_sync_state: &mut V2State,
) {
    const MAX_ITER: usize = 10;
    let mut iterations = 0;

    loop {
        let a_to_b = Sync::generate_sync_message(v2, v2_sync_state).unwrap();
        let a_to_b_is_none = a_to_b.is_none();
        if iterations > MAX_ITER {
            panic!("failed to sync in {} iterations", MAX_ITER);
        }
        if let Some(msg) = a_to_b {
            let encoded = msg.encode();
            let decoded =
                Message::decode(&encoded).expect("v1 message should decode as a v2 message");
            SyncV1::receive_sync_message_v1(v1, v1_sync_state, decoded).unwrap()
        }
        let b_to_a = SyncV1::generate_sync_message_v1(v1, v1_sync_state);
        let b_to_a_is_none = b_to_a.is_none();
        if let Some(msg) = b_to_a {
            let encoded = msg.encode();
            let decoded =
                crate::Message::decode(&encoded).expect("v1 message should decode as a v2 message");
            Sync::receive_sync_message(v2, v2_sync_state, decoded).unwrap()
        }
        if a_to_b_is_none && b_to_a_is_none {
            break;
        }
        iterations += 1;
    }
}
