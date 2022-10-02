use itertools::Itertools;
use serde::ser::SerializeMap;
use std::collections::{HashMap, HashSet};

use crate::{
    storage::{parse, Change as StoredChange, ReadChangeOpError},
    ApplyOptions, Automerge, AutomergeError, Change, ChangeHash, OpObserver,
};

mod bloom;
mod state;

pub use bloom::BloomFilter;
pub use state::DecodeError as DecodeStateError;
pub use state::{Have, State};

const MESSAGE_TYPE_SYNC: u8 = 0x42; // first byte of a sync message, for identification

impl Automerge {
    pub fn generate_sync_message(&self, sync_state: &mut State) -> Option<Message> {
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

        if heads_unchanged && heads_equal && changes_to_send.is_empty() {
            return None;
        }

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

        Some(sync_message)
    }

    pub fn receive_sync_message(
        &mut self,
        sync_state: &mut State,
        message: Message,
    ) -> Result<(), AutomergeError> {
        self.receive_sync_message_with::<()>(sync_state, message, ApplyOptions::default())
    }

    pub fn receive_sync_message_with<'a, Obs: OpObserver>(
        &mut self,
        sync_state: &mut State,
        message: Message,
        options: ApplyOptions<'a, Obs>,
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
            self.apply_changes_with(message_changes, options)?;
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

        let known_heads = message_heads
            .iter()
            .filter(|head| self.get_change_by_hash(head).is_some())
            .collect::<Vec<_>>();
        if known_heads.len() == message_heads.len() {
            sync_state.shared_heads = message_heads.clone();
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

    fn make_bloom_filter(&self, last_sync: Vec<ChangeHash>) -> Have {
        let new_changes = self
            .get_changes(&last_sync)
            .expect("Should have only used hashes that are in the document");
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

            let changes = self.get_changes(&last_sync_hashes)?;

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
                hashes_to_send.insert(*hash);
                if !change_hashes.contains(hash) {
                    let change = self.get_change_by_hash(hash);
                    if let Some(change) = change {
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
            buf.extend(change.raw_bytes().as_ref())
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
    use crate::types::gen::gen_hash;
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
}
