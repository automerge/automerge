use std::collections::BTreeSet;

use super::{encode_hashes, BloomFilter};
use crate::storage::parse;
use crate::ChangeHash;

const SYNC_STATE_TYPE: u8 = 0x43; // first byte of an encoded sync state, for identification

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("{0:?}")]
    Parse(String),
    #[error("wrong type: expected one of {expected_one_of:?} but found {found}")]
    WrongType { expected_one_of: Vec<u8>, found: u8 },
    #[error("not enough input")]
    NotEnoughInput,
}

impl From<parse::leb128::Error> for DecodeError {
    fn from(_: parse::leb128::Error) -> Self {
        Self::Parse("bad leb128 encoding".to_string())
    }
}

/// The state of synchronisation with a peer.
///
/// This should be persisted using [`Self::encode`] when you know you will be interacting with the
/// same peer in multiple sessions. [`Self::encode`] only encodes state which should be reused
/// across connections.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct State {
    /// The hashes which we know both peers have
    pub shared_heads: Vec<ChangeHash>,
    /// The heads we last sent
    pub last_sent_heads: Vec<ChangeHash>,
    /// The heads we last received from them
    pub their_heads: Option<Vec<ChangeHash>>,
    /// Any specific changes they last said they needed
    pub their_need: Option<Vec<ChangeHash>>,
    /// The bloom filters summarising what they said they have
    pub their_have: Option<Vec<Have>>,
    /// The hashes we have sent in this session
    pub sent_hashes: BTreeSet<ChangeHash>,

    /// `generate_sync_message` should return `None` if there are no new changes to send. In
    /// particular, if there are changes in flight which the other end has not yet acknowledged we
    /// do not wish to generate duplicate sync messages. This field tracks whether the changes we
    /// expect to send to the peer based on this sync state have been sent or not. If
    /// `in_flight` is `false` then `generate_sync_message` will return a new message (provided
    /// there are in fact changes to send). If it is `true` then we don't. This flag is cleared
    /// in `receive_sync_message`.
    pub in_flight: bool,
}

/// A summary of the changes that the sender of the message already has.
/// This is implicitly a request to the recipient to send all changes that the
/// sender does not already have.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, serde::Serialize)]
pub struct Have {
    /// The heads at the time of the last successful sync with this recipient.
    pub last_sync: Vec<ChangeHash>,
    /// A bloom filter summarising all of the changes that the sender of the message has added
    /// since the last sync.
    pub bloom: BloomFilter,
}

impl State {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = vec![SYNC_STATE_TYPE];
        encode_hashes(&mut buf, &self.shared_heads);
        buf
    }

    pub fn decode(input: &[u8]) -> Result<Self, DecodeError> {
        let input = parse::Input::new(input);
        match Self::parse(input) {
            Ok((_, state)) => Ok(state),
            Err(parse::ParseError::Incomplete(_)) => Err(DecodeError::NotEnoughInput),
            Err(parse::ParseError::Error(e)) => Err(e),
        }
    }

    pub(crate) fn parse(input: parse::Input<'_>) -> parse::ParseResult<'_, Self, DecodeError> {
        let (i, record_type) = parse::take1(input)?;
        if record_type != SYNC_STATE_TYPE {
            return Err(parse::ParseError::Error(DecodeError::WrongType {
                expected_one_of: vec![SYNC_STATE_TYPE],
                found: record_type,
            }));
        }

        let (i, shared_heads) = parse::length_prefixed(parse::change_hash)(i)?;
        Ok((
            i,
            Self {
                shared_heads,
                last_sent_heads: Vec::new(),
                their_heads: None,
                their_need: None,
                their_have: Some(Vec::new()),
                sent_hashes: BTreeSet::new(),
                in_flight: false,
            },
        ))
    }
}
