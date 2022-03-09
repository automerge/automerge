use std::collections::HashSet;

#[cfg(not(feature = "storage-v2"))]
use super::decode_hashes;
use super::{encode_hashes, BloomFilter};
#[cfg(feature = "storage-v2")]
use crate::storage::parse;
use crate::ChangeHash;
#[cfg(not(feature = "storage-v2"))]
use crate::{decoding, decoding::Decoder};
#[cfg(not(feature = "storage-v2"))]
use std::borrow::Cow;

const SYNC_STATE_TYPE: u8 = 0x43; // first byte of an encoded sync state, for identification

#[cfg(feature = "storage-v2")]
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("{0:?}")]
    Parse(String),
    #[error("wrong type: expected one of {expected_one_of:?} but found {found}")]
    WrongType { expected_one_of: Vec<u8>, found: u8 },
    #[error("not enough input")]
    NotEnoughInput,
}

#[cfg(feature = "storage-v2")]
impl From<parse::ErrorKind> for DecodeError {
    fn from(k: parse::ErrorKind) -> Self {
        Self::Parse(k.to_string())
    }
}

/// The state of synchronisation with a peer.
#[derive(Debug, Clone, Default)]
pub struct State {
    pub shared_heads: Vec<ChangeHash>,
    pub last_sent_heads: Vec<ChangeHash>,
    pub their_heads: Option<Vec<ChangeHash>>,
    pub their_need: Option<Vec<ChangeHash>>,
    pub their_have: Option<Vec<Have>>,
    pub sent_hashes: HashSet<ChangeHash>,
}

/// A summary of the changes that the sender of the message already has.
/// This is implicitly a request to the recipient to send all changes that the
/// sender does not already have.
#[derive(Debug, Clone, Default)]
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

    #[cfg(not(feature = "storage-v2"))]
    pub fn decode(bytes: &[u8]) -> Result<Self, decoding::Error> {
        let mut decoder = Decoder::new(Cow::Borrowed(bytes));

        let record_type = decoder.read::<u8>()?;
        if record_type != SYNC_STATE_TYPE {
            return Err(decoding::Error::WrongType {
                expected_one_of: vec![SYNC_STATE_TYPE],
                found: record_type,
            });
        }

        let shared_heads = decode_hashes(&mut decoder)?;
        Ok(Self {
            shared_heads,
            last_sent_heads: Vec::new(),
            their_heads: None,
            their_need: None,
            their_have: Some(Vec::new()),
            sent_hashes: HashSet::new(),
        })
    }

    #[cfg(feature = "storage-v2")]
    pub fn decode(input: &[u8]) -> Result<Self, DecodeError> {
        match Self::parse(input) {
            Ok((_, state)) => Ok(state),
            Err(parse::ParseError::Incomplete(_)) => Err(DecodeError::NotEnoughInput),
            Err(parse::ParseError::Error(e)) => Err(e),
        }
    }

    #[cfg(feature = "storage-v2")]
    pub(crate) fn parse(input: &[u8]) -> parse::ParseResult<'_, Self, DecodeError> {
        let (i, record_type) = parse::take1(input)?;
        if record_type != SYNC_STATE_TYPE {
            return Err(parse::ParseError::Error(DecodeError::WrongType {
                expected_one_of: vec![SYNC_STATE_TYPE],
                found: record_type,
            }));
        }

        let (i, shared_heads) = parse::length_prefixed(parse::leb128_u64, parse::change_hash)(i)?;
        Ok((
            i,
            Self {
                shared_heads,
                last_sent_heads: Vec::new(),
                their_heads: None,
                their_need: None,
                their_have: Some(Vec::new()),
                sent_hashes: HashSet::new(),
            },
        ))
    }
}
