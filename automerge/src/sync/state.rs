use std::{borrow::Cow, collections::HashSet};

use super::{decode_hashes, encode_hashes, BloomFilter};
use crate::{decoding, decoding::Decoder, ChangeHash};

const SYNC_STATE_TYPE: u8 = 0x43; // first byte of an encoded sync state, for identification

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
}
