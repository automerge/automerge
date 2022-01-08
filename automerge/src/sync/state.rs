use std::{borrow::Cow, collections::HashSet};

use super::{decode_hashes, encode_hashes};
use crate::{decoding, decoding::Decoder, encoding, BloomFilter, ChangeHash};

const SYNC_STATE_TYPE: u8 = 0x43; // first byte of an encoded sync state, for identification

#[derive(Debug, Clone, Default)]
pub struct SyncState {
    pub shared_heads: Vec<ChangeHash>,
    pub last_sent_heads: Vec<ChangeHash>,
    pub their_heads: Option<Vec<ChangeHash>>,
    pub their_need: Option<Vec<ChangeHash>>,
    pub their_have: Option<Vec<SyncHave>>,
    pub sent_hashes: HashSet<ChangeHash>,
}

#[derive(Debug, Clone, Default)]
pub struct SyncHave {
    pub last_sync: Vec<ChangeHash>,
    pub bloom: BloomFilter,
}

impl SyncState {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn encode(&self) -> Result<Vec<u8>, encoding::Error> {
        let mut buf = vec![SYNC_STATE_TYPE];
        encode_hashes(&mut buf, &self.shared_heads)?;
        Ok(buf)
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
