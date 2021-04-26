use std::{borrow::Cow, collections::HashSet};

use automerge_protocol::ChangeHash;

use super::{decode_hashes, encode_hashes};
use crate::{encoding::Decoder, AutomergeError, BloomFilter};

const SYNC_STATE_TYPE: u8 = 0x43; // first byte of an encoded sync state, for identification

#[derive(Debug, Clone)]
pub struct SyncState {
    pub shared_heads: Vec<ChangeHash>,
    pub last_sent_heads: Option<Vec<ChangeHash>>,
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
    pub fn encode(self) -> Result<Vec<u8>, AutomergeError> {
        let mut buf = vec![SYNC_STATE_TYPE];
        encode_hashes(&mut buf, &self.shared_heads)?;
        Ok(buf)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, AutomergeError> {
        let mut decoder = Decoder::new(Cow::Borrowed(bytes));

        let record_type = decoder.read::<u8>()?;
        if record_type != SYNC_STATE_TYPE {
            return Err(AutomergeError::EncodingError);
        }

        let shared_heads = decode_hashes(&mut decoder)?;
        Ok(Self {
            shared_heads,
            last_sent_heads: Some(Vec::new()),
            their_heads: None,
            their_need: None,
            their_have: Some(Vec::new()),
            sent_hashes: HashSet::new(),
        })
    }
}

impl Default for SyncState {
    fn default() -> Self {
        Self {
            shared_heads: Vec::new(),
            last_sent_heads: Some(Vec::new()),
            their_heads: None,
            their_need: None,
            their_have: None,
            sent_hashes: HashSet::new(),
        }
    }
}
