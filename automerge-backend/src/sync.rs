use std::convert::TryFrom;

use automerge_protocol::Patch;

use crate::AutomergeError;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
};

use automerge_protocol::ChangeHash;

use crate::{
    encoding::{Decoder, Encodable},
    Backend, Change,
};

extern crate web_sys;
#[allow(unused_macros)]
macro_rules! log {
    ( $( $t:tt )* ) => {
          web_sys::console::log_1(&format!( $( $t )* ).into());
    };
}

// These constants correspond to a 1% false positive rate. The values can be changed without
// breaking compatibility of the network protocol, since the parameters used for a particular
// Bloom filter are encoded in the wire format.
const BITS_PER_ENTRY: u32 = 10;
const NUM_PROBES: u32 = 7;

const MESSAGE_TYPE_SYNC: u8 = 0x42; // first byte of a sync message, for identification
const SYNC_STATE_TYPE: u8 = 0x43; // first byte of an encoded sync state, for identification

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilter {
    num_entries: u32,
    num_bits_per_entry: u32,
    num_probes: u32,
    bits: Vec<u8>,
}

impl BloomFilter {
    pub fn into_bytes(self) -> Result<Vec<u8>, AutomergeError> {
        if self.num_entries == 0 {
            Ok(Vec::new())
        } else {
            let mut buf = Vec::new();
            self.num_entries.encode(&mut buf)?;
            self.num_bits_per_entry.encode(&mut buf)?;
            self.num_probes.encode(&mut buf)?;
            buf.extend(self.bits);
            Ok(buf)
        }
    }

    fn get_probes(&self, hash: ChangeHash) -> Vec<u32> {
        let hash_bytes = hash.0.to_vec();
        let modulo = 8 * self.bits.len() as u32;

        let mut x =
            u32::from_le_bytes([hash_bytes[0], hash_bytes[1], hash_bytes[2], hash_bytes[3]])
                % modulo;
        let mut y =
            u32::from_le_bytes([hash_bytes[4], hash_bytes[5], hash_bytes[6], hash_bytes[7]])
                % modulo;
        let z = u32::from_le_bytes([hash_bytes[8], hash_bytes[9], hash_bytes[10], hash_bytes[11]])
            % modulo;

        let mut probes = vec![x];
        for _ in 1..self.num_probes {
            x = (x + y) % modulo;
            y = (y + z) % modulo;
            probes.push(x);
        }
        probes
    }

    fn add_hash(&mut self, hash: ChangeHash) {
        for probe in self.get_probes(hash) {
            self.set_bit(probe as usize)
        }
    }

    fn set_bit(&mut self, probe: usize) {
        if let Some(byte) = self.bits.get_mut(probe >> 3) {
            *byte |= 1 << (probe & 7)
        }
    }

    fn get_bit(&self, probe: usize) -> Option<u8> {
        self.bits
            .get(probe >> 3)
            .map(|byte| byte & (1 << (probe & 7)))
    }

    fn contains_hash(&self, hash: ChangeHash) -> bool {
        if self.num_entries == 0 {
            false
        } else {
            for probe in self.get_probes(hash) {
                if let Some(bit) = self.get_bit(probe as usize) {
                    if bit == 0 {
                        return false;
                    }
                }
            }
            true
        }
    }
}

fn bits_capacity(num_entries: u32, num_bits_per_entry: u32) -> usize {
    let f = ((num_entries as f64 * num_bits_per_entry as f64) / 8f64).ceil();
    f as usize
}

impl From<Vec<ChangeHash>> for BloomFilter {
    fn from(hashes: Vec<ChangeHash>) -> Self {
        let num_entries = hashes.len() as u32;
        let num_bits_per_entry = BITS_PER_ENTRY;
        let num_probes = NUM_PROBES;
        let bits = vec![0].repeat(bits_capacity(num_entries, num_bits_per_entry) as usize);
        let mut filter = Self {
            num_entries,
            num_bits_per_entry,
            num_probes,
            bits,
        };
        for hash in hashes {
            filter.add_hash(hash)
        }
        filter
    }
}

impl TryFrom<Vec<u8>> for BloomFilter {
    type Error = AutomergeError;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        if bytes.is_empty() {
            Ok(Self::default())
        } else {
            let mut decoder = Decoder::new(Cow::Owned(bytes));
            let num_entries = decoder.read()?;
            let num_bits_per_entry = decoder.read()?;
            let num_probes = decoder.read()?;
            let bits =
                decoder.read_bytes((bits_capacity(num_entries, num_bits_per_entry)) as usize)?;
            Ok(Self {
                num_entries,
                num_bits_per_entry,
                num_probes,
                bits: bits.to_vec(),
            })
        }
    }
}

impl Backend {
    pub fn generate_sync_message(
        &self,
        mut sync_state: SyncState,
    ) -> (SyncState, Option<SyncMessage>) {
        let our_heads = self.get_heads();

        let have = if sync_state.our_need.is_empty() {
            vec![self.make_bloom_filter(&sync_state.shared_heads)]
        } else {
            Vec::new()
        };

        if let Some(ref their_have) = sync_state.have {
            if let Some(first_have) = their_have.first().as_ref() {
                if !first_have
                    .last_sync
                    .iter()
                    .all(|hash| self.get_change_by_hash(hash).is_some())
                {
                    let reset_msg = SyncMessage {
                        heads: our_heads,
                        need: Vec::new(),
                        have: vec![SyncHave::default()],
                        changes: Vec::new(),
                    };
                    return (sync_state, Some(reset_msg));
                }
            }
        }

        let mut changes_to_send = if let (Some(their_have), Some(their_need)) =
            (sync_state.have.as_ref(), sync_state.their_need.as_ref())
        {
            self.get_changes_to_send(their_have, their_need)
        } else {
            Vec::new()
        };

        let heads_unchanged = if let Some(last_sent_heads) = sync_state.last_sent_heads.as_ref() {
            last_sent_heads == &our_heads
        } else {
            false
        };

        let heads_equal = if let Some(their_heads) = sync_state.their_heads.as_ref() {
            their_heads == &our_heads
        } else {
            false
        };

        if heads_unchanged
            && heads_equal
            && changes_to_send.is_empty()
            && sync_state.our_need.is_empty()
        {
            return (sync_state, None);
        }

        if !sync_state.sent_changes.is_empty() && !changes_to_send.is_empty() {
            changes_to_send = deduplicate_changes(&sync_state.sent_changes, changes_to_send)
        }

        let sync_message = SyncMessage {
            heads: our_heads.clone(),
            have,
            need: sync_state.our_need.clone(),
            changes: changes_to_send.clone(),
        };

        sync_state.last_sent_heads = Some(our_heads);
        sync_state.sent_changes.extend(changes_to_send);

        (sync_state, Some(sync_message))
    }

    pub fn receive_sync_message(
        &mut self,
        message: SyncMessage,
        mut old_sync_state: SyncState,
    ) -> Result<(SyncState, Option<Patch>), AutomergeError> {
        let mut patch = None;

        let before_heads = self.get_heads();

        if !message.changes.is_empty() {
            old_sync_state
                .unapplied_changes
                .extend(message.changes.clone());

            let our_need = self.get_missing_deps(&old_sync_state.unapplied_changes, &message.heads);

            if our_need.iter().all(|hash| message.heads.contains(hash)) {
                patch = Some(self.apply_changes(old_sync_state.unapplied_changes.to_vec())?);
                old_sync_state.unapplied_changes = Vec::new();
                old_sync_state.shared_heads = advance_heads(
                    &before_heads,
                    &self.get_heads(),
                    &old_sync_state.shared_heads,
                );
            }
        } else if message.heads == before_heads {
            old_sync_state.last_sent_heads = Some(message.heads.clone())
        }

        if message.heads.iter().all(|head| {
            let res = self.get_change_by_hash(head);

            res.is_some()
        }) {
            old_sync_state.shared_heads = message.heads.clone()
        }

        let new_sync_state = SyncState {
            shared_heads: old_sync_state.shared_heads,
            last_sent_heads: old_sync_state.last_sent_heads,
            have: Some(message.have),
            their_heads: Some(message.heads),
            their_need: Some(message.need),
            our_need: old_sync_state.our_need,
            unapplied_changes: old_sync_state.unapplied_changes,
            sent_changes: old_sync_state.sent_changes,
        };
        Ok((new_sync_state, patch))
    }

    pub fn make_bloom_filter(&self, last_sync: &[ChangeHash]) -> SyncHave {
        let new_changes = self.get_changes(last_sync);
        let hashes = new_changes
            .into_iter()
            .map(|change| change.hash)
            .collect::<Vec<_>>();
        SyncHave {
            last_sync: last_sync.to_vec(),
            bloom: BloomFilter::from(hashes),
        }
    }

    pub fn get_changes_to_send(&self, have: &[SyncHave], need: &[ChangeHash]) -> Vec<Change> {
        if have.is_empty() {
            need.iter()
                .filter_map(|hash| self.get_change_by_hash(hash).cloned())
                .collect()
        } else {
            let mut last_sync_hashes = HashSet::new();
            let mut bloom_filters = Vec::new();

            for h in have {
                for hash in &h.last_sync {
                    last_sync_hashes.insert(*hash);
                }
                bloom_filters.push(h.bloom.clone())
            }
            let last_sync_hashes = last_sync_hashes.into_iter().collect::<Vec<_>>();

            let changes = self.get_changes(&last_sync_hashes);

            let mut change_hashes = HashSet::new();
            let mut dependents: HashMap<ChangeHash, Vec<ChangeHash>> = HashMap::new();
            let mut hashes_to_send = HashSet::new();

            for change in &changes {
                change_hashes.insert(change.hash);

                for dep in &change.deps {
                    dependents.entry(*dep).or_default().push(change.hash);
                }

                if bloom_filters
                    .iter()
                    .all(|bloom| !bloom.contains_hash(change.hash))
                {
                    hashes_to_send.insert(change.hash);
                }
            }

            let mut stack = hashes_to_send.iter().cloned().collect::<Vec<_>>();
            while let Some(hash) = stack.pop() {
                for dep in dependents.get(&hash).cloned().unwrap_or_default() {
                    if hashes_to_send.insert(dep) {
                        stack.push(dep)
                    }
                }
            }

            let mut changes_to_send = Vec::new();
            for hash in need {
                hashes_to_send.insert(*hash);
                if !change_hashes.contains(&hash) {
                    let change = self.get_change_by_hash(&hash);
                    if let Some(change) = change {
                        changes_to_send.push(change.clone())
                    }
                }
            }

            for change in changes {
                if hashes_to_send.contains(&change.hash) {
                    changes_to_send.push(change.clone())
                }
            }
            changes_to_send
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncState {
    shared_heads: Vec<ChangeHash>,
    last_sent_heads: Option<Vec<ChangeHash>>,
    their_heads: Option<Vec<ChangeHash>>,
    their_need: Option<Vec<ChangeHash>>,
    our_need: Vec<ChangeHash>,
    have: Option<Vec<SyncHave>>,
    unapplied_changes: Vec<Change>,
    sent_changes: Vec<Change>,
}

impl SyncState {
    pub fn encode(self) -> Result<Vec<u8>, AutomergeError> {
        let mut buf = vec![SYNC_STATE_TYPE];
        encode_hashes(&mut buf, &self.shared_heads)?;
        Ok(buf)
    }

    pub fn decode(bytes: Vec<u8>) -> Result<Self, AutomergeError> {
        let mut decoder = Decoder::new(Cow::Owned(bytes));

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
            our_need: Vec::new(),
            have: Some(Vec::new()),
            unapplied_changes: Vec::new(),
            sent_changes: Vec::new(),
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
            our_need: Vec::new(),
            have: Some(Vec::new()),
            unapplied_changes: Vec::new(),
            sent_changes: Vec::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SyncMessage {
    heads: Vec<ChangeHash>,
    need: Vec<ChangeHash>,
    have: Vec<SyncHave>,
    changes: Vec<Change>,
}

impl SyncMessage {
    pub fn encode(self) -> Result<Vec<u8>, AutomergeError> {
        let mut buf = vec![MESSAGE_TYPE_SYNC];

        encode_hashes(&mut buf, &self.heads)?;
        encode_hashes(&mut buf, &self.need)?;
        (self.have.len() as u32).encode(&mut buf)?;
        for have in self.have {
            encode_hashes(&mut buf, &have.last_sync)?;
            have.bloom.into_bytes()?.encode(&mut buf)?;
        }

        (self.changes.len() as u32).encode(&mut buf)?;
        for change in self.changes {
            change.raw_bytes().encode(&mut buf)?;
        }

        Ok(buf)
    }

    pub fn decode(bytes: Vec<u8>) -> Result<SyncMessage, AutomergeError> {
        let mut decoder = Decoder::new(Cow::Owned(bytes));

        let message_type = decoder.read::<u8>()?;
        if message_type != MESSAGE_TYPE_SYNC {
            return Err(AutomergeError::EncodingError);
        }

        let heads = decode_hashes(&mut decoder)?;
        let need = decode_hashes(&mut decoder)?;
        let have_count = decoder.read::<u32>()?;
        let mut have = Vec::new();
        for _ in 0..have_count {
            let last_sync = decode_hashes(&mut decoder)?;
            let bloom_bytes: Vec<u8> = decoder.read()?;
            let bloom = BloomFilter::try_from(bloom_bytes)?;
            have.push(SyncHave { last_sync, bloom });
        }

        let change_count = decoder.read::<u32>()?;
        let mut changes = Vec::new();
        for _ in 0..change_count {
            let change = decoder.read()?;
            changes.push(Change::from_bytes(change)?);
        }

        Ok(Self {
            heads,
            need,
            have,
            changes,
        })
    }
}

fn encode_hashes(buf: &mut Vec<u8>, hashes: &[ChangeHash]) -> Result<(), AutomergeError> {
    (hashes.len() as u32).encode(buf)?;
    // debug_assert!(hashes.is_sorted());
    for hash in hashes {
        let bytes = &hash.0[..];
        buf.extend(bytes);
    }

    Ok(())
}

fn decode_hashes(decoder: &mut Decoder) -> Result<Vec<ChangeHash>, AutomergeError> {
    let length = decoder.read::<u32>()?;
    let mut hashes = Vec::new();

    const HASH_SIZE: usize = 32; // 256 bits = 32 bytes
    for _ in 0..length {
        let hash_bytes = decoder.read_bytes(HASH_SIZE)?;
        let hash = ChangeHash::try_from(hash_bytes)
            .map_err(|source| AutomergeError::ChangeBadFormat { source })?;
        hashes.push(hash);
    }

    Ok(hashes)
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SyncHave {
    pub last_sync: Vec<ChangeHash>,
    pub bloom: BloomFilter,
}

fn deduplicate_changes(previous_changes: &[Change], new_changes: Vec<Change>) -> Vec<Change> {
    let mut index: HashMap<u32, Vec<usize>> = HashMap::new();

    for (i, change) in previous_changes.iter().enumerate() {
        let checksum = change.checksum();
        index.entry(checksum).or_default().push(i);
    }

    new_changes
        .into_iter()
        .filter(|change| {
            if let Some(positions) = index.get(&change.checksum()) {
                !positions.iter().any(|i| change == &previous_changes[*i])
            } else {
                true
            }
        })
        .collect()
}

fn advance_heads(
    my_old_heads: &[ChangeHash],
    my_new_heads: &[ChangeHash],
    our_old_shared_heads: &[ChangeHash],
) -> Vec<ChangeHash> {
    let new_heads = my_new_heads
        .iter()
        .filter(|head| !my_old_heads.contains(head))
        .collect::<Vec<_>>();

    let common_heads = our_old_shared_heads
        .iter()
        .filter(|head| my_new_heads.contains(head))
        .collect::<Vec<_>>();

    let mut advanced_heads = HashSet::new();
    for head in new_heads {
        advanced_heads.insert(*head);
    }
    for head in common_heads {
        advanced_heads.insert(*head);
    }
    let mut advanced_heads = advanced_heads.into_iter().collect::<Vec<_>>();
    advanced_heads.sort();
    advanced_heads
}
