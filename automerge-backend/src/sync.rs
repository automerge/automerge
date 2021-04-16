use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
};

use automerge_protocol::ChangeHash;

use crate::{
    encoding::{Decoder, Encodable},
    protocol::SyncHave,
    Backend, Change,
};

// These constants correspond to a 1% false positive rate. The values can be changed without
// breaking compatibility of the network protocol, since the parameters used for a particular
// Bloom filter are encoded in the wire format.
const BITS_PER_ENTRY: u32 = 10;
const NUM_PROBES: u32 = 7;

#[derive(Default, Debug)]
pub struct BloomFilter {
    num_entries: u32,
    num_bits_per_entry: u32,
    num_probes: u32,
    bits: Vec<u8>,
}

impl BloomFilter {
    pub fn into_bytes(self) -> Vec<u8> {
        if self.num_entries == 0 {
            Vec::new()
        } else {
            let mut buf = Vec::new();
            self.num_entries.encode(&mut buf).unwrap();
            self.num_bits_per_entry.encode(&mut buf).unwrap();
            self.num_probes.encode(&mut buf).unwrap();
            buf.extend(self.bits);
            buf
        }
    }

    fn get_probes(&self, hash: ChangeHash) -> Vec<u32> {
        let hash_bytes = hash.0.to_vec();
        let modulo = 8 * self.bits.len() as u32;

        let mut x = (hash_bytes[0] as u32
            | ((hash_bytes[1] as u32) << 8)
            | (hash_bytes[2] as u32) << 16
            | (hash_bytes[3] as u32) << 24)
            % modulo;
        let mut y = (hash_bytes[4] as u32
            | (hash_bytes[5] as u32) << 8
            | (hash_bytes[6] as u32) << 16
            | (hash_bytes[7] as u32) << 24)
            % modulo;
        let z = (hash_bytes[8] as u32
            | (hash_bytes[9] as u32) << 8
            | (hash_bytes[10] as u32) << 16
            | (hash_bytes[11] as u32) << 24)
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
            let probe = probe as usize;
            self.bits[probe >> 3] |= 1 << (probe & 7);
        }
    }

    fn contains_hash(&self, hash: ChangeHash) -> bool {
        if self.num_entries == 0 {
            false
        } else {
            for probe in self.get_probes(hash) {
                let probe = probe as usize;
                if (self.bits[probe >> 3] & (1 << (probe & 7))) == 0 {
                    return false;
                }
            }
            true
        }
    }
}

impl From<Vec<ChangeHash>> for BloomFilter {
    fn from(hashes: Vec<ChangeHash>) -> Self {
        let num_entries = hashes.len() as u32;
        let num_bits_per_entry = BITS_PER_ENTRY;
        let num_probes = NUM_PROBES;
        let bits = Vec::with_capacity(((num_entries * num_bits_per_entry) / 8) as usize);
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

impl From<Vec<u8>> for BloomFilter {
    fn from(bytes: Vec<u8>) -> Self {
        if bytes.is_empty() {
            Self {
                num_entries: 0,
                num_bits_per_entry: 0,
                num_probes: 0,
                bits: bytes,
            }
        } else {
            let mut decoder = Decoder::new(Cow::Owned(bytes));
            let num_entries = decoder.read().unwrap();
            let num_bits_per_entry = decoder.read().unwrap();
            let num_probes = decoder.read().unwrap();
            let bits = decoder
                .read_bytes(((num_entries * num_bits_per_entry) / 8) as usize)
                .unwrap();
            Self {
                num_entries,
                num_bits_per_entry,
                num_probes,
                bits: bits.to_vec(),
            }
        }
    }
}

impl Backend {
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
                .map(|hash| self.get_change_by_hash(hash).unwrap().clone())
                .collect()
        } else {
            let mut last_sync_hashes = HashSet::new();
            let mut bloom_filters = Vec::new();

            for h in have {
                for hash in &h.last_sync {
                    last_sync_hashes.insert(*hash);
                }
                bloom_filters.push(&h.bloom)
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
