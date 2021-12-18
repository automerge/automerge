use std::{borrow::Cow, convert::TryFrom};

use crate::{decoding, decoding::Decoder, encoding, encoding::Encodable, ChangeHash};

// These constants correspond to a 1% false positive rate. The values can be changed without
// breaking compatibility of the network protocol, since the parameters used for a particular
// Bloom filter are encoded in the wire format.
const BITS_PER_ENTRY: u32 = 10;
const NUM_PROBES: u32 = 7;

#[derive(Default, Debug, Clone)]
pub struct BloomFilter {
    num_entries: u32,
    num_bits_per_entry: u32,
    num_probes: u32,
    bits: Vec<u8>,
}

impl BloomFilter {
    // FIXME - we can avoid a result here - why do we need to consume the bloom filter?  requires
    // me to clone in places I shouldn't need to
    pub fn into_bytes(self) -> Result<Vec<u8>, encoding::Error> {
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

    fn get_probes(&self, hash: &ChangeHash) -> Vec<u32> {
        let hash_bytes = hash.0;
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

    fn add_hash(&mut self, hash: &ChangeHash) {
        for probe in self.get_probes(hash) {
            self.set_bit(probe as usize);
        }
    }

    fn set_bit(&mut self, probe: usize) {
        if let Some(byte) = self.bits.get_mut(probe >> 3) {
            *byte |= 1 << (probe & 7);
        }
    }

    fn get_bit(&self, probe: usize) -> Option<u8> {
        self.bits
            .get(probe >> 3)
            .map(|byte| byte & (1 << (probe & 7)))
    }

    pub fn contains_hash(&self, hash: &ChangeHash) -> bool {
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
    let f = ((f64::from(num_entries) * f64::from(num_bits_per_entry)) / 8_f64).ceil();
    f as usize
}

impl From<&[ChangeHash]> for BloomFilter {
    fn from(hashes: &[ChangeHash]) -> Self {
        let num_entries = hashes.len() as u32;
        let num_bits_per_entry = BITS_PER_ENTRY;
        let num_probes = NUM_PROBES;
        let bits = vec![0; bits_capacity(num_entries, num_bits_per_entry) as usize];
        let mut filter = Self {
            num_entries,
            num_bits_per_entry,
            num_probes,
            bits,
        };
        for hash in hashes {
            filter.add_hash(hash);
        }
        filter
    }
}

impl TryFrom<&[u8]> for BloomFilter {
    type Error = decoding::Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.is_empty() {
            Ok(Self::default())
        } else {
            let mut decoder = Decoder::new(Cow::Borrowed(bytes));
            let num_entries = decoder.read()?;
            let num_bits_per_entry = decoder.read()?;
            let num_probes = decoder.read()?;
            let bits =
                decoder.read_bytes(bits_capacity(num_entries, num_bits_per_entry) as usize)?;
            Ok(Self {
                num_entries,
                num_bits_per_entry,
                num_probes,
                bits: bits.to_vec(),
            })
        }
    }
}
