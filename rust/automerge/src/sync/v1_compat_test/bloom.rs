use std::borrow::Borrow;

use crate::storage::parse;
use crate::ChangeHash;

// These constants correspond to a 1% false positive rate. The values can be changed without
// breaking compatibility of the network protocol, since the parameters used for a particular
// Bloom filter are encoded in the wire format.
const BITS_PER_ENTRY: u32 = 10;
const NUM_PROBES: u32 = 7;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize)]
pub(crate) struct BloomFilter {
    pub(crate) num_entries: u32,
    pub(crate) num_bits_per_entry: u32,
    pub(crate) num_probes: u32,
    pub(crate) bits: Vec<u8>,
}

impl Default for BloomFilter {
    fn default() -> Self {
        BloomFilter {
            num_entries: 0,
            num_bits_per_entry: BITS_PER_ENTRY,
            num_probes: NUM_PROBES,
            bits: Vec::new(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ParseError {
    #[error(transparent)]
    Leb128(#[from] parse::leb128::Error),
}

impl BloomFilter {
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        if self.num_entries != 0 {
            leb128::write::unsigned(&mut buf, self.num_entries as u64).unwrap();
            leb128::write::unsigned(&mut buf, self.num_bits_per_entry as u64).unwrap();
            leb128::write::unsigned(&mut buf, self.num_probes as u64).unwrap();
            buf.extend(&self.bits);
        }
        buf
    }

    pub(crate) fn parse(input: parse::Input<'_>) -> parse::ParseResult<'_, Self, ParseError> {
        if input.is_empty() {
            Ok((input, Self::default()))
        } else {
            let (i, num_entries) = parse::leb128_u32(input)?;
            let (i, num_bits_per_entry) = parse::leb128_u32(i)?;
            let (i, num_probes) = parse::leb128_u32(i)?;
            let (i, bits) = parse::take_n(bits_capacity(num_entries, num_bits_per_entry), i)?;
            Ok((
                i,
                Self {
                    num_entries,
                    num_bits_per_entry,
                    num_probes,
                    bits: bits.to_vec(),
                },
            ))
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

        let mut probes = Vec::with_capacity(self.num_probes as usize);
        probes.push(x);
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

    pub(crate) fn contains_hash(&self, hash: &ChangeHash) -> bool {
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

    pub(crate) fn from_hashes<H: Borrow<ChangeHash>>(
        hashes: impl ExactSizeIterator<Item = H>,
    ) -> Self {
        let num_entries = hashes.len() as u32;
        let num_bits_per_entry = BITS_PER_ENTRY;
        let num_probes = NUM_PROBES;
        let bits = vec![0; bits_capacity(num_entries, num_bits_per_entry)];
        let mut filter = Self {
            num_entries,
            num_bits_per_entry,
            num_probes,
            bits,
        };
        for hash in hashes {
            filter.add_hash(hash.borrow());
        }
        filter
    }
}

fn bits_capacity(num_entries: u32, num_bits_per_entry: u32) -> usize {
    let f = ((f64::from(num_entries) * f64::from(num_bits_per_entry)) / 8_f64).ceil();
    f as usize
}

#[derive(thiserror::Error, Debug)]
#[error("{0}")]
pub(crate) struct DecodeError(String);

impl TryFrom<&[u8]> for BloomFilter {
    type Error = DecodeError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::parse(parse::Input::new(bytes))
            .map(|(_, b)| b)
            .map_err(|e| DecodeError(e.to_string()))
    }
}
