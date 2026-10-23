use super::{HashEntries, SignaturesChunk};
use crate::storage::parse::Input;
use crate::storage::Chunk;
use crate::{ChangeHash, Signature, SignatureBytes, SignatureTag};
use proptest::prelude::*;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;

// Raw tuples give the oracle an ordering independent of SignatureEntry::Ord.
type Entry = (ChangeHash, u8, Vec<u8>);

fn signature_bytes() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        1 => Just(0usize),
        1 => Just(64usize),
        1 => Just(127usize),
        1 => Just(128usize),
        1 => Just(1024usize),
        4 => 0usize..=1024,
    ]
    .prop_flat_map(|len| proptest::collection::vec(any::<u8>(), len))
}

fn entries() -> impl Strategy<Value = Vec<Entry>> {
    // Group 0..=16 inputs under each hash. The ordinal makes hashes distinct
    // even if random suffixes collide, so the per-hash cap holds by construction.
    // Tags share a small domain often enough to exercise byte ordering too.
    proptest::collection::vec(
        (
            any::<[u8; 31]>(),
            proptest::collection::vec(
                (
                    prop_oneof![3 => 0u8..=3, 1 => any::<u8>()],
                    signature_bytes(),
                ),
                0..=16,
            ),
        ),
        0..=3,
    )
    .prop_map(|groups| {
        groups
            .into_iter()
            .enumerate()
            .flat_map(|(index, (suffix, entries))| {
                let mut hash = [0; 32];
                hash[0] = index as u8;
                hash[1..].copy_from_slice(&suffix);
                entries
                    .into_iter()
                    .map(move |(tag, bytes)| (ChangeHash(hash), tag, bytes))
            })
            .collect()
    })
}

fn encode(entries: &[Entry]) -> SignaturesChunk<'static> {
    let entries = HashEntries::canonicalize(entries.iter().map(|(hash, tag, bytes)| {
        (
            *hash,
            Signature::new(
                SignatureTag::new(*tag),
                SignatureBytes::new(bytes.clone()).expect("size-bounded input"),
            ),
        )
    }))
    .expect("generated entries satisfy the per-hash limit");
    SignaturesChunk::encode(entries)
}

fn decode(bytes: &[u8]) -> SignaturesChunk<'_> {
    let (remaining, chunk) = Chunk::parse(Input::new(bytes)).expect("encoded chunk parses");
    assert!(
        remaining.is_empty(),
        "the decoder consumes the entire chunk"
    );
    assert!(chunk.checksum_valid());
    let Chunk::Signatures(signatures) = chunk else {
        panic!("expected signatures chunk");
    };
    signatures
}

fn values(chunk: &SignaturesChunk<'_>) -> Vec<Entry> {
    // Read the observable entry values without reusing canonicalize or its
    // collection layout to compute the expected result.
    chunk
        .iter()
        .map(|(hash, signature)| {
            (
                hash,
                signature.tag().inner_tag(),
                signature.to_bytes().as_bytes().to_vec(),
            )
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn round_trip_matches_sorted_unique_input(entries in entries()) {
        let encoded = encode(&entries);
        let parsed = decode(encoded.bytes());
        let mut expected = entries;
        expected.sort();
        expected.dedup();
        prop_assert_eq!(values(&parsed), expected);
        prop_assert_eq!(parsed.bytes(), encoded.bytes());
    }

    #[test]
    fn permutation_does_not_change_encoding(entries in entries(), seed in any::<u64>()) {
        let mut shuffled = entries.clone();
        shuffled.shuffle(&mut StdRng::seed_from_u64(seed));
        let original = encode(&entries);
        let permuted = encode(&shuffled);
        prop_assert_eq!(permuted.bytes(), original.bytes());
    }

    #[test]
    fn duplicate_expansion_does_not_change_encoding(
        entries in entries(),
        repetitions in 2usize..=4,
        seed in any::<u64>(),
    ) {
        let mut duplicated: Vec<_> = (0..repetitions)
            .flat_map(|_| entries.iter().cloned())
            .collect();
        duplicated.shuffle(&mut StdRng::seed_from_u64(seed));
        let original = encode(&entries);
        let expanded = encode(&duplicated);
        prop_assert_eq!(expanded.bytes(), original.bytes());
    }

    #[test]
    fn reencoding_parsed_entries_preserves_bytes(entries in entries()) {
        let encoded = encode(&entries);
        let parsed = decode(encoded.bytes());
        let reencoded = encode(&values(&parsed));
        prop_assert_eq!(reencoded.bytes(), encoded.bytes());
        prop_assert!(reencoded.checksum_valid());
    }
}

#[test]
fn round_trip_preserves_typed_and_opaque_entries_in_wire_order() {
    let entries = vec![
        (ChangeHash([1; 32]), 0, vec![0]),
        (ChangeHash([1; 32]), 0, vec![1; 64]),
        (ChangeHash([1; 32]), 0, vec![2]),
        (ChangeHash([1; 32]), 255, vec![1; 64]),
    ];
    let encoded = encode(&entries);
    let parsed = decode(encoded.bytes());
    assert_eq!(values(&parsed), entries);
    assert_eq!(encode(&values(&parsed)).bytes(), encoded.bytes());
}

#[test]
fn round_trip_pins_length_boundaries_and_full_hash_group() {
    let entries: Vec<_> = [0usize, 127, 128, 1024]
        .into_iter()
        .flat_map(|len| (0u8..4).map(move |tag| (ChangeHash([1; 32]), tag, vec![tag; len])))
        .collect();
    let encoded = encode(&entries);
    let parsed = decode(encoded.bytes());
    let mut expected = entries;
    expected.sort();
    assert_eq!(values(&parsed), expected);
}
