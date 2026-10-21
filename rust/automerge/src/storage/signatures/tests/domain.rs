use std::cmp::Ordering;

use proptest::prelude::*;

use crate::storage::parse::Input;
use crate::storage::Chunk;
use crate::{ChangeHash, Ed25519Signature, Signature, SignatureBytes, SignatureTag};

use super::{error, flattened_entries, frame_body, signature_error, HashEntries, SignatureEntry};

fn wrapped(tag: u8, bytes: &[u8]) -> SignatureEntry {
    SignatureEntry(Signature::new(
        SignatureTag::new(tag),
        SignatureBytes::new(bytes.to_vec()).expect("size-bounded fixture"),
    ))
}

// Construct wire fixtures independently of the domain representation.
fn wire_body(entries: &[(u8, Vec<u8>)]) -> Vec<u8> {
    let mut body = Vec::new();
    leb128::write::unsigned(&mut body, entries.len() as u64).unwrap();
    for (tag, bytes) in entries {
        body.extend_from_slice(&[1; 32]);
        body.push(*tag);
        leb128::write::unsigned(&mut body, bytes.len() as u64).unwrap();
        body.extend_from_slice(bytes);
    }
    body
}

#[test]
fn parsing_classifies_ed25519_without_verifying_and_preserves_opaque_entries() {
    let cases = vec![
        (0, vec![]),
        (0, vec![0; 64]),
        (0, vec![0; 65]),
        (1, vec![0; 64]),
        (255, vec![7; 1024]),
    ];
    let bytes = frame_body(&wire_body(&cases));
    let (rest, chunk) = Chunk::parse(Input::new(&bytes)).unwrap();
    assert!(rest.is_empty());
    assert!(chunk.checksum_valid());
    let Chunk::Signatures(chunk) = chunk else {
        panic!("expected signatures chunk");
    };
    assert_eq!(chunk.bytes(), bytes);
    let entries = flattened_entries(&chunk);
    assert_eq!(entries.len(), cases.len());
    for ((hash, entry), (tag, original)) in entries.iter().zip(&cases) {
        assert_eq!(*hash, ChangeHash([1; 32]));
        assert_eq!(entry.0.tag().inner_tag(), *tag);
        assert_eq!(entry.0.to_bytes().as_bytes(), original);
        assert_eq!(
            entry.0.as_ed25519().is_some(),
            *tag == 0 && original.len() == 64
        );
    }
}

#[test]
fn wire_order_uses_bytes_not_typed_or_opaque_variant_order() {
    let opaque_before = wrapped(0, &[0]);
    let typed = SignatureEntry(Signature::ed25519(Ed25519Signature::from_bytes([1; 64])));
    let opaque_after = wrapped(0, &[2]);

    assert_eq!(opaque_before.cmp(&typed), Ordering::Less);
    assert_eq!(typed.cmp(&opaque_after), Ordering::Less);

    let error = signature_error(&frame_body(&wire_body(&[(0, vec![2]), (0, vec![1; 64])])));
    assert!(matches!(
        error,
        error::Signatures::Entries(error::HashEntries::Unsorted)
    ));
}

#[test]
fn typed_and_generic_construction_are_the_same_entry_at_capacity() {
    let hash = ChangeHash([1; 32]);
    let mut entries = HashEntries::new();
    for byte in 0..16 {
        entries.push_ordered(hash, wrapped(0, &[byte; 64])).unwrap();
    }
    let typed = SignatureEntry(Signature::ed25519(Ed25519Signature::from_bytes([0; 64])));
    assert_eq!(typed, wrapped(0, &[0; 64]));
    assert_eq!(typed.cmp(&wrapped(0, &[0; 64])), Ordering::Equal);

    let before = entries.clone();
    entries.insert_bounded(hash, typed).unwrap();
    assert_eq!(entries, before);
    assert_eq!(entries.len(), 16);

    let error = signature_error(&frame_body(&wire_body(&[
        (0, vec![0; 64]),
        (0, vec![0; 64]),
    ])));
    assert!(matches!(
        error,
        error::Signatures::Entries(error::HashEntries::Duplicate)
    ));
}

fn raw_entry() -> impl Strategy<Value = (u8, Vec<u8>)> {
    // Exercise all tags/lengths, with frequent typed values and their neighbours.
    (
        prop_oneof![Just(0), any::<u8>()],
        prop_oneof![
            any::<[u8; 64]>().prop_map(|bytes| bytes.to_vec()),
            proptest::collection::vec(any::<u8>(), 0..=1024),
        ],
    )
}

proptest! {
    #[test]
    fn entry_order_matches_raw_tag_and_byte_order(left in raw_entry(), right in raw_entry()) {
        let actual = wrapped(left.0, &left.1).cmp(&wrapped(right.0, &right.1));
        prop_assert_eq!(actual, left.cmp(&right));
    }

    #[test]
    fn parsing_preserves_arbitrary_domain_signature((tag, original) in raw_entry()) {
        let bytes = frame_body(&wire_body(&[(tag, original.clone())]));
        let (rest, chunk) = Chunk::parse(Input::new(&bytes)).unwrap();
        prop_assert!(rest.is_empty());
        let Chunk::Signatures(chunk) = chunk else {
            panic!("expected signatures chunk");
        };
        prop_assert_eq!(chunk.bytes(), bytes.as_slice());
        let entries = flattened_entries(&chunk);
        prop_assert_eq!(entries.len(), 1);
        let signature = &entries[0].1.0;
        let recovered = signature.to_bytes();
        prop_assert_eq!(signature.tag().inner_tag(), tag);
        prop_assert_eq!(recovered.as_bytes(), original.as_slice());
        prop_assert_eq!(signature.as_ed25519().is_some(), tag == 0 && original.len() == 64);
    }
}
