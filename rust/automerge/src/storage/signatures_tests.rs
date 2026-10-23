//! Exercise the signatures codec from outside its module, using domain values.

use crate::storage::parse::Input;
use crate::storage::signatures::{EntriesError, HashEntries, SignaturesChunk};
use crate::storage::{Chunk, ChunkType, Header};
use crate::{ChangeHash, Signature, SignatureBytes, SignatureTag};

fn signature(tag: u8, bytes: &[u8]) -> Signature {
    Signature::new(
        SignatureTag::new(tag),
        SignatureBytes::new(bytes.to_vec()).expect("size-bounded fixture"),
    )
}

fn values(chunk: &SignaturesChunk<'_>) -> Vec<(ChangeHash, Signature)> {
    chunk
        .iter()
        .map(|(hash, signature)| (hash, signature.clone()))
        .collect()
}

#[test]
fn signatures_codec_canonicalizes_domain_values_and_exposes_parsed_and_owned_entries() {
    let a = ChangeHash([1; 32]);
    let b = ChangeHash([2; 32]);
    let expected = vec![
        (a, signature(0, &[0])),     // malformed Ed25519 length remains representable
        (a, signature(0, &[1; 64])), // typed but unverified
        (a, signature(255, &[9])),   // unknown scheme remains opaque
        (b, signature(0, &[])),
    ];
    let inputs = expected.iter().rev().chain(expected.iter()).cloned();
    let entries = HashEntries::canonicalize(inputs).expect("four distinct entries");
    let chunk = SignaturesChunk::encode(entries);
    assert_eq!(values(&chunk), expected);
    assert_eq!(
        values(&chunk),
        expected,
        "inspection does not consume entries"
    );

    // Independent wire fixture pins ordering, deduplication and framing.
    let mut body = vec![4];
    for (hash, tag, bytes) in [
        (a, 0, vec![0]),
        (a, 0, vec![1; 64]),
        (a, 255, vec![9]),
        (b, 0, vec![]),
    ] {
        body.extend_from_slice(hash.as_bytes());
        body.push(tag);
        leb128::write::unsigned(&mut body, bytes.len() as u64).unwrap();
        body.extend_from_slice(&bytes);
    }
    let mut wire = Vec::new();
    Header::new(ChunkType::Signatures, &body).write(&mut wire);
    wire.extend_from_slice(&body);
    assert_eq!(chunk.bytes(), wire);

    let (rest, parsed) = Chunk::parse(Input::new(chunk.bytes())).unwrap();
    assert!(rest.is_empty());
    assert!(parsed.checksum_valid());
    let Chunk::Signatures(parsed) = parsed else {
        panic!("expected signatures chunk");
    };
    assert_eq!(values(&parsed), expected);
    let reencoded = SignaturesChunk::encode(
        HashEntries::canonicalize(
            parsed
                .iter()
                .map(|(hash, signature)| (hash, signature.clone())),
        )
        .unwrap(),
    );
    assert_eq!(reencoded.bytes(), chunk.bytes());
}

#[test]
fn signatures_codec_accepts_empty_domain_input() {
    let chunk = SignaturesChunk::encode(HashEntries::canonicalize([]).unwrap());
    assert_eq!(chunk.iter().count(), 0);
    let (_, Chunk::Signatures(parsed)) = Chunk::parse(Input::new(chunk.bytes())).unwrap() else {
        panic!("expected signatures chunk");
    };
    assert_eq!(parsed.iter().count(), 0);
}

#[test]
fn signatures_codec_counts_distinct_entries_per_hash_across_schemes() {
    let hash = ChangeHash([7; 32]);
    let entries: Vec<_> = (0..16).map(|tag| (hash, signature(tag, &[3]))).collect();
    let duplicates = entries.iter().chain(entries.iter()).cloned();
    let chunk = SignaturesChunk::encode(HashEntries::canonicalize(duplicates).unwrap());
    assert_eq!(chunk.iter().count(), 16);

    let error = HashEntries::canonicalize(entries.into_iter().chain([(hash, signature(16, &[3]))]))
        .unwrap_err();
    assert!(matches!(
        error,
        EntriesError::TooManyForHash(actual) if actual == hash
    ));
}
