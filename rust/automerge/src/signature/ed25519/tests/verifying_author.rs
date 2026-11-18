use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use proptest::prelude::*;

use crate::signature::ed25519::{error, Signature, VerifyingAuthor};
use crate::signature::signing_message;
use crate::{Author, ChangeHash};

#[test]
fn construction_retains_the_key_derived_from_borrowed_author_bytes() {
    let signer = SigningKey::from_bytes(&[7; 32]);
    let bytes = signer.verifying_key().to_bytes();
    let author = Author::from(bytes.as_slice());
    let verifying = VerifyingAuthor::new(author).unwrap();

    assert_eq!(verifying.author().as_bytes(), bytes);
    assert!(std::ptr::eq(
        verifying.author().as_bytes(),
        bytes.as_slice()
    ));
    // Private representation check: the parsed key is retained, not discarded
    // and reconstructed later. It must have been derived from these author bytes.
    assert_eq!(verifying.key.to_bytes(), bytes);
    assert!(!verifying.key.is_weak());
}

#[test]
fn owned_author_input_can_outlive_its_source_buffer() {
    let verifying = {
        let key = SigningKey::from_bytes(&[7; 32]);
        let bytes = key.verifying_key().to_bytes().to_vec();
        VerifyingAuthor::new(Author::from(bytes)).unwrap()
    };
    assert_eq!(verifying.author().as_bytes(), verifying.key.as_bytes());
}

#[test]
fn malformed_key_lengths_are_rejected() {
    for length in [0, 1, 31, 33, 64] {
        let failure = VerifyingAuthor::new(Author::from(vec![7; length])).unwrap_err();
        assert_eq!(failure.kind(), error::VerifyingAuthorKind::InvalidKey);
    }
}

#[test]
fn undecodable_key_bytes_are_rejected() {
    let bytes = (u8::MIN..=u8::MAX)
        .map(|byte| [byte; 32])
        .find(|bytes| VerifyingKey::from_bytes(bytes).is_err())
        .expect("a repeated-byte encoding is not a curve point");
    let failure = VerifyingAuthor::new(Author::from(bytes.as_slice())).unwrap_err();
    assert_eq!(failure.kind(), error::VerifyingAuthorKind::InvalidKey);
}

#[test]
fn small_order_key_is_rejected_even_if_dalek_can_decode_it() {
    let mut identity = [0; 32];
    identity[0] = 1;
    assert!(VerifyingKey::from_bytes(&identity).unwrap().is_weak());
    let failure = VerifyingAuthor::new(Author::from(identity.as_slice())).unwrap_err();
    assert_eq!(failure.kind(), error::VerifyingAuthorKind::WeakKey);
}

#[test]
fn key_decoding_error_retains_its_underlying_source() {
    let failure = VerifyingAuthor::new(Author::from(vec![7; 31])).unwrap_err();
    let source = std::error::Error::source(&failure)
        .expect("key decoding retains the library error directly");
    assert!(source.is::<ed25519_dalek::SignatureError>());
    assert_eq!(failure.kind(), error::VerifyingAuthorKind::InvalidKey);
    assert_eq!(
        failure.to_string(),
        format!("author bytes are not a usable Ed25519 public key: failed to parse key, due to {source}")
    );
}

#[test]
fn weak_key_error_has_a_reason_but_no_decoding_failure() {
    let mut identity = [0; 32];
    identity[0] = 1;
    let failure = VerifyingAuthor::new(Author::from(identity.as_slice())).unwrap_err();
    assert_eq!(failure.kind(), error::VerifyingAuthorKind::WeakKey);
    assert!(std::error::Error::source(&failure).is_none());
    assert_eq!(
        failure.to_string(),
        "author bytes are not a usable Ed25519 public key: the public key is considered weak, i.e., the public key has low order"
    );
}

#[test]
fn key_failure_kind_distinguishes_invalid_and_weak_keys_without_consuming_the_source() {
    fn assert_copy_clone<T: Copy + Clone>() {}
    assert_copy_clone::<error::VerifyingAuthorKind>();

    let undecodable = (u8::MIN..=u8::MAX)
        .map(|byte| [byte; 32])
        .find(|bytes| VerifyingKey::from_bytes(bytes).is_err())
        .expect("fixture must not decode as a curve point");
    let mut identity = [0; 32];
    identity[0] = 1;
    assert!(VerifyingKey::from_bytes(&identity).unwrap().is_weak());
    let cases = [
        (vec![7; 31], error::VerifyingAuthorKind::InvalidKey),
        (undecodable.to_vec(), error::VerifyingAuthorKind::InvalidKey),
        (identity.to_vec(), error::VerifyingAuthorKind::WeakKey),
    ];
    for (bytes, expected) in cases {
        let failure = VerifyingAuthor::new(Author::from(bytes)).unwrap_err();
        let before = failure.to_string();
        assert_eq!(failure.kind(), expected);
        assert_eq!(failure.kind(), expected);
        assert_eq!(failure.to_string(), before);
        let source = std::error::Error::source(&failure);
        if expected == error::VerifyingAuthorKind::InvalidKey {
            assert!(source.unwrap().is::<ed25519_dalek::SignatureError>());
        } else {
            assert!(source.is_none());
        }
    }
}

#[test]
fn retained_key_produces_evidence_for_each_verified_hash() {
    let signer = SigningKey::from_bytes(&[7; 32]);
    let verifying =
        VerifyingAuthor::new(Author::from(signer.verifying_key().to_bytes().to_vec())).unwrap();
    let first_hash = ChangeHash([3; 32]);
    let first = Signature::from_bytes(signer.sign(&signing_message(&first_hash)).to_bytes());
    let second_hash = ChangeHash([4; 32]);
    let second = Signature::from_bytes(signer.sign(&signing_message(&second_hash)).to_bytes());

    for (hash, signature) in [(&first_hash, &first), (&second_hash, &second)] {
        let witness = verifying.verify_hash(hash, signature).unwrap();
        assert_eq!(witness.author(), verifying.author());
        assert_eq!(witness.hash(), hash);
        assert_eq!(witness.signature(), signature);
    }
    assert!(verifying.verify_hash(&second_hash, &first).is_err());
}

#[test]
fn signature_from_another_key_is_rejected() {
    let signer = SigningKey::from_bytes(&[7; 32]);
    let other = SigningKey::from_bytes(&[8; 32]);
    let verifying =
        VerifyingAuthor::new(Author::from(signer.verifying_key().to_bytes().to_vec())).unwrap();
    let hash = ChangeHash([3; 32]);
    let signature = Signature::from_bytes(other.sign(&signing_message(&hash)).to_bytes());
    assert!(verifying.verify_hash(&hash, &signature).is_err());
}

#[test]
fn raw_hash_signing_does_not_bypass_domain_separation() {
    let signer = SigningKey::from_bytes(&[7; 32]);
    let verifying =
        VerifyingAuthor::new(Author::from(signer.verifying_key().to_bytes().to_vec())).unwrap();
    let hash = ChangeHash([3; 32]);
    let signature = Signature::from_bytes(signer.sign(&hash.0).to_bytes());
    assert!(verifying.verify_hash(&hash, &signature).is_err());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn generated_authors_retain_their_key_and_verify_their_signatures(
        seed in any::<[u8; 32]>(), hash in any::<[u8; 32]>(),
    ) {
        let signer = SigningKey::from_bytes(&seed);
        let bytes = signer.verifying_key().to_bytes();
        let verifying = VerifyingAuthor::new(Author::from(bytes.as_slice())).unwrap();
        prop_assert_eq!(verifying.key.to_bytes(), bytes);
        prop_assert_eq!(verifying.author().as_bytes(), bytes.as_slice());
        let hash = ChangeHash(hash);
        let signature = Signature::from_bytes(signer.sign(&signing_message(&hash)).to_bytes());
        prop_assert!(verifying.verify_hash(&hash, &signature).is_ok());
    }
}
