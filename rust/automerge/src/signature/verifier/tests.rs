use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey};
use proptest::prelude::*;

use crate::signature::{
    signing_message, Ed25519Signature, Signature, SignatureBytes, SignatureTag,
};
use crate::{Author, ChangeHash};

use super::{verify, Verification};

mod custom;

fn fixture() -> (SigningKey, Author<'static>, ChangeHash, Signature) {
    let key = SigningKey::from_bytes(&[7; 32]);
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    let hash = ChangeHash([3; 32]);
    let signature = Signature::ed25519(Ed25519Signature::from_bytes(
        key.sign(&signing_message(&hash)).to_bytes(),
    ));
    (key, author, hash, signature)
}

#[test]
fn valid_signature_verifies() {
    let (_, author, hash, signature) = fixture();

    assert_eq!(
        verify(&author, &hash, &signature, None),
        Verification::Valid
    );
}

#[test]
fn decoded_and_typed_signatures_verify_identically() {
    let (_, author, hash, typed) = fixture();
    let decoded = Signature::new(typed.tag(), typed.to_bytes());

    assert_eq!(verify(&author, &hash, &typed, None), Verification::Valid);
    assert_eq!(verify(&author, &hash, &decoded, None), Verification::Valid);
}

#[test]
fn correctly_sized_ed25519_bytes_are_not_necessarily_valid() {
    let (_, author, hash, _) = fixture();
    let decoded = Signature::new(
        SignatureTag::ED25519,
        SignatureBytes::new(vec![0; 64]).expect("64-byte input"),
    );
    let typed = Signature::ed25519(Ed25519Signature::from_bytes([0; 64]));

    assert!(decoded.as_ed25519().is_some());
    assert_eq!(
        verify(&author, &hash, &decoded, None),
        Verification::Invalid
    );
    assert_eq!(verify(&author, &hash, &typed, None), Verification::Invalid);
}

#[test]
fn different_hash_is_invalid() {
    let (_, author, mut hash, signature) = fixture();
    hash.0[0] ^= 1;

    assert_eq!(
        verify(&author, &hash, &signature, None),
        Verification::Invalid
    );
}

#[test]
fn different_author_key_is_invalid() {
    let (_, _, hash, signature) = fixture();
    let other_key = SigningKey::from_bytes(&[8; 32]);
    let other_author = Author::from(other_key.verifying_key().to_bytes().to_vec());

    assert_eq!(
        verify(&other_author, &hash, &signature, None),
        Verification::Invalid
    );
}

#[test]
fn modified_signature_is_invalid() {
    let (_, author, hash, signature) = fixture();
    let mut bytes = signature.to_bytes().as_bytes().to_vec();
    bytes[32] ^= 1;
    let modified = Signature::new(
        SignatureTag::ED25519,
        SignatureBytes::new(bytes).expect("64-byte signature"),
    );

    assert_eq!(
        verify(&author, &hash, &modified, None),
        Verification::Invalid
    );
}

#[test]
fn malformed_author_lengths_are_invalid() {
    let (_, _, hash, signature) = fixture();
    for len in [0, 31, 33, 64] {
        let author = Author::from(vec![7; len]);

        assert_eq!(
            verify(&author, &hash, &signature, None),
            Verification::Invalid,
            "author length {len}"
        );
    }
}

#[test]
fn undecodable_author_point_is_invalid() {
    let (_, _, hash, signature) = fixture();
    // Pick a reproducible 32-byte encoding that Dalek cannot decode as a point.
    let bytes = (u8::MIN..=u8::MAX)
        .map(|byte| [byte; 32])
        .find(|bytes| VerifyingKey::from_bytes(bytes).is_err())
        .expect("a repeated-byte encoding is not a curve point");
    let author = Author::from(bytes.as_slice());

    assert_eq!(
        verify(&author, &hash, &signature, None),
        Verification::Invalid
    );
}

#[test]
fn malformed_signature_lengths_are_invalid() {
    let (_, author, hash, _) = fixture();
    for len in [0, 63, 65, 1024] {
        let signature = Signature::new(
            SignatureTag::ED25519,
            SignatureBytes::new(vec![0; len]).expect("size-bounded bytes"),
        );

        assert_eq!(
            verify(&author, &hash, &signature, None),
            Verification::Invalid,
            "signature length {len}"
        );
    }
}

#[test]
fn other_scheme_tags_are_unsupported_even_with_valid_ed25519_bytes() {
    let (_, author, hash, signature) = fixture();
    for raw_tag in 1..=u8::MAX {
        let tagged = Signature::new(SignatureTag::new(raw_tag), signature.to_bytes());

        assert_eq!(
            verify(&author, &hash, &tagged, None),
            Verification::Unsupported,
            "scheme tag {raw_tag}"
        );
    }
}

#[test]
fn unsupported_scheme_does_not_require_ed25519_key_or_signature_lengths() {
    let author = Author::from(Vec::new());
    let signature = Signature::new(
        SignatureTag::new(255),
        SignatureBytes::new(Vec::new()).expect("empty input is permitted"),
    );

    assert_eq!(
        verify(&author, &ChangeHash([0; 32]), &signature, None),
        Verification::Unsupported
    );
}

#[test]
fn signature_over_raw_hash_without_domain_prefix_is_invalid() {
    let (key, author, hash, _) = fixture();
    let signature = Signature::ed25519(Ed25519Signature::from_bytes(key.sign(&hash.0).to_bytes()));

    assert_eq!(
        verify(&author, &hash, &signature, None),
        Verification::Invalid
    );
}

#[test]
fn signature_from_different_signing_domain_is_invalid() {
    let (key, author, hash, _) = fixture();
    let mut message = b"automerge-change-signature-v2".to_vec();
    message.extend_from_slice(&hash.0);
    let signature = Signature::ed25519(Ed25519Signature::from_bytes(key.sign(&message).to_bytes()));

    assert_eq!(
        verify(&author, &hash, &signature, None),
        Verification::Invalid
    );
}

#[test]
fn weak_key_forgery_accepted_by_ordinary_verification_is_invalid() {
    // A = identity, R = identity, S = 0 satisfies the ordinary verification
    // equation for any message, without a signing key. Strict verification
    // must reject this rather than treat it as evidence of authorship.
    let mut identity = [0; 32];
    identity[0] = 1;
    let key = VerifyingKey::from_bytes(&identity).expect("identity point decodes");
    let mut bytes = [0; 64];
    bytes[0] = 1;
    let dalek_signature = ed25519_dalek::Signature::from_bytes(&bytes);
    let hash = ChangeHash([3; 32]);
    let message = signing_message(&hash);

    assert!(key.is_weak());
    assert!(key.verify(&message, &dalek_signature).is_ok());
    assert!(key.verify_strict(&message, &dalek_signature).is_err());

    let author = Author::from(identity.as_slice());
    assert_eq!(
        verify(
            &author,
            &hash,
            &Signature::ed25519(Ed25519Signature::from_bytes(dalek_signature.to_bytes())),
            None,
        ),
        Verification::Invalid
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn signed_hashes_verify_but_changing_one_hash_byte_does_not(
        seed in any::<[u8; 32]>(),
        raw_hash in any::<[u8; 32]>(),
        index in 0usize..32,
    ) {
        let key = SigningKey::from_bytes(&seed);
        let author = Author::from(key.verifying_key().to_bytes().to_vec());
        let hash = ChangeHash(raw_hash);
        let signature = Signature::ed25519(Ed25519Signature::from_bytes(
            key.sign(&signing_message(&hash)).to_bytes(),
        ));

        prop_assert_eq!(
            verify(&author, &hash, &signature, None),
            Verification::Valid
        );

        let mut changed = raw_hash;
        changed[index] ^= 1;
        prop_assert_eq!(
            verify(&author, &ChangeHash(changed), &signature, None),
            Verification::Invalid
        );
    }
}
