use proptest::prelude::*;

use crate::signature::{verify_signature as verify, Verification};
use crate::signature::{Signature, SignatureBytes, SignatureTag};
use crate::{Author, ChangeHash};

use crate::signature::tests::verification::fixture;

use super::Verifier;

// Any call fails the test, including a call whose result the dispatcher ignores.
struct MustNotBeCalled;

impl Verifier for MustNotBeCalled {
    fn verify(
        &self,
        _: &Author<'_>,
        _: &[u8],
        _: SignatureTag,
        _: &SignatureBytes,
    ) -> Verification {
        panic!("Ed25519 must never reach a custom verifier")
    }
}

struct FixedVerdict(Verification);

impl Verifier for FixedVerdict {
    fn verify(
        &self,
        _: &Author<'_>,
        _: &[u8],
        _: SignatureTag,
        _: &SignatureBytes,
    ) -> Verification {
        self.0
    }
}

#[test]
fn valid_ed25519_never_calls_custom_verifier() {
    let (_, author, hash, typed) = fixture();
    let decoded = Signature::new(typed.tag(), typed.to_bytes());

    for signature in [typed, decoded] {
        assert_eq!(
            verify(&author, &hash, &signature, Some(&MustNotBeCalled)),
            Verification::Valid
        );
    }
}

#[test]
fn invalid_ed25519_never_falls_back_to_custom_verifier() {
    let (_, author, mut hash, signature) = fixture();
    hash.0[0] ^= 1;

    assert_eq!(
        verify(&author, &hash, &signature, Some(&MustNotBeCalled)),
        Verification::Invalid
    );
}

#[test]
fn malformed_ed25519_lengths_never_call_custom_verifier() {
    let (_, author, hash, _) = fixture();
    for length in (0..=1024).filter(|length| *length != 64) {
        let signature = Signature::new(
            SignatureTag::ED25519,
            SignatureBytes::new(vec![7; length]).expect("size-bounded input"),
        );

        assert_eq!(
            verify(&author, &hash, &signature, Some(&MustNotBeCalled)),
            Verification::Invalid,
            "signature length {length}"
        );
    }
}

#[test]
fn malformed_ed25519_author_never_calls_custom_verifier() {
    let (_, _, hash, signature) = fixture();
    let author = Author::from(Vec::new());

    assert_eq!(
        verify(&author, &hash, &signature, Some(&MustNotBeCalled)),
        Verification::Invalid
    );
}

#[test]
fn every_custom_verdict_passes_through_for_every_non_ed25519_tag() {
    // These are not Ed25519-shaped inputs. Custom schemes interpret their own
    // author and signature formats, even when either is empty.
    let author = Author::from(Vec::new());
    let hash = ChangeHash([3; 32]);
    let bytes = SignatureBytes::new(Vec::new()).expect("empty bytes are permitted");
    for raw_tag in 1..=u8::MAX {
        let signature = Signature::new(SignatureTag::new(raw_tag), bytes.clone());
        for verdict in [
            Verification::Valid,
            Verification::Invalid,
            Verification::Unsupported,
        ] {
            assert_eq!(
                verify(&author, &hash, &signature, Some(&FixedVerdict(verdict))),
                verdict,
                "scheme tag {raw_tag}"
            );
        }
    }
}

#[test]
fn verifier_can_borrow_configuration_without_static_ownership() {
    struct ExpectedAuthor<'a>(&'a [u8]);

    impl Verifier for ExpectedAuthor<'_> {
        fn verify(
            &self,
            author: &Author<'_>,
            _: &[u8],
            _: SignatureTag,
            _: &SignatureBytes,
        ) -> Verification {
            if author.as_bytes() == self.0 {
                Verification::Valid
            } else {
                Verification::Invalid
            }
        }
    }

    let config = vec![1, 2, 3];
    let custom = ExpectedAuthor(&config);
    let signature = Signature::new(
        SignatureTag::new(42),
        SignatureBytes::new(Vec::new()).expect("empty bytes are permitted"),
    );
    let hash = ChangeHash([0; 32]);

    assert_eq!(
        verify(
            &Author::from(config.as_slice()),
            &hash,
            &signature,
            Some(&custom)
        ),
        Verification::Valid
    );
    assert_eq!(
        verify(&Author::from(vec![9]), &hash, &signature, Some(&custom)),
        Verification::Invalid
    );
}

proptest! {
    #[test]
    fn custom_verifier_receives_exact_inputs(
        author_bytes in proptest::collection::vec(any::<u8>(), 0..=80),
        raw_hash in any::<[u8; 32]>(),
        raw_tag in 1u8..=u8::MAX,
        raw_signature in proptest::collection::vec(any::<u8>(), 0..=1024),
    ) {
        struct ExpectedInputs<'a> {
            author: &'a Author<'a>,
            message: &'a [u8],
            tag: SignatureTag,
            bytes: &'a SignatureBytes,
        }

        impl Verifier for ExpectedInputs<'_> {
            fn verify(
                &self,
                author: &Author<'_>,
                message: &[u8],
                tag: SignatureTag,
                bytes: &SignatureBytes,
            ) -> Verification {
                assert_eq!(author, self.author);
                assert_eq!(message, self.message);
                assert_eq!(tag, self.tag);
                assert_eq!(bytes, self.bytes);
                Verification::Valid
            }
        }

        let author = Author::from(author_bytes.as_slice());
        let hash = ChangeHash(raw_hash);
        let tag = SignatureTag::new(raw_tag);
        let bytes = SignatureBytes::new(raw_signature).expect("size-bounded input");
        let signature = Signature::new(tag, bytes.clone());
        // Independent literal checks that dispatch supplies the complete v1
        // message, not a raw hash, hex encoding, or another signing domain.
        let mut message = b"automerge-change-signature-v1".to_vec();
        message.extend_from_slice(&raw_hash);
        let custom = ExpectedInputs { author: &author, message: &message, tag, bytes: &bytes };

        prop_assert_eq!(
            verify(&author, &hash, &signature, Some(&custom)),
            Verification::Valid
        );
    }
}
