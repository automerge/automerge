use automerge::{
    signing_message, verify_signature, Author, ChangeHash, Ed25519Signature, Signature,
    SignatureBytes, SignatureBytesError, SignatureTag, Verification, Verifier,
};
use ed25519_dalek::{Signer, SigningKey};
use proptest::prelude::*;

#[test]
fn external_signer_can_construct_and_verify_a_signature_through_bytes() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    let hash = ChangeHash([3; 32]);
    let message: [u8; 61] = signing_message(&hash);
    // Dalek is this application's choice of signer. Only standard bytes cross
    // the Automerge interface, so another library or device can replace it.
    let signed_bytes: [u8; 64] = key.sign(&message).to_bytes();
    let ed25519 = Ed25519Signature::from_bytes(signed_bytes);
    let signature = Signature::ed25519(ed25519);
    let borrowed: &Ed25519Signature = signature.as_ed25519().expect("Ed25519 representation");

    assert_eq!(borrowed.to_bytes(), signed_bytes);
    assert_eq!(signature.tag(), SignatureTag::ED25519);
    assert_eq!(SignatureTag::ED25519.inner_tag(), 0);
    assert_eq!(
        verify_signature(&author, &hash, &signature, None),
        Verification::Valid
    );
    assert_eq!(
        verify_signature(&author, &ChangeHash([4; 32]), &signature, None),
        Verification::Invalid
    );
}

#[test]
fn owned_ed25519_representation_does_not_claim_validity() {
    let signature = Signature::ed25519(Ed25519Signature::from_bytes([0; 64]));
    let key = SigningKey::from_bytes(&[7; 32]);
    let author = Author::from(key.verifying_key().to_bytes().to_vec());

    assert!(signature.as_ed25519().is_some());
    assert_eq!(signature.to_bytes().as_bytes(), &[0; 64]);
    assert_eq!(
        verify_signature(&author, &ChangeHash([3; 32]), &signature, None),
        Verification::Invalid
    );
}

#[test]
fn bounded_bytes_error_is_public_and_matchable() {
    assert_eq!(SignatureBytes::MAX_SIZE, 1024);
    let error: SignatureBytesError = SignatureBytes::new(vec![0; 1025]).unwrap_err();

    assert!(matches!(
        error,
        SignatureBytesError::ExceededSize { actual: 1025 }
    ));
}

#[test]
fn applications_can_implement_custom_verification_using_only_automerge_types() {
    struct CheckInputs;

    impl Verifier for CheckInputs {
        fn verify(
            &self,
            author: &Author<'_>,
            message: &[u8],
            tag: SignatureTag,
            bytes: &SignatureBytes,
        ) -> Verification {
            assert_eq!(author.as_bytes(), &[1, 2, 3]);
            assert_eq!(&message[..29], b"automerge-change-signature-v1");
            assert_eq!(&message[29..], &[9; 32]);
            assert_eq!(tag.inner_tag(), 42);
            assert_eq!(bytes.as_bytes(), &[5, 6]);
            Verification::Valid
        }
    }

    let author = Author::from(vec![1, 2, 3]);
    let hash = ChangeHash([9; 32]);
    let signature = Signature::new(
        SignatureTag::new(42),
        SignatureBytes::new(vec![5, 6]).unwrap(),
    );

    assert_eq!(
        verify_signature(&author, &hash, &signature, None),
        Verification::Unsupported
    );
    assert_eq!(
        verify_signature(&author, &hash, &signature, Some(&CheckInputs)),
        Verification::Valid
    );
}

proptest! {
    #[test]
    fn owned_ed25519_bytes_round_trip_and_match_generic_construction(bytes in any::<[u8; 64]>()) {
        let ed25519 = Ed25519Signature::from_bytes(bytes);
        prop_assert_eq!(ed25519.to_bytes(), bytes);

        let typed = Signature::ed25519(ed25519);
        let generic = Signature::new(
            SignatureTag::ED25519,
            SignatureBytes::new(bytes.to_vec()).unwrap(),
        );
        prop_assert_eq!(typed.as_ed25519(), Some(&ed25519));
        prop_assert_eq!(generic.as_ed25519(), Some(&ed25519));
        prop_assert_eq!(typed, generic);
    }
}
