//! Signature values with bounded, opaque bytes and explicit scheme tags.
//!
//! Construction does not establish cryptographic validity. Storage codecs
//! control how these values are encoded and decoded.

use std::hash::{Hash, Hasher};

use crate::types;
use crate::ChangeHash;

mod verifier;

pub use error::SignatureBytes as SignatureBytesError;
pub use verifier::{verify as verify_signature, Verification, Verifier};

const SIGNING_MESSAGE_LEN: usize = SIGNING_MESSAGE_PREFIX.len() + types::HASH_SIZE;
const SIGNING_MESSAGE_PREFIX: &[u8] = b"automerge-change-signature-v1";

/// An owned, unverified Ed25519 signature representation.
///
/// Use [`Ed25519Signature::from_bytes`] with the 64 bytes returned by an external
/// signer. Construction accepts every 64-byte array and does not verify it
/// against a key or message. Wrap it with [`Signature::ed25519`] to use
/// [`verify_signature`]. The cryptographic library used internally is not part
/// of this interface.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ed25519Signature {
    inner: ed25519_dalek::Signature,
}

impl Ed25519Signature {
    /// Construct from the standard 64-byte Ed25519 signature representation.
    ///
    /// This is infallible and does not allocate or establish cryptographic validity.
    pub fn from_bytes(bytes: [u8; 64]) -> Self {
        Self {
            inner: ed25519_dalek::Signature::from_bytes(&bytes),
        }
    }

    /// Return the exact 64 signature bytes without allocating.
    pub fn to_bytes(&self) -> [u8; 64] {
        self.inner.to_bytes()
    }
}

impl Hash for Ed25519Signature {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.to_bytes().hash(state);
    }
}

/// A signature scheme tag and its size-checked signature bytes.
///
/// Construct with [`Signature::new`] using a [`SignatureTag`] and validated
/// [`SignatureBytes`], or wrap an [`Ed25519Signature`] with [`Signature::ed25519`].
/// The associated change hash and author are supplied separately when verifying
/// a signature.
///
/// Ed25519-sized bytes with the Ed25519 tag are retained as a typed
/// [`Ed25519Signature`]. Unknown schemes and malformed lengths remain opaque,
/// preserving their tag and bytes. Construction does not establish cryptographic
/// validity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Signature {
    repr: SignatureRepr,
}

// Both constructors classify tag-0, 64-byte values identically. Opaque tag-0
// values therefore have malformed lengths, not merely unverified contents.
#[derive(Clone, Debug, PartialEq, Eq)]
enum SignatureRepr {
    Ed25519(Ed25519Signature),
    Opaque {
        tag: SignatureTag,
        bytes: SignatureBytes,
    },
}

impl Signature {
    /// Classify a scheme tag and already size-checked bytes without verification.
    ///
    /// Unknown schemes and malformed Ed25519 lengths are retained, not rejected.
    pub fn new(tag: SignatureTag, bytes: SignatureBytes) -> Self {
        if tag == SignatureTag::ED25519 {
            if let Ok(bytes) = <[u8; 64]>::try_from(bytes.as_bytes()) {
                return Self::ed25519(Ed25519Signature::from_bytes(bytes));
            }
        }
        Self {
            repr: SignatureRepr::Opaque { tag, bytes },
        }
    }

    /// Retain an Ed25519 signature directly, without allocating a byte vector.
    ///
    /// Preserves its 64-byte representation, which is within the signature size
    /// limit. This does not verify the signature against a key or message.
    pub fn ed25519(signature: Ed25519Signature) -> Self {
        const { assert!(64 <= SignatureBytes::MAX_SIZE) };
        Self {
            repr: SignatureRepr::Ed25519(signature),
        }
    }

    /// Return the scheme used to interpret this signature.
    pub fn tag(&self) -> SignatureTag {
        match &self.repr {
            SignatureRepr::Ed25519(_) => SignatureTag::ED25519,
            SignatureRepr::Opaque { tag, .. } => *tag,
        }
    }

    /// Borrow the typed Ed25519 signature, if its tag and length identify one.
    ///
    /// Returns `None` for other schemes and malformed Ed25519 lengths. A `Some`
    /// result establishes representation only, not cryptographic validity.
    pub fn as_ed25519(&self) -> Option<&Ed25519Signature> {
        match &self.repr {
            SignatureRepr::Ed25519(signature) => Some(signature),
            SignatureRepr::Opaque { .. } => None,
        }
    }

    /// Copy the exact signature bytes into an owned, size-checked buffer.
    ///
    /// This does not encode the scheme tag or any storage framing.
    pub fn to_bytes(&self) -> SignatureBytes {
        match &self.repr {
            SignatureRepr::Ed25519(signature) => SignatureBytes {
                bytes: signature.to_bytes().to_vec(),
            },
            SignatureRepr::Opaque { bytes, .. } => bytes.clone(),
        }
    }
}

impl Hash for Signature {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.tag().hash(state);
        match &self.repr {
            SignatureRepr::Ed25519(signature) => signature.to_bytes().as_slice().hash(state),
            SignatureRepr::Opaque { bytes, .. } => bytes.as_bytes().hash(state),
        }
    }
}

/// Owned signature bytes with a validated length.
///
/// Accepts lengths from 0 through 1024 bytes, inclusive. The contents remain
/// opaque. Scheme-specific lengths and cryptographic validity are not checked
/// here. Use [`SignatureBytes::as_bytes`] to borrow the bytes without copying.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SignatureBytes {
    bytes: Vec<u8>,
}

impl SignatureBytes {
    /// Maximum permitted signature length, in bytes, across all schemes.
    pub const MAX_SIZE: usize = 1024;

    /// Take ownership of signature bytes after validating their length.
    ///
    /// Empty inputs are accepted. No scheme-specific or cryptographic
    /// validation is performed.
    ///
    /// # Errors
    ///
    /// Rejects inputs longer than 1024 bytes, reporting the actual length.
    pub fn new(bytes: Vec<u8>) -> Result<Self, SignatureBytesError> {
        let size = bytes.len();
        if size > Self::MAX_SIZE {
            Err(error::SignatureBytes::ExceededSize {
                actual: size as u64,
            })
        } else {
            Ok(Self { bytes })
        }
    }

    /// Borrow the signature bytes without permitting their length to change.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Identifies the scheme used to interpret a signature.
///
/// All byte values are accepted, including unknown schemes. A tag identifies
/// a signature scheme, not a signer or a storage codec version. Construction
/// does not establish that the scheme is supported.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SignatureTag {
    tag: u8,
}

impl SignatureTag {
    /// The Ed25519 signature scheme, assigned tag 0.
    pub const ED25519: Self = Self { tag: 0 };

    /// Construct a scheme tag from any byte, including unknown scheme values.
    pub fn new(tag: u8) -> Self {
        Self { tag }
    }

    /// Return the underlying scheme identifier.
    pub fn inner_tag(&self) -> u8 {
        self.tag
    }
}

/// Return the domain-separated message to sign for a change hash.
///
/// The message is the ASCII prefix `automerge-change-signature-v1` followed
/// immediately by the 32 raw hash bytes, for 61 bytes total. Sign these bytes
/// directly with Ed25519, without an additional prehash step.
///
/// # Examples
///
/// ```
/// use automerge::{
///     signing_message, verify_signature, Author, ChangeHash, Ed25519Signature,
///     Signature, Verification,
/// };
/// use ed25519_dalek::{Signer, SigningKey};
///
/// // The application chooses its signer. Only its public-key and signature
/// // bytes cross the Automerge interface, not the signer's library types.
/// let key = SigningKey::from_bytes(&[7; 32]);
/// let author = Author::from(key.verifying_key().to_bytes().to_vec());
/// let hash = ChangeHash([3; 32]);
/// let signed_bytes = key.sign(&signing_message(&hash)).to_bytes();
/// let signature = Signature::ed25519(Ed25519Signature::from_bytes(signed_bytes));
///
/// assert_eq!(
///     verify_signature(&author, &hash, &signature, None),
///     Verification::Valid,
/// );
/// ```
pub fn signing_message(hash: &ChangeHash) -> [u8; SIGNING_MESSAGE_LEN] {
    let mut message = [0; SIGNING_MESSAGE_LEN];
    let (prefix, hash_bytes) = message.split_at_mut(SIGNING_MESSAGE_PREFIX.len());

    prefix.copy_from_slice(SIGNING_MESSAGE_PREFIX);
    hash_bytes.copy_from_slice(&hash.0);

    message
}

mod error {
    /// A signature length exceeding the maximum of 1024 bytes.
    #[derive(Debug, thiserror::Error)]
    pub enum SignatureBytes {
        #[error("the signature size, '{actual}' bytes, exceeds the maximum")]
        ExceededSize {
            /// Length of the rejected signature bytes.
            actual: u64,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    use proptest::prelude::*;

    use crate::ChangeHash;

    use super::{
        error, signing_message, Ed25519Signature, Signature, SignatureBytes, SignatureTag,
    };

    #[test]
    fn signature_tag_preserves_every_byte_value() {
        for byte in u8::MIN..=u8::MAX {
            let tag = SignatureTag::new(byte);

            assert_eq!(tag.inner_tag(), byte);
        }
    }

    #[test]
    fn ed25519_tag_is_zero() {
        assert_eq!(SignatureTag::ED25519.inner_tag(), 0);
    }

    #[test]
    fn signature_bytes_accepts_empty_input() {
        let bytes = SignatureBytes::new(Vec::new()).expect("empty input is permitted");

        assert!(bytes.as_bytes().is_empty());
    }

    #[test]
    fn signature_bytes_accepts_1024_bytes() {
        let input: Vec<u8> = (u8::MIN..=u8::MAX).cycle().take(1024).collect();
        let bytes = SignatureBytes::new(input.clone()).expect("1024 bytes is permitted");

        assert_eq!(bytes.as_bytes(), input.as_slice());
    }

    #[test]
    fn signature_bytes_rejects_1025_bytes() {
        assert!(matches!(
            SignatureBytes::new(vec![7; 1025]),
            Err(error::SignatureBytes::ExceededSize { actual: 1025 })
        ));
    }

    #[test]
    fn signature_new_accepts_unknown_tag_and_empty_bytes() {
        let tag = SignatureTag::new(255);
        let bytes = SignatureBytes::new(Vec::new()).expect("empty input is permitted");

        let signature = Signature::new(tag, bytes);

        assert_eq!(signature.tag(), tag);
        assert!(signature.to_bytes().as_bytes().is_empty());
    }

    #[test]
    fn signature_ed25519_wraps_unverified_bytes() {
        let input = [0; 64];
        let signature = Signature::ed25519(Ed25519Signature::from_bytes(input));

        assert_eq!(signature.tag(), SignatureTag::ED25519);
        assert_eq!(signature.to_bytes().as_bytes(), input.as_slice());
    }

    #[test]
    fn malformed_ed25519_lengths_remain_opaque_and_preserve_bytes() {
        for len in (0..=1024).filter(|len| *len != 64) {
            let bytes = SignatureBytes::new(vec![7; len]).expect("size-bounded input");
            let signature = Signature::new(SignatureTag::ED25519, bytes.clone());

            assert!(signature.as_ed25519().is_none());
            assert_eq!(signature.tag(), SignatureTag::ED25519);
            assert_eq!(signature.to_bytes(), bytes);
        }
    }

    #[test]
    fn unknown_tags_do_not_classify_ed25519_sized_bytes() {
        let bytes = SignatureBytes::new(vec![7; 64]).expect("64-byte input");
        for raw_tag in 1..=u8::MAX {
            let tag = SignatureTag::new(raw_tag);
            let signature = Signature::new(tag, bytes.clone());

            assert!(signature.as_ed25519().is_none());
            assert_eq!(signature.tag(), tag);
            assert_eq!(signature.to_bytes(), bytes);
        }
    }

    #[test]
    fn signing_message_is_exact_v1_prefix_followed_by_raw_hash() {
        let hash = ChangeHash(std::array::from_fn(|i| i as u8));

        let message: [u8; 61] = signing_message(&hash);

        let expected = b"automerge-change-signature-v1\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f";
        assert_eq!(&message, expected);
    }

    proptest! {
        #[test]
        fn ed25519_construction_paths_preserve_the_same_typed_value(input in any::<[u8; 64]>()) {
            let ed25519 = Ed25519Signature::from_bytes(input);
            let typed = Signature::ed25519(ed25519);
            let bytes = SignatureBytes::new(input.to_vec()).expect("64-byte input");
            let decoded = Signature::new(SignatureTag::ED25519, bytes.clone());

            prop_assert_eq!(typed.as_ed25519(), Some(&ed25519));
            prop_assert_eq!(decoded.as_ed25519(), Some(&ed25519));
            prop_assert_eq!(&typed, &decoded);
            prop_assert_eq!(typed.to_bytes(), bytes);

            let mut typed_hash = DefaultHasher::new();
            let mut decoded_hash = DefaultHasher::new();
            typed.hash(&mut typed_hash);
            decoded.hash(&mut decoded_hash);
            prop_assert_eq!(typed_hash.finish(), decoded_hash.finish());
        }

        #[test]
        fn signing_message_preserves_arbitrary_hash_with_fixed_domain(
            raw_hash in any::<[u8; 32]>(),
        ) {
            let message: [u8; 61] = signing_message(&ChangeHash(raw_hash));

            prop_assert_eq!(&message[..29], b"automerge-change-signature-v1");
            prop_assert_eq!(&message[29..], raw_hash.as_slice());
        }

        #[test]
        fn signature_ed25519_preserves_wrapped_bytes(input in any::<[u8; 64]>()) {
            let signature = Signature::ed25519(Ed25519Signature::from_bytes(input));

            prop_assert_eq!(signature.tag(), SignatureTag::ED25519);
            let bytes = signature.to_bytes();
            prop_assert_eq!(bytes.as_bytes(), input.as_slice());
        }

        #[test]
        fn signature_new_preserves_tag_and_bytes(
            raw_tag in any::<u8>(),
            input in proptest::collection::vec(any::<u8>(), 0..=1024),
        ) {
            let tag = SignatureTag::new(raw_tag);
            let bytes = SignatureBytes::new(input.clone()).expect("size-bounded input");

            let signature = Signature::new(tag, bytes);

            prop_assert_eq!(signature.tag(), tag);
            let bytes = signature.to_bytes();
            prop_assert_eq!(bytes.as_bytes(), input.as_slice());
        }

        #[test]
        fn signature_bytes_preserves_arbitrary_bounded_input(
            input in proptest::collection::vec(any::<u8>(), 0..=1024),
        ) {
            let bytes = SignatureBytes::new(input.clone()).expect("size-bounded input");

            prop_assert_eq!(bytes.as_bytes(), input.as_slice());
        }

        #[test]
        fn signature_bytes_reports_actual_oversized_length(
            input in proptest::collection::vec(any::<u8>(), 1025..=4096),
        ) {
            let expected = input.len() as u64;
            match SignatureBytes::new(input) {
                Err(error::SignatureBytes::ExceededSize { actual }) => {
                    prop_assert_eq!(actual, expected);
                }
                Ok(_) => prop_assert!(false, "oversized input must be rejected"),
            }
        }
    }
}
