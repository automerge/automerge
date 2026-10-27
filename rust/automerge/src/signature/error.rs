//! Structural errors constructing scheme-tagged signature values.

/// A signature length exceeding the maximum of 1024 bytes.
#[derive(Debug, thiserror::Error)]
pub enum SignatureBytes {
    #[error("the signature size, '{actual}' bytes, exceeds the maximum")]
    ExceededSize {
        /// Length of the rejected signature bytes.
        actual: u64,
    },
}
