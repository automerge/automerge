use crate::{SignatureMode, TextEncoding};

/// How a document is configured when it is created.
///
/// These settings are fixed for the life of a document and inherited by its
/// forks, which is what separates them from the actor, the author or the
/// write-frontier: those describe what a document is doing now and may change.
/// A document's text encoding decides how its sequence indices are counted, and
/// its signature mode decides what its import paths mean, so neither can change
/// without making its existing history mean something different.
///
/// ```
/// # use automerge::{Automerge, Config, SignatureMode, TextEncoding};
/// let doc = Automerge::new_with_config(
///     Config::new()
///         .text_encoding(TextEncoding::Utf8CodeUnit)
///         .signature_mode(SignatureMode::Authenticated),
/// );
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    text_encoding: TextEncoding,
    signature_mode: SignatureMode,
}

impl Config {
    pub fn new() -> Self {
        Self::default()
    }

    /// How to count indices into sequences of text.
    ///
    /// Defaults to [`TextEncoding::platform_default`].
    pub fn text_encoding(self, text_encoding: TextEncoding) -> Self {
        Self {
            text_encoding,
            ..self
        }
    }

    /// Whether to authenticate the remote history this document accepts.
    ///
    /// Defaults to [`SignatureMode::Legacy`], which accepts it as Automerge
    /// always has.
    pub fn signature_mode(self, signature_mode: SignatureMode) -> Self {
        Self {
            signature_mode,
            ..self
        }
    }

    pub(crate) fn encoding(&self) -> TextEncoding {
        self.text_encoding
    }

    pub(crate) fn signatures(&self) -> SignatureMode {
        self.signature_mode
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            text_encoding: TextEncoding::platform_default(),
            signature_mode: SignatureMode::Legacy,
        }
    }
}
