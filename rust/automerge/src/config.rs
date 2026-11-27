//! Options for creating a document, independent of loading policy.

#[cfg(test)]
mod tests;

use crate::error::AttestationAuthorErrorInner;
use crate::signature::ed25519::VerifyingAuthor;
use crate::{ActorId, AttestationAuthorError, Author, TextEncoding};

/// Construction-time local identity and attestation intent.
#[derive(Clone, Debug, Default)]
pub(crate) enum DocumentAuthor<'a> {
    #[default]
    Unset,
    Set(Author<'a>),
    Verifier(VerifyingAuthor<'a>),
}

impl<'a> From<Option<Author<'a>>> for DocumentAuthor<'a> {
    fn from(author: Option<Author<'a>>) -> Self {
        match author {
            Some(author) => Self::Set(author),
            None => Self::Unset,
        }
    }
}

impl<'a> DocumentAuthor<'a> {
    pub(crate) fn attested(author: Author<'a>) -> Result<Self, AttestationAuthorError> {
        VerifyingAuthor::new(author)
            .map(Self::Verifier)
            .map_err(|error| AttestationAuthorError(AttestationAuthorErrorInner::InvalidKey(error)))
    }
}

/// Configure a new document before passing it to [`crate::Automerge::from_config`]
/// or [`crate::AutoCommit::from_config`].
///
/// The default is unattested, with no local author and the platform's text encoding.
/// [`Self::author`] chooses an ordinary unattested author; [`Self::attested`] prepares
/// a usable Ed25519 public key and selects an attested document. Either method
/// replaces the previous identity choice. No private signing key is retained.
#[derive(Clone, Debug)]
pub struct Config {
    pub(crate) text_encoding: TextEncoding,
    pub(crate) author: DocumentAuthor<'static>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            text_encoding: TextEncoding::platform_default(),
            author: DocumentAuthor::Unset,
        }
    }
}

/// Choices for a full writable fork. Omitted values inherit the local author
/// and generate a fresh actor; explicit `author(None)` chooses anonymous identity
/// where the document permits it. Attestation is always inherited.
#[derive(Clone, Debug, Default)]
pub struct ForkOptions {
    pub(crate) author: Option<Option<Author<'static>>>,
    pub(crate) actor: Option<ActorId>,
}

impl ForkOptions {
    /// Replace the fork's local author without rewriting historical bindings.
    pub fn author(self, author: Option<Author<'static>>) -> Self {
        Self {
            author: Some(author),
            ..self
        }
    }

    /// Choose an actor compatible with the fork's local author.
    pub fn actor(self, actor: ActorId) -> Self {
        Self {
            actor: Some(actor),
            ..self
        }
    }
}

impl Config {
    /// Select how text positions are measured.
    pub fn text_encoding(self, text_encoding: TextEncoding) -> Self {
        Self {
            text_encoding,
            ..self
        }
    }

    /// Select an unattested document with this optional local author.
    ///
    /// Author bytes are arbitrary; key-shaped bytes do not enable attestation.
    pub fn author(self, author: Option<Author<'static>>) -> Self {
        Self {
            author: author.into(),
            ..self
        }
    }

    /// Select an attested document and retain the author's prepared verifier.
    ///
    /// # Errors
    ///
    /// Returns [`AttestationAuthorError`] for malformed or weak Ed25519 keys.
    pub fn attested(self, author: Author<'static>) -> Result<Self, AttestationAuthorError> {
        Ok(Self {
            author: DocumentAuthor::attested(author)?,
            ..self
        })
    }
}
