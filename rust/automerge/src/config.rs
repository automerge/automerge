//! Options for creating a document, independent of loading policy.

#[cfg(test)]
mod tests;

use crate::error::SigningAuthorErrorInner;
use crate::signature::ed25519::VerifyingAuthor;
use crate::{ActorId, Author, SigningAuthorError, TextEncoding};

/// Construction-time local identity and signing intent.
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
    pub(crate) fn signing(author: Author<'a>) -> Result<Self, SigningAuthorError> {
        VerifyingAuthor::new(author)
            .map(Self::Verifier)
            .map_err(|error| SigningAuthorError(SigningAuthorErrorInner::InvalidKey(error)))
    }
}

/// Configure a new document before passing it to [`crate::Automerge::from_config`]
/// or [`crate::AutoCommit::from_config`].
///
/// The default is unsigned, with no local author and the platform's text encoding.
/// [`Self::author`] chooses an ordinary unsigned author; [`Self::signing`] prepares
/// a usable Ed25519 public key and selects required signing mode. Either method
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
/// where the document mode permits it. Signing mode is always inherited.
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

    /// Select unsigned mode with this optional local author.
    ///
    /// Author bytes are arbitrary; key-shaped bytes do not enable signing.
    pub fn author(self, author: Option<Author<'static>>) -> Self {
        Self {
            author: author.into(),
            ..self
        }
    }

    /// Select required signing mode and retain the author's prepared verifier.
    ///
    /// # Errors
    ///
    /// Returns [`SigningAuthorError`] for malformed or weak Ed25519 keys.
    pub fn signing(self, author: Author<'static>) -> Result<Self, SigningAuthorError> {
        Ok(Self {
            author: DocumentAuthor::signing(author)?,
            ..self
        })
    }
}
