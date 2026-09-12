use automerge::{self as am, ChangeHash};
use std::str::FromStr;

use crate::VerifyFlag;

#[derive(Debug, thiserror::Error)]
pub(super) enum ForkError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("invalid change hash {hash:?}: {source}")]
    BadHash {
        hash: String,
        #[source]
        source: am::ParseChangeHashError,
    },
    #[error(transparent)]
    Automerge(#[from] am::AutomergeError),
}

/// Load the document in `input`, fork it at `hash`, and write the forked
/// document to `output`.
///
/// The forked document contains only the changes which are ancestors of `hash`
/// (inclusive), and is given a fresh actor ID. If `hash` is not present in the
/// document an `AutomergeError::InvalidHash` is returned.
pub(super) fn fork<R: std::io::Read, W: std::io::Write>(
    mut input: R,
    mut output: W,
    hash: &str,
    skip: VerifyFlag,
) -> Result<(), ForkError> {
    let mut buf = Vec::new();
    input.read_to_end(&mut buf)?;
    let doc = skip.load(&buf)?;
    let hash = ChangeHash::from_str(hash).map_err(|source| ForkError::BadHash {
        hash: hash.to_string(),
        source,
    })?;
    let forked = doc.fork_at(&[hash])?;
    output.write_all(&forked.save())?;
    Ok(())
}
