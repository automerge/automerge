use std::{
    io::Read,
    path::{Path, PathBuf},
};

pub(super) enum Inputs {
    Stdin,
    Paths(Vec<PathBuf>),
}

impl From<Vec<PathBuf>> for Inputs {
    fn from(i: Vec<PathBuf>) -> Self {
        if i.is_empty() {
            Inputs::Stdin
        } else {
            Inputs::Paths(i)
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum MergeError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("failed to load {path}: {error}")]
    FailedToLoad {
        path: PathBuf,
        error: Box<dyn std::error::Error>,
    },
    #[error(transparent)]
    Automerge(#[from] automerge_backend::AutomergeError),
}

pub(super) fn merge<W: std::io::Write>(inputs: Inputs, mut output: W) -> Result<(), MergeError> {
    let mut backend = automerge_backend::Backend::new();
    match inputs {
        Inputs::Stdin => {
            let mut input = Vec::new();
            std::io::stdin().read_to_end(&mut input)?;
            let changes = automerge_backend::Change::load_document(&input)?;
            backend.load_changes(changes)?;
        }
        Inputs::Paths(paths) => {
            for path in paths {
                load_path(&mut backend, &path)
                    .map_err(|error| MergeError::FailedToLoad { path, error })?;
            }
        }
    }
    output.write_all(&backend.save().unwrap())?;
    Ok(())
}

fn load_path(
    backend: &mut automerge_backend::Backend,
    path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let input = std::fs::read(path).map_err(Box::new)?;
    let changes = automerge_backend::Change::load_document(&input).map_err(Box::new)?;
    backend.apply_changes(changes).map_err(Box::new)?;
    Ok(())
}
