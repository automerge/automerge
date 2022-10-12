use automerge as am;
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
    Automerge(#[from] am::AutomergeError),
}

pub(super) fn merge<W: std::io::Write>(inputs: Inputs, mut output: W) -> Result<(), MergeError> {
    let mut backend = am::Automerge::new();
    match inputs {
        Inputs::Stdin => {
            let mut input = Vec::new();
            std::io::stdin().read_to_end(&mut input)?;
            backend.load_incremental(&input)?;
        }
        Inputs::Paths(paths) => {
            for path in paths {
                load_path(&mut backend, &path)
                    .map_err(|error| MergeError::FailedToLoad { path, error })?;
            }
        }
    }
    output.write_all(&backend.save())?;
    Ok(())
}

fn load_path(backend: &mut am::Automerge, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let input = std::fs::read(path).map_err(Box::new)?;
    backend.load_incremental(&input).map_err(Box::new)?;
    Ok(())
}
