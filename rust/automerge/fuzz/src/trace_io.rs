use std::fs;
use std::path::Path;

use crate::trace::Trace;

pub fn load_trace(path: impl AsRef<Path>) -> Result<Trace, TraceIoError> {
    let path = path.as_ref();
    let data = fs::read_to_string(path).map_err(|source| TraceIoError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    serde_json::from_str(&data).map_err(|source| TraceIoError::Parse {
        path: path.to_path_buf(),
        source,
    })
}

pub fn save_trace(path: impl AsRef<Path>, trace: &Trace) -> Result<(), TraceIoError> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| TraceIoError::CreateDir {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let data = serde_json::to_string_pretty(trace).map_err(TraceIoError::Serialize)?;
    fs::write(path, data).map_err(|source| TraceIoError::Write {
        path: path.to_path_buf(),
        source,
    })
}

#[derive(Debug)]
pub enum TraceIoError {
    Read {
        path: std::path::PathBuf,
        source: std::io::Error,
    },
    Write {
        path: std::path::PathBuf,
        source: std::io::Error,
    },
    CreateDir {
        path: std::path::PathBuf,
        source: std::io::Error,
    },
    Parse {
        path: std::path::PathBuf,
        source: serde_json::Error,
    },
    Serialize(serde_json::Error),
}

impl std::fmt::Display for TraceIoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read { path, source } => write!(f, "failed to read {}: {source}", path.display()),
            Self::Write { path, source } => {
                write!(f, "failed to write {}: {source}", path.display())
            }
            Self::CreateDir { path, source } => {
                write!(f, "failed to create directory {}: {source}", path.display())
            }
            Self::Parse { path, source } => {
                write!(f, "failed to parse trace {}: {source}", path.display())
            }
            Self::Serialize(source) => write!(f, "failed to serialize trace: {source}"),
        }
    }
}

impl std::error::Error for TraceIoError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Read { source, .. } => Some(source),
            Self::Write { source, .. } => Some(source),
            Self::CreateDir { source, .. } => Some(source),
            Self::Parse { source, .. } => Some(source),
            Self::Serialize(source) => Some(source),
        }
    }
}
