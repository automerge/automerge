use std::error::Error;
use std::fmt;

#[derive(Debug, PartialEq)]
pub enum AutomergeCliError {
    InvalidChangesFile,
    BackendError,
}

impl fmt::Display for AutomergeCliError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for AutomergeCliError {}
