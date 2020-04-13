use std::error::Error;
use std::fmt;

#[derive(Debug, PartialEq)]
pub enum AutomergeFrontendError {
    InvalidChangeRequest
}

impl fmt::Display for AutomergeFrontendError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for AutomergeFrontendError {}
