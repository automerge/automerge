use crate::mutation::Path;
use automerge_protocol::ObjectID;
use std::error::Error;
use std::fmt;

#[derive(Debug, PartialEq)]
pub enum AutomergeFrontendError {
    InvalidChangeRequest,
    MissingObjectError(ObjectID),
    NoSuchPathError(Path),
    MismatchedSequenceNumber,
    InvalidActorIDString(String),
}

impl fmt::Display for AutomergeFrontendError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<automerge_protocol::error::InvalidActorID> for AutomergeFrontendError {
    fn from(e: automerge_protocol::error::InvalidActorID) -> AutomergeFrontendError {
        AutomergeFrontendError::InvalidActorIDString(e.0)
    }
}

impl Error for AutomergeFrontendError {}

#[derive(Debug, PartialEq)]
pub enum InvalidInitialStateError {
    InitialStateMustBeMap,
}

impl fmt::Display for InvalidInitialStateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for InvalidInitialStateError {}
