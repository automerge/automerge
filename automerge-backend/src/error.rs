use crate::protocol::ObjectID;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum AutomergeError {
    DuplicateObjectError,
    MissingObjectError(ObjectID),
    InvalidObjectType(String),
    InvalidLinkTarget,
    NotImplemented(String),
    InvalidChange(String),
}

impl fmt::Display for AutomergeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl Error for AutomergeError {}

#[derive(Debug)]
pub struct InvalidElementID(pub String);

impl fmt::Display for InvalidElementID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl Error for InvalidElementID {}

#[derive(Debug)]
pub struct InvalidChangeRequest(pub String);

impl Error for InvalidChangeRequest {}

impl fmt::Display for InvalidChangeRequest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}
