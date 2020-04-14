use crate::protocol::{Key, ObjectID, OpID, OpRequest};
use std::error::Error;
use std::fmt;

#[derive(Debug, PartialEq)]
pub enum AutomergeError {
    MissingObjectError(ObjectID),
    MissingIndex(OpID),
    MissingChildID(String),
    NoPathToObject(ObjectID),
    CantExtractObject(ObjectID),
    MissingElement(ObjectID, OpID),
    LinkMissingChild(OpID),
    SkipListError(String),
    GetChildFailed(ObjectID, Key),
    IndexOutOfBounds(usize),
    InvalidOpID(String),
    InvalidObjectID(String),
    MissingPrimitiveValue,
    MissingNumberValue(OpRequest),
    UnknownVersion(u64),
    DuplicateChange(String),
    InvalidChange(String),
    DivergedState(String),
}

impl fmt::Display for AutomergeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for AutomergeError {}

#[derive(Debug)]
pub struct InvalidElementID(pub String);

impl fmt::Display for InvalidElementID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for InvalidElementID {}
