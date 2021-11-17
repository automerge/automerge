#![allow(unused_variables)]
#![allow(dead_code)]

use thiserror::Error;

#[derive(Error, Debug)]
pub enum AutomergeError {
    #[error("begin() called inside of a transaction")]
    MismatchedBegin,
    #[error("commit() called outside of a transaction")]
    MismatchedCommit,
    #[error("change made outside of a transaction")]
    OpOutsideOfTransaction,
    #[error("begin() called with actor not set")]
    ActorNotSet,
    #[error("invalid opid format `{0}`")]
    InvalidOpId(String),
    #[error("invalid actor format `{0}`")]
    InvalidActor(String),
    #[error("invalid list pos `{0}:{1}`")]
    InvalidListAt(String, usize),
    #[error("there was an encoding problem")]
    Encoding,
    #[error("key must not be an empty string")]
    EmptyStringKey,
    #[error("invalid seq {0}")]
    InvalidSeq(u64),
    #[error("index {0} is out of bounds")]
    InvalidIndex(usize),
    #[error("invalid prop {0}")]
    InvalidProp(String),
}

impl From<std::io::Error> for AutomergeError {
    fn from(e: std::io::Error) -> Self {
        AutomergeError::Encoding
    }
}
