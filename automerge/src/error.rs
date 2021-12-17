#![allow(unused_variables)]
#![allow(dead_code)]

use crate::decoding;
use crate::value::DataType;
use crate::ScalarValue;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AutomergeError {
    #[error("invalid opid format `{0}`")]
    InvalidOpId(String),
    #[error("there was an ecoding problem")]
    Encoding,
    #[error("there was a decoding problem")]
    Decoding,
    #[error("key must not be an empty string")]
    EmptyStringKey,
    #[error("invalid seq {0}")]
    InvalidSeq(u64),
    #[error("index {0} is out of bounds")]
    InvalidIndex(usize),
}

impl From<std::io::Error> for AutomergeError {
    fn from(e: std::io::Error) -> Self {
        AutomergeError::Encoding
    }
}

impl From<decoding::Error> for AutomergeError {
    fn from(e: decoding::Error) -> Self {
        AutomergeError::Decoding
    }
}

#[derive(Error, Debug)]
#[error("Invalid actor ID: {0}")]
pub struct InvalidActorId(pub String);

#[derive(Error, Debug, PartialEq)]
#[error("Invalid scalar value, expected {expected} but received {unexpected}")]
pub struct InvalidScalarValue {
    pub raw_value: ScalarValue,
    pub datatype: DataType,
    pub unexpected: String,
    pub expected: String,
}
