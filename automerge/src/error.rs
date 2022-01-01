use crate::decoding;
use crate::types::ScalarValue;
use crate::value::DataType;
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
    #[error("generic automerge error")]
    Fail,
}

impl From<std::io::Error> for AutomergeError {
    fn from(_: std::io::Error) -> Self {
        AutomergeError::Encoding
    }
}

impl From<decoding::Error> for AutomergeError {
    fn from(_: decoding::Error) -> Self {
        AutomergeError::Decoding
    }
}

#[derive(Error, Debug)]
#[error("Invalid actor ID: {0}")]
pub struct InvalidActorId(pub String);

#[derive(Error, Debug, PartialEq)]
#[error("Invalid scalar value, expected {expected} but received {unexpected}")]
pub(crate) struct InvalidScalarValue {
    pub raw_value: ScalarValue,
    pub datatype: DataType,
    pub unexpected: String,
    pub expected: String,
}

#[derive(Error, Debug, PartialEq)]
#[error("Invalid change hash slice: {0:?}")]
pub struct InvalidChangeHashSlice(pub Vec<u8>);

#[derive(Error, Debug, PartialEq)]
#[error("Invalid object ID: {0}")]
pub struct InvalidObjectId(pub String);

#[derive(Error, Debug)]
#[error("Invalid element ID: {0}")]
pub struct InvalidElementId(pub String);

#[derive(Error, Debug)]
#[error("Invalid OpID: {0}")]
pub struct InvalidOpId(pub String);
