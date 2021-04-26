use thiserror::Error;

use crate::{DataType, ScalarValue};

#[derive(Error, Debug)]
#[error("Invalid OpID: {0}")]
pub struct InvalidOpId(pub String);

#[derive(Error, Debug, PartialEq)]
#[error("Invalid object ID: {0}")]
pub struct InvalidObjectId(pub String);

#[derive(Error, Debug)]
#[error("Invalid element ID: {0}")]
pub struct InvalidElementId(pub String);

#[derive(Error, Debug)]
#[error("Invalid actor ID: {0}")]
pub struct InvalidActorId(pub String);

#[derive(Error, Debug, PartialEq)]
#[error("Invalid change hash slice: {0:?}")]
pub struct InvalidChangeHashSlice(pub Vec<u8>);

#[derive(Error, Debug, PartialEq)]
#[error("Invalid scalar value, expected {expected} but received {unexpected}")]
pub struct InvalidScalarValue {
    pub raw_value: ScalarValue,
    pub datatype: DataType,
    pub unexpected: String,
    pub expected: String,
}
