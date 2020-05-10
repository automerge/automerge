use thiserror::Error;

#[derive(Error, Debug)]
#[error("Invalid OpID: {0}")]
pub struct InvalidOpID(pub String);

#[derive(Error, Debug)]
#[error("Invalid object ID: {0}")]
pub struct InvalidObjectID(pub String);

#[derive(Error, Debug)]
#[error("Invalid element ID: {0}")]
pub struct InvalidElementID(pub String);

#[derive(Error, Debug)]
#[error("Invalid change hash slice: {0:?}")]
pub struct InvalidChangeHashSlice(pub Vec<u8>);






