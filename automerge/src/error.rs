use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub struct InvalidChangeRequest(pub String);

impl Error for InvalidChangeRequest {}

impl fmt::Display for InvalidChangeRequest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}
