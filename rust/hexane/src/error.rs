/// Errors returned when decoding or validating column data.
#[derive(thiserror::Error, Debug)]
pub enum PackError {
    /// A LEB128 varint in the encoded data was malformed.
    #[error(transparent)]
    InvalidNumber(#[from] leb128::read::Error),
    /// A string column contained invalid UTF-8 bytes.
    #[error("invalid utf8")]
    InvalidUtf8,
    /// A value failed a caller-supplied validation check (from `load_with`).
    #[error("invalid value: {0}")]
    InvalidValue(String),
    /// The decoded column length did not match the expected length passed
    /// via [`LoadOpts::with_length`](crate::LoadOpts::with_length).
    #[error("invalid load length len={0}, expected={1}")]
    InvalidLength(usize, usize),
    /// The encoded data did not follow the expected format.
    #[error("malformed leb encoding")]
    BadFormat,
    /// An iterator state produced by `suspend()` could not be resumed
    /// because the underlying column was mutated after the suspend.
    #[error("invalid resume")]
    InvalidResume,
}
