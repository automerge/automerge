mod raw;

pub(crate) use raw::{RawDecoder, RawEncoder};
mod rle;
pub(crate) use rle::{RleDecoder, RleEncoder};
mod boolean;
pub(crate) use boolean::{BooleanDecoder, BooleanEncoder};
mod delta;
pub(crate) use delta::{DeltaDecoder, DeltaEncoder};
pub(crate) mod leb128;

#[cfg(test)]
pub(crate) mod properties;

pub(crate) trait Encodable {
    fn encode(&self, out: &mut Vec<u8>) -> usize;
}
mod encodable_impls;
pub(crate) use encodable_impls::RawBytes;

#[derive(thiserror::Error, Debug)]
pub(crate) enum DecodeError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("invalid integer")]
    FromInt(#[from] std::num::TryFromIntError),
    #[error("bad leb128")]
    BadLeb(#[from] ::leb128::read::Error),
    #[error("attempted to allocate {attempted} which is larger than the maximum of {maximum}")]
    OverlargeAllocation { attempted: usize, maximum: usize },
    #[error("invalid string encoding")]
    BadString,
}

pub(crate) trait Decodable: Sized {
    fn decode<R>(bytes: &mut R) -> Result<Self, DecodeError>
    where
        R: std::io::Read;
}
mod decodable_impls;

#[derive(Clone, thiserror::Error, Debug)]
pub(crate) enum DecodeColumnError {
    #[error("unexpected null decoding column {0}")]
    UnexpectedNull(String),
    #[error("invalid value in column {column}: {description}")]
    InvalidValue { column: String, description: String },
}
