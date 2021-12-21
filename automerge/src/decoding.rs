use core::fmt::Debug;
use std::{borrow::Cow, convert::TryFrom, io, io::Read, str};

use crate::error;
use crate::legacy as amp;
use crate::ActorId;
use smol_str::SmolStr;

/// The error type for decoding operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(
        "Expected the buffer size to change due to having something written to it but it did not"
    )]
    BufferSizeDidNotChange,
    #[error("Decode operation did not return a value")]
    NoDecodedValue,
    #[error("Trying to read past the end of the buffer")]
    TryingToReadPastEnd,
    #[error(
        "Found wrong of data while decoding, expected one of {expected_one_of:?} but found {found}"
    )]
    WrongType { expected_one_of: Vec<u8>, found: u8 },
    #[error("Bad change format: {0}")]
    BadChangeFormat(#[source] error::InvalidChangeHashSlice),
    #[error("Not enough bytes")]
    NotEnoughBytes,
    #[error("Found the wrong magic bytes in the document")]
    WrongMagicBytes,
    #[error("Bytes had wrong length, expected {expected} but found {found}")]
    WrongByteLength { expected: usize, found: usize },
    #[error("Columns were not in ascending order, last was {last} but found {found}")]
    ColumnsNotInAscendingOrder { last: u32, found: u32 },
    #[error("A change contained compressed columns, which is not supported")]
    ChangeContainedCompressedColumns,
    #[error("Found mismatching checksum values, calculated {calculated:?} but found {found:?}")]
    InvalidChecksum { found: [u8; 4], calculated: [u8; 4] },
    #[error("Invalid change: {0}")]
    InvalidChange(#[from] InvalidChangeError),
    #[error("Change decompression error: {0}")]
    ChangeDecompressFailed(String),
    #[error("No doc changes found")]
    NoDocChanges,
    #[error("An overflow would have occurred, the data may be corrupt")]
    Overflow,
    #[error("Calculated heads differed from actual heads")]
    MismatchedHeads,
    #[error("Failed to read leb128 number {0}")]
    Leb128(#[from] leb128::read::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum InvalidChangeError {
    #[error("Change contained an operation with action 'set' which did not have a 'value'")]
    SetOpWithoutValue,
    #[error("Received an inc operation which had an invalid value, value was: {op_value:?}")]
    IncOperationWithInvalidValue { op_value: Option<amp::ScalarValue> },
    #[error("Change contained an invalid object id: {}", source.0)]
    InvalidObjectId {
        #[from]
        source: error::InvalidObjectId,
    },
    #[error("Change contained an invalid hash: {:?}", source.0)]
    InvalidChangeHash {
        #[from]
        source: error::InvalidChangeHashSlice,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct Decoder<'a> {
    pub offset: usize,
    pub last_read: usize,
    data: Cow<'a, [u8]>,
}

impl<'a> Decoder<'a> {
    pub fn new(data: Cow<'a, [u8]>) -> Self {
        Decoder {
            offset: 0,
            last_read: 0,
            data,
        }
    }

    pub fn read<T: Decodable + Debug>(&mut self) -> Result<T, Error> {
        let mut buf = &self.data[self.offset..];
        let init_len = buf.len();
        let val = T::decode::<&[u8]>(&mut buf).ok_or(Error::NoDecodedValue)?;
        let delta = init_len - buf.len();
        if delta == 0 {
            Err(Error::BufferSizeDidNotChange)
        } else {
            self.last_read = delta;
            self.offset += delta;
            Ok(val)
        }
    }

    pub fn read_bytes(&mut self, index: usize) -> Result<&[u8], Error> {
        if self.offset + index > self.data.len() {
            Err(Error::TryingToReadPastEnd)
        } else {
            let head = &self.data[self.offset..self.offset + index];
            self.last_read = index;
            self.offset += index;
            Ok(head)
        }
    }

    pub fn done(&self) -> bool {
        self.offset >= self.data.len()
    }
}

/// See discussion on [`crate::encoding::BooleanEncoder`] for the format data is stored in.
pub(crate) struct BooleanDecoder<'a> {
    decoder: Decoder<'a>,
    last_value: bool,
    count: usize,
}

impl<'a> From<Cow<'a, [u8]>> for Decoder<'a> {
    fn from(bytes: Cow<'a, [u8]>) -> Decoder<'a> {
        Decoder::new(bytes)
    }
}

impl<'a> From<Cow<'a, [u8]>> for BooleanDecoder<'a> {
    fn from(bytes: Cow<'a, [u8]>) -> Self {
        BooleanDecoder {
            decoder: Decoder::from(bytes),
            last_value: true,
            count: 0,
        }
    }
}

// this is an endless iterator that returns false after input is exhausted
impl<'a> Iterator for BooleanDecoder<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<bool> {
        while self.count == 0 {
            if self.decoder.done() && self.count == 0 {
                return Some(false);
            }
            self.count = self.decoder.read().unwrap_or_default();
            self.last_value = !self.last_value;
        }
        self.count -= 1;
        Some(self.last_value)
    }
}

/// See discussion on [`crate::encoding::RleEncoder`] for the format data is stored in.
#[derive(Debug)]
pub(crate) struct RleDecoder<'a, T> {
    pub decoder: Decoder<'a>,
    last_value: Option<T>,
    count: isize,
    literal: bool,
}

impl<'a, T> From<Cow<'a, [u8]>> for RleDecoder<'a, T> {
    fn from(bytes: Cow<'a, [u8]>) -> Self {
        RleDecoder {
            decoder: Decoder::from(bytes),
            last_value: None,
            count: 0,
            literal: false,
        }
    }
}

// this decoder needs to be able to send type T or 'null'
// it is an endless iterator that will return all 'null's
// once input is exhausted
impl<'a, T> Iterator for RleDecoder<'a, T>
where
    T: Clone + Debug + Decodable,
{
    type Item = Option<T>;

    fn next(&mut self) -> Option<Option<T>> {
        while self.count == 0 {
            if self.decoder.done() {
                return Some(None);
            }
            match self.decoder.read::<i64>() {
                Ok(count) if count > 0 => {
                    // normal run
                    self.count = count as isize;
                    self.last_value = self.decoder.read().ok();
                    self.literal = false;
                }
                Ok(count) if count < 0 => {
                    // literal run
                    self.count = count.abs() as isize;
                    self.literal = true;
                }
                Ok(_) => {
                    // null run
                    // FIXME(jeffa5): handle usize > i64 here somehow
                    self.count = self.decoder.read::<usize>().unwrap() as isize;
                    self.last_value = None;
                    self.literal = false;
                }
                Err(e) => {
                    tracing::warn!(error=?e, "error during rle decoding");
                    return None;
                }
            }
        }
        self.count -= 1;
        if self.literal {
            Some(self.decoder.read().ok())
        } else {
            Some(self.last_value.clone())
        }
    }
}

/// See discussion on [`crate::encoding::DeltaEncoder`] for the format data is stored in.
pub(crate) struct DeltaDecoder<'a> {
    rle: RleDecoder<'a, i64>,
    absolute_val: u64,
}

impl<'a> From<Cow<'a, [u8]>> for DeltaDecoder<'a> {
    fn from(bytes: Cow<'a, [u8]>) -> Self {
        DeltaDecoder {
            rle: RleDecoder {
                decoder: Decoder::from(bytes),
                last_value: None,
                count: 0,
                literal: false,
            },
            absolute_val: 0,
        }
    }
}

impl<'a> Iterator for DeltaDecoder<'a> {
    type Item = Option<u64>;

    fn next(&mut self) -> Option<Option<u64>> {
        if let Some(delta) = self.rle.next()? {
            if delta < 0 {
                self.absolute_val -= delta.abs() as u64;
            } else {
                self.absolute_val += delta as u64;
            }
            Some(Some(self.absolute_val))
        } else {
            Some(None)
        }
    }
}

pub(crate) trait Decodable: Sized {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read;
}

impl Decodable for u8 {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        let mut buffer = [0; 1];
        bytes.read_exact(&mut buffer).ok()?;
        Some(buffer[0])
    }
}

impl Decodable for u32 {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        u64::decode::<R>(bytes).and_then(|val| Self::try_from(val).ok())
    }
}

impl Decodable for usize {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        u64::decode::<R>(bytes).and_then(|val| Self::try_from(val).ok())
    }
}

impl Decodable for isize {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        i64::decode::<R>(bytes).and_then(|val| Self::try_from(val).ok())
    }
}

impl Decodable for i32 {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        i64::decode::<R>(bytes).and_then(|val| Self::try_from(val).ok())
    }
}

impl Decodable for i64 {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        leb128::read::signed(bytes).ok()
    }
}

impl Decodable for f64 {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        let mut buffer = [0; 8];
        bytes.read_exact(&mut buffer).ok()?;
        Some(Self::from_le_bytes(buffer))
    }
}

impl Decodable for f32 {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        let mut buffer = [0; 4];
        bytes.read_exact(&mut buffer).ok()?;
        Some(Self::from_le_bytes(buffer))
    }
}

impl Decodable for u64 {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        leb128::read::unsigned(bytes).ok()
    }
}

impl Decodable for Vec<u8> {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        let len = usize::decode::<R>(bytes)?;
        if len == 0 {
            return Some(vec![]);
        }
        let mut buffer = vec![0; len];
        bytes.read_exact(buffer.as_mut_slice()).ok()?;
        Some(buffer)
    }
}

impl Decodable for SmolStr {
    fn decode<R>(bytes: &mut R) -> Option<SmolStr>
    where
        R: Read,
    {
        let buffer = Vec::decode(bytes)?;
        str::from_utf8(&buffer).map(|t| t.into()).ok()
    }
}

impl Decodable for String {
    fn decode<R>(bytes: &mut R) -> Option<String>
    where
        R: Read,
    {
        let buffer = Vec::decode(bytes)?;
        str::from_utf8(&buffer).map(|t| t.into()).ok()
    }
}

impl Decodable for Option<String> {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        let buffer = Vec::decode(bytes)?;
        if buffer.is_empty() {
            return Some(None);
        }
        Some(str::from_utf8(&buffer).map(|t| t.into()).ok())
    }
}

impl Decodable for ActorId {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        let buffer = Vec::decode(bytes)?;
        Some(buffer.into())
    }
}
