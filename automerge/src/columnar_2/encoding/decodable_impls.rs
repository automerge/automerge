use smol_str::SmolStr;
use std::{borrow::Cow, convert::TryFrom, io::Read, str};

use super::{Decodable, DecodeError};
use crate::ActorId;

// We don't allow decoding items which are larger than this. Almost nothing should be this large
// so this is really guarding against bad encodings which accidentally grab loads of memory
const MAX_ALLOCATION: usize = 1000000000;

impl Decodable for u8 {
    fn decode<R>(bytes: &mut R) -> Result<Self, DecodeError>
    where
        R: Read,
    {
        let mut buffer = [0; 1];
        bytes.read_exact(&mut buffer)?;
        Ok(buffer[0])
    }
}

impl Decodable for u32 {
    fn decode<R>(bytes: &mut R) -> Result<Self, DecodeError>
    where
        R: Read,
    {
        u64::decode::<R>(bytes).and_then(|val| Self::try_from(val).map_err(DecodeError::from))
    }
}

impl Decodable for usize {
    fn decode<R>(bytes: &mut R) -> Result<Self, DecodeError>
    where
        R: Read,
    {
        u64::decode::<R>(bytes).and_then(|val| Self::try_from(val).map_err(DecodeError::from))
    }
}

impl Decodable for isize {
    fn decode<R>(bytes: &mut R) -> Result<Self, DecodeError>
    where
        R: Read,
    {
        i64::decode::<R>(bytes).and_then(|val| Self::try_from(val).map_err(DecodeError::from))
    }
}

impl Decodable for i32 {
    fn decode<R>(bytes: &mut R) -> Result<Self, DecodeError>
    where
        R: Read,
    {
        i64::decode::<R>(bytes).and_then(|val| Self::try_from(val).map_err(DecodeError::from))
    }
}

impl Decodable for i64 {
    fn decode<R>(bytes: &mut R) -> Result<Self, DecodeError>
    where
        R: Read,
    {
        leb128::read::signed(bytes).map_err(DecodeError::from)
    }
}

impl Decodable for f64 {
    fn decode<R>(bytes: &mut R) -> Result<Self, DecodeError>
    where
        R: Read,
    {
        let mut buffer = [0; 8];
        bytes.read_exact(&mut buffer)?;
        Ok(Self::from_le_bytes(buffer))
    }
}

impl Decodable for f32 {
    fn decode<R>(bytes: &mut R) -> Result<Self, DecodeError>
    where
        R: Read,
    {
        let mut buffer = [0; 4];
        bytes.read_exact(&mut buffer)?;
        Ok(Self::from_le_bytes(buffer))
    }
}

impl Decodable for u64 {
    fn decode<R>(bytes: &mut R) -> Result<Self, DecodeError>
    where
        R: Read,
    {
        leb128::read::unsigned(bytes).map_err(DecodeError::from)
    }
}

impl Decodable for Vec<u8> {
    fn decode<R>(bytes: &mut R) -> Result<Self, DecodeError>
    where
        R: Read,
    {
        let len = usize::decode::<R>(bytes)?;
        if len == 0 {
            return Ok(vec![]);
        }
        if len > MAX_ALLOCATION {
            return Err(DecodeError::OverlargeAllocation {
                attempted: len,
                maximum: MAX_ALLOCATION,
            });
        }
        let mut buffer = vec![0; len];
        bytes.read_exact(buffer.as_mut_slice())?;
        Ok(buffer)
    }
}

impl Decodable for SmolStr {
    fn decode<R>(bytes: &mut R) -> Result<SmolStr, DecodeError>
    where
        R: Read,
    {
        let buffer = Vec::decode(bytes)?;
        str::from_utf8(&buffer)
            .map(|t| t.into())
            .map_err(|_| DecodeError::BadString)
    }
}

impl Decodable for Cow<'static, SmolStr> {
    fn decode<R>(bytes: &mut R) -> Result<Self, DecodeError>
    where
        R: std::io::Read,
    {
        SmolStr::decode(bytes).map(Cow::Owned)
    }
}

impl Decodable for String {
    fn decode<R>(bytes: &mut R) -> Result<String, DecodeError>
    where
        R: Read,
    {
        let buffer = Vec::decode(bytes)?;
        str::from_utf8(&buffer)
            .map(|t| t.into())
            .map_err(|_| DecodeError::BadString)
    }
}

impl Decodable for Option<String> {
    fn decode<R>(bytes: &mut R) -> Result<Self, DecodeError>
    where
        R: Read,
    {
        let buffer = Vec::decode(bytes)?;
        if buffer.is_empty() {
            return Ok(None);
        }
        str::from_utf8(&buffer)
            .map(|t| Some(t.into()))
            .map_err(|_| DecodeError::BadString)
    }
}

impl Decodable for ActorId {
    fn decode<R>(bytes: &mut R) -> Result<Self, DecodeError>
    where
        R: Read,
    {
        let buffer = Vec::decode(bytes)?;
        Ok(buffer.into())
    }
}
