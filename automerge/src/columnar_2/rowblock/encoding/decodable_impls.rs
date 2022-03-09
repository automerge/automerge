use std::{borrow::Cow, convert::TryFrom, str, io::Read};
use smol_str::SmolStr;

use super::Decodable;
use crate::ActorId;

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

impl Decodable for Cow<'static, SmolStr> {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
            R: std::io::Read {
        SmolStr::decode(bytes).map(|s| Cow::Owned(s))
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
