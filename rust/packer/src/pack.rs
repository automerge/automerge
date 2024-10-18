use super::cursor::ScanMeta;
use super::slab::WriteOp;

use std::borrow::{Borrow, Cow};
use std::fmt::Debug;

#[derive(thiserror::Error, Debug)]
pub enum PackError {
    #[error(transparent)]
    InvalidNumber(#[from] leb128::read::Error),
    #[error("invalid utf8")]
    InvalidUtf8,
    #[error("actor index out of range {0}/{1}")]
    ActorIndexOutOfRange(u64, usize),
    #[error("counter out of range {0}")]
    CounterOutOfRange(u64),
    #[error("invalid value for {typ}: {error}")]
    InvalidValue { typ: &'static str, error: String },
    #[error("malformed leb encoding")]
    BadFormat,
}

impl PackError {
    pub fn invalid_value(expected: &'static str, error: impl std::fmt::Display) -> Self {
        PackError::InvalidValue {
            typ: expected,
            error: error.to_string(),
        }
    }
}

pub trait Packable: PartialEq + Debug {
    type Unpacked<'a>: Clone
        + Copy
        + Debug
        + PartialEq
        + PartialOrd
        + ToOwned
        + Borrow<Self>
        + Into<WriteOp<'a>>
        + Default;
    type Owned: Clone + PartialEq + Debug;

    fn group(_item: Self::Unpacked<'_>) -> usize {
        0
    }

    fn validate(_val: &Option<Self::Unpacked<'_>>, _m: &ScanMeta) -> Result<(), PackError> {
        Ok(())
    }

    fn own(item: Self::Unpacked<'_>) -> Self::Owned;
    fn unpack(buff: &[u8]) -> Result<(usize, Self::Unpacked<'_>), PackError>;
}

impl Packable for i64 {
    type Unpacked<'a> = i64;
    type Owned = i64;

    fn validate(val: &Option<Self::Unpacked<'_>>, _m: &ScanMeta) -> Result<(), PackError> {
        if let Some(a) = val {
            if *a >= u32::MAX as Self {
                return Err(PackError::CounterOutOfRange(*a as u64));
            }
        }
        Ok(())
    }

    fn own(item: i64) -> i64 {
        item
    }

    fn unpack(mut buff: &[u8]) -> Result<(usize, i64), PackError> {
        let start_len = buff.len();
        let val = leb128::read::signed(&mut buff)?;
        Ok((start_len - buff.len(), val))
    }
}

impl Packable for u32 {
    type Unpacked<'a> = u32;
    type Owned = u32;

    fn validate(val: &Option<Self::Unpacked<'_>>, _m: &ScanMeta) -> Result<(), PackError> {
        if let Some(a) = val {
            if *a >= u32::MAX as Self {
                return Err(PackError::CounterOutOfRange(*a as u64));
            }
        }
        Ok(())
    }

    fn group(item: u32) -> usize {
        item as usize
    }

    fn own(item: u32) -> u32 {
        item
    }

    fn unpack(mut buff: &[u8]) -> Result<(usize, u32), PackError> {
        let start_len = buff.len();
        let val64 = leb128::read::unsigned(&mut buff)?;
        let val32 = u32::try_from(val64).map_err(|_| PackError::CounterOutOfRange(val64))?;
        Ok((start_len - buff.len(), val32))
    }
}

impl Packable for u64 {
    type Unpacked<'a> = u64;
    type Owned = u64;

    fn validate(val: &Option<Self::Unpacked<'_>>, _m: &ScanMeta) -> Result<(), PackError> {
        if let Some(a) = val {
            if *a >= u32::MAX as Self {
                return Err(PackError::CounterOutOfRange(*a));
            }
        }
        Ok(())
    }

    fn group(item: u64) -> usize {
        item as usize
    }

    fn own(item: u64) -> u64 {
        item
    }

    fn unpack(mut buff: &[u8]) -> Result<(usize, u64), PackError> {
        let start_len = buff.len();
        let val = leb128::read::unsigned(&mut buff)?;
        Ok((start_len - buff.len(), val))
    }
}

impl Packable for usize {
    type Unpacked<'a> = usize;
    type Owned = usize;

    fn own(item: usize) -> usize {
        item
    }
    fn unpack(buff: &[u8]) -> Result<(usize, Self::Unpacked<'_>), PackError> {
        let (len, val) = u64::unpack(buff)?;
        Ok((len, val as usize))
    }
}

impl Packable for bool {
    type Unpacked<'a> = bool;
    type Owned = bool;

    fn own(item: bool) -> bool {
        item
    }

    fn group(item: bool) -> usize {
        if item {
            1
        } else {
            0
        }
    }

    fn unpack(_buff: &[u8]) -> Result<(usize, Self::Unpacked<'_>), PackError> {
        panic!()
    }
}

impl Packable for [u8] {
    type Unpacked<'a> = &'a [u8];
    type Owned = Vec<u8>;

    fn own(item: &[u8]) -> Vec<u8> {
        item.to_vec()
    }

    fn unpack(buff: &[u8]) -> Result<(usize, Self::Unpacked<'_>), PackError> {
        let (start, bytes) = usize::unpack(buff)?;
        let end = start + bytes;
        let result = &buff[start..end];
        Ok((end, result))
    }
}

impl Packable for str {
    type Unpacked<'a> = &'a str;
    type Owned = String;

    fn own(item: &str) -> String {
        item.to_owned()
    }

    fn unpack(buff: &[u8]) -> Result<(usize, Self::Unpacked<'_>), PackError> {
        let (len, bytes) = <[u8]>::unpack(buff)?;
        let result = std::str::from_utf8(bytes).map_err(|_| PackError::InvalidUtf8)?;
        Ok((len, result))
    }
}

pub trait MaybePackable<T: Packable + ?Sized> {
    fn maybe_packable(&self) -> Option<T::Unpacked<'_>>;
    fn group(&self) -> usize {
        self.maybe_packable().map(|n| T::group(n)).unwrap_or(0)
    }
}

impl MaybePackable<i64> for i64 {
    fn maybe_packable(&self) -> Option<i64> {
        Some(*self)
    }
}

impl MaybePackable<i64> for Option<i64> {
    fn maybe_packable(&self) -> Option<i64> {
        *self
    }
}

impl MaybePackable<u64> for u64 {
    fn maybe_packable(&self) -> Option<u64> {
        Some(*self)
    }
}

impl MaybePackable<u32> for u32 {
    fn maybe_packable(&self) -> Option<u32> {
        Some(*self)
    }
}

impl MaybePackable<usize> for Option<usize> {
    fn maybe_packable(&self) -> Option<usize> {
        *self
    }
}

impl MaybePackable<usize> for usize {
    fn maybe_packable(&self) -> Option<usize> {
        Some(*self)
    }
}

impl MaybePackable<u64> for Option<u64> {
    fn maybe_packable(&self) -> Option<u64> {
        *self
    }
}

impl MaybePackable<[u8]> for &[u8] {
    fn maybe_packable(&self) -> Option<&[u8]> {
        Some(self)
    }
}

impl MaybePackable<[u8]> for Vec<u8> {
    fn maybe_packable(&self) -> Option<&[u8]> {
        Some(self.as_slice())
    }
}

impl<'a> MaybePackable<[u8]> for Option<Cow<'a, [u8]>> {
    fn maybe_packable(&self) -> Option<&[u8]> {
        self.as_ref().map(|c| c.borrow())
    }
}

impl MaybePackable<str> for &str {
    fn maybe_packable(&self) -> Option<&str> {
        Some(self)
    }
}

impl MaybePackable<str> for String {
    fn maybe_packable(&self) -> Option<&str> {
        Some(self.as_str())
    }
}

impl MaybePackable<str> for Option<&str> {
    fn maybe_packable(&self) -> Option<&str> {
        *self
    }
}

impl MaybePackable<str> for Option<String> {
    fn maybe_packable(&self) -> Option<&str> {
        self.as_ref().map(|s| s.as_str())
    }
}

impl MaybePackable<bool> for Option<bool> {
    fn maybe_packable(&self) -> Option<bool> {
        *self
    }
}

impl MaybePackable<bool> for bool {
    fn maybe_packable(&self) -> Option<bool> {
        Some(*self)
    }
}

impl<'a> MaybePackable<[u8]> for Cow<'a, [u8]> {
    fn maybe_packable(&self) -> Option<&[u8]> {
        Some(self.as_ref())
    }
}
