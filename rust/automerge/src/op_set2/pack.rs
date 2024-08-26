use super::WriteOp;
use crate::columnar::encoding::leb128::{lebsize, ulebsize};
use std::borrow::{Borrow, Cow};
use std::fmt::Debug;

use super::types::{Action, ActorIdx, ScalarValue};

#[derive(thiserror::Error, Debug)]
pub(crate) enum PackError {
    #[error(transparent)]
    InvalidNumber(#[from] leb128::read::Error),
    #[error("invalid utf8")]
    InvalidUtf8,
    #[error("index out of range {0}")]
    IndexOutOfRange(usize),
    #[error("slice out of range {0}..{1}")]
    SliceOutOfRange(usize, usize),
    #[error("invalid value for {typ}: {error}")]
    InvalidValue { typ: &'static str, error: String },
}

impl PackError {
    pub(crate) fn invalid_value(expected: &'static str, error: impl std::fmt::Display) -> Self {
        PackError::InvalidValue {
            typ: expected,
            error: error.to_string(),
        }
    }
}

pub(crate) trait Packable: PartialEq + Debug {
    type Unpacked<'a>: Clone
        + Copy
        + Debug
        + PartialEq
        + ToOwned
        + Borrow<Self>
        + Into<WriteOp<'a>>
        + Default;
    type Owned: Clone + PartialEq + Debug;

    fn group<'a>(item: Self::Unpacked<'a>) -> usize {
        0
    }

    fn len<'a>(item: Option<Self::Unpacked<'a>>) -> usize {
        1
    }
    fn own<'a>(item: Self::Unpacked<'a>) -> Self::Owned;
    fn width<'a>(item: Self::Unpacked<'a>) -> usize;
    fn unpack<'a>(buff: &'a [u8]) -> Result<(usize, Self::Unpacked<'a>), PackError>;
    fn pack(buff: &mut Vec<u8>, element: &Self) -> Result<usize, PackError>;
}

impl Packable for i64 {
    type Unpacked<'a> = i64;
    type Owned = i64;

    fn own<'a>(item: i64) -> i64 {
        item
    }
    fn width<'a>(item: i64) -> usize {
        lebsize(item) as usize
    }
    fn unpack<'a>(mut buff: &'a [u8]) -> Result<(usize, Self::Unpacked<'a>), PackError> {
        let start_len = buff.len();
        let val = leb128::read::signed(&mut buff)?;
        Ok((start_len - buff.len(), val))
    }

    fn pack(buff: &mut Vec<u8>, element: &i64) -> Result<usize, PackError> {
        let len = leb128::write::signed(buff, *element).unwrap();
        Ok(len)
    }
}

impl Packable for u64 {
    type Unpacked<'a> = u64;
    type Owned = u64;

    fn group<'a>(item: u64) -> usize {
        item as usize
    }

    fn width<'a>(item: u64) -> usize {
        ulebsize(item) as usize
    }
    fn own<'a>(item: u64) -> u64 {
        item
    }
    fn unpack<'a>(mut buff: &'a [u8]) -> Result<(usize, Self::Unpacked<'a>), PackError> {
        let start_len = buff.len();
        let val = leb128::read::unsigned(&mut buff)?;
        Ok((start_len - buff.len(), val))
    }

    fn pack(buff: &mut Vec<u8>, element: &u64) -> Result<usize, PackError> {
        let len = leb128::write::unsigned(buff, *element).unwrap();
        Ok(len)
    }
}

impl Packable for usize {
    type Unpacked<'a> = usize;
    type Owned = usize;

    fn width<'a>(item: usize) -> usize {
        ulebsize(item as u64) as usize
    }
    fn own<'a>(item: usize) -> usize {
        item
    }
    fn unpack<'a>(buff: &'a [u8]) -> Result<(usize, Self::Unpacked<'a>), PackError> {
        let (len, val) = u64::unpack(buff)?;
        Ok((len, val as usize))
    }

    fn pack(buff: &mut Vec<u8>, element: &usize) -> Result<usize, PackError> {
        let len = leb128::write::unsigned(buff, *element as u64).unwrap();
        Ok(len)
    }
}

impl Packable for bool {
    type Unpacked<'a> = bool;
    type Owned = bool;

    fn own<'a>(item: bool) -> bool {
        item
    }

    fn width<'a>(item: bool) -> usize {
        panic!()
    }

    fn unpack<'a>(buff: &'a [u8]) -> Result<(usize, Self::Unpacked<'a>), PackError> {
        panic!()
    }

    fn pack(buff: &mut Vec<u8>, element: &bool) -> Result<usize, PackError> {
        panic!()
    }
}

impl Packable for [u8] {
    type Unpacked<'a> = &'a [u8];
    type Owned = Vec<u8>;

    fn own<'a>(item: &'a [u8]) -> Vec<u8> {
        item.to_vec()
    }

    fn width<'a>(item: &'a [u8]) -> usize {
        usize::width(item.len()) + item.len()
    }

    fn unpack<'a>(buff: &'a [u8]) -> Result<(usize, Self::Unpacked<'a>), PackError> {
        let (start, bytes) = usize::unpack(buff)?;
        let end = start + bytes;
        let result = &buff[start..end];
        Ok((end, result))
    }

    fn pack(buff: &mut Vec<u8>, element: &[u8]) -> Result<usize, PackError> {
        let len1 = element.len();
        let len2 = leb128::write::unsigned(buff, element.len() as u64).unwrap();
        buff.extend(element);
        Ok(len1 + len2)
    }
}

impl Packable for str {
    type Unpacked<'a> = &'a str;
    type Owned = String;

    fn width<'a>(item: &'a str) -> usize {
        <[u8]>::width(item.as_bytes())
    }

    fn own<'a>(item: &'a str) -> String {
        item.to_owned()
    }

    fn unpack<'a>(buff: &'a [u8]) -> Result<(usize, Self::Unpacked<'a>), PackError> {
        let (len, bytes) = <[u8]>::unpack(buff)?;
        let result = std::str::from_utf8(bytes).map_err(|_| PackError::InvalidUtf8)?;
        Ok((len, result))
    }

    fn pack(buff: &mut Vec<u8>, element: &str) -> Result<usize, PackError> {
        <[u8]>::pack(buff, element.as_bytes())
    }
}

pub(crate) trait MaybePackable<T: Packable + ?Sized> {
    fn maybe_packable(&self) -> Option<T::Unpacked<'_>>;
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

impl Packable for Action {
    type Unpacked<'a> = Action;

    type Owned = Action;

    fn own<'a>(item: Self::Unpacked<'a>) -> Self::Owned {
        item
    }

    fn unpack<'a>(buff: &'a [u8]) -> Result<(usize, Self::Unpacked<'a>), super::PackError> {
        let (len, result) = u64::unpack(buff)?;
        let action = match result {
            0 => Action::MakeMap,
            1 => Action::Set,
            2 => Action::MakeList,
            3 => Action::Delete,
            4 => Action::MakeText,
            5 => Action::Increment,
            6 => Action::MakeTable,
            7 => Action::Mark,
            other => {
                return Err(super::PackError::invalid_value(
                    "valid action (integer between 0 and 7)",
                    format!("unexpected integer: {}", other),
                ))
            }
        };
        Ok((len, action))
    }

    fn width<'a>(item: Self::Unpacked<'a>) -> usize {
        u64::width(item as u64)
    }

    fn pack(buff: &mut Vec<u8>, element: &Self) -> Result<usize, super::PackError> {
        u64::pack(buff, &u64::from(*element))
    }
}

impl MaybePackable<Action> for Action {
    fn maybe_packable(&self) -> Option<Action> {
        Some(*self)
    }
}

impl MaybePackable<Action> for Option<Action> {
    fn maybe_packable(&self) -> Option<Action> {
        *self
    }
}

impl Packable for ActorIdx {
    type Unpacked<'a> = ActorIdx;

    type Owned = ActorIdx;

    fn own<'a>(item: Self::Unpacked<'a>) -> Self::Owned {
        item
    }

    fn width<'a>(item: Self::Unpacked<'a>) -> usize {
        u64::width(u64::from(item))
    }

    fn unpack<'a>(buff: &'a [u8]) -> Result<(usize, Self::Unpacked<'a>), super::PackError> {
        let (len, result) = u64::unpack(buff)?;
        Ok((len, ActorIdx::from(result)))
    }

    fn pack(buff: &mut Vec<u8>, element: &Self) -> Result<usize, super::PackError> {
        u64::pack(buff, &(u64::from(*element)))
    }
}

impl MaybePackable<ActorIdx> for ActorIdx {
    fn maybe_packable(&self) -> Option<ActorIdx> {
        Some(*self)
    }
}

impl MaybePackable<ActorIdx> for Option<ActorIdx> {
    fn maybe_packable(&self) -> Option<ActorIdx> {
        *self
    }
}

impl<'a> MaybePackable<[u8]> for Cow<'a, [u8]> {
    fn maybe_packable(&self) -> Option<&[u8]> {
        Some(self.as_ref())
    }
}
