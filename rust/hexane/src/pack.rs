use super::aggregate::Agg;
use super::Cow;
use super::{lebsize, ulebsize};

use std::fmt::Debug;

#[derive(thiserror::Error, Debug)]
pub enum PackError {
    #[error(transparent)]
    InvalidNumber(#[from] leb128::read::Error),
    #[error("invalid utf8")]
    InvalidUtf8,
    #[error("invalid value: {0}")]
    InvalidValue(String),
    #[error("invalid load length len={0}, expected={0}")]
    InvalidLength(usize, usize),
    #[error("malformed leb encoding")]
    BadFormat,
    #[error("invalid resume")]
    InvalidResume,
}

pub trait Packable:
    PartialEq + Debug + ToOwned<Owned: Debug + Clone + PartialEq> + PartialOrd
{
    fn abs(_item: &Self) -> i64 {
        0
    }

    fn agg(_item: &Self) -> Agg {
        Agg::default()
    }

    fn width(item: &Self) -> usize;

    fn pack(item: &Self, out: &mut Vec<u8>);

    fn save(item: &Self) -> Vec<u8> {
        let mut bytes = vec![];
        Self::pack(item, &mut bytes);
        bytes
    }

    fn maybe_agg(_item: &Option<Cow<'_, Self>>) -> Agg {
        Agg::default()
    }

    fn validate<F>(val: Option<&Self>, validate: &F) -> Result<(), PackError>
    where
        F: Fn(Option<&Self>) -> Option<String>,
    {
        match validate(val) {
            Some(msg) => Err(PackError::InvalidValue(msg)),
            None => Ok(()),
        }
    }

    fn unpack(buff: &[u8]) -> Result<(usize, Cow<'_, Self>), PackError>;
}

impl Packable for i64 {
    fn abs(item: &Self) -> i64 {
        *item
    }

    fn maybe_agg(item: &Option<Cow<'_, i64>>) -> Agg {
        Agg::from(item.as_deref().cloned().unwrap_or(0))
    }

    fn width(item: &i64) -> usize {
        lebsize(*item) as usize
    }

    fn pack(item: &i64, out: &mut Vec<u8>) {
        leb128::write::signed(out, *item).unwrap();
    }

    fn unpack(mut buff: &[u8]) -> Result<(usize, Cow<'_, i64>), PackError> {
        let start_len = buff.len();
        let val = leb128::read::signed(&mut buff)?;
        Ok((start_len - buff.len(), Cow::Owned(val)))
    }
}

impl Packable for u32 {
    fn agg(item: &u32) -> Agg {
        Agg::from(*item)
    }

    fn width(item: &u32) -> usize {
        ulebsize(*item as u64) as usize
    }

    fn pack(item: &u32, out: &mut Vec<u8>) {
        leb128::write::unsigned(out, *item as u64).unwrap();
    }

    fn unpack(mut buff: &[u8]) -> Result<(usize, Cow<'static, u32>), PackError> {
        let start_len = buff.len();
        let val64 = leb128::read::unsigned(&mut buff)?;
        let val32 = u32::try_from(val64).map_err(|_| {
            PackError::InvalidValue(format!(
                "unpacked value '{}' too large for u32 column",
                val64
            ))
        })?;
        Ok((start_len - buff.len(), Cow::Owned(val32)))
    }
}

impl Packable for u64 {
    fn maybe_agg(item: &Option<Cow<'_, u64>>) -> Agg {
        Agg::from(item.as_deref().cloned().unwrap_or(0))
    }

    fn agg(item: &u64) -> Agg {
        Agg::from(*item)
    }

    fn width(item: &u64) -> usize {
        ulebsize(*item) as usize
    }

    fn pack(item: &u64, out: &mut Vec<u8>) {
        leb128::write::unsigned(out, *item).unwrap();
    }

    fn unpack(mut buff: &[u8]) -> Result<(usize, Cow<'static, u64>), PackError> {
        let start_len = buff.len();
        let val = leb128::read::unsigned(&mut buff)?;
        Ok((start_len - buff.len(), Cow::Owned(val)))
    }
}

impl Packable for usize {
    fn width(item: &Self) -> usize {
        ulebsize(*item as u64) as usize
    }
    fn pack(item: &usize, out: &mut Vec<u8>) {
        leb128::write::unsigned(out, *item as u64).unwrap();
    }

    fn unpack(buff: &[u8]) -> Result<(usize, Cow<'static, Self>), PackError> {
        let (len, val) = u64::unpack(buff)?;
        Ok((len, Cow::Owned(*val as usize)))
    }
}

impl Packable for bool {
    fn agg(item: &bool) -> Agg {
        if *item {
            Agg::from(1_u32)
        } else {
            Agg::from(0_u32)
        }
    }

    fn width(_item: &bool) -> usize {
        panic!()
    }

    fn pack(_item: &bool, _out: &mut Vec<u8>) {
        panic!()
    }

    fn unpack(_buff: &[u8]) -> Result<(usize, Cow<'_, Self>), PackError> {
        panic!()
    }
}

impl Packable for [u8] {
    fn width(item: &[u8]) -> usize {
        ulebsize(item.len() as u64) as usize + item.len()
    }
    fn pack(item: &[u8], out: &mut Vec<u8>) {
        leb128::write::unsigned(out, item.len() as u64).unwrap();
        out.extend_from_slice(item);
    }

    fn unpack(buff: &[u8]) -> Result<(usize, Cow<'_, Self>), PackError> {
        let (start, bytes) = usize::unpack(buff)?;
        let end = start + *bytes;
        let result = &buff[start..end];
        Ok((end, Cow::Borrowed(result)))
    }
}

impl Packable for str {
    fn width(item: &str) -> usize {
        <[u8]>::width(item.as_bytes())
    }
    fn pack(item: &str, out: &mut Vec<u8>) {
        let item = item.as_bytes();
        leb128::write::unsigned(out, item.len() as u64).unwrap();
        out.extend_from_slice(item);
    }

    fn unpack(buff: &[u8]) -> Result<(usize, Cow<'_, Self>), PackError> {
        let (start, bytes) = usize::unpack(buff)?;
        let end = start + *bytes;
        let bytes = &buff[start..end];
        let result = std::str::from_utf8(bytes).map_err(|_| PackError::InvalidUtf8)?;
        Ok((end, Cow::Borrowed(result)))
    }
}

pub trait MaybePackable<'a, T: Packable + ?Sized> {
    fn maybe_packable(self) -> Option<Cow<'a, T>>;
    fn agg(&self) -> Agg;
}

impl<'a, T: Packable> MaybePackable<'a, T> for T {
    fn maybe_packable(self) -> Option<Cow<'a, T>> {
        Some(Cow::Owned(self.to_owned()))
    }
    fn agg(&self) -> Agg {
        T::agg(self)
    }
}

impl<'a> MaybePackable<'a, str> for Option<String> {
    fn maybe_packable(self) -> Option<Cow<'a, str>> {
        self.map(Cow::Owned)
    }
    fn agg(&self) -> Agg {
        self.as_deref().map(str::agg).unwrap_or_default()
    }
}

impl<'a> MaybePackable<'a, str> for String {
    fn maybe_packable(self) -> Option<Cow<'a, str>> {
        Some(Cow::Owned(self))
    }
    fn agg(&self) -> Agg {
        str::agg(self)
    }
}

impl<'a> MaybePackable<'a, [u8]> for Vec<u8> {
    fn maybe_packable(self) -> Option<Cow<'a, [u8]>> {
        Some(Cow::Owned(self))
    }
    fn agg(&self) -> Agg {
        <[u8]>::agg(self)
    }
}

impl<'a> MaybePackable<'a, [u8]> for Option<Vec<u8>> {
    fn maybe_packable(self) -> Option<Cow<'a, [u8]>> {
        self.map(Cow::Owned)
    }
    fn agg(&self) -> Agg {
        self.as_deref().map(<[u8]>::agg).unwrap_or_default()
    }
}

impl<'a, T: Packable + ?Sized> MaybePackable<'a, T> for &'a T {
    fn maybe_packable(self) -> Option<Cow<'a, T>> {
        Some(Cow::Borrowed(self))
    }
    fn agg(&self) -> Agg {
        T::agg(*self)
    }
}

impl<'a, T: Packable + ?Sized> MaybePackable<'a, T> for Cow<'a, T> {
    fn maybe_packable(self) -> Option<Cow<'a, T>> {
        Some(self)
    }
    fn agg(&self) -> Agg {
        T::agg(self)
    }
}

impl<'a, T: Packable + ?Sized> MaybePackable<'a, T> for Option<Cow<'a, T>> {
    fn maybe_packable(self) -> Option<Cow<'a, T>> {
        self
    }
    fn agg(&self) -> Agg {
        self.as_deref().map(T::agg).unwrap_or_default()
    }
}

impl<'a, T: Packable> MaybePackable<'a, T> for Option<T> {
    fn maybe_packable(self) -> Option<Cow<'a, T>> {
        self.map(|t| Cow::Owned(t.to_owned()))
    }
    fn agg(&self) -> Agg {
        self.as_ref().map(T::agg).unwrap_or_default()
    }
}

impl<'a, T: Packable + ?Sized> MaybePackable<'a, T> for Option<&'a T> {
    fn maybe_packable(self) -> Option<Cow<'a, T>> {
        self.map(Cow::Borrowed)
    }
    fn agg(&self) -> Agg {
        self.map(T::agg).unwrap_or_default()
    }
}
