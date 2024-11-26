use super::aggregate::Agg;
use super::cursor::ScanMeta;
use super::slab::WriteOp;
use super::Cow;

use std::borrow::Borrow;
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

pub trait Packable:
    PartialEq + Debug + ToOwned<Owned: Debug + Clone + PartialEq> + PartialOrd
{
    //type Unpacked<'a>: Clone + Copy + Debug + PartialEq + PartialOrd + ToOwned + Borrow<Self>;

    fn agg(_item: &Self) -> Agg {
        Agg::default()
    }

    fn pack(item: Cow<'_, Self>) -> WriteOp<'_>;

    fn maybe_agg(_item: &Option<Cow<'_, Self>>) -> Agg {
        Agg::default()
    }

    fn validate(_val: Option<&Self>, _m: &ScanMeta) -> Result<(), PackError> {
        Ok(())
    }

    //fn own(item: Self::Unpacked<'_>) -> Self::Owned;
    fn unpack(buff: &[u8]) -> Result<(usize, Cow<'_, Self>), PackError>;
}

impl Packable for i64 {
    //type Unpacked<'a> = i64;

    fn validate(val: Option<&Self>, _m: &ScanMeta) -> Result<(), PackError> {
        if let Some(a) = val {
            if *a >= u32::MAX as Self {
                return Err(PackError::CounterOutOfRange(*a as u64));
            }
        }
        Ok(())
    }

    //    fn own(item: i64) -> i64 { item }

    fn maybe_agg(item: &Option<Cow<'_, i64>>) -> Agg {
        Agg::from(item.as_deref().cloned().unwrap_or(0))
    }

    fn pack(item: Cow<'_, i64>) -> WriteOp<'static> {
        WriteOp::Int(*item)
    }

    fn unpack(mut buff: &[u8]) -> Result<(usize, Cow<'_, i64>), PackError> {
        let start_len = buff.len();
        let val = leb128::read::signed(&mut buff)?;
        Ok((start_len - buff.len(), Cow::Owned(val)))
    }
}

impl Packable for u32 {
    //type Unpacked<'a> = u32;

    fn validate(val: Option<&Self>, _m: &ScanMeta) -> Result<(), PackError> {
        if let Some(a) = val {
            if *a >= u32::MAX as Self {
                return Err(PackError::CounterOutOfRange(*a as u64));
            }
        }
        Ok(())
    }

    fn agg(item: &u32) -> Agg {
        Agg::from(*item)
    }

    //    fn own(item: u32) -> u32 { item }

    fn pack(item: Cow<'_, u32>) -> WriteOp<'static> {
        WriteOp::UIntAcc(*item as u64, Agg::from(*item))
    }

    fn unpack(mut buff: &[u8]) -> Result<(usize, Cow<'static, u32>), PackError> {
        let start_len = buff.len();
        let val64 = leb128::read::unsigned(&mut buff)?;
        let val32 = u32::try_from(val64).map_err(|_| PackError::CounterOutOfRange(val64))?;
        Ok((start_len - buff.len(), Cow::Owned(val32)))
    }
}

impl Packable for u64 {
    //type Unpacked<'a> = u64;

    fn maybe_agg(item: &Option<Cow<'_, u64>>) -> Agg {
        Agg::from(item.as_deref().cloned().unwrap_or(0))
    }

    fn validate(val: Option<&Self>, _m: &ScanMeta) -> Result<(), PackError> {
        if let Some(a) = val {
            if *a >= u32::MAX as Self {
                return Err(PackError::CounterOutOfRange(*a));
            }
        }
        Ok(())
    }

    fn agg(item: &u64) -> Agg {
        Agg::from(*item)
    }

    //    fn own(item: u64) -> u64 { item }

    fn pack(item: Cow<'_, u64>) -> WriteOp<'static> {
        WriteOp::UIntAcc(*item, Agg::from(*item))
    }

    fn unpack(mut buff: &[u8]) -> Result<(usize, Cow<'static, u64>), PackError> {
        let start_len = buff.len();
        let val = leb128::read::unsigned(&mut buff)?;
        Ok((start_len - buff.len(), Cow::Owned(val)))
    }
}

impl Packable for usize {
    //type Unpacked<'a> = usize;

    //    fn own(item: usize) -> usize { item }

    fn pack(item: Cow<'_, usize>) -> WriteOp<'static> {
        WriteOp::UIntAcc(*item as u64, Agg::from(*item))
    }

    fn unpack(buff: &[u8]) -> Result<(usize, Cow<'static, Self>), PackError> {
        let (len, val) = u64::unpack(buff)?;
        Ok((len, Cow::Owned(*val as usize)))
    }
}

impl Packable for bool {
    //type Unpacked<'a> = bool;

    //    fn own(item: bool) -> bool { item }

    fn agg(item: &bool) -> Agg {
        if *item {
            Agg::from(1_u32)
        } else {
            Agg::from(0_u32)
        }
    }

    fn pack(_item: Cow<'_, bool>) -> WriteOp<'static> {
        panic!()
    }

    fn unpack(_buff: &[u8]) -> Result<(usize, Cow<'_, Self>), PackError> {
        panic!()
    }
}

impl Packable for [u8] {
    //type Unpacked<'a> = &'a [u8];

    fn pack(item: Cow<'_, [u8]>) -> WriteOp<'_> {
        WriteOp::Bytes(item)
    }

    fn unpack(buff: &[u8]) -> Result<(usize, Cow<'_, Self>), PackError> {
        let (start, bytes) = usize::unpack(buff)?;
        let end = start + *bytes;
        let result = &buff[start..end];
        Ok((end, Cow::Borrowed(result)))
    }
}

impl Packable for str {
    //type Unpacked<'a> = &'a str;

    fn pack(item: Cow<'_, str>) -> WriteOp<'_> {
        match item {
            Cow::Owned(s) => WriteOp::Bytes(Cow::from(s.into_bytes())),
            Cow::Borrowed(s) => WriteOp::Bytes(Cow::from(s.as_bytes())),
        }
    }

    fn unpack(buff: &[u8]) -> Result<(usize, Cow<'_, Self>), PackError> {
        let (start, bytes) = usize::unpack(buff)?;
        let end = start + *bytes;
        let bytes = &buff[start..end];
        let result = std::str::from_utf8(bytes).map_err(|_| PackError::InvalidUtf8)?;
        Ok((end, Cow::Borrowed(result)))
    }
}

pub trait MaybePackable2<'a, T: Packable + ?Sized> {
    fn maybe_packable2(self) -> Option<Cow<'a, T>>;
    fn agg2(&self) -> Agg;
}

impl<'a, T: Packable> MaybePackable2<'a, T> for T {
    fn maybe_packable2(self) -> Option<Cow<'a, T>> {
        Some(Cow::Owned(self.to_owned()))
    }
    fn agg2(&self) -> Agg {
        T::agg(self)
    }
}

impl<'a> MaybePackable2<'a, str> for Option<String> {
    fn maybe_packable2(self) -> Option<Cow<'a, str>> {
        self.map(Cow::Owned)
    }
    fn agg2(&self) -> Agg {
        self.as_deref().map(str::agg).unwrap_or_default()
    }
}

impl<'a> MaybePackable2<'a, str> for String {
    fn maybe_packable2(self) -> Option<Cow<'a, str>> {
        Some(Cow::Owned(self))
    }
    fn agg2(&self) -> Agg {
        str::agg(self)
    }
}

impl<'a> MaybePackable2<'a, [u8]> for Vec<u8> {
    fn maybe_packable2(self) -> Option<Cow<'a, [u8]>> {
        Some(Cow::Owned(self))
    }
    fn agg2(&self) -> Agg {
        <[u8]>::agg(self)
    }
}

impl<'a, T: Packable + ?Sized> MaybePackable2<'a, T> for &'a T {
    fn maybe_packable2(self) -> Option<Cow<'a, T>> {
        Some(Cow::Borrowed(self))
    }
    fn agg2(&self) -> Agg {
        T::agg(*self)
    }
}

impl<'a, T: Packable + ?Sized> MaybePackable2<'a, T> for Cow<'a, T> {
    fn maybe_packable2(self) -> Option<Cow<'a, T>> {
        Some(self)
    }
    fn agg2(&self) -> Agg {
        T::agg(self)
    }
}

impl<'a, T: Packable + ?Sized> MaybePackable2<'a, T> for Option<Cow<'a, T>> {
    fn maybe_packable2(self) -> Option<Cow<'a, T>> {
        self
    }
    fn agg2(&self) -> Agg {
        self.as_deref().map(T::agg).unwrap_or_default()
    }
}

impl<'a, T: Packable> MaybePackable2<'a, T> for Option<T> {
    fn maybe_packable2(self) -> Option<Cow<'a, T>> {
        self.map(|t| Cow::Owned(t.to_owned()))
    }
    fn agg2(&self) -> Agg {
        self.as_ref().map(|t| T::agg(t)).unwrap_or_default()
    }
}

impl<'a, T: Packable + ?Sized> MaybePackable2<'a, T> for Option<&'a T> {
    fn maybe_packable2(self) -> Option<Cow<'a, T>> {
        self.map(|t| Cow::Borrowed(t))
    }
    fn agg2(&self) -> Agg {
        self.map(|t| T::agg(t)).unwrap_or_default()
    }
}

pub trait MaybePackable<T: Packable + ?Sized> {
    fn maybe_packable(&self) -> Option<Cow<'_, T>>;
    fn agg(&self) -> Agg {
        self.maybe_packable()
            .map(|n| T::agg(&n))
            .unwrap_or_default()
    }
}

impl MaybePackable<i64> for i64 {
    fn maybe_packable(&self) -> Option<Cow<'static, i64>> {
        Some(Cow::Owned(*self))
    }
}

impl MaybePackable<i64> for Option<i64> {
    fn maybe_packable(&self) -> Option<Cow<'static, i64>> {
        self.map(Cow::Owned)
    }
}

impl MaybePackable<u64> for u64 {
    fn maybe_packable(&self) -> Option<Cow<'static, u64>> {
        Some(Cow::Owned(*self))
    }
}

impl MaybePackable<u32> for u32 {
    fn maybe_packable(&self) -> Option<Cow<'static, u32>> {
        Some(Cow::Owned(*self))
    }
}

impl MaybePackable<usize> for Option<usize> {
    fn maybe_packable(&self) -> Option<Cow<'static, usize>> {
        self.map(Cow::Owned)
    }
}

impl MaybePackable<usize> for usize {
    fn maybe_packable(&self) -> Option<Cow<'static, usize>> {
        Some(Cow::Owned(*self))
    }
}

impl MaybePackable<u64> for Option<u64> {
    fn maybe_packable(&self) -> Option<Cow<'static, u64>> {
        self.map(Cow::Owned)
    }
}

impl MaybePackable<[u8]> for &[u8] {
    fn maybe_packable(&self) -> Option<Cow<'_, [u8]>> {
        Some(Cow::Borrowed(self))
    }
}

impl MaybePackable<[u8]> for Vec<u8> {
    fn maybe_packable(&self) -> Option<Cow<'_, [u8]>> {
        Some(Cow::Borrowed(self.as_slice()))
    }
}

impl<'a> MaybePackable<[u8]> for Option<Cow<'a, [u8]>> {
    fn maybe_packable(&self) -> Option<Cow<'_, [u8]>> {
        self.as_ref().map(|c| Cow::Borrowed(c.borrow()))
    }
}

impl<'a> MaybePackable<[u8]> for Cow<'a, [u8]> {
    fn maybe_packable(&self) -> Option<Cow<'_, [u8]>> {
        Some(Cow::Borrowed(self.borrow()))
    }
}

/*
impl<'a> MaybePackable<[u8]> for std::borrow::Cow<'a, [u8]> {
    fn maybe_packable(&self) -> Option<Cow<'_, [u8]>> {
        Some(Cow::Borrowed(self.borrow()))
    }
}
*/

impl MaybePackable<str> for &str {
    fn maybe_packable(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(self))
    }
}

impl MaybePackable<str> for String {
    fn maybe_packable(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(self.as_str()))
    }
}

impl MaybePackable<str> for Option<&str> {
    fn maybe_packable(&self) -> Option<Cow<'_, str>> {
        self.map(Cow::Borrowed)
    }
}

impl MaybePackable<str> for Option<String> {
    fn maybe_packable(&self) -> Option<Cow<'_, str>> {
        self.as_ref().map(|s| Cow::Borrowed(s.as_str()))
    }
}

impl<'a> MaybePackable<str> for Option<Cow<'a, str>> {
    fn maybe_packable(&self) -> Option<Cow<'_, str>> {
        self.as_ref().map(|s| Cow::Borrowed(s.borrow()))
    }
}

impl MaybePackable<bool> for Option<bool> {
    fn maybe_packable(&self) -> Option<Cow<'static, bool>> {
        self.map(Cow::Owned)
    }
}

impl MaybePackable<bool> for bool {
    fn maybe_packable(&self) -> Option<Cow<'static, bool>> {
        Some(Cow::Owned(*self))
    }
}

/*
impl<'a> MaybePackable<[u8]> for Cow<'a, [u8]> {
    fn maybe_packable(&self) -> Option<Cow<'_, [u8]>> {
        Some(Cow::Borrowed(self.as_ref()))
    }
}
*/
