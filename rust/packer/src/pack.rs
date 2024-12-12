use super::aggregate::Agg;
use super::cursor::ScanMeta;
use super::slab::WriteOp;
use super::Cow;

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

    fn unpack(buff: &[u8]) -> Result<(usize, Cow<'_, Self>), PackError>;
}

impl Packable for i64 {
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
    fn pack(item: Cow<'_, usize>) -> WriteOp<'static> {
        WriteOp::UIntAcc(*item as u64, Agg::from(*item))
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

    fn pack(_item: Cow<'_, bool>) -> WriteOp<'static> {
        panic!()
    }

    fn unpack(_buff: &[u8]) -> Result<(usize, Cow<'_, Self>), PackError> {
        panic!()
    }
}

impl Packable for [u8] {
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
