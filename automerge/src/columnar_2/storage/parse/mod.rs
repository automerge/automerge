use core::num::NonZeroUsize;
use std::convert::TryInto;

mod leb128;
use crate::{ActorId, ChangeHash};

pub(in crate::columnar_2) use self::leb128::{leb128_u32, leb128_u64, leb128_i32, leb128_i64};

pub(crate) type ParseResult<'a, O> = Result<(&'a [u8], O), ParseError<ErrorKind>>;

pub(super) trait Parser<'a, O> {
    fn parse(&mut self, input: &'a [u8]) -> ParseResult<'a, O>;
}

impl<'a, O, F> Parser<'a, O> for F
where
    F: FnMut(&'a [u8]) -> ParseResult<'a, O>,
{
    fn parse(&mut self, input: &'a [u8]) -> ParseResult<'a, O> {
        (self)(input)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ParseError<E> {
    Error(E),
    Incomplete(Needed),
}

impl<E: std::fmt::Display> std::fmt::Display for ParseError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Error(e) => write!(f, "{}", e),
            Self::Incomplete(_) => write!(f, "not enough data"),
        }
    }
}

impl<E: std::fmt::Display + std::fmt::Debug> std::error::Error for ParseError<E> {}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Needed {
    Unknown,
    Size(NonZeroUsize),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ErrorKind {
    Leb128TooLarge,
    InvalidMagicBytes,
    UnknownChunkType(u8),
    InvalidColumnMetadataSort,
    InvalidUtf8,
}

impl std::fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Leb128TooLarge => write!(f, "invalid leb 128"),
            Self::InvalidMagicBytes => write!(f, "invalid magic bytes"),
            Self::UnknownChunkType(t) => write!(f, "unknown chunk type: {}", t),
            Self::InvalidColumnMetadataSort => write!(f, "invalid sort of column metadata"),
            Self::InvalidUtf8 => write!(f, "invalid utf8"),
        }
    }
}

pub(super) fn map<'a, O1, O2, F, G>(
    mut parser: F,
    mut f: G,
) -> impl FnMut(&'a [u8]) -> ParseResult<'a, O2>
where
    F: Parser<'a, O1>,
    G: FnMut(O1) -> O2,
{
    move |input: &[u8]| {
        let (input, o1) = parser.parse(input)?;
        Ok((input, f(o1)))
    }
}

pub(super) fn take1(input: &[u8]) -> ParseResult<u8> {
    if let Some(need) = NonZeroUsize::new(1_usize.saturating_sub(input.len())) {
        Err(ParseError::Incomplete(Needed::Size(need)))
    } else {
        let (result, remaining) = input.split_at(1);
        Ok((remaining, result[0]))
    }
}

pub(super) fn take4(input: &[u8]) -> ParseResult<[u8; 4]> {
    if let Some(need) = NonZeroUsize::new(4_usize.saturating_sub(input.len())) {
        Err(ParseError::Incomplete(Needed::Size(need)))
    } else {
        let (result, remaining) = input.split_at(4);
        Ok((remaining, result.try_into().expect("we checked the length")))
    }
}

pub(super) fn take_n<'a>(n: usize, input: &'a [u8]) -> ParseResult<&'a [u8]> {
    if let Some(need) = NonZeroUsize::new(n.saturating_sub(input.len())) {
        Err(ParseError::Incomplete(Needed::Size(need)))
    } else {
        let (result, remaining) = input.split_at(n);
        Ok((remaining, result))
    }
}

pub(super) fn length_prefixed<'a, F, G, E>(
    mut f: F,
    mut g: G,
) -> impl FnMut(&'a [u8]) -> ParseResult<'a, Vec<E>>
where
    F: Parser<'a, u64>,
    G: Parser<'a, E>,
{
    move |input: &'a [u8]| {
        let (i, count) = f.parse(input)?;
        let mut res = Vec::new();
        let mut input = i;
        for _ in 0..count {
            match g.parse(input) {
                Ok((i, e)) => {
                    input = i;
                    res.push(e);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok((input, res))
    }
}

pub(super) fn tuple2<'a, F, E, G, H>(
    mut f: F,
    mut g: G,
) -> impl FnMut(&'a [u8]) -> ParseResult<(E, H)>
where
    F: Parser<'a, E>,
    G: Parser<'a, H>,
{
    move |input: &'a [u8]| {
        let (i, one) = f.parse(input)?;
        let (i, two) = g.parse(i)?;
        Ok((i, (one, two)))
    }
}

pub(super) fn apply_n<'a, F, E>(n: usize, mut f: F) -> impl FnMut(&'a [u8]) -> ParseResult<Vec<E>>
where
    F: Parser<'a, E>,
{
    move |input: &'a [u8]| {
        let mut i = input;
        let mut result = Vec::new();
        for _ in 0..n {
            let (new_i, e) = f.parse(i)?;
            result.push(e);
            i = new_i;
        }
        Ok((i, result))
    }
}

/// Parse a length prefixed actor ID
pub(super) fn actor_id(input: &[u8]) -> ParseResult<ActorId> {
    let (i, length) = leb128_u64(input)?;
    let (i, bytes) = take_n(length as usize, i)?;
    Ok((i, bytes.into()))
}

pub(super) fn change_hash(input: &[u8]) -> ParseResult<ChangeHash> {
    let (i, bytes) = take_n(32, input)?;
    let byte_arr: ChangeHash = bytes.try_into().expect("we checked the length above");
    Ok((i, byte_arr))
}

pub(super) fn utf_8(len: usize, input: &[u8]) -> ParseResult<String> {
    let (i, bytes) = take_n(len, input)?;
    let result = String::from_utf8(bytes.to_vec()).map_err(|_| ParseError::Error(ErrorKind::InvalidUtf8))?;
    Ok((i, result))
}
