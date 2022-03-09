use core::num::NonZeroUsize;
use std::convert::TryInto;

mod leb128;
use crate::{ActorId, ChangeHash};

const HASH_SIZE: usize = 32; // 256 bits = 32 bytes

#[allow(unused_imports)]
pub(crate) use self::leb128::{leb128_i32, leb128_i64, leb128_u32, leb128_u64, nonzero_leb128_u64};

pub(crate) type ParseResult<'a, O, E = ErrorKind> = Result<(&'a [u8], O), ParseError<E>>;

pub(crate) trait Parser<'a, O, E> {
    fn parse(&mut self, input: &'a [u8]) -> ParseResult<'a, O, E>;
}

impl<'a, O, F, E> Parser<'a, O, E> for F
where
    F: FnMut(&'a [u8]) -> ParseResult<'a, O, E>,
{
    fn parse(&mut self, input: &'a [u8]) -> ParseResult<'a, O, E> {
        (self)(input)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ParseError<E> {
    Error(E),
    Incomplete(Needed),
}

impl ParseError<ErrorKind> {
    pub(crate) fn parse_columns<E: std::error::Error>(
        col: &'static str,
        error: E,
    ) -> ParseError<ErrorKind> {
        ParseError::Error(ErrorKind::InvalidColumns {
            columns_type: col,
            description: error.to_string(),
        })
    }
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
    #[allow(dead_code)]
    Unknown,
    Size(NonZeroUsize),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ErrorKind {
    Leb128TooLarge,
    InvalidMagicBytes,
    UnknownChunkType(u8),
    InvalidUtf8,
    UnexpectedZero,
    LeftoverData,
    InvalidRawColumns,
    /// Thrown when parsing generic column metadata into specific layouts
    InvalidColumns {
        /// The layout type (e.g. "change" or "docop")
        columns_type: &'static str,
        /// The error that was thrown
        description: String,
    },
    Deflate,
    CompressedChangeCols,
}

impl std::fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Leb128TooLarge => write!(f, "invalid leb 128"),
            Self::InvalidMagicBytes => write!(f, "invalid magic bytes"),
            Self::UnknownChunkType(t) => write!(f, "unknown chunk type: {}", t),
            Self::InvalidUtf8 => write!(f, "invalid utf8"),
            Self::UnexpectedZero => write!(f, "unexpected zero"),
            Self::LeftoverData => write!(f, "unexpected leftover data"),
            Self::InvalidRawColumns => write!(f, "raw columns were not valid"),
            Self::InvalidColumns {
                columns_type,
                description,
            } => {
                write!(
                    f,
                    "error parsing columns of type {}: {}",
                    columns_type, description
                )
            }
            Self::Deflate => write!(f, "error decompressing compressed chunk"),
            Self::CompressedChangeCols => write!(f, "compressed columns found in change chunk"),
        }
    }
}

pub(crate) fn map<'a, O1, O2, F, G, Er>(
    mut parser: F,
    mut f: G,
) -> impl FnMut(&'a [u8]) -> ParseResult<'a, O2, Er>
where
    F: Parser<'a, O1, Er>,
    G: FnMut(O1) -> O2,
{
    move |input: &[u8]| {
        let (input, o1) = parser.parse(input)?;
        Ok((input, f(o1)))
    }
}

pub(crate) fn take1<E>(input: &[u8]) -> ParseResult<'_, u8, E> {
    if let Some(need) = NonZeroUsize::new(1_usize.saturating_sub(input.len())) {
        Err(ParseError::Incomplete(Needed::Size(need)))
    } else {
        let (result, remaining) = input.split_at(1);
        Ok((remaining, result[0]))
    }
}

pub(crate) fn take4<E>(input: &[u8]) -> ParseResult<'_, [u8; 4], E> {
    if let Some(need) = NonZeroUsize::new(4_usize.saturating_sub(input.len())) {
        Err(ParseError::Incomplete(Needed::Size(need)))
    } else {
        let (result, remaining) = input.split_at(4);
        Ok((remaining, result.try_into().expect("we checked the length")))
    }
}

/// Parse a slice of length `n` from `input`. If there is not enough `input` this will fail with
/// `ParseResult::Incomplete(Needed(<amount needed>))`.
pub(crate) fn take_n<'a, E>(n: usize, input: &'a [u8]) -> ParseResult<'_, &'a [u8], E> {
    if let Some(need) = NonZeroUsize::new(n.saturating_sub(input.len())) {
        Err(ParseError::Incomplete(Needed::Size(need)))
    } else {
        let (result, remaining) = input.split_at(n);
        Ok((remaining, result))
    }
}

/// The same as `take_n` but discards the result
pub(crate) fn drop_n<E>(n: usize, input: &[u8]) -> ParseResult<'_, (), E> {
    take_n(n, input).map(|(i, _)| (i, ()))
}

pub(crate) fn length_prefixed<'a, F, G, O, Ef, Er>(
    mut f: F,
    mut g: G,
) -> impl FnMut(&'a [u8]) -> ParseResult<'a, Vec<O>, Er>
where
    F: Parser<'a, u64, Ef>,
    G: Parser<'a, O, Er>,
    Er: From<Ef>,
{
    move |input: &'a [u8]| {
        let (i, count) = f.parse(input).map_err(lift_errorkind)?;
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

pub(crate) fn length_prefixed_bytes<'a, E>(input: &'a [u8]) -> ParseResult<'_, &'a [u8], E>
where
    E: From<ErrorKind>,
{
    let (i, len) = leb128_u64(input).map_err(lift_errorkind)?;
    take_n(len as usize, i)
}

pub(super) fn tuple2<'a, F, E, G, H, Er>(
    mut f: F,
    mut g: G,
) -> impl FnMut(&'a [u8]) -> ParseResult<'_, (E, H), Er>
where
    F: Parser<'a, E, Er>,
    G: Parser<'a, H, Er>,
{
    move |input: &'a [u8]| {
        let (i, one) = f.parse(input)?;
        let (i, two) = g.parse(i)?;
        Ok((i, (one, two)))
    }
}

pub(super) fn apply_n<'a, F, E, Er>(
    n: usize,
    mut f: F,
) -> impl FnMut(&'a [u8]) -> ParseResult<'_, Vec<E>, Er>
where
    F: Parser<'a, E, Er>,
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
pub(crate) fn actor_id<E>(input: &[u8]) -> ParseResult<'_, ActorId, E>
where
    E: From<ErrorKind>,
{
    let (i, length) = leb128_u64(input).map_err(lift_errorkind)?;
    let (i, bytes) = take_n(length as usize, i)?;
    Ok((i, bytes.into()))
}

pub(crate) fn change_hash<E>(input: &[u8]) -> ParseResult<'_, ChangeHash, E> {
    let (i, bytes) = take_n(HASH_SIZE, input)?;
    let byte_arr: ChangeHash = bytes.try_into().expect("we checked the length above");
    Ok((i, byte_arr))
}

pub(crate) fn utf_8<E>(len: usize, input: &[u8]) -> ParseResult<'_, String, E>
where
    E: From<ErrorKind>,
{
    let (i, bytes) = take_n(len, input)?;
    let result = String::from_utf8(bytes.to_vec())
        .map_err(|_| ParseError::Error(ErrorKind::InvalidUtf8))
        .map_err(lift_errorkind)?;
    Ok((i, result))
}

pub(crate) fn lift_errorkind<Ef, Eg: From<Ef>>(e: ParseError<Ef>) -> ParseError<Eg> {
    match e {
        ParseError::Error(e) => ParseError::Error(Eg::from(e)),
        ParseError::Incomplete(n) => ParseError::Incomplete(n),
    }
}
