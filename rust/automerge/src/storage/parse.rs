//! A small parser combinator library inspired by [`nom`](https://docs.rs/crate/nom/5.0.0).
//!
//! The primary reason for using this rather than `nom` is that this is only a few hundred lines of
//! code because we don't need a fully fledged combinator library - automerge is a low level
//! library so it's good to avoid dependencies where we can.
//!
//! # Basic Usage
//!
//! The basic components of this library are [`Parser`]s, which parse [`Input`]s and produce
//! [`ParseResult`]s. `Input` is a combination of an `&[u8]` which is the incoming data along with
//! the position it has read up to in the data. `Parser` is a trait but has a blanket `impl` for
//! `FnMut(Input<'a>) -> ParseResult<'a, O, E>` so in practice you can think of parsers as a
//! function which takes some input and returns a result plus any remaining input. This final part
//! is encapsulated by the `ParseResult` which is a type alias for a `Result`. This means that
//! typical usage will look something like this:
//!
//! ```rust,ignore
//! use automerge::storage::parse::{ParseResult, take_1};
//! fn do_something<'a>(input: Input<'a>) -> ParseResult<'a, [u8; 3], ()> {
//!     let (i, a) = take_1::<()>(input)?;
//!     let (i, b) = take_1::<()>(i)?;
//!     let (i, c) = take_1::<()>(i)?;
//!     let result = [a, b, c];
//!     Ok((i, result))
//! }
//!
//! let input = Input::new(&[b"12345"]);
//! let result = do_something(input);
//! if let Ok((_, result)) = result {
//!     assert_eq!(&result, &['1', '2', '3']);
//! } else {
//!     panic!();
//! }
//! ```
//!
//! Three things to note here:
//!
//! 1. The rebinding of the input (in `i`) after each call to `take_1`, this is how parser state is passed from
//!    one call to the next
//! 2. We return a tuple containing the remaining input plus the result
//! 3. `take_1` has a type parameter we must pass to it representing the error type. Generally you
//!    don't need to do that as type inference is often good enough.
//!
//! # Errors
//!
//! The error branch of `ParseError` is an enum containing either `ParseError::Incomplete`
//! indicating that with more input we might be able to succeed, or a `ParseError::Error`. The
//! latter branch is where parser specific errors (e.g. "this u8 is not a valid chunk type") are
//! passed. This has implications for returning and handling errors.
//!
//! ## Returning Errors
//!
//! If you want to return an error from a parser you will need to wrap the error in
//! `ParseError::Error`.
//!
//! ```rust,ignore
//! struct MyError;
//! fn my_bad_parser() -> ParseResult<(), MyError> {
//!     Err(ParseError::Error(MyError))
//! }
//! ```
//!
//! ## Handling Errors
//!
//! Handling errors is generally important when you want to compose parsers with different error
//! types. In this case you will often have an error type you want to map each of the underlying
//! errors into. For this purpose you can use `ParseError::lift`
//!
//! ```rust,ignore
//! # use automerge::parse::{ParseResult, Input};
//! #[derive(thiserror::Error, Debug)]
//! #[error("this is a bad string")]
//! struct BadString;
//!
//! #[derive(thiserror::Error, Debug)]
//! #[error("this is a bad number")]
//! struct BadNumber;
//!
//! fn parse_string<'a>(input: Input<'a>) -> ParseResult<'a, String, BadString> {
//!     Err(ParseError::Error(BadString))
//! }
//!
//! fn parse_number<'a>(input: Input<'a>) -> ParseResult<'a, u32, BadNumber> {
//!     Err(ParseError::Error(BadNumber))
//! }
//!
//! #[derive(thiserror::Error, Debug)]
//! struct CombinedError{
//!     #[error(transparent)]
//!     String(#[from] BadString),
//!     #[error(transparent)]
//!     Number(#[from] BadNumber),
//! }
//!
//! fn parse_string_then_number<'a>(input: Input<'a>) -> ParseResult<'a, (String, u32), CombinedError> {
//!     // Note the `e.lift()` here, this works because of the `From<BadString>` impl generated by
//!     // `thiserror::Error`
//!     let (i, thestring) = parse_string(input).map_err(|e| e.lift())?;
//!     let (i, thenumber) = parse_number(i).map_err(|e| e.lift())?;
//!     Ok((i, (thestring, thenumber)))
//! }
//! ```

use core::num::NonZeroUsize;
use std::convert::TryInto;

pub(crate) mod leb128;
use crate::{ActorId, ChangeHash};

const HASH_SIZE: usize = 32; // 256 bits = 32 bytes

#[allow(unused_imports)]
pub(crate) use self::leb128::{leb128_i64, leb128_u32, leb128_u64, nonzero_leb128_u64};

pub(crate) type ParseResult<'a, O, E> = Result<(Input<'a>, O), ParseError<E>>;

/// The input to be parsed. This is a combination of an underlying slice, plus an offset into that
/// slice. Consequently it is very cheap to copy.
#[derive(PartialEq, Clone, Copy)]
pub(crate) struct Input<'a> {
    bytes: &'a [u8],
    position: usize,
    original: &'a [u8],
}

impl std::fmt::Debug for Input<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Input(len: {}, position: {}, original_len: {})",
            self.bytes.len(),
            self.position,
            self.original.len()
        )
    }
}

impl<'a> Input<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            position: 0,
            original: bytes,
        }
    }

    #[cfg(test)]
    pub(in crate::storage::parse) fn with_position(bytes: &'a [u8], position: usize) -> Input<'a> {
        let remaining = &bytes[position..];
        Self {
            bytes: remaining,
            position,
            original: bytes,
        }
    }

    pub(crate) fn empty() -> Self {
        Self {
            bytes: &[],
            position: 0,
            original: &[],
        }
    }

    fn take_1<E>(&self) -> ParseResult<'a, u8, E> {
        if let Some(need) = NonZeroUsize::new(1_usize.saturating_sub(self.bytes.len())) {
            Err(ParseError::Incomplete(Needed::Size(need)))
        } else {
            let (result, remaining) = self.bytes.split_at(1);
            let new_input = Input {
                bytes: remaining,
                original: self.original,
                position: self.position + 1,
            };
            Ok((new_input, result[0]))
        }
    }

    fn take_n<E>(&self, n: usize) -> ParseResult<'a, &'a [u8], E> {
        if let Some(need) = NonZeroUsize::new(n.saturating_sub(self.bytes.len())) {
            Err(ParseError::Incomplete(Needed::Size(need)))
        } else {
            let (result, remaining) = self.bytes.split_at(n);
            let new_input = Input {
                bytes: remaining,
                original: self.original,
                position: self.position + n,
            };
            Ok((new_input, result))
        }
    }

    fn take_4<E>(&self) -> ParseResult<'a, [u8; 4], E> {
        if let Some(need) = NonZeroUsize::new(4_usize.saturating_sub(self.bytes.len())) {
            Err(ParseError::Incomplete(Needed::Size(need)))
        } else {
            let (result, remaining) = self.bytes.split_at(4);
            let new_input = Input {
                bytes: remaining,
                original: self.original,
                position: self.position + 4,
            };
            Ok((new_input, result.try_into().expect("we checked the length")))
        }
    }

    fn range_of<P, R, E>(&self, mut parser: P) -> ParseResult<'a, RangeOf<R>, E>
    where
        P: Parser<'a, R, E>,
    {
        let (new_input, value) = parser.parse(*self)?;
        let range = self.position..new_input.position;
        Ok((new_input, RangeOf { range, value }))
    }

    fn rest<E>(&self) -> ParseResult<'a, &'a [u8], E> {
        let position = self.position + self.bytes.len();
        let new_input = Self {
            position,
            original: self.original,
            bytes: &[],
        };
        Ok((new_input, self.bytes))
    }

    fn truncate(&self, length: usize) -> Input<'a> {
        let length = if length > self.bytes.len() {
            self.bytes.len()
        } else {
            length
        };
        Input {
            bytes: &self.bytes[..length],
            position: self.position,
            original: &self.original[..(self.position + length)],
        }
    }

    fn skip(&self, length: usize) -> Input<'a> {
        if length > self.bytes.len() {
            Input {
                bytes: &[],
                position: self.bytes.len(),
                original: self.original,
            }
        } else {
            Input {
                bytes: &self.bytes[length..],
                position: self.position + length,
                original: &self.original[(self.position + length)..],
            }
        }
    }

    /// Split this input into two separate inputs, the first is the same as the current input but
    /// with the remaining unconsumed_bytes set to at most length. The remaining `Input` is the bytes
    /// after `length`.
    ///
    /// This is useful if you are parsing input which contains length delimited chunks. In this
    /// case you may have a single input where you parse a header, then you want to parse the
    /// current input up until the length and then parse the next chunk from the remainign input.
    /// For example:
    ///
    /// ```rust,ignore
    /// # use automerge::storage::parse::{Input, ParseResult};
    ///
    /// fn parse_chunk(input: Input<'_>) -> ParseResult<(), ()> {
    ///     Ok(())
    /// }
    ///
    /// # fn main() -> ParseResult<(), ()> {
    /// let incoming_bytes: &[u8] = todo!();
    /// let mut input = Input::new(incoming_bytes);
    /// let mut chunks = Vec::new();
    /// while !input.is_empty() {
    ///     let (i, chunk_len) = leb128_u64(input)?;
    ///     let Split{first: i, remaining} = i.split(chunk_len);
    ///     // Note that here, the `i` we pass into `parse_chunk` has already parsed the header,
    ///     // so the logic of the `parse_chunk` function doesn't need to reimplement the header
    ///     // parsing
    ///     let (i, chunk) = parse_chunk(i)?;
    ///     let input = remaining;
    /// }
    /// parse_chunk(i);
    /// # }
    /// ```
    pub(crate) fn split(&self, length: usize) -> Split<'a> {
        Split {
            first: self.truncate(length),
            remaining: self.skip(length),
        }
    }

    /// Return a new `Input` which forgets about the consumed input. The new `Input` will have it's
    /// position set to 0. This is equivalent to `Input::new(self.bytes())`
    pub(crate) fn reset(&self) -> Input<'a> {
        Input::new(self.bytes)
    }

    /// Check if there are any more bytes left to consume
    pub(crate) fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// The bytes which have not yet been consumed
    pub(crate) fn unconsumed_bytes(&self) -> &'a [u8] {
        self.bytes
    }

    /// The bytes behind this input - including bytes which have been consumed
    #[allow(clippy::misnamed_getters)]
    pub(crate) fn bytes(&self) -> &'a [u8] {
        self.original
    }
}

/// Returned by [`Input::split`]
pub(crate) struct Split<'a> {
    /// The input up to the length passed to `split`. This is identical to the original input
    /// except that [`Input::bytes`] and [`Input::unconsumed_bytes`] will only return the original
    /// input up to `length` bytes from the point at which `split` was called.
    pub(crate) first: Input<'a>,
    /// The remaining input after the length passed to `split`. This is equivalent to
    ///
    /// ```rust,ignore
    /// # use automerge::storage::parse::Input;
    /// # let split_length = 1;
    /// let original_input = todo!();
    /// Input::new(original_input.bytes()[split_length..])
    /// ```
    pub(crate) remaining: Input<'a>,
}

pub(crate) trait Parser<'a, O, E> {
    fn parse(&mut self, input: Input<'a>) -> ParseResult<'a, O, E>;
}

impl<'a, O, F, E> Parser<'a, O, E> for F
where
    F: FnMut(Input<'a>) -> ParseResult<'a, O, E>,
{
    fn parse(&mut self, input: Input<'a>) -> ParseResult<'a, O, E> {
        (self)(input)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ParseError<E> {
    /// Some application specific error occurred
    Error(E),
    /// A combinator requested more data than we have available
    Incomplete(Needed),
}

impl<E> ParseError<E> {
    /// Convert any underlying `E` into `F`. This is useful when you are composing parsers
    pub(crate) fn lift<F>(self) -> ParseError<F>
    where
        F: From<E>,
    {
        match self {
            Self::Error(e) => ParseError::Error(F::from(e)),
            Self::Incomplete(n) => ParseError::Incomplete(n),
        }
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

/// How much more input we need
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Needed {
    /// We don't know how much more
    #[allow(dead_code)]
    Unknown,
    /// We need _at least_ this much more
    Size(NonZeroUsize),
}

/// Map the function `f` over the result of `parser` returning a new parser
pub(crate) fn map<'a, O1, O2, F, G, Er>(
    mut parser: F,
    mut f: G,
) -> impl FnMut(Input<'a>) -> ParseResult<'a, O2, Er>
where
    F: Parser<'a, O1, Er>,
    G: FnMut(O1) -> O2,
{
    move |input: Input<'a>| {
        let (input, o1) = parser.parse(input)?;
        Ok((input, f(o1)))
    }
}

/// Pull one byte from the input
pub(crate) fn take1<E>(input: Input<'_>) -> ParseResult<'_, u8, E> {
    input.take_1()
}

/// Parse an array of four bytes from the input
pub(crate) fn take4<E>(input: Input<'_>) -> ParseResult<'_, [u8; 4], E> {
    input.take_4()
}

/// Parse a slice of length `n` from `input`
pub(crate) fn take_n<E>(n: usize, input: Input<'_>) -> ParseResult<'_, &[u8], E> {
    input.take_n(n)
}

/// Parse a length prefixed collection of `g`
///
/// This first parses a LEB128 encoded `u64` from the input, then applies the parser `g` this many
/// times, returning the result in a `Vec`.
pub(crate) fn length_prefixed<'a, G, O, Er>(
    mut g: G,
) -> impl FnMut(Input<'a>) -> ParseResult<'a, Vec<O>, Er>
where
    G: Parser<'a, O, Er>,
    Er: From<leb128::Error>,
{
    move |input: Input<'a>| {
        let (i, count) = leb128_u64(input).map_err(|e| e.lift())?;
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

/// Parse a length prefixed array of bytes from the input
///
/// This first parses a LEB128 encoded `u64` from the input, then parses this many bytes from the
/// underlying input.
pub(crate) fn length_prefixed_bytes<E>(input: Input<'_>) -> ParseResult<'_, &[u8], E>
where
    E: From<leb128::Error>,
{
    let (i, len) = leb128_u64(input).map_err(|e| e.lift())?;
    take_n(len as usize, i)
}

/// Apply two parsers, returning the result in a 2 tuple
///
/// This first applies `f`, then `g` and returns the result as `(f, g)`.
pub(super) fn tuple2<'a, F, E, G, H, Er>(
    mut f: F,
    mut g: G,
) -> impl FnMut(Input<'a>) -> ParseResult<'a, (E, H), Er>
where
    F: Parser<'a, E, Er>,
    G: Parser<'a, H, Er>,
{
    move |input: Input<'a>| {
        let (i, one) = f.parse(input)?;
        let (i, two) = g.parse(i)?;
        Ok((i, (one, two)))
    }
}

/// Apply the parser `f` `n` times and reutrn the result in a `Vec`
pub(super) fn apply_n<'a, F, E, Er>(
    n: usize,
    mut f: F,
) -> impl FnMut(Input<'a>) -> ParseResult<'a, Vec<E>, Er>
where
    F: Parser<'a, E, Er>,
{
    move |input: Input<'a>| {
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
///
/// This first parses a LEB128 encoded u64 from the input, then the corresponding number of bytes
/// which are returned wrapped in an `ActorId`
pub(crate) fn actor_id<E>(input: Input<'_>) -> ParseResult<'_, ActorId, E>
where
    E: From<leb128::Error>,
{
    let (i, length) = leb128_u64(input).map_err(|e| e.lift())?;
    let (i, bytes) = take_n(length as usize, i)?;
    Ok((i, bytes.into()))
}

/// Parse a change hash.
///
/// This is just a nice wrapper around `take_4`
pub(crate) fn change_hash<E>(input: Input<'_>) -> ParseResult<'_, ChangeHash, E> {
    let (i, bytes) = take_n(HASH_SIZE, input)?;
    let byte_arr: ChangeHash = bytes.try_into().expect("we checked the length above");
    Ok((i, byte_arr))
}

#[derive(thiserror::Error, Debug)]
#[error("invalid UTF-8")]
pub(crate) struct InvalidUtf8;

/// Parse a length prefixed UTF-8 string
///
/// This first parses a LEB128 encode `u64` from the input, then parses this many bytes from the
/// input before attempting to convert these bytes into a `String`, returning
/// `ParseError::Error(InvalidUtf8)` if that fails.
pub(crate) fn utf_8<E>(len: usize, input: Input<'_>) -> ParseResult<'_, String, E>
where
    E: From<InvalidUtf8>,
{
    let (i, bytes) = take_n(len, input)?;
    let result = String::from_utf8(bytes.to_vec())
        .map_err(|_| ParseError::Error(InvalidUtf8))
        .map_err(|e| e.lift())?;
    Ok((i, result))
}

/// Returned from `range_of`
pub(crate) struct RangeOf<T> {
    /// The range in the input where we parsed from
    pub(crate) range: std::ops::Range<usize>,
    /// The value we parsed
    pub(crate) value: T,
}

/// Evaluate `parser` and then return the value parsed, as well as the range in the input which we
/// just parsed.
///
/// This is useful when you want to parse some data from an input in order to check that is valid,
/// but you will also be holding on to the input data and want to know where in the input data the
/// valid data was parsed from.
///
/// # Example
///
/// Imagine that we are parsing records of some kind from a file, as well as parsing the record we
/// want to record the offset in the file where the record is so we can update it in place.
///
/// ```rust,ignore
/// # use automerge::storage::parse::{ParseResult, Input};
/// struct Message;
/// struct Record {
///     message: Message,
///     location: std::ops::Range<usize>
/// }
///
/// fn parse_message<'a>(input: Input<'a>) -> ParseResult<'a, Message, ()> {
///     unimplemented!()
/// }
///
/// fn parse_record<'a>(input: Input<'a>) -> ParseResult<'a, Record, ()> {
///     let (i, RangeOf{range: location, value: message}) = range_of(|i| parse_message(i), i)?;
///     Ok((i, Record {
///         location, // <- this is the location in the input where the message was parsed from
///         message,
///     }))
/// }
///
/// let file_contents: Vec<u8> = unimplemented!();
/// let input = Input::new(&file_contents);
/// let record = parse_record(input).unwrap().1;
/// ```
pub(crate) fn range_of<'a, P, R, E>(parser: P, input: Input<'a>) -> ParseResult<'a, RangeOf<R>, E>
where
    P: Parser<'a, R, E>,
{
    input.range_of(parser)
}

pub(crate) fn range_only_unless_empty<'a, P, R, E>(
    parser: P,
    input: Input<'a>,
) -> ParseResult<'a, std::ops::Range<usize>, E>
where
    P: Parser<'a, R, E>,
{
    if input.is_empty() {
        Ok((input, 0..0))
    } else {
        range_only(parser, input)
    }
}

pub(crate) fn range_only<'a, P, R, E>(
    parser: P,
    input: Input<'a>,
) -> ParseResult<'a, std::ops::Range<usize>, E>
where
    P: Parser<'a, R, E>,
{
    input.range_of(parser).map(|(i, r)| (i, r.range))
}

/// Parse all the remaining input from the parser. This can never fail
pub(crate) fn take_rest<E>(input: Input<'_>) -> ParseResult<'_, &'_ [u8], E> {
    input.rest()
}
