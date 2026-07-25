//! Byte-slice parser combinators for the sync wire format.
//!
//! Deliberately a private copy rather than a dependency on automerge's
//! internal storage parser: the sync format and the document format are
//! independent, and the only encoding they genuinely share is the
//! 32-byte [`ChangeHash`], which automerge exposes publicly. Keeping our
//! own copy means the document format's parser can change freely.
//!
//! Only what the sync messages, bloom filters and sync state need is
//! here; there is no ambition for this to be a general parser library.

use automerge::ChangeHash;
use std::num::NonZeroUsize;

const HASH_SIZE: usize = 32;

pub(crate) type ParseResult<'a, O, E> = Result<(Input<'a>, O), ParseError<E>>;

/// A byte slice plus a cursor into it. Cheap to copy — parsers take it
/// by value and hand back the advanced input alongside their output.
#[derive(PartialEq, Clone, Copy)]
pub(crate) struct Input<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl std::fmt::Debug for Input<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Input(remaining: {}, position: {})",
            self.bytes.len(),
            self.position
        )
    }
}

impl<'a> Input<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// The bytes not yet consumed.
    #[cfg(test)]
    pub(crate) fn bytes(&self) -> &'a [u8] {
        self.bytes
    }

    fn advance<E>(&self, n: usize) -> ParseResult<'a, &'a [u8], E> {
        match NonZeroUsize::new(n.saturating_sub(self.bytes.len())) {
            Some(need) => Err(ParseError::Incomplete(Needed::Size(need))),
            None => {
                let (taken, rest) = self.bytes.split_at(n);
                Ok((
                    Input {
                        bytes: rest,
                        position: self.position + n,
                    },
                    taken,
                ))
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ParseError<E> {
    /// An error from the parser itself
    Error(E),
    /// The input ran out before the parser was satisfied
    Incomplete(Needed),
}

impl<E> ParseError<E> {
    /// Convert the inner error, for composing parsers with different
    /// error types.
    pub(crate) fn lift<F: From<E>>(self) -> ParseError<F> {
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
            Self::Incomplete(_) => write!(f, "not enough input"),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Needed {
    /// At least this much more input is required
    Size(NonZeroUsize),
}

pub(crate) fn take1<E>(input: Input<'_>) -> ParseResult<'_, u8, E> {
    let (i, bytes) = input.advance(1)?;
    Ok((i, bytes[0]))
}

pub(crate) fn take_n<E>(n: usize, input: Input<'_>) -> ParseResult<'_, &[u8], E> {
    input.advance(n)
}

/// A uleb128-prefixed run of `g`.
pub(crate) fn length_prefixed<'a, G, O, E>(
    mut g: G,
) -> impl FnMut(Input<'a>) -> ParseResult<'a, Vec<O>, E>
where
    G: FnMut(Input<'a>) -> ParseResult<'a, O, E>,
    E: From<leb128::Error>,
{
    move |input: Input<'a>| {
        let (i, count) = leb128_u64(input).map_err(|e| e.lift())?;
        // `count` is wire-supplied: grow the vec as entries actually
        // parse rather than reserving what it claims
        let mut out = Vec::new();
        let mut input = i;
        for _ in 0..count {
            let (i, entry) = g(input)?;
            input = i;
            out.push(entry);
        }
        Ok((input, out))
    }
}

/// A uleb128-prefixed byte string.
pub(crate) fn length_prefixed_bytes<E>(input: Input<'_>) -> ParseResult<'_, &[u8], E>
where
    E: From<leb128::Error>,
{
    let (i, len) = leb128_u64(input).map_err(|e| e.lift())?;
    take_n(len as usize, i)
}

pub(crate) fn change_hash<E>(input: Input<'_>) -> ParseResult<'_, ChangeHash, E> {
    let (i, bytes) = take_n(HASH_SIZE, input)?;
    let hash = ChangeHash::try_from(bytes).expect("we checked the length");
    Ok((i, hash))
}

pub(crate) mod leb128 {
    use super::{take1, Input, ParseError, ParseResult};

    #[derive(Clone, Debug, PartialEq, thiserror::Error)]
    pub(crate) enum Error {
        #[error("leb128 was too large for the destination type")]
        Leb128TooLarge,
        #[error("leb128 was not encoded in the minimal number of bytes")]
        Leb128Overlong,
    }

    macro_rules! impl_leb {
        ($name: ident, $ty: ty, $bits: expr) => {
            pub(crate) fn $name<E>(input: Input<'_>) -> ParseResult<'_, $ty, E>
            where
                E: From<Error>,
            {
                let mut result: $ty = 0;
                let mut shift = 0;
                let mut input = input;
                loop {
                    let (i, byte) = take1::<E>(input)?;
                    input = i;
                    if shift >= $bits {
                        return Err(ParseError::Error(E::from(Error::Leb128TooLarge)));
                    }
                    let low = (byte & 0x7f) as $ty;
                    // the last byte must not shift bits off the top, and
                    // must not be a redundant zero continuation
                    if shift + 7 > $bits && (low >> ($bits - shift)) != 0 {
                        return Err(ParseError::Error(E::from(Error::Leb128TooLarge)));
                    }
                    result |= low << shift;
                    shift += 7;
                    if byte & 0x80 == 0 {
                        if byte == 0 && shift > 7 {
                            return Err(ParseError::Error(E::from(Error::Leb128Overlong)));
                        }
                        return Ok((input, result));
                    }
                }
            }
        };
    }

    impl_leb!(leb128_u64, u64, 64);
    impl_leb!(leb128_u32, u32, 32);
}

pub(crate) use leb128::{leb128_u32, leb128_u64};

#[cfg(test)]
mod tests {
    use super::*;

    fn u64_of(bytes: &[u8]) -> Result<u64, ParseError<leb128::Error>> {
        leb128_u64(Input::new(bytes)).map(|(_, v)| v)
    }

    #[test]
    fn leb128_roundtrips() {
        for v in [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            ::leb128::write::unsigned(&mut buf, v).unwrap();
            assert_eq!(u64_of(&buf).unwrap(), v, "value {v}");
        }
    }

    #[test]
    fn leb128_rejects_overlong_and_oversized() {
        // 0x80 0x00 is a non-minimal encoding of zero
        assert!(matches!(
            u64_of(&[0x80, 0x00]),
            Err(ParseError::Error(leb128::Error::Leb128Overlong))
        ));
        // eleven continuation bytes cannot fit in a u64
        let too_big = [0xff; 11];
        assert!(matches!(
            u64_of(&too_big),
            Err(ParseError::Error(leb128::Error::Leb128TooLarge))
        ));
    }

    #[test]
    fn incomplete_input_is_reported() {
        assert!(matches!(
            take_n::<leb128::Error>(4, Input::new(&[1, 2])),
            Err(ParseError::Incomplete(_))
        ));
        assert!(matches!(u64_of(&[0x80]), Err(ParseError::Incomplete(_))));
    }

    #[test]
    fn length_prefixed_reads_exactly_the_claimed_count() {
        // count 2, then two single bytes
        let bytes = [0x02, 0xaa, 0xbb, 0xcc];
        let (rest, out) =
            length_prefixed::<_, _, leb128::Error>(take1)(Input::new(&bytes)).unwrap();
        assert_eq!(out, vec![0xaa, 0xbb]);
        assert_eq!(rest.bytes(), &[0xcc]);
    }

    /// A count far larger than the input must fail, not allocate.
    #[test]
    fn length_prefixed_does_not_trust_the_count() {
        let mut bytes = Vec::new();
        ::leb128::write::unsigned(&mut bytes, u64::MAX / 2).unwrap();
        bytes.push(0xaa);
        assert!(matches!(
            length_prefixed::<_, _, leb128::Error>(take1)(Input::new(&bytes)),
            Err(ParseError::Incomplete(_))
        ));
    }

    #[test]
    fn change_hash_needs_all_32_bytes() {
        let bytes = [7u8; 32];
        let (rest, h) = change_hash::<leb128::Error>(Input::new(&bytes)).unwrap();
        assert_eq!(h.as_bytes(), &bytes);
        assert!(rest.is_empty());
        assert!(matches!(
            change_hash::<leb128::Error>(Input::new(&[7u8; 31])),
            Err(ParseError::Incomplete(_))
        ));
    }
}
