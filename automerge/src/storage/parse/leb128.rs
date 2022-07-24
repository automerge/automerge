use core::mem::size_of;
use std::num::NonZeroU64;

use super::{take1, Input, ParseError, ParseResult};

#[derive(PartialEq, thiserror::Error, Debug, Clone)]
pub(crate) enum Error {
    #[error("leb128 was too large for the destination type")]
    Leb128TooLarge,
    #[error("leb128 was zero when it was expected to be nonzero")]
    UnexpectedZero,
}

macro_rules! impl_leb {
    ($parser_name: ident, $ty: ty) => {
        #[allow(dead_code)]
        pub(crate) fn $parser_name<'a, E>(input: Input<'a>) -> ParseResult<'a, $ty, E>
        where
            E: From<Error>,
        {
            let mut res = 0;
            let mut shift = 0;

            let mut input = input;
            let mut pos = 0;
            loop {
                let (i, byte) = take1(input)?;
                input = i;
                if (byte & 0x80) == 0 {
                    res |= (byte as $ty) << shift;
                    return Ok((input, res));
                } else if pos == leb128_size::<$ty>() - 1 {
                    return Err(ParseError::Error(Error::Leb128TooLarge.into()));
                } else {
                    res |= ((byte & 0x7F) as $ty) << shift;
                }
                pos += 1;
                shift += 7;
            }
        }
    };
}

impl_leb!(leb128_u64, u64);
impl_leb!(leb128_u32, u32);
impl_leb!(leb128_i64, i64);
impl_leb!(leb128_i32, i32);

/// Parse a LEB128 encoded u64 from the input, throwing an error if it is `0`
pub(crate) fn nonzero_leb128_u64<E>(input: Input<'_>) -> ParseResult<'_, NonZeroU64, E>
where
    E: From<Error>,
{
    let (input, num) = leb128_u64(input)?;
    let result =
        NonZeroU64::new(num).ok_or_else(|| ParseError::Error(Error::UnexpectedZero.into()))?;
    Ok((input, result))
}

/// Maximum LEB128-encoded size of an integer type
const fn leb128_size<T>() -> usize {
    let bits = size_of::<T>() * 8;
    (bits + 6) / 7 // equivalent to ceil(bits/7) w/o floats
}

#[cfg(test)]
mod tests {
    use super::super::Needed;
    use super::*;
    use std::{convert::TryFrom, num::NonZeroUsize};

    const NEED_ONE: Needed = Needed::Size(unsafe { NonZeroUsize::new_unchecked(1) });

    #[test]
    fn leb_128_unsigned() {
        let one = &[0b00000001_u8];
        let one_two_nine = &[0b10000001, 0b00000001];
        let one_and_more = &[0b00000001, 0b00000011];

        let scenarios: Vec<(&'static [u8], ParseResult<'_, u64, Error>)> = vec![
            (one, Ok((Input::with_position(one, 1), 1))),
            (&[0b10000001_u8], Err(ParseError::Incomplete(NEED_ONE))),
            (
                one_two_nine,
                Ok((Input::with_position(one_two_nine, 2), 129)),
            ),
            (one_and_more, Ok((Input::with_position(one_and_more, 1), 1))),
            (
                &[129, 129, 129, 129, 129, 129, 129, 129, 129, 129, 129, 129],
                Err(ParseError::Error(Error::Leb128TooLarge)),
            ),
        ];
        for (index, (input, expected)) in scenarios.clone().into_iter().enumerate() {
            let result = leb128_u64(Input::new(input));
            if result != expected {
                panic!(
                    "Scenario {} failed for u64: expected {:?} got {:?}",
                    index + 1,
                    expected,
                    result
                );
            }
        }

        for (index, (input, expected)) in scenarios.into_iter().enumerate() {
            let u32_expected = expected.map(|(i, e)| (i, u32::try_from(e).unwrap()));
            let result = leb128_u32(Input::new(input));
            if result != u32_expected {
                panic!(
                    "Scenario {} failed for u32: expected {:?} got {:?}",
                    index + 1,
                    u32_expected,
                    result
                );
            }
        }
    }
}
