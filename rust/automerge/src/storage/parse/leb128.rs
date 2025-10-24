use std::num::NonZeroU64;

use super::{take1, Input, ParseError, ParseResult};

#[derive(PartialEq, thiserror::Error, Debug, Clone)]
pub(crate) enum Error {
    #[error("leb128 was too large for the destination type")]
    Leb128TooLarge,
    #[error("leb128 was improperly encoded")]
    Leb128Overlong,
    #[error("leb128 was zero when it was expected to be nonzero")]
    UnexpectedZero,
}

pub(crate) fn leb128_u64<E>(input: Input<'_>) -> ParseResult<'_, u64, E>
where
    E: From<Error>,
{
    let mut res = 0;
    let mut shift = 0;
    let mut input = input;

    loop {
        let (i, byte) = take1(input)?;
        input = i;
        res |= ((byte & 0x7F) as u64) << shift;
        shift += 7;

        if (byte & 0x80) == 0 {
            if shift > 64 && byte > 1 {
                return Err(ParseError::Error(Error::Leb128TooLarge.into()));
            } else if shift > 7 && byte == 0 {
                return Err(ParseError::Error(Error::Leb128Overlong.into()));
            }
            return Ok((input, res));
        } else if shift > 64 {
            return Err(ParseError::Error(Error::Leb128TooLarge.into()));
        }
    }
}

pub(crate) fn leb128_i64<E>(input: Input<'_>) -> ParseResult<'_, i64, E>
where
    E: From<Error>,
{
    let mut res = 0;
    let mut shift = 0;

    let mut input = input;
    let mut prev = 0;
    loop {
        let (i, byte) = take1(input)?;
        input = i;
        res |= ((byte & 0x7F) as i64) << shift;
        shift += 7;

        if (byte & 0x80) == 0 {
            if shift > 64 && byte != 0 && byte != 0x7f {
                // the 10th byte (if present) must contain only the sign-extended sign bit
                return Err(ParseError::Error(Error::Leb128TooLarge.into()));
            } else if shift > 7
                && ((byte == 0 && prev & 0x40 == 0) || (byte == 0x7f && prev & 0x40 > 0))
            {
                // overlong if the sign bit of penultimate byte has been extended
                return Err(ParseError::Error(Error::Leb128Overlong.into()));
            } else if shift < 64 && byte & 0x40 > 0 {
                // sign extend negative numbers
                res |= -1 << shift;
            }
            return Ok((input, res));
        } else if shift > 64 {
            return Err(ParseError::Error(Error::Leb128TooLarge.into()));
        }
        prev = byte;
    }
}

pub(crate) fn leb128_u32<E>(input: Input<'_>) -> ParseResult<'_, u32, E>
where
    E: From<Error>,
{
    let (i, num) = leb128_u64(input)?;
    let result = u32::try_from(num).map_err(|_| ParseError::Error(Error::Leb128TooLarge.into()))?;
    Ok((i, result))
}

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

#[cfg(test)]
mod tests {
    use super::super::Needed;
    use super::*;
    use std::num::NonZeroUsize;

    const NEED_ONE: Needed = Needed::Size(NonZeroUsize::new(1).unwrap());

    #[test]
    fn leb_128_u64() {
        let one = &[0b00000001_u8];
        let one_two_nine = &[0b10000001, 0b00000001];
        let one_and_more = &[0b00000001, 0b00000011];

        let scenarios: Vec<(&'static [u8], ParseResult<'_, u64, Error>)> = vec![
            (one, Ok((Input::with_position(one, 1), 1))),
            (
                one_two_nine,
                Ok((Input::with_position(one_two_nine, 2), 129)),
            ),
            (one_and_more, Ok((Input::with_position(one_and_more, 1), 1))),
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

        let error_cases: Vec<(&'static str, &'static [u8], ParseError<_>)> = vec![
            (
                "too many bytes",
                &[129, 129, 129, 129, 129, 129, 129, 129, 129, 129, 129, 129],
                ParseError::Error(Error::Leb128TooLarge),
            ),
            (
                "too many bits",
                &[129, 129, 129, 129, 129, 129, 129, 129, 129, 2],
                ParseError::Error(Error::Leb128TooLarge),
            ),
            (
                "overlong encoding",
                &[129, 0],
                ParseError::Error(Error::Leb128Overlong),
            ),
            ("missing data", &[255], ParseError::Incomplete(NEED_ONE)),
        ];
        error_cases.into_iter().for_each(|(desc, input, expected)| {
            match leb128_u64::<Error>(Input::new(input)) {
                Ok((_, x)) => panic!("leb128_u64 should fail with {}, got {}", desc, x),
                Err(error) => {
                    if error != expected {
                        panic!("leb128_u64 should fail with {}, got {}", expected, error)
                    }
                }
            }
        });

        let success_cases: Vec<(&'static [u8], u64)> = vec![
            (&[0], 0),
            (&[0x7f], 127),
            (&[0x80, 0x01], 128),
            (&[0xff, 0x7f], 16383),
            (
                &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1],
                u64::MAX,
            ),
        ];
        success_cases.into_iter().for_each(|(input, expected)| {
            match leb128_u64::<Error>(Input::new(input)) {
                Ok((_, x)) => {
                    if x != expected {
                        panic!("leb128_u64 should succeed with {}, got {}", expected, x)
                    }
                }
                Err(error) => panic!("leb128_u64 should succeed with {}, got {}", expected, error),
            }
        });
    }

    #[test]
    fn leb_128_u32() {
        let error_cases: Vec<(&'static str, &'static [u8], ParseError<_>)> = vec![
            (
                "too many bytes",
                &[129, 129, 129, 129, 129, 129, 129, 129, 129, 129, 129, 129],
                ParseError::Error(Error::Leb128TooLarge),
            ),
            (
                "too many bits",
                &[0xff, 0xff, 0xff, 0xff, 0x1f],
                ParseError::Error(Error::Leb128TooLarge),
            ),
            (
                "overlong encoding",
                &[129, 0],
                ParseError::Error(Error::Leb128Overlong),
            ),
            ("missing data", &[0xaa], ParseError::Incomplete(NEED_ONE)),
        ];
        error_cases.into_iter().for_each(|(desc, input, expected)| {
            match leb128_u32::<Error>(Input::new(input)) {
                Ok((_, x)) => panic!("leb128_u32 should fail with {}, got {}", desc, x),
                Err(error) => {
                    if error != expected {
                        panic!("leb128_u32 should fail with {}, got {}", expected, error)
                    }
                }
            }
        });

        let success_cases: Vec<(&'static [u8], u32)> = vec![
            (&[0], 0),
            (&[0x7f], 127),
            (&[0x80, 0x01], 128),
            (&[0xff, 0x7f], 16383),
            (&[0xff, 0xff, 0xff, 0xff, 0x0f], u32::MAX),
        ];
        success_cases.into_iter().for_each(|(input, expected)| {
            match leb128_u32::<Error>(Input::new(input)) {
                Ok((_, x)) => {
                    if x != expected {
                        panic!("leb128_u32 should succeed with {}, got {}", expected, x)
                    }
                }
                Err(error) => panic!("leb128_u64 should succeed with {}, got {}", expected, error),
            }
        });
    }

    #[test]
    fn leb_128_i64() {
        let error_cases: Vec<(&'static str, &'static [u8], ParseError<_>)> = vec![
            (
                "too many bytes",
                &[129, 129, 129, 129, 129, 129, 129, 129, 129, 129, 129, 129],
                ParseError::Error(Error::Leb128TooLarge),
            ),
            (
                "too many positive bits",
                &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01],
                ParseError::Error(Error::Leb128TooLarge),
            ),
            (
                "too many negative bits",
                &[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x7e],
                ParseError::Error(Error::Leb128TooLarge),
            ),
            (
                "overlong positive encoding",
                &[0xbf, 0],
                ParseError::Error(Error::Leb128Overlong),
            ),
            (
                "overlong negative encoding",
                &[0x81, 0xff, 0x7f],
                ParseError::Error(Error::Leb128Overlong),
            ),
            ("missing data", &[0x90], ParseError::Incomplete(NEED_ONE)),
        ];
        error_cases.into_iter().for_each(|(desc, input, expected)| {
            match leb128_i64::<Error>(Input::new(input)) {
                Ok((_, x)) => panic!("leb128_i64 should fail with {}, got {}", desc, x),
                Err(error) => {
                    if error != expected {
                        panic!("leb128_i64 should fail with {}, got {}", expected, error)
                    }
                }
            }
        });

        let success_cases: Vec<(&'static [u8], i64)> = vec![
            (&[0], 0),
            (&[0x7f], -1),
            (&[0x3f], 63),
            (&[0x40], -64),
            (&[0x80, 0x01], 128),
            (&[0xff, 0x3f], 8191),
            (&[0x80, 0x40], -8192),
            (
                &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x0],
                i64::MAX,
            ),
            (
                &[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x7f],
                i64::MIN,
            ),
        ];
        success_cases.into_iter().for_each(|(input, expected)| {
            match leb128_i64::<Error>(Input::new(input)) {
                Ok((_, x)) => {
                    if x != expected {
                        panic!("leb128_i64 should succeed with {}, got {}", expected, x)
                    }
                }
                Err(error) => panic!("leb128_u64 should succeed with {}, got {}", expected, error),
            }
        });
    }
}
