use core::{mem::size_of, num::NonZeroUsize};
use std::num::NonZeroU64;

use super::{ErrorKind, Needed, ParseError, ParseResult};

macro_rules! impl_leb {
    ($parser_name: ident, $ty: ty) => {
        #[allow(dead_code)]
        pub(crate) fn $parser_name<'a>(input: &'a [u8]) -> ParseResult<'a, $ty> {
            let mut res = 0;
            let mut shift = 0;

            for (pos, byte) in input.iter().enumerate() {
                if (byte & 0x80) == 0 {
                    res |= (*byte as $ty) << shift;
                    return Ok((&input[pos + 1..], res));
                } else if pos == leb128_size::<$ty>() - 1 {
                    return Err(ParseError::Error(ErrorKind::Leb128TooLarge));
                } else {
                    res |= ((byte & 0x7F) as $ty) << shift;
                }
                shift += 7;
            }
            Err(ParseError::Incomplete(NEED_ONE))
        }
    };
}

impl_leb!(leb128_u64, u64);
impl_leb!(leb128_u32, u32);
impl_leb!(leb128_i64, i64);
impl_leb!(leb128_i32, i32);

pub(crate) fn nonzero_leb128_u64(input: &[u8]) -> ParseResult<'_, NonZeroU64> {
    let (input, num) = leb128_u64(input)?;
    let result = NonZeroU64::new(num).ok_or(ParseError::Error(ErrorKind::UnexpectedZero))?;
    Ok((input, result))
}

/// Maximum LEB128-encoded size of an integer type
const fn leb128_size<T>() -> usize {
    let bits = size_of::<T>() * 8;
    (bits + 6) / 7 // equivalent to ceil(bits/7) w/o floats
}

const NEED_ONE: Needed = Needed::Size(unsafe { NonZeroUsize::new_unchecked(1) });

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryFrom;

    #[test]
    fn leb_128_unsigned() {
        let scenarios: Vec<(&'static [u8], ParseResult<'_, u64>)> = vec![
            (&[0b00000001_u8], Ok((&[], 1))),
            (&[0b10000001_u8], Err(ParseError::Incomplete(NEED_ONE))),
            (&[0b10000001, 0b00000001], Ok((&[], 129))),
            (&[0b00000001, 0b00000011], Ok((&[0b00000011], 1))),
            (
                &[129, 129, 129, 129, 129, 129, 129, 129, 129, 129, 129, 129],
                Err(ParseError::Error(ErrorKind::Leb128TooLarge)),
            ),
        ];
        for (index, (input, expected)) in scenarios.clone().into_iter().enumerate() {
            let result = leb128_u64(input);
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
            let result = leb128_u32(input);
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
