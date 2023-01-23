/// The number of bytes required to encode `val` as a LEB128 integer
pub(crate) fn lebsize(mut val: i64) -> u64 {
    if val < 0 {
        val = !val
    }
    // 1 extra for the sign bit
    leb_bytes(1 + 64 - val.leading_zeros() as u64)
}

/// The number of bytes required to encode `val` as a uLEB128 integer
pub(crate) fn ulebsize(val: u64) -> u64 {
    if val == 0 {
        return 1;
    }
    leb_bytes(64 - val.leading_zeros() as u64)
}

fn leb_bytes(bits: u64) -> u64 {
    (bits + 6) / 7
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_ulebsize(val in 0..u64::MAX) {
            let mut out = Vec::new();
            leb128::write::unsigned(&mut out, val).unwrap();
            let expected = out.len() as u64;
            assert_eq!(expected, ulebsize(val))
        }

        #[test]
        fn test_lebsize(val in i64::MIN..i64::MAX) {
            let mut out = Vec::new();
            leb128::write::signed(&mut out, val).unwrap();
            let expected = out.len() as u64;
            assert_eq!(expected, lebsize(val))
        }
    }

    #[test]
    fn ulebsize_examples() {
        let scenarios = vec![0, 1, 127, 128, 129, 169, u64::MAX];
        for val in scenarios {
            let mut out = Vec::new();
            leb128::write::unsigned(&mut out, val).unwrap();
            let expected = out.len() as u64;
            assert_eq!(ulebsize(val), expected, "value: {}", val)
        }
    }

    #[test]
    fn lebsize_examples() {
        let scenarios = vec![
            0,
            1,
            -1,
            63,
            64,
            -64,
            -65,
            127,
            128,
            -127,
            -128,
            -2097152,
            169,
            i64::MIN,
            i64::MAX,
        ];
        for val in scenarios {
            let mut out = Vec::new();
            leb128::write::signed(&mut out, val).unwrap();
            let expected = out.len() as u64;
            assert_eq!(lebsize(val), expected, "value: {}", val)
        }
    }
}
