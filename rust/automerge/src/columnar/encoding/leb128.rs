/// The number of bytes required to encode `val` as a LEB128 integer
pub(crate) fn lebsize(val: i64) -> u64 {
    let numbits = numbits_i64(val);
    (numbits as f64 / 7.0).floor() as u64 + 1
}

/// The number of bytes required to encode `val` as a uLEB128 integer
pub(crate) fn ulebsize(val: u64) -> u64 {
    if val <= 1 {
        return 1;
    }
    let numbits = numbits_u64(val);
    let mut numblocks = (numbits as f64 / 7.0).floor() as u64;
    if numbits % 7 != 0 {
        numblocks += 1;
    }
    numblocks
}

fn numbits_i64(val: i64) -> u64 {
    // Is this right? This feels like it's not right
    (std::mem::size_of::<i64>() as u32 * 8 - val.abs().leading_zeros()) as u64
}

fn numbits_u64(val: u64) -> u64 {
    (std::mem::size_of::<u64>() as u32 * 8 - val.leading_zeros()) as u64
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
        let scenarios = vec![0, 1, 127, 128, 129, 169];
        for val in scenarios {
            let mut out = Vec::new();
            leb128::write::unsigned(&mut out, val).unwrap();
            let expected = out.len() as u64;
            assert_eq!(ulebsize(val), expected, "value: {}", val)
        }
    }

    #[test]
    fn lebsize_examples() {
        let scenarios = vec![0, 1, -1, 127, 128, -127, -128, -2097152, 169];
        for val in scenarios {
            let mut out = Vec::new();
            leb128::write::signed(&mut out, val).unwrap();
            let expected = out.len() as u64;
            assert_eq!(lebsize(val), expected, "value: {}", val)
        }
    }
}
