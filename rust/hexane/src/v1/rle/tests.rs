use super::state::{RleCow, RleState};
use super::*;
use crate::v1::leb::{encode_signed, encode_unsigned};
use crate::v1::{AsColumnRef, Column, RleValue};

pub(crate) fn rle_encode_state<T: RleValue>(
    values: impl Iterator<Item = T::Get<'static>>,
    buf: &mut Vec<u8>,
) -> (usize, usize)
where
    T::Get<'static>: AsColumnRef<T>,
{
    let mut state = RleState::<'static, T, T::Get<'static>>::Empty;
    let mut segments = 0;
    let mut items = 0;
    for v in values {
        items += 1;
        segments += state.append(buf, RleCow::Ref(v)).segments;
    }
    let f = state.flush(buf);
    segments += f.segments;
    (items, segments)
}

mod load_verify_tests {
    use super::*;

    #[test]
    fn load_verify_roundtrip() {
        let mut buf = Vec::new();
        rle_encode_state::<u64>((0..1000u64).map(|i| i % 7), &mut buf);
        let slabs = load::rle_load_and_verify::<u64>(&buf, 16, None).unwrap();
        let total: usize = slabs.iter().map(|s| s.len).sum();
        assert_eq!(total, 1000);
        let vals: Vec<u64> = slabs
            .iter()
            .flat_map(|s| RleDecoder::<u64>::new(&s.data))
            .collect();
        let expected: Vec<u64> = (0..1000u64).map(|i| i % 7).collect();
        assert_eq!(vals, expected);
        for (i, s) in slabs.iter().enumerate() {
            assert!(
                rle_validate_encoding::<u64>(&s.data).is_ok(),
                "slab {i} invalid"
            );
            assert!(s.segments <= 16, "slab {i} exceeds max_segments");
        }
    }

    #[test]
    fn load_verify_nullable() {
        let mut buf = Vec::new();
        rle_encode_state::<Option<u64>>(
            [Some(1u64), None, None, Some(2), Some(2), None].into_iter(),
            &mut buf,
        );
        let slabs = load::rle_load_and_verify::<Option<u64>>(&buf, 16, None).unwrap();
        let vals: Vec<Option<u64>> = slabs
            .iter()
            .flat_map(|s| RleDecoder::<Option<u64>>::new(&s.data))
            .collect();
        assert_eq!(vals, vec![Some(1), None, None, Some(2), Some(2), None]);
    }

    #[test]
    fn load_verify_rejects_null_in_non_nullable() {
        // Encode a null run (0, count=1) into raw bytes.
        let mut buf = Vec::new();
        buf.extend(encode_signed(0));
        buf.extend(encode_unsigned(1));

        // Non-nullable u64 should reject.
        let result = load::rle_load_and_verify::<u64>(
            &buf,
            16,
            Some(|_v: u64| {
                // This shouldn't even be called — the null check should fire first.
                Some("unexpected value validation call".to_string())
            }),
        );
        assert!(result.is_err());
    }

    /// Test that splice across the LEB128 signed boundary (64 literal items)
    /// triggers a rewrite_lit_header that changes the header byte count.
    #[test]
    fn splice_lit_crosses_leb128_boundary() {
        let initial: Vec<u64> = (0..60).collect();
        let mut col = Column::<u64>::from_values_with_max_segments(initial, 256);
        assert_eq!(col.slab_count(), 1);
        assert_eq!(col.len(), 60);

        let new_vals: Vec<u64> = (1000..1010).collect();
        col.splice(30, 0, new_vals);

        col.validate_encoding().unwrap();
        assert_eq!(col.len(), 70);
        let vals: Vec<u64> = col.iter().collect();
        let mut expected: Vec<u64> = (0..30).collect();
        expected.extend(1000..1010);
        expected.extend(30..60);
        assert_eq!(vals, expected);
    }
}
