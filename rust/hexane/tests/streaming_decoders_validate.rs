//! `hexane::decoder` and `DeltaDecoder::new` decode bytes without
//! checking them. On malformed bytes they panic (`[0x05]`, a run of five
//! with no value), loop forever (`decoder::<bool>` on `[0x02, 0xff]`, a
//! count cut off by the end), or yield values outside the type
//! (`DeltaDecoder::<u32>` on a delta of -1 gives 4294967295). automerge
//! runs them over the bytes of documents it loads. `try_decoder` and
//! `DeltaDecoder::try_new` check the bytes first, as `Column::load` does,
//! and return an error instead.

use hexane::{Column, ColumnValueRef, DeltaColumn, DeltaDecoder, DeltaValue};

fn next(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

#[test]
fn malformed_bytes_are_an_error() {
    let malformed: &[&[u8]] = &[
        &[0x05],
        &[0x02, 0xff],
        &[0x7f],
        &[0x7e, 0x05],
        &[0x7f, 0x80],
    ];
    for bytes in malformed {
        assert!(hexane::try_decoder::<u64>(bytes).is_err(), "{bytes:x?}");
        assert!(
            hexane::try_decoder::<Option<u64>>(bytes).is_err(),
            "{bytes:x?}"
        );
        assert!(hexane::try_decoder::<i64>(bytes).is_err(), "{bytes:x?}");
        assert!(
            hexane::try_decoder::<Option<String>>(bytes).is_err(),
            "{bytes:x?}"
        );
        assert!(DeltaDecoder::<u32>::try_new(bytes).is_err(), "{bytes:x?}");
        assert!(
            DeltaDecoder::<Option<i64>>::try_new(bytes).is_err(),
            "{bytes:x?}"
        );
    }
    for bytes in [&[0x02, 0xff][..], &[0x02, 0x80], &[0x05, 0x01, 0x80]] {
        assert!(hexane::try_decoder::<bool>(bytes).is_err(), "{bytes:x?}");
    }
    assert!(hexane::leb128::try_decoder::<u64>(&[0x05]).is_err());
    #[cfg(feature = "bijou64")]
    assert!(hexane::bijou::try_decoder::<u64>(&[0x05]).is_err());
}

#[test]
fn deltas_out_of_the_domain_are_an_error() {
    // a delta of -1: the first value is -1
    let minus_one = [0x7f, 0x7f];
    assert!(DeltaDecoder::<u32>::try_new(&minus_one).is_err());
    assert!(DeltaDecoder::<u64>::try_new(&minus_one).is_err());
    assert_eq!(
        DeltaDecoder::<i64>::try_new(&minus_one)
            .unwrap()
            .collect::<Vec<_>>(),
        vec![-1]
    );
    // a run of two deltas of i64::MAX: the second value doesn't fit
    let mut two_max = vec![0x02];
    two_max.extend([0xff; 8]);
    two_max.push(0xff);
    two_max.push(0x00);
    assert_eq!(
        hexane::try_decoder::<i64>(&two_max)
            .unwrap()
            .collect::<Vec<_>>(),
        vec![i64::MAX; 2]
    );
    assert!(DeltaDecoder::<i64>::try_new(&two_max).is_err());
    assert!(DeltaDecoder::<Option<i64>>::try_new(&two_max).is_err());
    // big deltas whose running sums fit are fine
    let fits = hexane::DeltaEncoder::<i64>::encode(vec![i64::MAX, 0, i64::MAX, 5]);
    assert_eq!(
        DeltaDecoder::<i64>::try_new(&fits)
            .unwrap()
            .collect::<Vec<_>>(),
        vec![i64::MAX, 0, i64::MAX, 5]
    );
}

/// The checked decoder accepts exactly what `load` accepts, and then
/// decodes the same values.
fn agrees_with_load<T>(bytes: &[u8])
where
    T: ColumnValueRef + std::fmt::Debug,
    for<'a> T::Get<'a>: PartialEq<T::Get<'a>> + std::fmt::Debug,
{
    let loaded = Column::<T>::load(bytes);
    let checked = hexane::try_decoder::<T>(bytes);
    match (checked, &loaded) {
        (Ok(dec), Ok(col)) => {
            // (a changed byte can make a valid run of billions)
            assert_eq!(
                dec.take(1000).collect::<Vec<_>>(),
                col.iter().take(1000).collect::<Vec<_>>()
            );
        }
        (Err(_), Err(_)) => {}
        (c, l) => panic!(
            "{bytes:x?}: checked {:?}, load {:?}",
            c.err(),
            l.as_ref().err()
        ),
    };
}

fn delta_agrees_with_load<T: DeltaValue + std::fmt::Debug + PartialEq>(bytes: &[u8]) {
    let loaded = DeltaColumn::<T>::load(bytes);
    match (DeltaDecoder::<T>::try_new(bytes), &loaded) {
        (Ok(dec), Ok(col)) => assert_eq!(
            dec.take(1000).collect::<Vec<_>>(),
            col.iter().take(1000).collect::<Vec<_>>()
        ),
        (Err(_), Err(_)) => {}
        (c, l) => panic!(
            "{bytes:x?}: checked {:?}, load {:?}",
            c.err(),
            l.as_ref().err()
        ),
    };
}

/// Valid encodings with one byte changed, dropped or added, or the last
/// number cut off. The values' numbers are at most three bytes, and a
/// changed or added byte ends a number, so a number in a mutant is at most
/// six bytes: counts stay far from overflowing (which loads on main don't
/// all catch).
fn mutants(seed: &mut u64, valid: Vec<u8>, out: &mut Vec<Vec<u8>>) {
    out.push(valid.clone());
    for _ in 0..8 {
        let mut b = valid.clone();
        let i = next(seed) as usize % (b.len() + 1);
        match next(seed) % 4 {
            0 if i < b.len() => b[i] = next(seed) as u8 & 0x7f,
            1 if i < b.len() => {
                b.remove(i);
            }
            2 if !b.is_empty() => *b.last_mut().unwrap() |= 0x80,
            _ => b.insert(i, next(seed) as u8 & 0x7f),
        }
        out.push(b);
    }
}

#[test]
fn checked_decoders_agree_with_load() {
    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    for _ in 0..300 {
        let len = (next(&mut seed) % 12) as usize;
        let small: Vec<u64> = (0..len).map(|_| next(&mut seed) % 4).collect();
        let wide: Vec<u64> = (0..len)
            .map(|_| next(&mut seed) % (1 << (next(&mut seed) % 20)))
            .collect();
        let mut cases = vec![];
        for vals in [&small, &wide] {
            let col: Column<u64> = vals.iter().copied().collect();
            mutants(&mut seed, col.save(), &mut cases);
            let col: Column<Option<u64>> =
                vals.iter().map(|&v| (v % 3 != 0).then_some(v)).collect();
            mutants(&mut seed, col.save(), &mut cases);
            let col: Column<bool> = vals.iter().map(|&v| v % 2 == 0).collect();
            mutants(&mut seed, col.save(), &mut cases);
            let col: Column<String> = vals.iter().map(|v| v.to_string()).collect();
            mutants(&mut seed, col.save(), &mut cases);
            let col: DeltaColumn<i64> = vals.iter().map(|&v| v as i64).collect();
            mutants(&mut seed, col.save(), &mut cases);
            let col: DeltaColumn<Option<i64>> = vals
                .iter()
                .map(|&v| (v % 3 != 0).then_some(v as i64))
                .collect();
            mutants(&mut seed, col.save(), &mut cases);
        }
        for bytes in &cases {
            agrees_with_load::<u64>(bytes);
            agrees_with_load::<Option<u64>>(bytes);
            agrees_with_load::<i64>(bytes);
            agrees_with_load::<bool>(bytes);
            agrees_with_load::<Option<String>>(bytes);
            agrees_with_load::<String>(bytes);
            delta_agrees_with_load::<i64>(bytes);
            delta_agrees_with_load::<Option<i64>>(bytes);
            delta_agrees_with_load::<u32>(bytes);
            delta_agrees_with_load::<Option<u64>>(bytes);
        }
    }
}
