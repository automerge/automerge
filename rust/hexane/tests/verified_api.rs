//! The verified public API (`hexane::verified`) and `Column` are two
//! doors to one format: for the same values they save the same bytes, each
//! loads what the other saved, and they refuse the same bytes.
//!
//! Verus proves the verified API against its own specifications. These
//! tests are what ties those specifications to `Column`'s format, which
//! no proof does.

use hexane::verified::VerifiedColumn;
use hexane::Column;

/// A small deterministic generator (xorshift64*).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    /// A value: often a repeat of `last`, sometimes a small number,
    /// sometimes one of any size.
    fn value(&mut self, last: u64) -> u64 {
        match self.below(8) {
            0..=3 => last,
            4 | 5 => self.next() % 4,
            6 => self.next() >> self.below(64),
            _ => [0, 1, 127, 128, u64::MAX, u64::MAX - 1, 1 << 63][self.below(7)],
        }
    }

    fn values(&mut self, max: usize) -> Vec<u64> {
        let n = self.below(max + 1);
        let mut last = self.next() % 4;
        (0..n)
            .map(|_| {
                last = self.value(last);
                last
            })
            .collect()
    }
}

/// Both columns hold `model`, read the same way through every reader, and
/// save to the same bytes.
fn check(v: &VerifiedColumn<u64>, c: &Column<u64>, model: &[u64]) {
    assert_eq!(v.len(), model.len());
    assert_eq!(v.is_empty(), model.is_empty());
    assert_eq!(v.to_vec(), model);
    assert_eq!(c.to_vec(), model);
    let mut it = v.iter();
    for &m in model {
        assert_eq!(it.next(), Some(m));
    }
    assert_eq!(it.next(), None);
    assert_eq!(it.next(), None);
    assert_eq!(v.get(model.len()), None);
    assert_eq!(v.get(usize::MAX), None);
    assert_eq!(v.save(), c.save());
}

/// Each API loads the bytes the other saved, as the same values.
fn check_loads(v: &VerifiedColumn<u64>, c: &Column<u64>, model: &[u64]) {
    let bytes = v.save();
    assert_eq!(bytes, c.save());
    let v2 = VerifiedColumn::<u64>::load(&c.save()).unwrap();
    let c2 = Column::<u64>::load(&bytes).unwrap();
    assert_eq!(v2.to_vec(), model);
    assert_eq!(c2.to_vec(), model);
    assert_eq!(v2.save(), bytes);
    assert_eq!(c2.save(), bytes);
}

#[test]
fn the_shapes_of_a_column() {
    let shapes: Vec<Vec<u64>> = vec![
        vec![],
        vec![0],
        vec![u64::MAX],
        vec![7; 1000],
        (0..1000).collect(),
        (0..1000).map(|i| i / 7).collect(),
        (0..1000).map(|i| if i % 5 < 3 { 9 } else { i }).collect(),
        (0..5000u64)
            .map(|i| i.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> (i % 64))
            .collect(),
        vec![0, 0, 1, 2, 2, 2, 3, u64::MAX, u64::MAX, 0],
    ];
    for model in shapes {
        let mut v = VerifiedColumn::<u64>::new();
        v.splice(0, 0, &model);
        let mut c = Column::<u64>::new();
        c.splice(0, 0, model.iter().copied());
        check(&v, &c, &model);
        check_loads(&v, &c, &model);
    }
}

#[test]
fn an_empty_column() {
    let v = VerifiedColumn::<u64>::new();
    let d = VerifiedColumn::<u64>::default();
    assert!(v.is_empty() && d.is_empty());
    assert_eq!(v.save(), Column::<u64>::new().save());
    assert_eq!(v.save(), Vec::<u8>::new());
    assert!(VerifiedColumn::<u64>::load(&[]).unwrap().is_empty());
}

#[test]
fn edits_agree() {
    for seed in 1..=40u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let budget = [2, 3, 4, 8, 64][rng.below(5)];
        let mut v = VerifiedColumn::<u64>::with_max_segments(budget);
        let mut c = Column::<u64>::with_max_segments(budget);
        let mut model: Vec<u64> = vec![];
        for step in 0..120 {
            match rng.below(10) {
                0..=3 => {
                    let at = rng.below(model.len() + 1);
                    let del = rng.below((model.len() - at).min(12) + 1);
                    let ins = rng.values(40);
                    v.splice(at, del, &ins);
                    c.splice(at, del, ins.iter().copied());
                    model.splice(at..at + del, ins);
                }
                4 | 5 => {
                    let at = rng.below(model.len() + 1);
                    let x = rng.value(at.checked_sub(1).map(|i| model[i]).unwrap_or(0));
                    v.insert(at, x);
                    c.insert(at, x);
                    model.insert(at, x);
                }
                6 => {
                    let x = rng.value(model.last().copied().unwrap_or(0));
                    v.push(x);
                    c.push(x);
                    model.push(x);
                }
                7 | 8 => {
                    if !model.is_empty() {
                        let at = rng.below(model.len());
                        v.remove(at);
                        c.remove(at);
                        model.remove(at);
                    }
                }
                _ => {
                    // sometimes past the end, which changes nothing
                    let len = rng.below(model.len() + 3);
                    v.truncate(len);
                    c.truncate(len);
                    model.truncate(len);
                }
            }
            check(&v, &c, &model);
            for _ in 0..4 {
                if !model.is_empty() {
                    let i = rng.below(model.len());
                    assert_eq!(v.get(i), Some(model[i]));
                    assert_eq!(v.get(i), c.get(i));
                }
                let start = rng.below(model.len() + 1);
                let end = start + rng.below(model.len() - start + 1);
                let mut it = v.iter_range(start, end);
                for &m in &model[start..end] {
                    assert_eq!(it.next(), Some(m));
                }
                assert_eq!(it.next(), None);
            }
            if step % 10 == 9 {
                check_loads(&v, &c, &model);
            }
        }
        check_loads(&v, &c, &model);
    }
}

/// The two loaders take the same bytes, as the same values.
fn check_same_load(bytes: &[u8]) -> bool {
    let v = VerifiedColumn::<u64>::load(bytes);
    let c = Column::<u64>::load(bytes);
    assert_eq!(v.is_ok(), c.is_ok(), "the loaders disagree on {bytes:?}");
    match (v, c) {
        (Ok(v), Ok(c)) => {
            assert_eq!(v.len(), c.len());
            // a few bytes can be a run of billions: read those in places
            if v.len() <= 100_000 {
                assert_eq!(v.to_vec(), c.to_vec());
            } else {
                for i in [0, 1, v.len() / 2, v.len() - 1] {
                    assert_eq!(v.get(i), c.get(i));
                }
            }
            // what loads is canonical: it is what its values save to
            assert_eq!(v.save(), bytes);
            assert_eq!(c.save(), bytes);
            true
        }
        _ => false,
    }
}

#[test]
fn loads_refuse_the_same_bytes() {
    // a run of zero values; a run of one (a literal's place); two runs of
    // the same value; an overlong number; a truncated one; a null run in
    // a column with no nulls; two literal runs in a row
    let bad: [&[u8]; 8] = [
        &[0x02, 0x80, 0x00],
        &[0x01, 0x05],
        &[0x02, 0x05, 0x02, 0x05],
        &[0x82, 0x00, 0x05],
        &[0x02, 0x85],
        &[0x00, 0x03],
        &[0x7f, 0x01, 0x7f, 0x02],
        &[0x02],
    ];
    for b in bad {
        assert!(!check_same_load(b), "loaded {b:?}");
    }
    // a column with nulls is no column of `u64`
    let mut nullable = Column::<Option<u64>>::new();
    nullable.splice(0, 0, [Some(1), None, None, Some(1)]);
    assert!(!check_same_load(&nullable.save()));

    let mut rng = Rng(0x5eed);
    let (mut took, mut refused) = (0, 0);
    // bytes at random, mostly not a column
    for _ in 0..20_000 {
        let n = rng.below(12);
        let bytes: Vec<u8> = (0..n)
            .map(|_| match rng.below(4) {
                0 => rng.next() as u8,
                1 => [0x7f, 0x7e, 0x00, 0x80][rng.below(4)],
                _ => (rng.next() % 6) as u8,
            })
            .collect();
        if check_same_load(&bytes) {
            took += 1;
        } else {
            refused += 1;
        }
    }
    // saved columns with a byte changed, dropped or added
    for _ in 0..4_000 {
        let model = rng.values(30);
        let mut v = VerifiedColumn::<u64>::new();
        v.splice(0, 0, &model);
        let mut bytes = v.save();
        assert!(check_same_load(&bytes));
        if bytes.is_empty() {
            continue;
        }
        let at = rng.below(bytes.len());
        match rng.below(3) {
            0 => bytes[at] ^= 1 << rng.below(8),
            1 => {
                bytes.remove(at);
            }
            _ => bytes.insert(at, rng.next() as u8),
        }
        if check_same_load(&bytes) {
            took += 1;
        } else {
            refused += 1;
        }
    }
    // both kinds were seen
    assert!(
        took > 100 && refused > 100,
        "{took} taken, {refused} refused"
    );
}
