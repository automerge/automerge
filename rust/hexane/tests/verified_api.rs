//! The verified public API (`hexane::verified`) and `Column` are two
//! doors to one format: for the same values they save the same bytes, each
//! loads what the other saved, and they refuse the same bytes. That holds
//! for every value type of the verified API (numbers, `bool`, byte strings,
//! each with nulls where it has them), and for a type of a client's own.
//!
//! Verus proves the verified API against its own specifications. These
//! tests are what ties those specifications to `Column`'s format, which
//! no proof does.

use hexane::verified::{refused, Get, VNewtype, VValue, VerifiedColumn};
use hexane::{Codec, Column, ColumnValue, ColumnValueRef, PackError, RleEncoding, RleValue};
use std::fmt::Debug;
use std::num::NonZeroU32;

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

    /// A value: often a repeat of `last`, else a new one.
    fn value<T: Kind>(&mut self, last: &T) -> T {
        match self.below(8) {
            0..=3 => last.clone(),
            _ => T::fresh(self),
        }
    }

    fn values<T: Kind>(&mut self, max: usize) -> Vec<T> {
        let n = self.below(max + 1);
        let mut last = T::fresh(self);
        (0..n)
            .map(|_| {
                last = self.value(&last);
                last.clone()
            })
            .collect()
    }

    /// A number: small, of any size, or one at an edge of a type.
    fn number(&mut self) -> u64 {
        const EDGES: [u64; 16] = [
            0,
            1,
            63,
            64,
            127,
            128,
            i32::MAX as u64,
            i32::MAX as u64 + 1,
            u32::MAX as u64,
            u32::MAX as u64 + 1,
            i64::MAX as u64,
            1 << 63,
            u64::MAX - 1,
            u64::MAX,
            i32::MIN as u64,
            i32::MIN as u64 - 1,
        ];
        match self.below(4) {
            0 | 1 => self.next() % 4,
            2 => self.next() >> self.below(64),
            _ => EDGES[self.below(EDGES.len())],
        }
    }
}

/// A value type of both APIs. A test holds its values owned. The verified
/// API takes and gives a value in the form `lend` makes: the value itself,
/// or for a byte string a borrow of it.
trait Kind: VValue + ColumnValueRef + Clone + PartialEq + Debug {
    /// A value that is not likely the one before it.
    fn fresh(rng: &mut Rng) -> Self;

    fn lend(&self) -> Get<'_, Self>;

    fn keep(v: Get<'_, Self>) -> Self;
}

/// A type that is its own form, and the same type with nulls.
macro_rules! plain_kind {
    ($t:ty, $fresh:expr) => {
        impl Kind for $t {
            fn fresh(rng: &mut Rng) -> $t {
                let fresh: fn(&mut Rng) -> $t = $fresh;
                fresh(rng)
            }

            fn lend(&self) -> $t {
                *self
            }

            fn keep(v: $t) -> $t {
                v
            }
        }

        impl Kind for Option<$t> {
            fn fresh(rng: &mut Rng) -> Option<$t> {
                match rng.below(3) {
                    0 => None,
                    _ => Some(<$t>::fresh(rng)),
                }
            }

            fn lend(&self) -> Option<$t> {
                *self
            }

            fn keep(v: Option<$t>) -> Option<$t> {
                v
            }
        }
    };
}

plain_kind!(u64, |rng| rng.number());
plain_kind!(i64, |rng| rng.number() as i64);
plain_kind!(u32, |rng| rng.number() as u32);
plain_kind!(i32, |rng| rng.number() as i32);
plain_kind!(usize, |rng| rng.number() as usize);
plain_kind!(NonZeroU32, |rng| {
    NonZeroU32::new(rng.number() as u32).unwrap_or(NonZeroU32::MAX)
});

impl Kind for bool {
    fn fresh(rng: &mut Rng) -> bool {
        rng.below(2) == 1
    }

    fn lend(&self) -> bool {
        *self
    }

    fn keep(v: bool) -> bool {
        v
    }
}

/// Byte strings: mostly short, the empty one among them; some with a
/// length of two bytes on the wire, a few with one of three.
impl Kind for Vec<u8> {
    fn fresh(rng: &mut Rng) -> Vec<u8> {
        let n = match rng.below(64) {
            0 => 16_384 + rng.below(100),
            1..=4 => 100 + rng.below(100),
            _ => rng.below(4),
        };
        (0..n).map(|_| rng.next() as u8).collect()
    }

    fn lend(&self) -> &[u8] {
        self
    }

    fn keep(v: &[u8]) -> Vec<u8> {
        v.to_vec()
    }
}

impl Kind for Option<Vec<u8>> {
    fn fresh(rng: &mut Rng) -> Option<Vec<u8>> {
        match rng.below(3) {
            0 => None,
            _ => Some(Vec::fresh(rng)),
        }
    }

    fn lend(&self) -> Option<&[u8]> {
        self.as_deref()
    }

    fn keep(v: Option<&[u8]>) -> Option<Vec<u8>> {
        v.map(<[u8]>::to_vec)
    }
}

/// A value type of a client's own, as a client compiled without Verus
/// writes it: one of four actions, each stored as a number. Any other
/// number is no action.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Action {
    Make,
    Set,
    Delete,
    Increment,
}

impl VNewtype for Action {
    type Base = u64;

    fn to_base(self) -> u64 {
        match self {
            Action::Make => 0,
            Action::Set => 1,
            Action::Delete => 3,
            Action::Increment => 4,
        }
    }

    fn from_base(b: u64) -> Result<Action, PackError> {
        match b {
            0 => Ok(Action::Make),
            1 => Ok(Action::Set),
            3 => Ok(Action::Delete),
            4 => Ok(Action::Increment),
            _ => Err(refused("not an action")),
        }
    }
}

// the same type for `Column`
impl ColumnValue for Action {
    type Encoding<C: Codec> = RleEncoding<Action, C>;
}

impl RleValue for Action {
    fn try_unpack<C: Codec>(data: &[u8]) -> Result<(usize, Action), PackError> {
        let (n, b) = C::try_read_unsigned(data)?;
        Ok((n, Action::from_base(b)?))
    }

    fn pack<C: Codec>(value: Action, out: &mut Vec<u8>) -> bool {
        out.extend(C::encode_unsigned(value.to_base()));
        true
    }
}

plain_kind!(Action, |rng| {
    [Action::Make, Action::Set, Action::Delete, Action::Increment][rng.below(4)]
});

/// What a `Column` holds.
fn old_values<T: Kind>(c: &Column<T>) -> Vec<T> {
    c.to_vec().into_iter().map(old).collect()
}

/// A value a `Column` gave.
fn old<T: Kind>(v: <T as ColumnValueRef>::Get<'_>) -> T {
    <T as ColumnValueRef>::to_owned(v)
}

/// Values in the form the verified API takes.
fn lent<T: Kind>(vs: &[T]) -> Vec<Get<'_, T>> {
    vs.iter().map(T::lend).collect()
}

/// What a `VerifiedColumn` holds.
fn new_values<T: Kind>(v: &VerifiedColumn<T>) -> Vec<T> {
    v.to_vec().into_iter().map(T::keep).collect()
}

/// Both columns hold `model`, read the same way through every reader, and
/// save to the same bytes.
fn check<T: Kind>(v: &VerifiedColumn<T>, c: &Column<T>, model: &[T]) {
    assert_eq!(v.len(), model.len());
    assert_eq!(v.is_empty(), model.is_empty());
    assert_eq!(new_values(v), model);
    assert_eq!(old_values(c), model);
    let mut it = v.iter();
    for m in model {
        assert_eq!(it.next().map(T::keep).as_ref(), Some(m));
    }
    assert!(it.next().is_none());
    assert!(it.next().is_none());
    assert!(v.get(model.len()).is_none());
    assert!(v.get(usize::MAX).is_none());
    assert_eq!(v.save(), c.save());
    // the same slabs, and so the same of what is printed: but for a cleared
    // column, which has no slab where `Column` keeps an empty one
    if model.is_empty() {
        assert!(v.slab_count() <= c.slab_count() && c.slab_count() <= 1);
    } else {
        assert_eq!(v.slab_count(), c.slab_count());
        assert_eq!(format!("{v:?}"), format!("Verified{c:?}"));
    }
    // saved after what a buffer holds: the same bytes, in the same place
    let (mut a, mut b) = (vec![9u8; 3], vec![9u8; 3]);
    let range = v.save_to(&mut a);
    assert_eq!(range, c.save_to(&mut b));
    assert_eq!(a, b);
    assert_eq!(a[..3], [9, 9, 9]);
    assert_eq!(a[range], v.save()[..]);
    for x in model.first().into_iter().chain(model.last()) {
        check_only(v, c, model, x);
    }
}

/// Whether the column holds `x` alone, and what is saved unless it does.
fn check_only<T: Kind>(v: &VerifiedColumn<T>, c: &Column<T>, model: &[T], x: &T) {
    let only = model.iter().all(|m| m == x);
    assert_eq!(v.is_only(x.lend()), only);
    assert_eq!(c.is_only(x.as_column_ref()), only);
    let (mut a, mut b) = (vec![9u8; 3], vec![9u8; 3]);
    let range = v.save_to_unless(&mut a, x.lend());
    assert_eq!(range, c.save_to_unless(&mut b, x.as_column_ref()));
    assert_eq!(a, b);
    if only {
        assert_eq!((a, range), (vec![9u8; 3], 3..3));
    } else {
        assert_eq!(a[range], v.save()[..]);
    }
}

/// Each API loads the bytes the other saved, as the same values.
fn check_loads<T: Kind>(v: &VerifiedColumn<T>, c: &Column<T>, model: &[T]) {
    let bytes = v.save();
    assert_eq!(bytes, c.save());
    let v2 = VerifiedColumn::<T>::load(&c.save()).unwrap();
    let c2 = Column::<T>::load(&bytes).unwrap();
    assert_eq!(new_values(&v2), model);
    assert_eq!(old_values(&c2), model);
    assert_eq!(v2.save(), bytes);
    assert_eq!(c2.save(), bytes);
}

/// A column of each API with the values `model`.
fn both<T: Kind>(model: &[T]) -> (VerifiedColumn<T>, Column<T>) {
    let mut v = VerifiedColumn::<T>::new();
    v.splice(0, 0, &lent(model));
    let mut c = Column::<T>::new();
    c.splice(0, 0, model.iter().cloned());
    (v, c)
}

/// The same checks for each value type.
macro_rules! for_each_kind {
    ($f:ident) => {
        $f::<u64>();
        $f::<i64>();
        $f::<u32>();
        $f::<i32>();
        $f::<usize>();
        $f::<NonZeroU32>();
        $f::<bool>();
        $f::<Action>();
        $f::<Option<u64>>();
        $f::<Option<i64>>();
        $f::<Option<u32>>();
        $f::<Option<i32>>();
        $f::<Option<usize>>();
        $f::<Option<NonZeroU32>>();
        $f::<Option<Action>>();
        $f::<Vec<u8>>();
        $f::<Option<Vec<u8>>>();
    };
}

fn shapes<T: Kind>() {
    let mut rng = Rng(0x5a17);
    let (a, b, c) = (T::fresh(&mut rng), T::fresh(&mut rng), T::fresh(&mut rng));
    let pick = |i: usize| [&a, &b, &c][i].clone();
    let mut shapes: Vec<Vec<T>> = vec![
        vec![],
        vec![pick(0)],
        vec![pick(0); 1000],
        (0..1000).map(|i| pick(i / 7 % 2)).collect(),
        (0..1000).map(|i| pick(i % 3)).collect(),
        (0..5000).map(|_| T::fresh(&mut rng)).collect(),
        [0, 0, 1, 2, 2, 2, 0, 1, 1, 0].map(pick).to_vec(),
    ];
    for _ in 0..20 {
        shapes.push(rng.values(300));
    }
    for model in shapes {
        let (v, c) = both(&model);
        check(&v, &c, &model);
        check_loads(&v, &c, &model);
    }
}

#[test]
fn the_shapes_of_a_column() {
    for_each_kind!(shapes);
}

#[test]
fn numbers_of_every_size() {
    let model: Vec<u64> = (0..5000u64)
        .map(|i| i.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> (i % 64))
        .chain([0, 0, 1, 2, 2, 2, 3, u64::MAX, u64::MAX, 0])
        .chain(0..1000)
        .collect();
    let (v, c) = both(&model);
    check(&v, &c, &model);
    check_loads(&v, &c, &model);

    let signed: Vec<i64> = model.iter().map(|&x| x as i64).collect();
    let (v, c) = both(&signed);
    check(&v, &c, &signed);
    check_loads(&v, &c, &signed);
}

fn empty<T: Kind>() {
    let v = VerifiedColumn::<T>::new();
    let d = VerifiedColumn::<T>::default();
    assert!(v.is_empty() && d.is_empty());
    assert_eq!(v.save(), Column::<T>::new().save());
    assert_eq!(v.save(), Vec::<u8>::new());
    assert!(VerifiedColumn::<T>::load(&[]).unwrap().is_empty());
}

#[test]
fn an_empty_column() {
    for_each_kind!(empty);
}

/// The format, in bytes: so that the two APIs agreeing is not both of
/// them drifting.
#[test]
fn the_bytes_of_a_column() {
    fn bytes<T: Kind>(model: &[T]) -> Vec<u8> {
        let (v, c) = both(model);
        check(&v, &c, model);
        v.save()
    }
    // a run of three; a literal run of two
    assert_eq!(bytes::<u64>(&[5, 5, 5, 1, 2]), [3, 5, 0x7e, 1, 2]);
    // signed numbers: -1 is 0x7f, 64 is two bytes
    assert_eq!(bytes::<i64>(&[-1, -1, 64]), [2, 0x7f, 0x7f, 0xc0, 0x00]);
    assert_eq!(bytes::<i32>(&[-1, -1, 64]), [2, 0x7f, 0x7f, 0xc0, 0x00]);
    // unsigned ones: 64 is one byte
    assert_eq!(bytes::<u32>(&[64, 64]), [2, 64]);
    assert_eq!(bytes::<usize>(&[64, 64]), [2, 64]);
    // nulls are a null run: a zero, then how many
    assert_eq!(
        bytes::<Option<u64>>(&[Some(1), None, None, Some(1)]),
        [0x7f, 1, 0, 2, 0x7f, 1]
    );
    assert_eq!(bytes::<Option<i64>>(&[None, None, None]), [0, 3]);
    // a client's type is its numbers
    assert_eq!(
        bytes::<Action>(&[Action::Delete, Action::Delete, Action::Increment]),
        bytes::<u64>(&[3, 3, 4])
    );
    assert_eq!(
        bytes::<Option<Action>>(&[Some(Action::Delete), None]),
        bytes::<Option<u64>>(&[Some(3), None])
    );
    // `bool`: the lengths of its runs, the first of them of `false`
    assert_eq!(bytes::<bool>(&[false, false, true]), [2, 1]);
    assert_eq!(bytes::<bool>(&[true, false]), [0, 1, 1]);
    // a byte string is its length, then its bytes; the empty one is a zero
    let s = |b: &[u8]| b.to_vec();
    assert_eq!(
        bytes::<Vec<u8>>(&[s(b"ab"), s(b"ab"), s(b"")]),
        [2, 2, b'a', b'b', 0x7f, 0]
    );
    assert_eq!(
        bytes::<Vec<u8>>(&[s(b"a"), s(b"b"), s(b"ab")]),
        [0x7d, 1, b'a', 1, b'b', 2, b'a', b'b']
    );
    // a length past 127 is two bytes
    let long = vec![7u8; 200];
    let saved = bytes::<Vec<u8>>(&[long.clone(), long.clone()]);
    assert_eq!(saved[..3], [2, 0xc8, 0x01]);
    assert_eq!(saved[3..], long[..]);
    // a null string is a null run, which the empty string is not
    assert_eq!(
        bytes::<Option<Vec<u8>>>(&[Some(s(b"x")), None, None, Some(s(b""))]),
        [0x7f, 1, b'x', 0, 2, 0x7f, 0]
    );
}

/// What a column of byte strings gives is a borrow of the column: it
/// lives as long as the column is not changed, across other reads.
#[test]
fn byte_strings_are_lent() {
    let mut v = VerifiedColumn::<Vec<u8>>::new();
    v.splice(0, 0, &[b"one", b"two", b"two"]);
    v.push(b"three");
    let first: &[u8] = v.get(0).unwrap();
    let all: Vec<&[u8]> = v.to_vec();
    let mut it = v.iter_range(1, 3);
    let (x, y) = (it.next().unwrap(), it.next().unwrap());
    assert!(it.next().is_none());
    assert_eq!(first, b"one");
    assert_eq!(all, [&b"one"[..], b"two", b"two", b"three"]);
    assert_eq!((x, y), (&b"two"[..], &b"two"[..]));

    let mut o = VerifiedColumn::<Option<Vec<u8>>>::new();
    o.splice(0, 0, &[Some(b"one"), None]);
    o.insert(1, Some(&[]));
    o.push(None);
    let all: Vec<Option<&[u8]>> = o.to_vec();
    assert_eq!(all, [Some(&b"one"[..]), Some(&[]), None, None]);
    assert_eq!(o.get(3), Some(None));
    assert_eq!(o.get(4), None);
}

fn edits<T: Kind>() {
    for seed in 1..=12u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let budget = [2, 3, 4, 8, 64][rng.below(5)];
        let mut v = VerifiedColumn::<T>::with_max_segments(budget);
        let mut c = Column::<T>::with_max_segments(budget);
        let mut model: Vec<T> = vec![];
        for step in 0..120 {
            match rng.below(12) {
                0..=3 => {
                    let at = rng.below(model.len() + 1);
                    let del = rng.below((model.len() - at).min(12) + 1);
                    let ins: Vec<T> = rng.values(40);
                    v.splice(at, del, &lent(&ins));
                    c.splice(at, del, ins.iter().cloned());
                    model.splice(at..at + del, ins);
                }
                4 | 5 => {
                    let at = rng.below(model.len() + 1);
                    let x = match at.checked_sub(1) {
                        Some(i) => rng.value(&model[i]),
                        None => T::fresh(&mut rng),
                    };
                    v.insert(at, x.lend());
                    c.insert(at, x.clone());
                    model.insert(at, x);
                }
                6 => {
                    let x = match model.last() {
                        Some(last) => rng.value(last),
                        None => T::fresh(&mut rng),
                    };
                    v.push(x.lend());
                    c.push(x.clone());
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
                9 => {
                    // sometimes past the end, which changes nothing
                    let len = rng.below(model.len() + 3);
                    v.truncate(len);
                    c.truncate(len);
                    model.truncate(len);
                }
                10 => {
                    // sometimes none, sometimes all of them
                    let at = rng.below(model.len() + 1);
                    let n = match rng.below(8) {
                        0 => 0,
                        1 => model.len() - at,
                        _ => rng.below((model.len() - at).min(40) + 1),
                    };
                    v.remove_n(at, n);
                    c.remove_n(at, n);
                    model.drain(at..at + n);
                }
                _ => {
                    if rng.below(4) == 0 {
                        v.clear();
                        c.clear();
                        model.clear();
                    }
                }
            }
            check(&v, &c, &model);
            let x = T::fresh(&mut rng);
            check_only(&v, &c, &model, &x);
            for _ in 0..4 {
                if !model.is_empty() {
                    let i = rng.below(model.len());
                    assert_eq!(v.get(i).map(T::keep).as_ref(), Some(&model[i]));
                    assert_eq!(v.get(i).map(T::keep), c.get(i).map(old));
                }
                let start = rng.below(model.len() + 1);
                let end = start + rng.below(model.len() - start + 1);
                let mut it = v.iter_range(start, end);
                for m in &model[start..end] {
                    assert_eq!(it.next().map(T::keep).as_ref(), Some(m));
                }
                assert!(it.next().is_none());
            }
            if step % 10 == 9 {
                check_loads(&v, &c, &model);
            }
        }
        check_loads(&v, &c, &model);
    }
}

#[test]
fn edits_agree() {
    for_each_kind!(edits);
}

/// Columns of one value, in one slab and in many, and the same with one
/// other value at each place: `is_only` and `save_to_unless` say the same
/// of them in both APIs, for the value, for the other and for a third
/// (where the type has a third).
fn one_value<T: Kind>() {
    let mut rng = Rng(0x0e1e);
    let a = T::fresh(&mut rng);
    // another value, and a third where the type has one (`bool` has not)
    let mut others: Vec<T> = vec![];
    for _ in 0..200 {
        let x = T::fresh(&mut rng);
        if x != a && !others.contains(&x) && others.len() < 2 {
            others.push(x);
        }
    }
    let (b, third) = (others[0].clone(), others[others.len() - 1].clone());
    for budget in [2, 3, 64] {
        for n in [0, 1, 2, 3, 7, 40, 700] {
            let model = vec![a.clone(); n];
            // one at a time and at both ends, so that a small budget
            // makes slabs
            let build = || {
                let mut v = VerifiedColumn::<T>::with_max_segments(budget);
                let mut c = Column::<T>::with_max_segments(budget);
                for (i, x) in model.iter().enumerate() {
                    let at = if i % 2 == 0 { 0 } else { v.len() };
                    v.insert(at, x.lend());
                    c.insert(at, x.clone());
                }
                (v, c)
            };
            let (v, c) = build();
            for x in [&a, &b, &third] {
                check_only(&v, &c, &model, x);
            }
            assert!(v.is_only(a.lend()));
            assert_eq!(v.is_only(b.lend()), n == 0);
            // one other value, at each of a few places
            for at in [0, 1, n / 2, n.saturating_sub(1), n] {
                if at > n {
                    continue;
                }
                let (mut v2, mut c2) = build();
                let mut m2 = model.clone();
                v2.insert(at, b.lend());
                c2.insert(at, b.clone());
                m2.insert(at, b.clone());
                assert!(!v2.is_only(a.lend()));
                assert_eq!(v2.is_only(b.lend()), n == 0);
                for x in [&a, &b, &third] {
                    check_only(&v2, &c2, &m2, x);
                }
                // and taken out again
                v2.remove_n(at, 1);
                c2.remove_n(at, 1);
                check(&v2, &c2, &model);
                assert!(v2.is_only(a.lend()));
                // cleared, a column holds any value alone, and keeps its budget
                v2.clear();
                c2.clear();
                check(&v2, &c2, &[]);
                assert!(v2.is_only(a.lend()) && v2.is_only(third.lend()));
            }
        }
    }
}

/// One value alone in many slabs: what is left of a column of two values
/// in turn when the other is taken out, one at a time.
fn one_value_in_many_slabs<T: Kind>() {
    let mut rng = Rng(0x51ab);
    let a = T::fresh(&mut rng);
    let mut b = T::fresh(&mut rng);
    while b == a {
        b = T::fresh(&mut rng);
    }
    for budget in [2, 3, 8] {
        let mut model: Vec<T> = (0..300).map(|i| [&a, &b][i % 2].clone()).collect();
        let mut v = VerifiedColumn::<T>::with_max_segments(budget);
        let mut c = Column::<T>::with_max_segments(budget);
        for x in &model {
            v.push(x.lend());
            c.push(x.clone());
        }
        let mut most = 0;
        for i in (0..model.len()).rev().filter(|i| i % 2 == 1) {
            v.remove(i);
            c.remove(i);
            model.remove(i);
            check_only(&v, &c, &model, &a);
            check_only(&v, &c, &model, &b);
            most = most.max(v.slab_count());
        }
        check(&v, &c, &model);
        assert!(v.is_only(a.lend()) && !v.is_only(b.lend()));
        // the case is the one meant: under a small budget, `a` alone and
        // in many slabs
        assert!(most > 30 && (budget > 3 || v.slab_count() > 30));
    }
}

#[test]
fn a_column_of_one_value() {
    for_each_kind!(one_value);
    for_each_kind!(one_value_in_many_slabs);
}

/// The two loaders take the same bytes, as the same values, and refuse
/// the rest with the same error.
fn check_same_load<T: Kind>(bytes: &[u8]) -> bool {
    let v = VerifiedColumn::<T>::load(bytes);
    let c = Column::<T>::load(bytes);
    match (v, c) {
        (Ok(v), Ok(c)) => {
            assert_eq!(v.len(), c.len());
            // a few bytes can be a run of billions: read those in places
            if v.len() <= 100_000 {
                assert_eq!(new_values(&v), old_values(&c));
            } else {
                for i in [0, 1, v.len() / 2, v.len() - 1] {
                    assert_eq!(v.get(i).map(T::keep), c.get(i).map(old));
                }
            }
            // what loads is canonical: it is what its values save to
            assert_eq!(v.save(), bytes);
            assert_eq!(c.save(), bytes);
            true
        }
        (Err(v), Err(c)) => {
            assert_eq!(
                v.to_string(),
                c.to_string(),
                "the loaders refuse {bytes:?} differently"
            );
            false
        }
        (v, c) => panic!(
            "the loaders disagree on {bytes:?}: {:?}, {:?}",
            v.map(|v| v.len()),
            c.map(|c| c.len())
        ),
    }
}

/// The error of a load that both APIs refuse.
fn refusal<T: Kind>(bytes: &[u8]) -> String {
    assert!(!check_same_load::<T>(bytes), "loaded {bytes:?}");
    VerifiedColumn::<T>::load(bytes).err().unwrap().to_string()
}

#[test]
fn loads_refuse_bad_runs() {
    // a run of zero values; a run of one (a literal's place); two runs of
    // the same value; an overlong number; a truncated one; two literal
    // runs in a row; a run with no value
    let bad: [&[u8]; 7] = [
        &[0x02, 0x80, 0x00],
        &[0x01, 0x05],
        &[0x02, 0x05, 0x02, 0x05],
        &[0x82, 0x00, 0x05],
        &[0x02, 0x85],
        &[0x7f, 0x01, 0x7f, 0x02],
        &[0x02],
    ];
    fn refuses<T: Kind>(bad: &[&[u8]]) {
        for b in bad {
            assert!(!check_same_load::<T>(b), "loaded {b:?}");
        }
    }
    refuses::<u64>(&bad);
    refuses::<i64>(&bad);
    refuses::<u32>(&bad);
    refuses::<i32>(&bad);
    refuses::<usize>(&bad);
    refuses::<NonZeroU32>(&bad);
    refuses::<Action>(&bad);
    refuses::<Option<u64>>(&bad);
    refuses::<Option<i64>>(&bad);
    refuses::<Option<NonZeroU32>>(&bad);
    refuses::<Option<Action>>(&bad);
    refuses::<Vec<u8>>(&bad);
    refuses::<Option<Vec<u8>>>(&bad);

    // `bool`: a run of none that is not the first; a truncated count; an
    // overlong one
    let bad: [&[u8]; 4] = [&[1, 0, 1], &[1, 1, 0], &[0x85], &[0x81, 0x00]];
    refuses::<bool>(&bad);
}

#[test]
fn loads_refuse_what_the_type_has_no_value_for() {
    // a null run in a column with no nulls
    let nulls = both::<Option<u64>>(&[Some(1), None, None, Some(1)])
        .0
        .save();
    assert!(!check_same_load::<u64>(&nulls));
    assert!(!check_same_load::<i64>(&nulls));
    assert!(!check_same_load::<u32>(&nulls));
    assert!(!check_same_load::<i32>(&nulls));
    assert!(!check_same_load::<usize>(&nulls));
    assert!(!check_same_load::<NonZeroU32>(&nulls));
    assert!(!check_same_load::<Action>(&nulls));
    assert!(check_same_load::<Option<u64>>(&nulls));
    assert!(check_same_load::<Option<NonZeroU32>>(&nulls));
    assert!(check_same_load::<Option<Action>>(&nulls));
    // a null run is no column of `bool`: its count of `true` is none
    assert!(!check_same_load::<bool>(&nulls));

    // numbers past the type
    let edge = u32::MAX as u64;
    let fits = both::<u64>(&[7, edge, edge, 0]).0.save();
    let over = both::<u64>(&[7, edge + 1, edge + 1, 0]).0.save();
    assert!(check_same_load::<u32>(&fits));
    assert!(check_same_load::<Option<u32>>(&fits));
    assert_eq!(refusal::<u32>(&over), "invalid value: u32 overflow");
    assert_eq!(refusal::<Option<u32>>(&over), "invalid value: u32 overflow");
    // in a literal run too
    let over = both::<u64>(&[7, edge + 1, 0]).0.save();
    assert_eq!(refusal::<u32>(&over), "invalid value: u32 overflow");

    let (lo, hi) = (i32::MIN as i64, i32::MAX as i64);
    let fits = both::<i64>(&[lo, lo, hi, -1]).0.save();
    assert!(check_same_load::<i32>(&fits));
    assert!(check_same_load::<Option<i32>>(&fits));
    for model in [[0, hi + 1, hi + 1], [lo - 1, 0, 0], [3, i64::MIN, i64::MAX]] {
        let over = both::<i64>(&model).0.save();
        assert_eq!(refusal::<i32>(&over), "invalid value: i32 overflow");
        assert_eq!(refusal::<Option<i32>>(&over), "invalid value: i32 overflow");
    }

    // zero is no `NonZeroU32`, nor is a number past `u32`
    let fits = both::<u64>(&[1, edge, edge, 7]).0.save();
    assert!(check_same_load::<NonZeroU32>(&fits));
    assert!(check_same_load::<Option<NonZeroU32>>(&fits));
    for model in [[1, 0, 0], [0, 5, 6], [3, 3, 0]] {
        let zero = both::<u64>(&model).0.save();
        assert_eq!(
            refusal::<NonZeroU32>(&zero),
            "invalid value: NonZeroU32 is zero"
        );
        assert_eq!(
            refusal::<Option<NonZeroU32>>(&zero),
            "invalid value: NonZeroU32 is zero"
        );
    }
    let over = both::<u64>(&[7, edge + 1, edge + 1, 1]).0.save();
    assert_eq!(refusal::<NonZeroU32>(&over), "invalid value: u32 overflow");
    assert_eq!(
        refusal::<Option<NonZeroU32>>(&over),
        "invalid value: u32 overflow"
    );

    // every `u64` is a `usize` only where a `usize` is that wide: the two
    // loaders agree either way
    let wide = both::<u64>(&[1, u64::MAX, u64::MAX, edge + 1]).0.save();
    assert_eq!(
        check_same_load::<usize>(&wide),
        usize::try_from(u64::MAX).is_ok()
    );
    assert_eq!(
        check_same_load::<Option<usize>>(&wide),
        usize::try_from(u64::MAX).is_ok()
    );
    if usize::try_from(u64::MAX).is_err() {
        assert_eq!(refusal::<usize>(&wide), "invalid value: usize overflow");
    }

    // a number the client's type has no value for: the client's error
    let actions = both::<u64>(&[0, 1, 3, 3, 4]).0.save();
    assert!(check_same_load::<Action>(&actions));
    assert!(check_same_load::<Option<Action>>(&actions));
    for model in [[0, 2, 3], [5, 5, 5], [4, 4, u64::MAX]] {
        let none = both::<u64>(&model).0.save();
        assert_eq!(refusal::<Action>(&none), "invalid value: not an action");
        assert_eq!(
            refusal::<Option<Action>>(&none),
            "invalid value: not an action"
        );
    }
}

/// Byte strings: a length is a number like any other, and the bytes it
/// counts must be there.
#[test]
fn loads_refuse_bad_byte_strings() {
    let bad_format = PackError::BadFormat.to_string();
    // a length past the data's end: in a run, in a literal run, in the
    // second value of one; as long as a length can be; of no bytes at all
    let short: [&[u8]; 6] = [
        &[2, 3, 1, 2],
        &[0x7f, 3, 1, 2],
        &[0x7e, 1, 7, 2, 9],
        &[
            2, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01,
        ],
        &[2, 0xff, 0xff, 0xff, 0xff, 0x0f, 1, 2, 3],
        &[2, 1],
    ];
    for b in short {
        assert_eq!(refusal::<Vec<u8>>(b), bad_format, "{b:?}");
        assert_eq!(refusal::<Option<Vec<u8>>>(b), bad_format, "{b:?}");
    }
    // a truncated length; an overlong one; two runs of one string; one
    // string twice in a literal run; a run with no string; a literal run
    // that ends early
    let bad: [&[u8]; 6] = [
        &[2, 0x81],
        &[2, 0x81, 0x00, 7],
        &[2, 1, 7, 2, 1, 7],
        &[0x7e, 1, 7, 1, 7],
        &[2, 1, 7, 9],
        &[0x7d, 1, 7, 0],
    ];
    for b in bad {
        assert!(!check_same_load::<Vec<u8>>(b), "loaded {b:?}");
        assert!(!check_same_load::<Option<Vec<u8>>>(b), "loaded {b:?}");
    }
    // these are columns: the empty string in a run, and one string after
    // another that it is the start of
    let good: [&[u8]; 3] = [&[3, 0], &[0x7e, 1, 7, 2, 7, 7], &[2, 1, 7, 0x7f, 0]];
    for b in good {
        assert!(check_same_load::<Vec<u8>>(b), "refused {b:?}");
        assert!(check_same_load::<Option<Vec<u8>>>(b), "refused {b:?}");
    }
    // a null run, where the type has nulls
    assert!(!check_same_load::<Vec<u8>>(&[2, 1, 7, 0, 2]));
    assert!(check_same_load::<Option<Vec<u8>>>(&[2, 1, 7, 0, 2]));
    // nulls and no nulls save alike where there is no null
    let model: Vec<Vec<u8>> = Rng(0xb17e5).values(200);
    let some: Vec<Option<Vec<u8>>> = model.iter().cloned().map(Some).collect();
    assert_eq!(both(&model).0.save(), both(&some).0.save());
}

/// Bytes at random and saved columns with a byte changed, dropped or
/// added: the two loaders take the same ones. Both kinds are seen.
fn fuzz_loads<T: Kind>() {
    let mut rng = Rng(0x5eed);
    let (mut took, mut refused) = (0, 0);
    // bytes at random, mostly not a column
    for _ in 0..6_000 {
        let n = rng.below(12);
        let bytes: Vec<u8> = (0..n)
            .map(|_| match rng.below(4) {
                0 => rng.next() as u8,
                1 => [0x7f, 0x7e, 0x00, 0x80][rng.below(4)],
                _ => (rng.next() % 6) as u8,
            })
            .collect();
        if check_same_load::<T>(&bytes) {
            took += 1;
        } else {
            refused += 1;
        }
    }
    for _ in 0..2_000 {
        let model: Vec<T> = rng.values(30);
        let mut bytes = both(&model).0.save();
        assert!(check_same_load::<T>(&bytes));
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
        if check_same_load::<T>(&bytes) {
            took += 1;
        } else {
            refused += 1;
        }
    }
    assert!(
        took > 100 && refused > 100,
        "{took} taken, {refused} refused"
    );
}

#[test]
fn loads_refuse_the_same_bytes() {
    for_each_kind!(fuzz_loads);
}

/// One type's bytes loaded as another's: the two APIs agree on what that
/// gives, which is the other type's reading of the numbers or a refusal.
#[test]
fn loads_of_another_types_bytes() {
    fn cross<T: Kind>(bytes: &[u8]) {
        check_same_load::<T>(bytes);
    }
    let mut rng = Rng(0xc405);
    for _ in 0..300 {
        let all: [Vec<u8>; 8] = [
            both(&rng.values::<u64>(30)).0.save(),
            both(&rng.values::<i64>(30)).0.save(),
            both(&rng.values::<Option<u32>>(30)).0.save(),
            both(&rng.values::<Option<i32>>(30)).0.save(),
            both(&rng.values::<bool>(30)).0.save(),
            both(&rng.values::<Option<Action>>(30)).0.save(),
            both(&rng.values::<Vec<u8>>(30)).0.save(),
            both(&rng.values::<Option<Vec<u8>>>(30)).0.save(),
        ];
        for bytes in &all {
            cross::<u64>(bytes);
            cross::<i64>(bytes);
            cross::<u32>(bytes);
            cross::<i32>(bytes);
            cross::<usize>(bytes);
            cross::<NonZeroU32>(bytes);
            cross::<bool>(bytes);
            cross::<Action>(bytes);
            cross::<Option<u64>>(bytes);
            cross::<Option<i64>>(bytes);
            cross::<Option<u32>>(bytes);
            cross::<Option<i32>>(bytes);
            cross::<Option<usize>>(bytes);
            cross::<Option<NonZeroU32>>(bytes);
            cross::<Option<Action>>(bytes);
            cross::<Vec<u8>>(bytes);
            cross::<Option<Vec<u8>>>(bytes);
        }
    }
}
