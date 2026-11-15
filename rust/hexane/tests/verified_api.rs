//! The verified public API (`hexane::verified`) and `Column` are two
//! doors to one format: for the same values they save the same bytes, each
//! loads what the other saved, and they refuse the same bytes. That holds
//! for every value type of the verified API (numbers, `bool`, byte strings,
//! each with nulls where it has them), and for a type of a client's own.
//!
//! Verus proves the verified API against its own specifications. These
//! tests are what ties those specifications to `Column`'s format, which
//! no proof does.

// `nth(0)` is here for what `nth` does, which is under test
#![allow(clippy::iter_nth_zero)]

use hexane::verified::{refused, Get, VNewtype, VValue, VerifiedColumn, VerifiedIter};
use hexane::{
    Codec, Column, ColumnValue, ColumnValueRef, LoadOpts, MaybeFill, NoFill, PackError,
    RleEncoding, RleValue, Run,
};
use std::cell::{Cell, RefCell};
use std::cmp::Ordering;
use std::fmt::Debug;
use std::num::NonZeroU32;
use std::ops::Range;

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

    /// How the value stands to `other` in the order of a column's values,
    /// as the standard library has it for the type.
    fn order(&self, other: &Self) -> Ordering;

    /// Where `Column` finds the value among its values `range`.
    fn old_scope(&self, c: &Column<Self>, range: Range<usize>) -> Range<usize>;
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

            fn order(&self, other: &$t) -> Ordering {
                self.cmp(other)
            }

            fn old_scope(&self, c: &Column<$t>, range: Range<usize>) -> Range<usize> {
                c.scope_to_value(*self, range)
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

            fn order(&self, other: &Option<$t>) -> Ordering {
                self.cmp(other)
            }

            fn old_scope(&self, c: &Column<Option<$t>>, range: Range<usize>) -> Range<usize> {
                c.scope_to_value(*self, range)
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

    fn order(&self, other: &bool) -> Ordering {
        self.cmp(other)
    }

    fn old_scope(&self, c: &Column<bool>, range: Range<usize>) -> Range<usize> {
        c.scope_to_value(*self, range)
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

    fn order(&self, other: &Vec<u8>) -> Ordering {
        self.cmp(other)
    }

    fn old_scope(&self, c: &Column<Vec<u8>>, range: Range<usize>) -> Range<usize> {
        c.scope_to_value(self.as_slice(), range)
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

    fn order(&self, other: &Option<Vec<u8>>) -> Ordering {
        self.cmp(other)
    }

    fn old_scope(&self, c: &Column<Option<Vec<u8>>>, range: Range<usize>) -> Range<usize> {
        c.scope_to_value(self.as_deref(), range)
    }
}

/// A value type of a client's own, as a client compiled without Verus
/// writes it: one of four actions, each stored as a number. Any other
/// number is no action. The actions are declared in the order of their
/// numbers, which is the order a column has them in.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
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

/// Both columns hold `model`, read the same way through every reader,
/// save to the same bytes and lie in the same slabs, which pass the checks
/// of `check_invariants`. For columns that edits have made of values.
fn check<T: Kind>(v: &VerifiedColumn<T>, c: &Column<T>, model: &[T]) {
    check_with(v, c, model, true);
}

/// [`check`] for columns that were loaded from bytes, and for what edits
/// have made of those. Of such a column `check_invariants` may say that
/// two slabs should have been merged (see
/// `check_invariants_takes_a_loaded_column_as_edited`): here it is to say
/// the same of both columns.
fn check_loaded<T: Kind>(v: &VerifiedColumn<T>, c: &Column<T>, model: &[T]) {
    check_with(v, c, model, false);
}

fn check_with<T: Kind>(v: &VerifiedColumn<T>, c: &Column<T>, model: &[T], built: bool) {
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
    check_layout(v, c, model, built);
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

/// What the layout tools say of the two columns. The slabs of the
/// verified column hold `model.len()` values between them; they are as
/// long, and of as many runs, as `Column`'s (but for a column of no
/// values, which may have no slab in one API and an empty one in the
/// other); the checks of `check_invariants` pass, in both, if the columns
/// are `built` of values, and say the same of both if they were loaded;
/// and the bytes the column saves are a column's.
fn check_layout<T: Kind>(v: &VerifiedColumn<T>, c: &Column<T>, model: &[T], built: bool) {
    let (lens, segments) = (v.slab_lens(), v.slab_segments());
    assert_eq!(lens.len(), v.slab_count());
    assert_eq!(segments.len(), v.slab_count());
    assert_eq!(lens.iter().sum::<usize>(), model.len());
    if model.is_empty() {
        assert_eq!(segments.iter().sum::<usize>(), 0);
    } else {
        assert_eq!(lens, c.slab_lens());
        assert_eq!(segments, c.slab_segments());
        assert!(lens.iter().all(|&n| n > 0));
    }
    if built {
        v.check_invariants();
        c.check_invariants();
    } else {
        check_invariants_agree(v, c);
    }
    assert!(v.validate_encoding().is_ok());
    assert!(c.validate_encoding().is_ok());
}

/// `check_invariants` says the same of both columns: nothing, or the same
/// words. (Where a panic can be caught: not on wasm32.)
#[cfg(panic = "unwind")]
fn check_invariants_agree<T: Kind>(v: &VerifiedColumn<T>, c: &Column<T>) {
    let new = panic_of(std::panic::AssertUnwindSafe(|| v.check_invariants()));
    let old = panic_of(std::panic::AssertUnwindSafe(|| c.check_invariants()));
    assert_eq!(new, old);
}

#[cfg(not(panic = "unwind"))]
fn check_invariants_agree<T: Kind>(_v: &VerifiedColumn<T>, _c: &Column<T>) {}

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

/// A clone of each API's column holds what the column holds, in the same
/// slabs, and is a column of its own: an edit of the clones (`salt` picks
/// it) leaves the columns as they were, and the clones agree after it.
fn check_clone<T: Kind>(v: &VerifiedColumn<T>, c: &Column<T>, model: &[T], salt: u64) {
    let mut v2 = v.clone();
    let mut c2 = c.clone();
    check(&v2, &c2, model);
    assert_eq!(v2.slab_count(), v.slab_count());
    assert_eq!(format!("{v2:?}"), format!("{v:?}"));
    assert_eq!(v2.save(), v.save());

    let mut rng = Rng(salt | 1);
    let mut m2 = model.to_vec();
    let at = rng.below(m2.len() + 1);
    let del = rng.below((m2.len() - at).min(12) + 1);
    let ins: Vec<T> = rng.values(40);
    v2.splice(at, del, &lent(&ins));
    c2.splice(at, del, ins.iter().cloned());
    m2.splice(at..at + del, ins);
    let x = T::fresh(&mut rng);
    v2.push(x.lend());
    c2.push(x.clone());
    m2.push(x);
    check(&v2, &c2, &m2);
    check(v, c, model);

    // a clone of the clone, and the clone gone
    let v3 = v2.clone();
    drop(v2);
    assert_eq!(new_values(&v3), m2);
    assert_eq!(v3.slab_count(), c2.slab_count());
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
    ($f:ident $(, $arg:expr)*) => {
        $f::<u64>($($arg),*);
        $f::<i64>($($arg),*);
        $f::<u32>($($arg),*);
        $f::<i32>($($arg),*);
        $f::<usize>($($arg),*);
        $f::<NonZeroU32>($($arg),*);
        $f::<bool>($($arg),*);
        $f::<Action>($($arg),*);
        $f::<Option<u64>>($($arg),*);
        $f::<Option<i64>>($($arg),*);
        $f::<Option<u32>>($($arg),*);
        $f::<Option<i32>>($($arg),*);
        $f::<Option<usize>>($($arg),*);
        $f::<Option<NonZeroU32>>($($arg),*);
        $f::<Option<Action>>($($arg),*);
        $f::<Vec<u8>>($($arg),*);
        $f::<Option<Vec<u8>>>($($arg),*);
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
    for (i, model) in shapes.into_iter().enumerate() {
        let (v, c) = both(&model);
        check(&v, &c, &model);
        check_loads(&v, &c, &model);
        check_clone(&v, &c, &model, 0xc10e + i as u64);
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
    // a new column has no slab, in either API; one made of no values
    // has an empty one, in both
    let c = Column::<T>::new();
    check(&v, &c, &[]);
    check(&d, &c, &[]);
    assert_eq!((v.slab_lens(), v.slab_segments()), (vec![], vec![]));
    assert_eq!((c.slab_lens(), c.slab_segments()), (vec![], vec![]));
    let (v, c) = (
        VerifiedColumn::<T>::from_values(&[]),
        Column::<T>::from_values(vec![]),
    );
    check(&v, &c, &[]);
    assert_eq!((v.slab_lens(), v.slab_segments()), (vec![0], vec![0]));
    assert_eq!((c.slab_lens(), c.slab_segments()), (vec![0], vec![0]));
}

#[test]
fn an_empty_column() {
    for_each_kind!(empty);
}

/// A clone keeps the column's segment budget and is its own column: a
/// clone of an empty column, of one emptied, and of one of small slabs,
/// each filled after the column it came from is gone.
fn clones<T: Kind>() {
    let mut rng = Rng(0xc0de);
    let more: Vec<T> = (0..300).map(|_| T::fresh(&mut rng)).collect();
    for budget in [2, 3, 16, 64] {
        // of an empty column, which has no slab yet
        let v = VerifiedColumn::<T>::with_max_segments(budget);
        let c = Column::<T>::with_max_segments(budget);
        check_clone(&v, &c, &[], budget as u64);
        let (mut v2, mut c2) = (v.clone(), c.clone());
        drop((v, c));
        v2.splice(0, 0, &lent(&more));
        c2.splice(0, 0, more.iter().cloned());
        check(&v2, &c2, &more);

        // of a column of many slabs, and then of that column emptied
        let (mut v3, mut c3) = (v2.clone(), c2.clone());
        drop((v2, c2));
        check(&v3, &c3, &more);
        check_clone(&v3, &c3, &more, 7 * budget as u64);
        v3.clear();
        c3.clear();
        let (mut v4, mut c4) = (v3.clone(), c3.clone());
        check(&v4, &c4, &[]);
        v4.splice(0, 0, &lent(&more));
        c4.splice(0, 0, more.iter().cloned());
        check(&v4, &c4, &more);
        check(&v3, &c3, &[]);
    }
    let d = VerifiedColumn::<T>::default().clone();
    assert!(d.is_empty() && d.save().is_empty());
}

#[test]
fn a_clone_is_a_column_of_its_own() {
    for_each_kind!(clones);
}

/// What a clone of byte strings lends is its own: it outlives the column
/// it was cloned from.
#[test]
fn a_clone_owns_its_bytes() {
    let mut v = VerifiedColumn::<Vec<u8>>::new();
    v.push(b"one");
    v.push(b"two");
    let w = v.clone();
    let lent = w.get(1);
    v.remove(1);
    v.push(b"three");
    drop(v);
    assert_eq!(lent, Some(&b"two"[..]));
    assert_eq!(w.to_vec(), [&b"one"[..], &b"two"[..]]);
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
                check_clone(&v, &c, &model, seed * 1000 + step);
            }
        }
        check_loads(&v, &c, &model);
    }
}

#[test]
fn edits_agree() {
    for_each_kind!(edits);
}

/// Runs: often of the value before, some of no value at all, a few long.
fn some_runs<T: Kind>(rng: &mut Rng, max: usize) -> Vec<(usize, T)> {
    let n = rng.below(max + 1);
    let mut last = T::fresh(rng);
    (0..n)
        .map(|_| {
            last = rng.value(&last);
            let count = [0, 0, 1, 1, 1, 2, 3, 9, 40, 130][rng.below(10)];
            (count, last.clone())
        })
        .collect()
}

/// The values of `runs`: of each its value, as often as its count says.
fn run_model<T: Kind>(runs: &[(usize, T)]) -> Vec<T> {
    runs.iter()
        .flat_map(|(n, x)| std::iter::repeat_n(x, *n).cloned())
        .collect()
}

/// The other ways to put values in. A column made in one step of a slice
/// or of what an iterator gives, and then edited by the splices that take
/// an iterator (of several kinds, some that do not say how many values
/// they have) or runs, and by `extend`: after each step both APIs hold the
/// same values in the same slabs.
fn other_forms<T: Kind>() {
    for seed in 1..=8u64 {
        let mut rng = Rng(seed.wrapping_mul(0x2545_f491_4f6c_dd1d));
        let budget = [2, 3, 4, 8, 64][rng.below(5)];
        let mut model: Vec<T> = rng.values(200);

        let v = VerifiedColumn::<T>::from_values(&lent(&model));
        let c = Column::<T>::from_values(model.clone());
        check(&v, &c, &model);
        let v: VerifiedColumn<T> = model.iter().map(T::lend).collect();
        let c: Column<T> = model.iter().cloned().collect();
        check(&v, &c, &model);
        let v = VerifiedColumn::<T>::from_iter(lent(&model));
        check(&v, &c, &model);

        let mut v = VerifiedColumn::<T>::from_values_with_max_segments(&lent(&model), budget);
        let mut c = Column::<T>::from_values_with_max_segments(model.clone(), budget);
        check(&v, &c, &model);
        check_loads(&v, &c, &model);
        for step in 0..60 {
            let at = rng.below(model.len() + 1);
            let del = rng.below((model.len() - at).min(30) + 1);
            match rng.below(10) {
                0..=4 => {
                    let ins: Vec<T> = rng.values(40);
                    let gets = lent(&ins);
                    match rng.below(6) {
                        0 => v.splice_iter(at, del, ins.iter().map(T::lend)),
                        1 => v.splice_iter(at, del, gets),
                        2 => v.splice_iter(at, del, gets.iter().copied().filter(|_| true)),
                        3 => {
                            let (a, b) = gets.split_at(gets.len() / 2);
                            v.splice_iter(at, del, a.iter().chain(b).copied());
                        }
                        4 => {
                            let mut i = 0;
                            let next = || {
                                i += 1;
                                gets.get(i - 1).copied()
                            };
                            v.splice_iter(at, del, std::iter::from_fn(next));
                        }
                        _ => v.splice_iter(at, del, gets.iter().rev().rev().copied()),
                    }
                    c.splice(at, del, ins.iter().cloned());
                    model.splice(at..at + del, ins.iter().cloned());
                }
                5..=7 => {
                    let runs = some_runs::<T>(&mut rng, 6);
                    let lent_runs = runs.iter().map(|(n, x)| Run {
                        count: *n,
                        value: x.lend(),
                    });
                    v.splice_runs(at, del, lent_runs);
                    let owned_runs = runs.iter().map(|(n, x)| Run {
                        count: *n,
                        value: x.clone(),
                    });
                    c.splice_runs(at, del, owned_runs);
                    model.splice(at..at + del, run_model(&runs));
                }
                8 => {
                    let more: Vec<T> = rng.values(40);
                    v.extend(more.iter().map(T::lend));
                    c.extend(more.iter().cloned());
                    model.extend(more);
                }
                _ => {
                    // nothing to add, and now and then most of it gone
                    v.extend(std::iter::empty());
                    c.extend(std::iter::empty::<T>());
                    v.splice_runs(at, 0, []);
                    c.splice_runs(at, 0, std::iter::empty::<Run<T>>());
                    if model.len() > 1500 {
                        let keep = rng.below(300);
                        v.truncate(keep);
                        c.truncate(keep);
                        model.truncate(keep);
                    }
                }
            }
            check(&v, &c, &model);
            if step % 10 == 9 {
                check_loads(&v, &c, &model);
                check_clone(&v, &c, &model, seed * 1000 + step);
            }
        }
        check_loads(&v, &c, &model);
    }
}

#[test]
fn the_other_forms_agree() {
    for_each_kind!(other_forms);
}

/// A column of one value repeated, made in one step: the column that
/// `Column::fill` makes, and the one made of the values one by one. It is
/// edited like any other.
fn fills<T: Kind>() {
    let mut rng = Rng(0xf111);
    for n in [0, 1, 2, 3, 63, 64, 65, 1000] {
        let x = T::fresh(&mut rng);
        let mut v = VerifiedColumn::<T>::fill(n, x.lend());
        let mut c = Column::<T>::fill(n, x.as_column_ref());
        let mut model = vec![x.clone(); n];
        check(&v, &c, &model);
        check_loads(&v, &c, &model);
        assert!(v.is_only(x.lend()));
        assert_eq!(
            v.save(),
            VerifiedColumn::<T>::from_values(&lent(&model)).save()
        );
        check_clone(&v, &c, &model, n as u64);

        let y = T::fresh(&mut rng);
        v.insert(n / 2, y.lend());
        c.insert(n / 2, y.clone());
        model.insert(n / 2, y);
        check(&v, &c, &model);
        // more of the same value, as one run, after the first `n`
        let run = |value| Run {
            count: 2 * n,
            value,
        };
        v.splice_runs(n, 0, [run(x.lend())]);
        c.splice_runs(
            n,
            0,
            [Run {
                count: 2 * n,
                value: x.clone(),
            }],
        );
        model.splice(n..n, vec![x.clone(); 2 * n]);
        check(&v, &c, &model);
    }
}

#[test]
fn a_column_filled_with_one_value() {
    for_each_kind!(fills);
}

/// What a closure panics with, if it does.
#[cfg(any(target_pointer_width = "64", panic = "unwind"))]
fn panic_of<R>(f: impl FnOnce() -> R + std::panic::UnwindSafe) -> Option<String> {
    let e = std::panic::catch_unwind(f).err()?;
    let text = e.downcast_ref::<String>().cloned();
    Some(text.unwrap_or_else(|| {
        e.downcast_ref::<&str>()
            .map(|s| s.to_string())
            .unwrap_or_default()
    }))
}

/// Columns too long to hold their values one by one, made of one run.
/// The format writes the count of a run of equal numbers with a sign: a
/// run of `i64::MAX` is the longest, both APIs make it, and both panic
/// rather than write a longer one. Of `bool`s and of nulls the count has
/// no sign: there the bound is what a `usize` counts, with three to
/// spare. (Where a `usize` has 64 bits: one of 32 bits counts less than
/// either.)
#[cfg(target_pointer_width = "64")]
#[test]
fn the_longest_runs() {
    let most = i64::MAX as usize;
    let v = VerifiedColumn::<u64>::fill(most, 7);
    let c = Column::<u64>::fill(most, 7);
    assert_eq!(
        (v.len(), v.get(most - 1), v.get(most)),
        (most, Some(7), None)
    );
    assert_eq!(v.save(), c.save());
    assert!(v.is_only(7) && !v.is_only(8));
    let back = VerifiedColumn::<u64>::load(&v.save()).unwrap();
    assert_eq!((back.len(), back.get(most - 1)), (most, Some(7)));
    let nulls = VerifiedColumn::<Option<Vec<u8>>>::fill(most, None);
    assert_eq!(
        nulls.save(),
        Column::<Option<Vec<u8>>>::fill(most, None).save()
    );
    assert_eq!((nulls.len(), nulls.get(most - 1)), (most, Some(None)));

    // by a run, and then one value more
    let by_run = |count: usize, then: Option<u64>| {
        let new = panic_of(move || {
            let mut v = VerifiedColumn::<u64>::new();
            v.splice_runs(0, 0, [Run { count, value: 7 }]);
            if let Some(x) = then {
                v.push(x);
            }
            (v.len(), v.save())
        });
        let old = panic_of(move || {
            let mut c = Column::<u64>::new();
            c.splice_runs(0, 0, [Run { count, value: 7u64 }]);
            if let Some(x) = then {
                c.push(x);
            }
            (c.len(), c.save())
        });
        assert_eq!(new, old);
        new
    };
    assert_eq!(by_run(most, None), None);
    assert_eq!(by_run(most, Some(8)), None);
    let said = by_run(most, Some(7)).unwrap();
    assert!(said.contains("is not encodable"), "{said}");
    assert_eq!(by_run(most + 1, None), Some(said));
    assert!(by_run(usize::MAX, None).is_some());

    // counts with no sign
    let bools = |count: usize, then: Option<bool>| {
        let new = panic_of(move || {
            let mut v = VerifiedColumn::<bool>::new();
            v.splice_runs(0, 0, [Run { count, value: true }]);
            if let Some(x) = then {
                v.push(x);
            }
            (v.len(), v.get(count - 1), v.save())
        });
        let old = panic_of(move || {
            let mut c = Column::<bool>::new();
            c.splice_runs(0, 0, [Run { count, value: true }]);
            if let Some(x) = then {
                c.push(x);
            }
            (c.len(), c.get(count - 1), c.save())
        });
        assert_eq!(new, old);
        new
    };
    assert_eq!(bools(most + 1, Some(true)), None);
    assert_eq!(bools(usize::MAX - 3, None), None);
    let said = bools(usize::MAX - 3, Some(false)).unwrap();
    assert!(said.contains("too long"), "{said}");
    assert_eq!(bools(usize::MAX - 2, None), Some(said));
}

/// An iterator is asked for no value after it has said that it has none.
#[test]
fn an_iterator_is_run_to_its_end_once() {
    let items = [3u64, 3, 4];
    let feed = |asked: &mut usize| {
        let at = *asked;
        *asked += 1;
        assert!(at <= items.len(), "asked again after the end");
        items.get(at).copied()
    };
    let mut asked = 0;
    let mut v = VerifiedColumn::<u64>::new();
    v.splice_iter(0, 0, std::iter::from_fn(|| feed(&mut asked)));
    assert_eq!((v.to_vec(), asked), (items.to_vec(), 4));
    asked = 0;
    v.extend(std::iter::from_fn(|| feed(&mut asked)));
    assert_eq!((v.len(), asked), (6, 4));
    asked = 0;
    let w: VerifiedColumn<u64> = std::iter::from_fn(|| feed(&mut asked)).collect();
    assert_eq!((w.to_vec(), asked), (items.to_vec(), 4));
    asked = 0;
    let runs = std::iter::from_fn(|| feed(&mut asked)).map(|x| Run { count: 2, value: x });
    v.splice_runs(1, 4, runs);
    assert_eq!((v.to_vec(), asked), (vec![3, 3, 3, 3, 3, 4, 4, 4], 4));
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

/// Where the two APIs find `x` among the values `start..end`. Both give
/// a range within the window, and the same one: they run one search over
/// the same slabs. Where the window is in order as far as `x` can tell
/// (the values before `x` first, those after it last), which the result
/// says, the range is that of the places of `x`, or the empty range where
/// `x` would stand: by the model, in the order the standard library has
/// for the type.
fn check_scope<T: Kind>(
    v: &VerifiedColumn<T>,
    c: &Column<T>,
    model: &[T],
    x: &T,
    start: usize,
    end: usize,
) -> bool {
    let r = v.scope_to_value(x.lend(), start, end);
    assert!(
        start <= r.start && r.start <= r.end && r.end <= end,
        "{r:?} of {start}..{end}"
    );
    assert_eq!(r, x.old_scope(c, start..end), "{x:?} in {start}..{end}");
    let sides: Vec<Ordering> = model[start..end].iter().map(|m| m.order(x)).collect();
    let in_order = sides.windows(2).all(|w| w[0] <= w[1]);
    if in_order {
        let before = sides.iter().filter(|s| **s == Ordering::Less).count();
        let after = sides.iter().filter(|s| **s == Ordering::Greater).count();
        assert_eq!(r, start + before..end - after, "{x:?} in {start}..{end}");
        assert!(model[r].iter().all(|m| m == x));
    }
    in_order
}

/// Windows of a column of `n` values in slabs of the lengths `lens`: all
/// of it, empty ones, ones that start or end where a slab does or one
/// place from there, one inside each of a few slabs, and some at random.
fn windows(rng: &mut Rng, lens: &[usize], n: usize) -> Vec<(usize, usize)> {
    let mut ws = vec![(0, n), (0, 0), (n, n), (n / 2, n / 2), (n / 3, n - n / 3)];
    let mut at = 0;
    let step = lens.len() / 6 + 1;
    for (i, len) in lens.iter().enumerate() {
        if i % step == 0 {
            let to = at + len;
            ws.extend([(at, n), (0, at), (at, at), (at, to), (0, to), (to, n)]);
            ws.extend([
                (at.saturating_sub(1), (to + 1).min(n)),
                (at, (at + 1).min(to)),
            ]);
            if *len > 2 {
                ws.push((at + 1, to - 1));
            }
        }
        at += len;
    }
    for _ in 0..12 {
        let a = rng.below(n + 1);
        ws.push((a, a + rng.below(n - a + 1)));
    }
    ws
}

/// Some of the values of `model`, the first and the last among them, and
/// some that it may not hold.
fn targets<T: Kind>(rng: &mut Rng, model: &[T]) -> Vec<T> {
    let mut xs: Vec<T> = (0..8).map(|_| T::fresh(rng)).collect();
    xs.extend(model.first().cloned());
    xs.extend(model.last().cloned());
    for _ in 0..12.min(model.len()) {
        xs.push(model[rng.below(model.len())].clone());
    }
    xs
}

/// A column of each API with the values `model`, of slabs of at most
/// `budget` runs.
fn both_with<T: Kind>(model: &[T], budget: usize) -> (VerifiedColumn<T>, Column<T>) {
    let mut v = VerifiedColumn::<T>::with_max_segments(budget);
    v.splice(0, 0, &lent(model));
    let mut c = Column::<T>::with_max_segments(budget);
    c.splice(0, 0, model.iter().cloned());
    (v, c)
}

/// A column of each API with the values `model`, of slabs of at most
/// `budget` runs and in many of them even where `model` is one value
/// many times over: made with another value after each, which is then
/// taken out again.
fn both_in_slabs<T: Kind>(
    rng: &mut Rng,
    model: &[T],
    budget: usize,
) -> (VerifiedColumn<T>, Column<T>) {
    let mut v = VerifiedColumn::<T>::with_max_segments(budget);
    let mut c = Column::<T>::with_max_segments(budget);
    for x in model {
        let mut other = T::fresh(rng);
        while other == *x {
            other = T::fresh(rng);
        }
        v.push(x.lend());
        v.push(other.lend());
        c.push(x.clone());
        c.push(other);
    }
    for i in (0..model.len()).rev() {
        v.remove(2 * i + 1);
        c.remove(2 * i + 1);
    }
    (v, c)
}

/// Values in order, in one slab and in many: the search finds each value
/// where the model has it, and says where one that is not there would
/// stand, in every window. So too in columns loaded from the saved bytes.
fn scopes<T: Kind>() {
    let mut rng = Rng(0x5c0e);
    for n in [0, 1, 2, 3, 50, 400, 3000] {
        let mut model: Vec<T> = (0..n).map(|_| T::fresh(&mut rng)).collect();
        if n == 400 {
            // few values, each many times over
            model = (0..n).map(|i| model[i % 5].clone()).collect();
        }
        model.sort_by(T::order);
        for budget in [3, 16, 64] {
            let (v, c) = if budget < 64 {
                both_in_slabs(&mut rng, &model, budget)
            } else {
                both_with(&model, budget)
            };
            check(&v, &c, &model);
            assert!(n < 400 || budget > 3 || v.slab_count() > 30);
            let bytes = v.save();
            let v2 = VerifiedColumn::<T>::load(&bytes).unwrap();
            let c2 = Column::<T>::load(&bytes).unwrap();
            let ws = windows(&mut rng, &v.slab_lens(), n);
            for x in targets(&mut rng, &model) {
                for &(start, end) in &ws {
                    assert!(check_scope(&v, &c, &model, &x, start, end));
                }
                assert!(check_scope(&v2, &c2, &model, &x, 0, n));
                assert!(check_scope(&v2, &c2, &model, &x, n / 4, n - n / 4));
            }
        }
    }
}

#[test]
fn a_search_of_values_in_order() {
    for_each_kind!(scopes);
}

/// Values in no order. The search still gives a range within the window,
/// and the two APIs the same one. And where the values are in order only
/// as far as the value looked for can tell (those before it first, in any
/// order, and those after it last, in any order), the search gives its
/// places, or the place where it would stand.
fn scopes_out_of_order<T: Kind>() {
    let mut rng = Rng(0x0dd5);
    let mut exact = 0;
    for round in 0..40 {
        let n = [0, 1, 7, 60, 300, 2000][round % 6];
        let model: Vec<T> = (0..n).map(|_| T::fresh(&mut rng)).collect();
        let (v, c) = both_with(&model, [3, 64][round % 2]);
        let ws = windows(&mut rng, &v.slab_lens(), n);
        for x in targets(&mut rng, &model) {
            for &(start, end) in &ws {
                check_scope(&v, &c, &model, &x, start, end);
            }
            // the same values, those before `x` first and those after it
            // last, each part in the order it had
            let mut parted: Vec<T> = vec![];
            for side in [Ordering::Less, Ordering::Equal, Ordering::Greater] {
                parted.extend(model.iter().filter(|m| m.order(&x) == side).cloned());
            }
            let (v, c) = both_with(&parted, [3, 64][round % 2]);
            for (start, end) in windows(&mut rng, &v.slab_lens(), n) {
                assert!(check_scope(&v, &c, &parted, &x, start, end));
            }
            exact += 1;
        }
    }
    assert!(exact > 300);
}

#[test]
fn a_search_of_values_in_no_order() {
    for_each_kind!(scopes_out_of_order);
}

/// The order of byte strings is that of their bytes, a string before
/// every longer one that starts with it, and the null before them all:
/// for strings that are long and differ late, or only in their lengths.
#[test]
fn a_search_of_long_byte_strings() {
    let stem: Vec<u8> = (0..400u32).map(|i| (i * 7 % 251) as u8).collect();
    let mut strings: Vec<Vec<u8>> = vec![vec![], vec![0], vec![0, 0], vec![1], vec![255]];
    for cut in [
        1, 2, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 255, 256, 399, 400,
    ] {
        // the stem's first `cut` bytes; then with one more byte that is
        // less than the stem's next, the same or greater
        let s = stem[..cut].to_vec();
        strings.push(s.clone());
        for last in [0u8, 1, 128, 250, 255] {
            let mut t = s.clone();
            t.push(last);
            strings.push(t);
        }
        if cut < 400 {
            let mut t = stem.clone();
            t[cut] = t[cut].wrapping_add(1);
            strings.push(t.clone());
            t[cut] = t[cut].wrapping_sub(2);
            strings.push(t);
        }
    }
    strings.sort();
    // some of them more than once
    let model: Vec<Vec<u8>> = strings
        .iter()
        .enumerate()
        .flat_map(|(i, s)| vec![s.clone(); 1 + i % 3])
        .collect();
    let mut rng = Rng(0xb17e);
    for budget in [3, 64] {
        let (v, c) = both_with(&model, budget);
        let ws = windows(&mut rng, &v.slab_lens(), model.len());
        for x in &strings {
            for &(start, end) in &ws {
                assert!(check_scope(&v, &c, &model, x, start, end));
            }
        }
        // a string that is not there stands between its neighbours
        let mut absent = stem[..300].to_vec();
        absent.extend([0, 0, 0]);
        assert!(!model.contains(&absent));
        let at = model.iter().filter(|m| **m < absent).count();
        assert_eq!(v.scope_to_value(&absent, 0, model.len()), at..at);
        assert!(0 < at && at < model.len());

        // with nulls, which come first
        let mut nullable: Vec<Option<Vec<u8>>> = vec![None; 5];
        nullable.extend(model.iter().cloned().map(Some));
        let (v, c) = both_with(&nullable, budget);
        let n = nullable.len();
        assert_eq!(v.scope_to_value(None, 0, n), 0..5);
        assert_eq!(v.scope_to_value(Some(&[]), 0, n), 5..6);
        for x in strings.iter().cloned().map(Some).chain([None]) {
            for (start, end) in windows(&mut rng, &v.slab_lens(), n) {
                assert!(check_scope(&v, &c, &nullable, &x, start, end));
            }
        }
    }
}

/// How two byte strings stand in the order of the values is how their
/// slices stand in Rust, wherever the first difference falls and however
/// long the two are.
#[test]
fn the_order_of_byte_strings_is_the_order_of_slices() {
    fn check(a: &[u8], b: &[u8]) {
        assert_eq!(<Vec<u8> as VValue>::compare(a, b), a.cmp(b), "{a:?} {b:?}");
        assert_eq!(<Vec<u8> as VValue>::compare(b, a), b.cmp(a), "{b:?} {a:?}");
        assert_eq!(
            <Option<Vec<u8>> as VValue>::compare(Some(a), Some(b)),
            a.cmp(b)
        );
        assert_eq!(<Vec<u8> as VValue>::same(a, b), a == b);
    }
    let mut rng = Rng(0x0bde);
    let base: Vec<u8> = (0..300u32).map(|i| (i * 11 % 253) as u8).collect();
    let lens = (0..=150).chain([191, 192, 193, 199, 200, 201, 255, 256, 257, 300]);
    for len in lens {
        let a = &base[..len];
        check(a, a);
        // what `a` starts, and what starts with it
        for more in [1, 2, 7, 8, 9, 63, 64, 65] {
            let mut b = a.to_vec();
            b.extend((0..more).map(|_| rng.next() as u8));
            check(a, &b);
            check(&a[..len.saturating_sub(more)], a);
        }
        // a first difference at `d`: one more or one less there, or any
        // other byte; after it the same bytes, others, fewer or more
        for d in 0..len {
            for delta in [1, 255, 1 + rng.below(255) as u8] {
                let mut b = a.to_vec();
                b[d] = b[d].wrapping_add(delta);
                check(a, &b);
                let mut c = b.clone();
                for x in &mut c[d + 1..] {
                    *x = rng.next() as u8;
                }
                check(a, &c);
                check(&b, &c);
                let cut = d + 1 + rng.below(len - d);
                check(a, &b[..cut]);
                check(&a[..cut], &b);
                c.extend((0..rng.below(70)).map(|_| rng.next() as u8));
                check(a, &c);
            }
        }
    }
    assert_eq!(
        <Option<Vec<u8>> as VValue>::compare(None, Some(&[])),
        Ordering::Less
    );
    assert_eq!(
        <Option<Vec<u8>> as VValue>::compare(Some(&[]), None),
        Ordering::Greater
    );
    assert_eq!(
        <Option<Vec<u8>> as VValue>::compare(None, None),
        Ordering::Equal
    );
}

/// A type of a client's own is in the order of the numbers it is stored
/// as, whatever order the type has in Rust: here the two are opposite.
#[test]
fn a_search_is_in_the_order_of_the_stored_numbers() {
    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
    enum Rank {
        High,
        Mid,
        Low,
    }

    impl VNewtype for Rank {
        type Base = u32;

        fn to_base(self) -> u32 {
            match self {
                Rank::High => 900,
                Rank::Mid => 50,
                Rank::Low => 1,
            }
        }

        fn from_base(b: u32) -> Result<Rank, PackError> {
            match b {
                900 => Ok(Rank::High),
                50 => Ok(Rank::Mid),
                1 => Ok(Rank::Low),
                _ => Err(refused("not a rank")),
            }
        }
    }

    assert!(Rank::High < Rank::Mid && Rank::Mid < Rank::Low);
    let by_number = [Rank::Low, Rank::Low, Rank::Mid, Rank::High, Rank::High];
    let v = VerifiedColumn::<Rank>::from_values(&by_number);
    assert_eq!(v.scope_to_value(Rank::Low, 0, 5), 0..2);
    assert_eq!(v.scope_to_value(Rank::Mid, 0, 5), 2..3);
    assert_eq!(v.scope_to_value(Rank::High, 0, 5), 3..5);
    assert_eq!(v.scope_to_value(Rank::Mid, 3, 5), 3..3);
    assert_eq!(v.scope_to_value(Rank::Mid, 0, 2), 2..2);

    let mut nullable = vec![None, None];
    nullable.extend(by_number.map(Some));
    let v = VerifiedColumn::<Option<Rank>>::from_values(&nullable);
    assert_eq!(v.scope_to_value(None, 0, 7), 0..2);
    assert_eq!(v.scope_to_value(Some(Rank::Low), 0, 7), 2..4);
    assert_eq!(v.scope_to_value(Some(Rank::High), 0, 7), 5..7);
    assert_eq!(v.scope_to_value(Some(Rank::Mid), 0, 2), 2..2);

    // many of them, in many slabs: made with another value after each,
    // which is then taken out again
    let n = 3000;
    let ranks = [Rank::Low, Rank::Mid, Rank::High];
    let mut v = VerifiedColumn::<Rank>::with_max_segments(3);
    for i in 0..n {
        v.push(ranks[i * 3 / n]);
        v.push(ranks[(i * 3 / n + 1) % 3]);
    }
    for i in (0..n).rev() {
        v.remove(2 * i + 1);
    }
    assert!(v.slab_count() > 30);
    assert_eq!(
        v.to_vec()[998..1002],
        [Rank::Low, Rank::Low, Rank::Mid, Rank::Mid]
    );
    assert_eq!(v.scope_to_value(Rank::Low, 0, n), 0..1000);
    assert_eq!(v.scope_to_value(Rank::Mid, 0, n), 1000..2000);
    assert_eq!(v.scope_to_value(Rank::High, 0, n), 2000..3000);
    assert_eq!(v.scope_to_value(Rank::Mid, 1500, 2500), 1500..2000);
    assert_eq!(v.scope_to_value(Rank::Low, 1500, 2500), 1500..1500);
}

/// The runs of equal values of `model`, each as long as it can be: a
/// remap calls its function with the value of each, once, wherever the
/// column's slabs end.
fn runs_of<T: Kind>(model: &[T]) -> Vec<(usize, T)> {
    let mut runs: Vec<(usize, T)> = vec![];
    for x in model {
        match runs.last_mut() {
            Some((count, y)) if y == x => *count += 1,
            _ => runs.push((1, x.clone())),
        }
    }
    runs
}

/// A remap of both columns by `f`, which is given how many calls came
/// before and the value. Both call it once for each run of equal values,
/// in the column's order, and each place has what the call for its run gave:
/// the columns are then checked as columns of those values, which are
/// returned. A remapped column's slabs are cut as a loaded one's are, so
/// it is checked as one of those ([`check_loaded`]).
fn check_remap<T: Kind>(
    v: &mut VerifiedColumn<T>,
    c: &mut Column<T>,
    model: &[T],
    f: impl Fn(usize, &T) -> T,
) -> Vec<T> {
    let runs = runs_of(model);
    let (seen_v, seen_c) = (RefCell::new(vec![]), RefCell::new(vec![]));
    v.remap(|x: T| {
        let mut seen = seen_v.borrow_mut();
        seen.push(x.clone());
        f(seen.len() - 1, &x)
    });
    c.remap(|x: T| {
        let mut seen = seen_c.borrow_mut();
        seen.push(x.clone());
        f(seen.len() - 1, &x)
    });
    let called: Vec<T> = runs.iter().map(|(_, x)| x.clone()).collect();
    assert_eq!(seen_v.into_inner(), called);
    assert_eq!(seen_c.into_inner(), called);
    let mut want = vec![];
    for (i, (count, x)) in runs.iter().enumerate() {
        let y = f(i, x);
        want.extend((0..*count).map(|_| y.clone()));
    }
    check_loaded(v, c, &want);
    want
}

/// Remaps of columns in one slab and in many, also of ones loaded from
/// saved bytes, by functions of several kinds: one that changes nothing,
/// one that gives one value for all, ones that give few values or many
/// for the column's values, one that turns the column's values round
/// among themselves, and one that gives another value at each call. The
/// remapped column keeps its segment budget, which the edits after show.
fn remaps<T: Kind>() {
    let mut rng = Rng(0x4e3a);
    let (a, b) = (T::fresh(&mut rng), T::fresh(&mut rng));
    let models: Vec<Vec<T>> = vec![
        vec![],
        vec![a.clone()],
        vec![a.clone(); 300],
        (0..400).map(|i| [&a, &b][i / 9 % 2].clone()).collect(),
        (0..400).map(|i| [&a, &b][i % 2].clone()).collect(),
        (0..500).map(|_| T::fresh(&mut rng)).collect(),
        rng.values(300),
        rng.values(700),
    ];
    for (i, model) in models.iter().enumerate() {
        let mut keys: Vec<T> = vec![];
        for x in model {
            if !keys.contains(x) {
                keys.push(x.clone());
            }
        }
        let key = |x: &T| keys.iter().position(|k| k == x).unwrap();
        let few: Vec<T> = (0..3).map(|_| T::fresh(&mut rng)).collect();
        let many: Vec<T> = (0..keys.len() + 7).map(|_| T::fresh(&mut rng)).collect();
        for (budget, loaded) in [(3, false), (3, true), (16, false), (64, true)] {
            for kind in 0..6 {
                let (mut v, mut c) = if budget < 64 {
                    both_in_slabs(&mut rng, model, budget)
                } else {
                    both_with(model, budget)
                };
                if loaded {
                    let bytes = v.save();
                    let opts = LoadOpts::new().with_max_segments(budget);
                    v = VerifiedColumn::<T>::load_with(&bytes, opts).unwrap();
                    c = Column::<T>::load_with(&bytes, opts).unwrap();
                }
                assert!(i != 5 || budget > 3 || loaded || v.slab_count() > 40);
                let mut now = match kind {
                    0 => check_remap(&mut v, &mut c, model, |_, x| x.clone()),
                    1 => check_remap(&mut v, &mut c, model, |_, _| b.clone()),
                    2 => check_remap(&mut v, &mut c, model, |_, x| few[key(x) % 3].clone()),
                    3 => check_remap(&mut v, &mut c, model, |_, x| many[key(x)].clone()),
                    4 => check_remap(&mut v, &mut c, model, |_, x| {
                        keys[(key(x) + 1) % keys.len()].clone()
                    }),
                    _ => check_remap(&mut v, &mut c, model, |n, _| many[n % many.len()].clone()),
                };
                assert_eq!(now.len(), model.len());
                if kind == 0 {
                    assert_eq!(&now, model);
                }
                // a column like any other after it: edits, a second
                // remap, and what it saves loads
                for _ in 0..3 {
                    let at = rng.below(now.len() + 1);
                    let del = rng.below((now.len() - at).min(4) + 1);
                    let put: Vec<T> = rng.values(40);
                    v.splice(at, del, &lent(&put));
                    c.splice(at, del, put.iter().cloned());
                    now.splice(at..at + del, put);
                }
                check_loaded(&v, &c, &now);
                let again = check_remap(&mut v, &mut c, &now, |n, x| {
                    if n % 2 == 0 {
                        x.clone()
                    } else {
                        a.clone()
                    }
                });
                check_loads(&v, &c, &again);
            }
        }
    }
}

#[test]
fn a_remap_gives_each_run_what_the_function_gave() {
    for_each_kind!(remaps);
}

/// A column that has been emptied, and one that never held a value: the
/// function is not called.
#[test]
fn a_remap_of_no_values() {
    fn none<T: Kind>() {
        let mut rng = Rng(0x0e3a);
        let (mut v, mut c) = both(&rng.values::<T>(200));
        v.clear();
        c.clear();
        v.remap(|_| unreachable!());
        c.remap(|_| unreachable!());
        check(&v, &c, &[]);
        let (mut v, mut c) = (VerifiedColumn::<T>::new(), Column::<T>::new());
        v.remap(|_| unreachable!());
        c.remap(|_| unreachable!());
        check(&v, &c, &[]);
        let x = T::fresh(&mut rng);
        v.push(x.lend());
        c.push(x.clone());
        check(&v, &c, &[x]);
    }
    for_each_kind!(none);
}

/// Byte strings made longer, shorter and empty, nulls made strings and
/// strings nulls: the function is given a string of its own for each run,
/// and what it hands back is copied into the column.
#[test]
fn a_remap_of_byte_strings() {
    let mut rng = Rng(0xb3a9);
    let model: Vec<Vec<u8>> = (0..2000)
        .map(|i| vec![(i / 3 % 251) as u8; rng.below(40)])
        .collect();
    let (mut v, mut c) = both_with(&model, 16);
    assert!(v.slab_count() > 10);
    let longer = check_remap(&mut v, &mut c, &model, |_, x| {
        let mut y = x.clone();
        y.extend_from_slice(&[7; 300]);
        y
    });
    assert!(longer.iter().all(|x| x.len() >= 300));
    let shorter = check_remap(&mut v, &mut c, &longer, |_, x| x[..x.len() - 299].to_vec());
    assert!(shorter
        .iter()
        .zip(&model)
        .all(|(s, m)| s[..m.len()] == m[..]));
    let empty = check_remap(&mut v, &mut c, &shorter, |_, _| vec![]);
    assert_eq!(empty, vec![vec![]; 2000]);
    assert!(v.is_only(&[]));

    let model: Vec<Option<Vec<u8>>> = (0..2000)
        .map(|i| match i / 5 % 3 {
            0 => None,
            k => Some(vec![k as u8; i % 7]),
        })
        .collect();
    let (mut v, mut c) = both_with(&model, 16);
    let turned = check_remap(&mut v, &mut c, &model, |_, x| match x {
        None => Some(b"null".to_vec()),
        Some(x) if x.is_empty() => Some(vec![]),
        Some(_) => None,
    });
    assert_eq!(turned.iter().filter(|x| x.is_none()).count(), 1140);
    let nulls = check_remap(&mut v, &mut c, &turned, |_, _| None);
    assert!(v.is_only(None) && nulls.iter().all(|x| x.is_none()));
}

/// A function that panics leaves the column as it was, in both APIs:
/// the new column is put in the old one's place only when it is whole.
#[cfg(any(target_pointer_width = "64", panic = "unwind"))]
#[test]
fn a_remap_whose_function_panics() {
    fn stopped<T: Kind>() {
        let mut rng = Rng(0x9a1c);
        let model: Vec<T> = rng.values(1200);
        let (mut v, mut c) = both_with(&model, 8);
        let runs = runs_of(&model).len();
        for stop in [0, 1, runs / 2, runs - 1] {
            let calls = Cell::new(0);
            let f = |x: T| {
                calls.set(calls.get() + 1);
                if calls.get() > stop {
                    panic!("no more");
                }
                x
            };
            let new = panic_of(std::panic::AssertUnwindSafe(|| v.remap(f))).unwrap();
            assert_eq!((new.as_str(), calls.get()), ("no more", stop + 1));
            calls.set(0);
            let old = panic_of(std::panic::AssertUnwindSafe(|| c.remap(f))).unwrap();
            assert_eq!((old.as_str(), calls.get()), ("no more", stop + 1));
            check(&v, &c, &model);
        }
        // and takes a remap, and an edit, as before
        let x = T::fresh(&mut rng);
        let all = check_remap(&mut v, &mut c, &model, |_, _| x.clone());
        v.push(x.lend());
        c.push(x.clone());
        check_loaded(&v, &c, &vec![x; all.len() + 1]);
    }
    for_each_kind!(stopped);
}

/// The iterators of both APIs over the values `a..b`, read in each way
/// the standard library's `Iterator` has: they give the values of `model`
/// there and count the same number left at every step.
fn check_iters<T: Kind>(
    v: &VerifiedColumn<T>,
    c: &Column<T>,
    model: &[T],
    (a, b): (usize, usize),
    rng: &mut Rng,
) {
    let want = &model[a..b];
    let new = || v.iter_range(a, b);
    let old_it = || c.iter_range(a..b);
    let same = |x: &VerifiedIter<'_, T>, y: &hexane::Iter<'_, T>, left: usize| {
        assert_eq!((x.len(), y.len()), (left, left));
        assert_eq!(x.size_hint(), (left, Some(left)));
        assert_eq!(format!("{x:?}"), format!("Verified{y:?}"));
    };

    // one value at a time, and nothing after the last
    let (mut x, mut y) = (new(), old_it());
    for (i, m) in want.iter().enumerate() {
        same(&x, &y, want.len() - i);
        assert_eq!(x.next().map(T::keep).as_ref(), Some(m));
        assert_eq!(y.next().map(old).as_ref(), Some(m));
    }
    for _ in 0..2 {
        same(&x, &y, 0);
        assert!(x.next().is_none() && y.next().is_none());
    }

    // all of them at once
    assert_eq!(new().map(T::keep).collect::<Vec<_>>(), want);
    assert_eq!(new().count(), want.len());
    assert_eq!(new().last().map(T::keep).as_ref(), want.last());
    let folded = new().fold(Vec::new(), |mut all, x| {
        all.push(T::keep(x));
        all
    });
    assert_eq!(folded, want);
    let mut each = Vec::new();
    new().for_each(|x| each.push(T::keep(x)));
    assert_eq!(each, want);
    let mut looped = Vec::new();
    for x in new() {
        looped.push(T::keep(x));
    }
    assert_eq!(looped, want);

    // steps of one length: each gives the value that far on, and a step
    // past the last value uses the iterator up
    for k in [0, 1, 2, 5, 63, 64, 1000, usize::MAX - 1, usize::MAX] {
        let (mut x, mut y) = (new(), old_it());
        let mut at = 0usize;
        loop {
            let to = at.checked_add(k).filter(|i| *i < want.len());
            assert_eq!(x.nth(k).map(T::keep).as_ref(), to.map(|i| &want[i]));
            assert_eq!(y.nth(k).map(old).as_ref(), to.map(|i| &want[i]));
            match to {
                Some(i) => at = i + 1,
                None => break,
            }
            same(&x, &y, want.len() - at);
        }
        same(&x, &y, 0);
        assert!(x.next().is_none() && x.nth(0).is_none() && x.nth(k).is_none());
        same(&x, &y, 0);
    }

    // steps of every length mixed with single values, and copies made on
    // the way, each of which reads on from where it was made
    let (mut x, mut y) = (new(), old_it());
    let mut at = 0usize;
    let mut copies = vec![(x.clone(), 0)];
    while at < want.len() {
        let left = want.len() - at;
        if rng.below(3) == 0 {
            copies.push((x.clone(), at));
        }
        if rng.below(3) == 0 {
            assert_eq!(x.next().map(T::keep).as_ref(), Some(&want[at]));
            assert_eq!(y.next().map(old).as_ref(), Some(&want[at]));
            at += 1;
        } else {
            let k = match rng.below(4) {
                0 => rng.below(3),
                1 => rng.below(left + 2),
                2 => rng.below(200),
                _ => rng.below(left / 4 + 1),
            };
            let to = Some(at + k).filter(|i| *i < want.len());
            assert_eq!(x.nth(k).map(T::keep).as_ref(), to.map(|i| &want[i]));
            assert_eq!(y.nth(k).map(old).as_ref(), to.map(|i| &want[i]));
            at = to.map_or(want.len(), |i| i + 1);
        }
        same(&x, &y, want.len() - at);
    }
    copies.push((x.clone(), at));
    for (i, (mut copy, at)) in copies.into_iter().enumerate() {
        assert_eq!(copy.len(), want.len() - at);
        if i % 2 == 0 {
            assert_eq!(copy.map(T::keep).collect::<Vec<_>>(), &want[at..]);
        } else {
            // and steps on its own
            let k = rng.below(want.len() - at + 1);
            assert_eq!(copy.nth(k).map(T::keep).as_ref(), want.get(at + k));
            assert_eq!(copy.next().map(T::keep).as_ref(), want.get(at + k + 1));
        }
    }

    // through the standard library's adaptors
    let kept = |it: &mut dyn Iterator<Item = Get<'_, T>>| it.map(T::keep).collect::<Vec<T>>();
    let s = rng.below(want.len() + 2);
    assert_eq!(
        kept(&mut new().skip(s).take(20)),
        want.iter().skip(s).take(20).cloned().collect::<Vec<_>>()
    );
    assert_eq!(
        kept(&mut new().step_by(7)),
        want.iter().step_by(7).cloned().collect::<Vec<_>>()
    );
    let mut by_ref = new();
    assert_eq!(
        kept(&mut by_ref.by_ref().take(s)),
        &want[..s.min(want.len())]
    );
    assert_eq!(kept(&mut by_ref), &want[s.min(want.len())..]);
    let pairs: Vec<(T, T)> = new()
        .zip(v.iter())
        .map(|(p, q)| (T::keep(p), T::keep(q)))
        .collect();
    let model_pairs: Vec<(T, T)> = want.iter().cloned().zip(model.iter().cloned()).collect();
    assert_eq!(pairs, model_pairs);
    assert_eq!(
        new().enumerate().last().map(|(i, x)| (i, T::keep(x))),
        want.iter().cloned().enumerate().last()
    );
    if let Some(x) = want.get(rng.below(want.len() + 1)) {
        assert_eq!(
            new().position(|y| T::keep(y) == *x),
            want.iter().position(|y| y == x)
        );
        assert!(new().any(|y| T::keep(y) == *x));
    }
    let mut ahead = new().peekable();
    let mut seen = 0;
    while let Some(next) = ahead.peek().copied() {
        assert_eq!(ahead.next().map(T::keep), Some(T::keep(next)));
        seen += 1;
    }
    assert_eq!(seen, want.len());
}

/// Columns in one slab and in many, as built and as loaded: over every
/// window the iterator reads what `Column`'s does, in every way the
/// standard library has of reading one. And a `for` loop over the column
/// itself reads all of it.
fn iterators<T: Kind>() {
    let mut rng = Rng(0x17e2);
    let (p, q) = (T::fresh(&mut rng), T::fresh(&mut rng));
    let models: Vec<Vec<T>> = vec![
        vec![],
        vec![p.clone()],
        vec![p.clone(), q.clone()],
        vec![p.clone(); 300],
        (0..400).map(|i| [&p, &q][i / 9 % 2].clone()).collect(),
        (0..400).map(|i| [&p, &q][i % 2].clone()).collect(),
        (0..1500).map(|_| T::fresh(&mut rng)).collect(),
        rng.values(300),
        rng.values(900),
    ];
    for (i, model) in models.iter().enumerate() {
        let n = model.len();
        for (budget, loaded) in [(3, false), (3, true), (16, false), (64, true)] {
            let (mut v, mut c) = if budget < 64 {
                both_in_slabs(&mut rng, model, budget)
            } else {
                both_with(model, budget)
            };
            if loaded {
                let bytes = v.save();
                let opts = LoadOpts::new().with_max_segments(budget);
                v = VerifiedColumn::<T>::load_with(&bytes, opts).unwrap();
                c = Column::<T>::load_with(&bytes, opts).unwrap();
            }
            assert!(i != 6 || budget > 3 || loaded || v.slab_count() > 100);
            let mut ws = windows(&mut rng, &v.slab_lens(), n);
            if n > 600 {
                // the long ones are read whole a few times only
                ws.retain(|(a, b)| b - a < 300 || rng.below(4) == 0);
                ws.push((0, n));
            }
            for w in ws {
                check_iters(&v, &c, model, w, &mut rng);
            }
            let mut all = Vec::new();
            for x in &v {
                all.push(T::keep(x));
            }
            assert_eq!(&all, model);
            let it = (&v).into_iter();
            assert_eq!(it.len(), n);
            assert_eq!(it.count(), n);
        }
    }
}

#[test]
fn an_iterator_reads_as_the_standard_library_has_it() {
    for_each_kind!(iterators);
}

/// A column that edits have made: its iterators read what it holds now.
/// (An iterator borrows its column, so none lives through an edit.)
#[test]
fn an_iterator_after_edits() {
    fn edited<T: Kind>() {
        let mut rng = Rng(0xed17);
        for budget in [2, 5, 64] {
            let mut model: Vec<T> = rng.values(200);
            let (mut v, mut c) = both_with(&model, budget);
            for round in 0..40 {
                let at = rng.below(model.len() + 1);
                let del = rng.below((model.len() - at).min(30) + 1);
                let put: Vec<T> = rng.values(60);
                v.splice(at, del, &lent(&put));
                c.splice(at, del, put.iter().cloned());
                model.splice(at..at + del, put);
                if round % 13 == 12 {
                    v.truncate(model.len() / 2);
                    c.truncate(model.len() / 2);
                    model.truncate(model.len() / 2);
                }
                let n = model.len();
                let a = rng.below(n + 1);
                let b = a + rng.below(n - a + 1);
                check_iters(&v, &c, &model, (a, b), &mut rng);
                check_iters(&v, &c, &model, (0, n), &mut rng);
            }
            v.clear();
            c.clear();
            check_iters(&v, &c, &[], (0, 0), &mut rng);
        }
    }
    for_each_kind!(edited);
}

/// An iterator of no column, which `Default` makes: it has nothing to
/// give, however it is asked.
#[test]
fn an_iterator_of_no_column() {
    fn none<T: Kind>() {
        let mut it = VerifiedIter::<T>::default();
        assert_eq!(format!("{it:?}"), "VerifiedIter { pos: 0, items_left: 0 }");
        let old_it = hexane::Iter::<T>::default();
        assert_eq!(format!("{it:?}"), format!("Verified{old_it:?}"));
        for _ in 0..2 {
            assert_eq!((it.len(), it.size_hint()), (0, (0, Some(0))));
            assert!(it.next().is_none());
            assert!(it.nth(0).is_none() && it.nth(7).is_none());
            assert!(it.nth(usize::MAX).is_none());
            assert!(it.clone().next().is_none());
        }
        assert_eq!(it.fold(3, |n, _| n + 1), 3);
        assert_eq!(VerifiedIter::<T>::default().count(), 0);
        assert!(VerifiedIter::<T>::default().last().is_none());
    }
    for_each_kind!(none);
}

/// `fold` reads a run at a time: its function is called once for each
/// value all the same, in order, and a panic in it leaves the column as
/// it was.
#[test]
fn a_fold_calls_its_function_for_each_value() {
    let (v, _) = both_with(&[7u64, 7, 7, 8, 8, 9, 7, 7], 3);
    let calls = Cell::new(0);
    let sum = v.iter().fold(0, |s, x| {
        calls.set(calls.get() + 1);
        s * 10 + x
    });
    assert_eq!((sum, calls.get()), (77788977, 8));
    #[cfg(panic = "unwind")]
    {
        let text = panic_of(std::panic::AssertUnwindSafe(|| {
            v.iter_range(1, 8).fold(0, |s, x| {
                assert!(x != 9, "nine");
                s + x
            })
        }));
        assert_eq!(text.as_deref(), Some("nine"));
        assert_eq!(new_values(&v), [7u64, 7, 7, 8, 8, 9, 7, 7]);
    }
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

/// A load with options, by both APIs: the length asked for, a fill value,
/// the most runs of a slab. They give the same column, in the same slabs,
/// or refuse with the same error. The column's values, or the error.
fn load_both_with<T: Kind>(
    bytes: &[u8],
    length: Option<usize>,
    fill: Option<&T>,
    max_segments: Option<usize>,
) -> Result<(VerifiedColumn<T>, Column<T>), PackError> {
    let opts = || {
        let mut opts = LoadOpts::new();
        if let Some(n) = length {
            opts = opts.with_length(n);
        }
        if let Some(n) = max_segments {
            opts = opts.with_max_segments(n);
        }
        opts
    };
    let (v, c) = match fill {
        Some(x) => (
            VerifiedColumn::<T>::load_with(bytes, opts().with_fill(x.lend())),
            Column::<T>::load_with(bytes, opts().with_fill(x.as_column_ref())),
        ),
        None => (
            VerifiedColumn::<T>::load_with(bytes, opts()),
            Column::<T>::load_with(bytes, opts()),
        ),
    };
    match (v, c) {
        (Ok(v), Ok(c)) => {
            assert_eq!(v.len(), c.len());
            assert_eq!(v.slab_count(), c.slab_count());
            assert_eq!(v.save(), c.save());
            Ok((v, c))
        }
        (Err(v), Err(c)) => {
            assert_eq!(
                v.to_string(),
                c.to_string(),
                "the loaders refuse {bytes:?} differently"
            );
            Err(v)
        }
        (v, c) => panic!(
            "the loaders disagree on {bytes:?} ({length:?}, {fill:?}, {max_segments:?}): {:?}, {:?}",
            v.map(|v| v.len()),
            c.map(|c| c.len())
        ),
    }
}

/// A saved column loaded with options. It loads with its own length asked
/// for, or none, and with no other; a fill value is of no use to it unless
/// it is empty; and it has the same values whatever a slab may hold. What
/// is loaded is edited like any other column.
fn load_options<T: Kind>() {
    let mut rng = Rng(0x10ad);
    let x = T::fresh(&mut rng);
    let models: Vec<Vec<T>> = vec![
        vec![],
        vec![x.clone()],
        vec![x.clone(); 1000],
        rng.values(300),
        (0..2000).map(|_| T::fresh(&mut rng)).collect(),
    ];
    for model in models {
        let len = model.len();
        let bytes = both(&model).0.save();
        assert_eq!(bytes.is_empty(), model.is_empty());
        let lengths = [
            None,
            Some(len),
            Some(len + 1),
            Some(len.saturating_sub(1)),
            Some(0),
            Some(7),
            Some(usize::MAX),
        ];
        for length in lengths {
            for fill in [None, Some(&x)] {
                for max_segments in [None, Some(2), Some(3), Some(16), Some(1000)] {
                    let r = load_both_with(&bytes, length, fill, max_segments);
                    // what the options ask for
                    let want = match (length, fill) {
                        (Some(n), Some(x)) if len == 0 && n <= 1000 => Ok(vec![x.clone(); n]),
                        (Some(n), _) if n != len => Err((len, n)),
                        _ => Ok(model.clone()),
                    };
                    match (r, want) {
                        (Ok((mut v, mut c)), Ok(want)) => {
                            check_loaded(&v, &c, &want);
                            // and is edited as a column made of the values is
                            let mut m = want;
                            let at = rng.below(m.len() + 1);
                            let ins: Vec<T> = rng.values(20);
                            v.splice(at, 0, &lent(&ins));
                            c.splice(at, 0, ins.iter().cloned());
                            m.splice(at..at, ins);
                            check_loaded(&v, &c, &m);
                        }
                        (Err(e), Err((has, asked))) => {
                            assert!(
                                matches!(e, PackError::InvalidLength(a, b) if a == has && b == asked),
                                "{e}"
                            );
                        }
                        (r, want) => panic!(
                            "{length:?}, {max_segments:?}: loaded {:?}, not {:?}",
                            r.map(|(v, _)| v.len()),
                            want.map(|m| m.len())
                        ),
                    }
                }
            }
        }
    }
}

#[test]
fn loads_with_options_agree() {
    for_each_kind!(load_options);
}

/// No bytes, a length and a fill value: the column `fill` makes, in both
/// APIs, for the lengths around a slab's.
fn load_fills<T: Kind>() {
    let mut rng = Rng(0xf1110ad);
    for n in [0, 1, 2, 3, 63, 64, 65, 1000] {
        let x = T::fresh(&mut rng);
        for max_segments in [None, Some(2), Some(64)] {
            let (v, c) = load_both_with(&[], Some(n), Some(&x), max_segments).unwrap();
            let model = vec![x.clone(); n];
            check(&v, &c, &model);
            assert!(v.is_only(x.lend()));
            assert_eq!(v.save(), VerifiedColumn::<T>::fill(n, x.lend()).save());
            check_loads(&v, &c, &model);
        }
        // a fill value and no length fills nothing
        let (v, c) = load_both_with(&[], None, Some(&x), None).unwrap();
        check(&v, &c, &[]);
    }
}

#[test]
fn loads_of_a_fill_agree() {
    for_each_kind!(load_fills);
}

/// The longest fill, and longer ones. A fill is one run in one column, so
/// it is at most `i64::MAX` values and at most `usize::MAX - 3`, whichever
/// is less on the target: that many load, in both APIs, and one more is an
/// error and no panic. (No `catch_unwind` here: wasm32 aborts on a panic,
/// which would end this test binary.)
#[test]
fn a_fill_too_long_is_an_error() {
    let most = (i64::MAX as u64).min((usize::MAX - 3) as u64) as usize;
    fn longest<T: Kind>(most: usize, x: T) {
        let (v, _) = load_both_with(&[], Some(most), Some(&x), None).unwrap();
        assert_eq!(v.len(), most);
        assert_eq!(v.get(most - 1).map(T::keep), Some(x.clone()));
        assert!(v.get(most).is_none());
        assert!(v.is_only(x.lend()));
        for n in [most + 1, most + 2, usize::MAX - 1, usize::MAX] {
            for max_segments in [None, Some(2)] {
                let e = load_both_with(&[], Some(n), Some(&x), max_segments)
                    .err()
                    .unwrap();
                assert!(matches!(e, PackError::InvalidLength(0, m) if m == n), "{e}");
            }
        }
    }
    longest::<u64>(most, 7);
    longest::<Option<i64>>(most, None);
    longest::<Option<i64>>(most, Some(-1));
    longest::<bool>(most, true);
    longest::<bool>(most, false);
    longest::<Action>(most, Action::Delete);
    longest::<Vec<u8>>(most, b"ab".to_vec());
    longest::<Option<Vec<u8>>>(most, None);
}

/// Bytes at random and saved columns with a byte changed, loaded with a
/// length asked for and a fill value: the two loaders take the same ones,
/// as the same values. Both kinds are seen.
fn fuzz_loads_with<T: Kind>() {
    let mut rng = Rng(0x5eed0f);
    let (mut took, mut refused) = (0, 0);
    for round in 0..3_000 {
        let model: Vec<T> = rng.values(12);
        let mut bytes = both(&model).0.save();
        if round % 2 == 0 && !bytes.is_empty() {
            let at = rng.below(bytes.len());
            match rng.below(3) {
                0 => bytes[at] ^= 1 << rng.below(8),
                1 => {
                    bytes.remove(at);
                }
                _ => bytes.insert(at, rng.next() as u8),
            }
        }
        let length = match rng.below(4) {
            0 => None,
            1 => Some(rng.below(14)),
            _ => Some(model.len()),
        };
        let x = T::fresh(&mut rng);
        let fill = (rng.below(2) == 0).then_some(&x);
        let max_segments = [None, Some(2), Some(5)][rng.below(3)];
        match load_both_with(&bytes, length, fill, max_segments) {
            Ok((v, c)) => {
                // a changed byte can make a run of billions: read those
                // in places
                if v.len() <= 100_000 {
                    assert_eq!(new_values(&v), old_values(&c));
                } else {
                    for i in [0, 1, v.len() / 2, v.len() - 1] {
                        assert_eq!(v.get(i).map(T::keep), c.get(i).map(old));
                    }
                }
                if let Some(n) = length {
                    assert_eq!(v.len(), n);
                }
                took += 1;
            }
            Err(_) => refused += 1,
        }
    }
    assert!(
        took > 100 && refused > 100,
        "{took} taken, {refused} refused"
    );
}

#[test]
fn loads_with_options_refuse_the_same_bytes() {
    for_each_kind!(fuzz_loads_with);
}

/// `MaybeFill` is a public trait, which a client's type can implement.
/// It has a specification now, for Verus; a client compiled without Verus
/// writes what it wrote before, the one method.
#[test]
fn a_fill_state_of_a_clients_own() {
    struct Seven;
    impl MaybeFill<u64> for Seven {
        fn fill_value(self) -> Option<u64> {
            Some(7)
        }
    }
    assert_eq!(Seven.fill_value(), Some(7));
    assert_eq!(MaybeFill::<u64>::fill_value(NoFill), None);
    let opts = LoadOpts::default().with_length(2).with_fill(7u64);
    let twice = [opts, opts].map(|o| VerifiedColumn::<u64>::load_with(&[], o).unwrap());
    assert_eq!(twice[0].to_vec(), [7, 7]);
    assert_eq!(twice[1].to_vec(), [7, 7]);
}

/// Bytes that hold more values than a column can. A column holds
/// `usize::MAX - 3` values, and bytes of that many load, in both APIs;
/// bytes of one more are an error, and so are bytes of more than a
/// `usize` counts, whatever the options: no load panics. (That is not
/// proved. No `catch_unwind` here either, for wasm32.)
#[test]
fn bytes_of_too_many_values_are_an_error() {
    fn uleb(mut n: u128) -> Vec<u8> {
        let mut out = vec![];
        loop {
            let b = (n & 0x7f) as u8;
            n >>= 7;
            if n == 0 {
                out.push(b);
                return out;
            }
            out.push(b | 0x80);
        }
    }
    // the count of a run of equal numbers has a sign
    fn sleb(n: i64) -> Vec<u8> {
        let mut out = uleb(n as u128);
        if out.last().unwrap() & 0x40 != 0 {
            *out.last_mut().unwrap() |= 0x80;
            out.push(0);
        }
        out
    }
    // `total` numbers in runs of 7s and 8s, each as long as a run can be
    // (a run of one is no run), and the last of them
    fn numbers(total: u128) -> (Vec<u8>, u64) {
        let (mut bytes, mut left, mut value) = (vec![], total, 8u8);
        while left > 0 {
            let mut n = left.min(i64::MAX as u128);
            if left - n == 1 {
                n -= 1;
            }
            value ^= 15;
            bytes.extend(sleb(n as i64));
            bytes.push(value);
            left -= n;
        }
        (bytes, value as u64)
    }
    fn case<T: Kind>(bytes: &[u8], total: u128, first: T, last: T) {
        let most = usize::MAX - 3;
        for length in [None, Some(most), Some(usize::MAX), Some(0)] {
            for fill in [None, Some(&first)] {
                let r = load_both_with(bytes, length, fill, None);
                let fits = total <= most as u128 && length.is_none_or(|n| n as u128 == total);
                match r {
                    Ok((v, _)) => {
                        assert!(fits, "loaded {total} values, asked for {length:?}");
                        assert_eq!(v.len() as u128, total);
                        assert_eq!(v.get(0).map(T::keep), Some(first.clone()));
                        assert_eq!(v.get(v.len() - 1).map(T::keep), Some(last.clone()));
                    }
                    Err(e) => {
                        // (of the length, or of a count past a `usize`)
                        assert!(!fits, "{total} values: {e}");
                    }
                }
            }
        }
    }
    let most = (usize::MAX - 3) as u128;
    let totals = [most, most + 1, most + 3, most + 4, most * 3];
    for total in totals {
        let (bytes, last) = numbers(total);
        case::<u64>(&bytes, total, 7, last);
        case::<Option<u64>>(&bytes, total, Some(7), Some(last));
        // nulls, and `bool`s, are counted with no sign
        if total <= u64::MAX as u128 {
            let mut nulls = vec![0];
            nulls.extend(uleb(total));
            case::<Option<u64>>(&nulls, total, None, None);
            case::<Option<Vec<u8>>>(&nulls, total, None, None);
            case::<bool>(&nulls, total, true, true);
        }
    }
    // two runs of `bool`s, of more than a `usize` counts together
    let mut bools = uleb(most);
    bools.extend(uleb(most));
    case::<bool>(&bools, 2 * most, false, true);
}

/// Columns of both APIs with slabs to look at, their values, and whether
/// they are built of values and not loaded: of one slab and of many,
/// built at once, edited in small slabs, loaded in small slabs, and
/// filled. None is empty.
#[allow(clippy::type_complexity)]
fn layouts<T: Kind>() -> Vec<(VerifiedColumn<T>, Column<T>, Vec<T>, bool)> {
    let mut rng = Rng(0x51ab);
    let x = T::fresh(&mut rng);
    let mut models: Vec<Vec<T>> = vec![
        vec![x.clone()],
        vec![x.clone(); 1000],
        (0..3000).map(|_| T::fresh(&mut rng)).collect(),
    ];
    for _ in 0..4 {
        let mut model: Vec<T> = rng.values(300);
        model.push(T::fresh(&mut rng));
        models.push(model);
    }
    let mut out = vec![];
    for model in models {
        let (v, c) = both(&model);
        out.push((v, c, model, true));
    }
    for budget in [2, 3, 4, 8] {
        let mut v = VerifiedColumn::<T>::with_max_segments(budget);
        let mut c = Column::<T>::with_max_segments(budget);
        let mut model = vec![T::fresh(&mut rng)];
        v.push(model[0].lend());
        c.push(model[0].clone());
        for _ in 0..30 {
            // after the first value, which stays
            let at = 1 + rng.below(model.len());
            let del = rng.below((model.len() - at).min(8) + 1);
            let ins: Vec<T> = rng.values(20);
            v.splice(at, del, &lent(&ins));
            c.splice(at, del, ins.iter().cloned());
            model.splice(at..at + del, ins);
        }
        let (loaded_v, loaded_c) =
            load_both_with::<T>(&v.save(), None, None, Some(budget)).unwrap();
        out.push((v, c, model.clone(), true));
        out.push((loaded_v, loaded_c, model, false));
    }
    let mut v = VerifiedColumn::<T>::fill(500, x.lend());
    let mut c = Column::<T>::fill(500, x.as_column_ref());
    let mut model = vec![x.clone(); 500];
    out.push((v.clone(), c.clone(), model.clone(), true));
    let y = T::fresh(&mut rng);
    v.insert(250, y.lend());
    c.insert(250, y.clone());
    model.insert(250, y);
    out.push((v, c, model, true));
    out
}

/// The lines `dump_slabs` prints of a column of the values `model`, going
/// by `slab_lens` and `slab_segments`: of each slab where it starts, how
/// many values and how many runs it holds, and the first and the last of
/// its runs, which are the equal values the slab starts with and those it
/// ends with.
fn slab_lines<T: Kind>(v: &VerifiedColumn<T>, model: &[T]) -> Vec<String>
where
    for<'a> Get<'a, T>: Debug,
{
    let mut lines = vec![];
    let mut offset = 0;
    for (i, (len, segments)) in v.slab_lens().into_iter().zip(v.slab_segments()).enumerate() {
        let slab = &model[offset..offset + len];
        let first = slab.first().map(|x| Run {
            count: slab.iter().take_while(|y| *y == x).count(),
            value: x.lend(),
        });
        let last = slab.last().map(|x| Run {
            count: slab.iter().rev().take_while(|y| *y == x).count(),
            value: x.lend(),
        });
        lines.push(format!(
            "  slab[{i}]: offset={offset} len={len} segments={segments} first={first:?} last={last:?}"
        ));
        offset += len;
    }
    assert_eq!(offset, model.len());
    lines
}

/// The slabs of the columns of `layouts` pass every check, and are the
/// same in both APIs; what `dump_slabs` is to print of them goes to
/// `lines`, after a line that names the column.
fn layout_lines<T: Kind>(lines: &mut Vec<String>)
where
    for<'a> Get<'a, T>: Debug,
{
    for (i, (v, c, model, built)) in layouts::<T>().iter().enumerate() {
        check_with(v, c, model, *built);
        assert_eq!(v.slab_count(), c.slab_count());
        lines.push(format!("== {} {i}", std::any::type_name::<T>()));
        lines.extend(slab_lines(v, model));
    }
}

/// The variable that tells `dump_slabs_child` which API to print with.
#[cfg(not(target_family = "wasm"))]
const DUMP_API: &str = "HEXANE_TEST_DUMP_SLABS";

#[cfg(not(target_family = "wasm"))]
fn dump_layouts<T: Kind>(old: bool)
where
    for<'a> Get<'a, T>: Debug,
{
    for (i, (v, c, _, _)) in layouts::<T>().iter().enumerate() {
        println!("== {} {i}", std::any::type_name::<T>());
        if old {
            c.dump_slabs();
        } else {
            v.dump_slabs();
        }
    }
}

/// Not a test of its own. `dump_slabs_prints_each_slab` runs it in a
/// process of its own, to read what `dump_slabs` prints there; run with
/// the other tests it does nothing.
#[cfg(not(target_family = "wasm"))]
#[test]
fn dump_slabs_child() {
    if let Ok(api) = std::env::var(DUMP_API) {
        let old = api == "old";
        // the test harness has written the test's name on this line
        println!();
        for_each_kind!(dump_layouts, old);
    }
}

/// `dump_slabs` prints a line for each slab, the same line in both APIs,
/// and the line says of the slab what the column holds there. What is
/// printed is not what Verus checks, and the printing itself is outside
/// of what it checks: this test is all there is for it. (It prints to the
/// standard output, so the test reads that of a process that runs
/// `dump_slabs_child`. Not on wasm32, which starts no process.)
#[cfg(not(target_family = "wasm"))]
#[test]
fn dump_slabs_prints_each_slab() {
    let printed = |api: &str| -> Vec<String> {
        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "dump_slabs_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(DUMP_API, api)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout)
            .unwrap()
            .lines()
            .filter(|l| l.starts_with("== ") || l.starts_with("  slab["))
            .map(String::from)
            .collect()
    };
    let mut expected = vec![];
    for_each_kind!(layout_lines, &mut expected);
    // more slabs than columns: many of the columns have several
    let columns = expected.iter().filter(|l| l.starts_with("== ")).count();
    assert_eq!(columns, 17 * 17);
    assert!(expected.len() > 20 * columns);
    for api in ["new", "old"] {
        let lines = printed(api);
        for (i, (said, want)) in lines.iter().zip(&expected).enumerate() {
            assert_eq!(said, want, "line {i} of the {api} API");
        }
        assert_eq!(lines.len(), expected.len(), "lines of the {api} API");
    }
}

/// The layout tools on wasm32 too, where `dump_slabs` is not read: the
/// columns of `layouts` pass every check.
#[test]
fn the_layout_of_a_column() {
    let mut lines = vec![];
    for_each_kind!(layout_lines, &mut lines);
    assert!(lines.iter().any(|l| l.starts_with("  slab[7]")));
}

/// What `check_invariants` says of a loaded column. A load cuts slabs of
/// half the budget and leaves the rest to the last slab, which may be
/// one that an edit would have merged into the slab before it. And
/// `check_invariants` takes every column as one that edits have made: it
/// panics for a column loaded so, in both APIs and with the same words,
/// though nothing is wrong with the column. That is what `Column` does,
/// and the verified API does the same: this test says so, it does not
/// say that it should be so.
#[cfg(panic = "unwind")]
#[test]
fn check_invariants_takes_a_loaded_column_as_edited() {
    let model: Vec<u64> = (0..70).collect();
    let (v, c) = both(&model);
    assert_eq!(v.slab_segments(), [64, 6]);
    check(&v, &c, &model);
    let (v, c) = load_both_with::<u64>(&v.save(), None, None, None).unwrap();
    assert_eq!(v.slab_segments(), [32, 32, 6]);
    assert_eq!(c.slab_segments(), [32, 32, 6]);
    let new = panic_of(std::panic::AssertUnwindSafe(|| v.check_invariants())).unwrap();
    let old = panic_of(std::panic::AssertUnwindSafe(|| c.check_invariants())).unwrap();
    assert_eq!(
        new,
        "slabs 1 and 2 (32 and 6 segments) should have been merged (min 16, max 64)"
    );
    assert_eq!(new, old);
    check_loaded(&v, &c, &model);
    // a column of the same values that is loaded in slabs of a size
    // that leaves the last one long enough passes
    let (w, d) = load_both_with::<u64>(&v.save(), None, None, Some(140)).unwrap();
    assert_eq!(w.slab_segments(), [70]);
    check(&w, &d, &model);
    // an edit away from the short slab leaves it as it is; an edit next
    // to it merges it, and then the column passes
    let (mut v, mut c, mut model) = (v, c, model);
    for (at, x, after) in [(0, 1000, vec![33, 32, 6]), (41, 2000, vec![33, 39])] {
        v.insert(at, x);
        c.insert(at, x);
        model.insert(at, x);
        assert_eq!(v.slab_segments(), after);
        assert_eq!(c.slab_segments(), after);
        let new = panic_of(std::panic::AssertUnwindSafe(|| v.check_invariants()));
        assert_eq!(new.is_some(), after.len() == 3);
        check_loaded(&v, &c, &model);
    }
    check(&v, &c, &model);
}

/// The edge of a merge. An edit merges two neighbours, one of them short
/// of the least a slab should hold, if they fit the budget together, also
/// if they fill it exactly. With one run more it leaves them alone, and
/// `check_invariants` has nothing against them. The neighbours here are
/// the first and the last slab of a loaded column, once the slab between
/// them is deleted.
///
/// (Neighbours that fill the budget exactly and are not merged, which
/// `check_invariants` is to complain of, are in no test: no load and no
/// edit was found to leave such.)
#[test]
fn neighbours_at_the_edge_of_a_merge() {
    for (grow, after) in [(26, vec![64]), (27, vec![59, 6])] {
        let mut model: Vec<u64> = (0..70).collect();
        let (v, _) = both(&model);
        let (mut v, mut c) = load_both_with::<u64>(&v.save(), None, None, None).unwrap();
        // runs of their own in the first slab: the others are left alone
        for k in 0..grow {
            let x = 1000 + k as u64;
            v.insert(0, x);
            c.insert(0, x);
            model.insert(0, x);
        }
        assert_eq!(v.slab_segments(), [32 + grow, 32, 6]);
        assert_eq!(c.slab_segments(), [32 + grow, 32, 6]);
        check_loaded(&v, &c, &model);
        let lens = v.slab_lens();
        v.splice(lens[0], lens[1], &[]);
        c.splice(lens[0], lens[1], Vec::<u64>::new());
        model.drain(lens[0]..lens[0] + lens[1]);
        // the budget is 64 runs, and 16 the least a slab should hold
        assert_eq!(v.slab_segments(), after);
        assert_eq!(c.slab_segments(), after);
        check(&v, &c, &model);
    }
}

/// A panic in the middle of an edit leaves a column without its values.
/// The layout tools take it as an empty column of no slab, in both APIs,
/// and `check_invariants` has nothing to say against it. (One of the
/// panics that `the_longest_runs` looks at, so on 64 bits only.)
#[cfg(target_pointer_width = "64")]
#[test]
fn the_layout_after_a_panic_in_an_edit() {
    let most = i64::MAX as usize;
    let mut v = VerifiedColumn::<u64>::new();
    v.splice_runs(
        0,
        0,
        [Run {
            count: most,
            value: 7,
        }],
    );
    let mut c = Column::<u64>::new();
    c.splice_runs(
        0,
        0,
        [Run {
            count: most,
            value: 7u64,
        }],
    );
    assert_eq!((v.slab_lens(), v.slab_segments()), (vec![most], vec![1]));
    assert_eq!((c.slab_lens(), c.slab_segments()), (vec![most], vec![1]));
    v.check_invariants();
    c.check_invariants();
    assert!(v.validate_encoding().is_ok() && c.validate_encoding().is_ok());

    let new = panic_of(std::panic::AssertUnwindSafe(|| v.push(7))).unwrap();
    let old = panic_of(std::panic::AssertUnwindSafe(|| c.push(7))).unwrap();
    assert!(new.contains("is not encodable"), "{new}");
    assert_eq!(new, old);
    assert_eq!((v.len(), c.len()), (0, 0));
    assert_eq!((v.slab_count(), c.slab_count()), (0, 0));
    assert_eq!((v.slab_lens(), v.slab_segments()), (vec![], vec![]));
    assert_eq!((c.slab_lens(), c.slab_segments()), (vec![], vec![]));
    v.check_invariants();
    c.check_invariants();
    assert!(v.validate_encoding().is_ok() && c.validate_encoding().is_ok());
    assert_eq!(format!("{v:?}"), format!("Verified{c:?}"));
    // a search of no values asks for none of the column's parts
    assert_eq!(v.scope_to_value(7, 0, 0), 0..0);
    assert_eq!(c.scope_to_value(7u64, 0..0), 0..0);
    // and an iterator has nothing to give, however it is asked
    let mut it = v.iter();
    assert_eq!((it.len(), it.next(), it.nth(0)), (0, None, None));
    assert_eq!(
        (it.nth(usize::MAX), it.nth(70_000), it.len()),
        (None, None, 0)
    );
    assert_eq!(v.iter().fold(1, |a, x| a + x), 1);
    assert_eq!(v.iter_range(0, 0).count(), 0);
    assert_eq!((&v).into_iter().last(), None);
    assert_eq!((c.iter().len(), c.iter().next()), (0, None));
    // and neither takes an edit any more
    let new = panic_of(std::panic::AssertUnwindSafe(|| v.push(7))).unwrap();
    let old = panic_of(std::panic::AssertUnwindSafe(|| c.push(7))).unwrap();
    assert!(new.contains("after a panic"), "{new}");
    assert_eq!(new, old);
}
