//! The verified public API (`hexane::verified`) and `Column` are two
//! doors to one format: for the same values they save the same bytes, each
//! loads what the other saved, and they refuse the same bytes. That holds
//! for every value type of the verified API (numbers, `bool`, byte strings
//! and strings, each with nulls where it has them), and for a type of a
//! client's own.
//!
//! Verus proves the verified API against its own specifications. These
//! tests are what ties those specifications to `Column`'s format, which
//! no proof does.

// `nth(0)` is here for what `nth` does, which is under test
#![allow(clippy::iter_nth_zero)]

use hexane::column::LenWeight;
use hexane::delta::DeltaEncoderState;
use hexane::encoder::{BoolEncoder, BoolEncoderState, RleEncoder, RleEncoderState};
use hexane::index::BitIndex;
use hexane::prefix::{PrefixSeek, PrefixWeightFn};
use hexane::verified::{
    refused, AccIndex, Get, LenBitIndex, LenIndex, RawColumn, SumIndex, VAccum, VAccumulated,
    VAdds, VAmount, VCodec, VDeltaValue, VGroup, VNewtype, VPack, VSummed, VValue,
    VerifiedAccColumn, VerifiedAccIter, VerifiedAccIterState, VerifiedAccSeek, VerifiedAccValue,
    VerifiedColumn, VerifiedDecoder, VerifiedDeltaColumn, VerifiedDeltaDecoder,
    VerifiedDeltaEncoder, VerifiedDeltaEncoderState, VerifiedEncoder, VerifiedEncoderState,
    VerifiedIter, VerifiedIterState, VerifiedPrefixColumn, VerifiedPrefixIter,
    VerifiedPrefixIterState, VerifiedPrefixSeek, VerifiedPrefixedValue, VerifiedRuns,
};
use hexane::{
    Codec, Column, ColumnEncoding, ColumnValue, ColumnValueRef, DeltaColumn, DeltaDecoder,
    DeltaEncoder, DeltaValue, Encoder, EncoderApi, IterState, Leb128, LoadOpts, MaybeFill, NoFill,
    PackError, PrefixColumn, PrefixIter, PrefixValue, PrefixedValue, RleEncoding, RleValue, Run,
    RunDecoder,
};
use std::cell::{Cell, RefCell};
use std::cmp::Ordering;
use std::fmt::Debug;
use std::num::NonZeroU32;
use std::ops::{Add, AddAssign, Bound, Range, Sub, SubAssign};

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
/// or for a byte string or a string a borrow of it.
trait Kind: VValue + ColumnValueRef + Clone + PartialEq + Debug {
    /// The old API's state for the type that writes into a buffer of
    /// its caller's.
    type OldState<'a>: OldStream<'a, Self>;

    /// A value that is not likely the one before it.
    fn fresh(rng: &mut Rng) -> Self;

    fn lend(&self) -> Get<'_, Self>;

    fn keep(v: Get<'_, Self>) -> Self;

    /// How the value stands to `other` in the order of a column's values,
    /// as the standard library has it for the type.
    fn order(&self, other: &Self) -> Ordering;

    /// Where `Column` finds the value among its values `range`.
    fn old_scope(&self, c: &Column<Self>, range: Range<usize>) -> Range<usize>;

    /// Where `Column`'s iterator finds the value in `range` of the column,
    /// among the values it has left.
    fn old_seek(&self, it: &mut hexane::Iter<'_, Self>, range: Range<usize>) -> Range<usize>;

    /// The next place where `Column`'s iterator has the value.
    fn old_scan(&self, it: &mut hexane::Iter<'_, Self>) -> Option<usize>;

    /// What the old API's decoder returns from a scan for the value
    /// (`RunDecoder::scan_for`).
    fn old_scan_for<'a>(&'a self, d: &mut hexane::Decoder<'a, Self>, max: usize) -> (usize, usize);

    /// `Column`'s encoder asked to save what `f` gives for its values
    /// (`RleEncoder::save_to_and_remap`): the range it returns. `None`
    /// for `bool`, whose encoder in the old API has no such form.
    fn old_remap(
        e: Encoder<'_, Self>,
        out: &mut Vec<u8>,
        f: &dyn Fn(Self) -> Self,
    ) -> Option<Range<usize>>;

    /// The same of `RleEncoder::save_to_unless_and_remap`.
    fn old_remap_unless<'a>(
        e: Encoder<'a, Self>,
        out: &mut Vec<u8>,
        unless: &'a Self,
        f: &dyn Fn(Self) -> Self,
    ) -> Option<Range<usize>>;
}

/// A type that is its own form, and the same type with nulls.
macro_rules! plain_kind {
    ($t:ty, $fresh:expr) => {
        impl Kind for $t {
            type OldState<'a> = RleEncoderState<'a, $t>;

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

            fn old_seek(&self, it: &mut hexane::Iter<'_, $t>, range: Range<usize>) -> Range<usize> {
                it.seek_to_value(*self, range)
            }

            fn old_scan(&self, it: &mut hexane::Iter<'_, $t>) -> Option<usize> {
                it.scan_to_value(*self)
            }

            fn old_scan_for<'a>(
                &'a self,
                d: &mut hexane::Decoder<'a, $t>,
                max: usize,
            ) -> (usize, usize) {
                d.scan_for(*self, max)
            }

            fn old_remap(
                e: Encoder<'_, $t>,
                out: &mut Vec<u8>,
                f: &dyn Fn($t) -> $t,
            ) -> Option<Range<usize>> {
                Some(e.save_to_and_remap(out, f))
            }

            fn old_remap_unless<'a>(
                e: Encoder<'a, $t>,
                out: &mut Vec<u8>,
                unless: &'a $t,
                f: &dyn Fn($t) -> $t,
            ) -> Option<Range<usize>> {
                Some(e.save_to_unless_and_remap(out, *unless, f))
            }
        }

        impl Kind for Option<$t> {
            type OldState<'a> = RleEncoderState<'a, Option<$t>>;

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

            fn old_seek(
                &self,
                it: &mut hexane::Iter<'_, Option<$t>>,
                range: Range<usize>,
            ) -> Range<usize> {
                it.seek_to_value(*self, range)
            }

            fn old_scan(&self, it: &mut hexane::Iter<'_, Option<$t>>) -> Option<usize> {
                it.scan_to_value(*self)
            }

            fn old_scan_for<'a>(
                &'a self,
                d: &mut hexane::Decoder<'a, Option<$t>>,
                max: usize,
            ) -> (usize, usize) {
                d.scan_for(*self, max)
            }

            fn old_remap(
                e: Encoder<'_, Option<$t>>,
                out: &mut Vec<u8>,
                f: &dyn Fn(Option<$t>) -> Option<$t>,
            ) -> Option<Range<usize>> {
                Some(e.save_to_and_remap(out, f))
            }

            fn old_remap_unless<'a>(
                e: Encoder<'a, Option<$t>>,
                out: &mut Vec<u8>,
                unless: &'a Option<$t>,
                f: &dyn Fn(Option<$t>) -> Option<$t>,
            ) -> Option<Range<usize>> {
                Some(e.save_to_unless_and_remap(out, *unless, f))
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
    type OldState<'a> = BoolEncoderState;

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

    fn old_seek(&self, it: &mut hexane::Iter<'_, bool>, range: Range<usize>) -> Range<usize> {
        it.seek_to_value(*self, range)
    }

    fn old_scan(&self, it: &mut hexane::Iter<'_, bool>) -> Option<usize> {
        it.scan_to_value(*self)
    }

    fn old_scan_for<'a>(&'a self, d: &mut hexane::Decoder<'a, bool>, max: usize) -> (usize, usize) {
        d.scan_for(*self, max)
    }

    fn old_remap(
        _: Encoder<'_, bool>,
        _: &mut Vec<u8>,
        _: &dyn Fn(bool) -> bool,
    ) -> Option<Range<usize>> {
        None
    }

    fn old_remap_unless<'a>(
        _: Encoder<'a, bool>,
        _: &mut Vec<u8>,
        _: &'a bool,
        _: &dyn Fn(bool) -> bool,
    ) -> Option<Range<usize>> {
        None
    }
}

/// Byte strings: mostly short, the empty one among them; some with a
/// length of two bytes on the wire, a few with one of three.
impl Kind for Vec<u8> {
    type OldState<'a> = RleEncoderState<'a, Vec<u8>>;

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

    fn old_seek(&self, it: &mut hexane::Iter<'_, Vec<u8>>, range: Range<usize>) -> Range<usize> {
        it.seek_to_value(self.as_slice(), range)
    }

    fn old_scan(&self, it: &mut hexane::Iter<'_, Vec<u8>>) -> Option<usize> {
        it.scan_to_value(self.as_slice())
    }

    fn old_scan_for<'a>(
        &'a self,
        d: &mut hexane::Decoder<'a, Vec<u8>>,
        max: usize,
    ) -> (usize, usize) {
        d.scan_for(self.as_slice(), max)
    }

    fn old_remap(
        e: Encoder<'_, Vec<u8>>,
        out: &mut Vec<u8>,
        f: &dyn Fn(Vec<u8>) -> Vec<u8>,
    ) -> Option<Range<usize>> {
        Some(e.save_to_and_remap(out, f))
    }

    fn old_remap_unless<'a>(
        e: Encoder<'a, Vec<u8>>,
        out: &mut Vec<u8>,
        unless: &'a Vec<u8>,
        f: &dyn Fn(Vec<u8>) -> Vec<u8>,
    ) -> Option<Range<usize>> {
        Some(e.save_to_unless_and_remap(out, unless.as_slice(), f))
    }
}

impl Kind for Option<Vec<u8>> {
    type OldState<'a> = RleEncoderState<'a, Option<Vec<u8>>>;

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

    fn old_seek(
        &self,
        it: &mut hexane::Iter<'_, Option<Vec<u8>>>,
        range: Range<usize>,
    ) -> Range<usize> {
        it.seek_to_value(self.as_deref(), range)
    }

    fn old_scan(&self, it: &mut hexane::Iter<'_, Option<Vec<u8>>>) -> Option<usize> {
        it.scan_to_value(self.as_deref())
    }

    fn old_scan_for<'a>(
        &'a self,
        d: &mut hexane::Decoder<'a, Option<Vec<u8>>>,
        max: usize,
    ) -> (usize, usize) {
        d.scan_for(self.as_deref(), max)
    }

    fn old_remap(
        e: Encoder<'_, Option<Vec<u8>>>,
        out: &mut Vec<u8>,
        f: &dyn Fn(Option<Vec<u8>>) -> Option<Vec<u8>>,
    ) -> Option<Range<usize>> {
        Some(e.save_to_and_remap(out, f))
    }

    fn old_remap_unless<'a>(
        e: Encoder<'a, Option<Vec<u8>>>,
        out: &mut Vec<u8>,
        unless: &'a Option<Vec<u8>>,
        f: &dyn Fn(Option<Vec<u8>>) -> Option<Vec<u8>>,
    ) -> Option<Range<usize>> {
        Some(e.save_to_unless_and_remap(out, unless.as_deref(), f))
    }
}

/// A character at random: of one byte in UTF-8, or of two, three or four,
/// the least and the greatest of each among them.
fn a_char(rng: &mut Rng) -> char {
    const ENDS: [char; 10] = [
        '\0',
        '\u{7f}',
        '\u{80}',
        '\u{7ff}',
        '\u{800}',
        '\u{d7ff}',
        '\u{e000}',
        '\u{ffff}',
        '\u{10000}',
        '\u{10ffff}',
    ];
    match rng.below(8) {
        0 => ENDS[rng.below(ENDS.len())],
        1 => char::from_u32(0x80 + rng.below(0x780) as u32).unwrap(),
        2 => char::from_u32(0x800 + rng.below(0xd000) as u32).unwrap(),
        3 => char::from_u32(0x1_0000 + rng.below(0x10_0000) as u32).unwrap(),
        _ => (b'a' + rng.below(26) as u8) as char,
    }
}

/// The character after `c` of all characters, and round to the first.
fn char_after(c: char) -> char {
    match c {
        '\u{d7ff}' => '\u{e000}',
        '\u{10ffff}' => '\0',
        c => char::from_u32(c as u32 + 1).unwrap(),
    }
}

/// The character before `c`, and round to the last.
fn char_before(c: char) -> char {
    match c {
        '\u{e000}' => '\u{d7ff}',
        '\0' => '\u{10ffff}',
        c => char::from_u32(c as u32 - 1).unwrap(),
    }
}

/// Strings: mostly short, the empty one among them, of characters of one
/// to four bytes each; some with a length of two bytes on the wire, a few
/// with one of three.
impl Kind for String {
    type OldState<'a> = RleEncoderState<'a, String>;

    fn fresh(rng: &mut Rng) -> String {
        let n = match rng.below(64) {
            0 => 16_384 + rng.below(100),
            1..=4 => 100 + rng.below(100),
            _ => rng.below(4),
        };
        let mut s = String::new();
        while s.len() < n {
            s.push(a_char(rng));
        }
        s
    }

    fn lend(&self) -> &str {
        self
    }

    fn keep(v: &str) -> String {
        v.to_owned()
    }

    fn order(&self, other: &String) -> Ordering {
        self.cmp(other)
    }

    fn old_scope(&self, c: &Column<String>, range: Range<usize>) -> Range<usize> {
        c.scope_to_value(self.as_str(), range)
    }

    fn old_seek(&self, it: &mut hexane::Iter<'_, String>, range: Range<usize>) -> Range<usize> {
        it.seek_to_value(self.as_str(), range)
    }

    fn old_scan(&self, it: &mut hexane::Iter<'_, String>) -> Option<usize> {
        it.scan_to_value(self.as_str())
    }

    fn old_scan_for<'a>(
        &'a self,
        d: &mut hexane::Decoder<'a, String>,
        max: usize,
    ) -> (usize, usize) {
        d.scan_for(self.as_str(), max)
    }

    fn old_remap(
        e: Encoder<'_, String>,
        out: &mut Vec<u8>,
        f: &dyn Fn(String) -> String,
    ) -> Option<Range<usize>> {
        Some(e.save_to_and_remap(out, f))
    }

    fn old_remap_unless<'a>(
        e: Encoder<'a, String>,
        out: &mut Vec<u8>,
        unless: &'a String,
        f: &dyn Fn(String) -> String,
    ) -> Option<Range<usize>> {
        Some(e.save_to_unless_and_remap(out, unless.as_str(), f))
    }
}

/// Strings that may be null: a null one time in three, and otherwise a
/// string as for `String`.
impl Kind for Option<String> {
    type OldState<'a> = RleEncoderState<'a, Option<String>>;

    fn fresh(rng: &mut Rng) -> Option<String> {
        match rng.below(3) {
            0 => None,
            _ => Some(String::fresh(rng)),
        }
    }

    fn lend(&self) -> Option<&str> {
        self.as_deref()
    }

    fn keep(v: Option<&str>) -> Option<String> {
        v.map(str::to_owned)
    }

    fn order(&self, other: &Option<String>) -> Ordering {
        self.cmp(other)
    }

    fn old_scope(&self, c: &Column<Option<String>>, range: Range<usize>) -> Range<usize> {
        c.scope_to_value(self.as_deref(), range)
    }

    fn old_seek(
        &self,
        it: &mut hexane::Iter<'_, Option<String>>,
        range: Range<usize>,
    ) -> Range<usize> {
        it.seek_to_value(self.as_deref(), range)
    }

    fn old_scan(&self, it: &mut hexane::Iter<'_, Option<String>>) -> Option<usize> {
        it.scan_to_value(self.as_deref())
    }

    fn old_scan_for<'a>(
        &'a self,
        d: &mut hexane::Decoder<'a, Option<String>>,
        max: usize,
    ) -> (usize, usize) {
        d.scan_for(self.as_deref(), max)
    }

    fn old_remap(
        e: Encoder<'_, Option<String>>,
        out: &mut Vec<u8>,
        f: &dyn Fn(Option<String>) -> Option<String>,
    ) -> Option<Range<usize>> {
        Some(e.save_to_and_remap(out, f))
    }

    fn old_remap_unless<'a>(
        e: Encoder<'a, Option<String>>,
        out: &mut Vec<u8>,
        unless: &'a Option<String>,
        f: &dyn Fn(Option<String>) -> Option<String>,
    ) -> Option<Range<usize>> {
        Some(e.save_to_unless_and_remap(out, unless.as_deref(), f))
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
        $f::<Tag>($($arg),*);
        $f::<Option<u64>>($($arg),*);
        $f::<Option<i64>>($($arg),*);
        $f::<Option<u32>>($($arg),*);
        $f::<Option<i32>>($($arg),*);
        $f::<Option<usize>>($($arg),*);
        $f::<Option<NonZeroU32>>($($arg),*);
        $f::<Option<Action>>($($arg),*);
        $f::<Option<Tag>>($($arg),*);
        $f::<Vec<u8>>($($arg),*);
        $f::<Option<Vec<u8>>>($($arg),*);
        $f::<String>($($arg),*);
        $f::<Option<String>>($($arg),*);
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
    // a string is the byte string of its UTF-8: its length is in bytes
    assert_eq!(
        bytes::<String>(&["é".to_owned(), "é".to_owned(), String::new()]),
        [2, 2, 0xc3, 0xa9, 0x7f, 0]
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
    assert_eq!(
        bytes::<Option<String>>(&[Some("é".to_owned()), None, None, Some(String::new())]),
        [0x7f, 2, 0xc3, 0xa9, 0, 2, 0x7f, 0]
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
                check_moves(&v, &c, &model, (a, b), &mut rng);
                check_moves(&v, &c, &model, (0, n), &mut rng);
            }
            v.clear();
            c.clear();
            check_iters(&v, &c, &[], (0, 0), &mut rng);
            check_moves(&v, &c, &[], (0, 0), &mut rng);
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

/// The two iterators stand at the same place, with windows that end at
/// the same place, and count the same number left. And they stand in the
/// same slab, where their columns lie in the same slabs (`slabs`).
fn stand<T: Kind>(x: &VerifiedIter<'_, T>, y: &hexane::Iter<'_, T>, slabs: bool) {
    assert_eq!((x.pos(), x.end_pos()), (y.pos(), y.end_pos()));
    assert!(!slabs || x.get_slab() == y.get_slab());
    assert_eq!((x.len(), x.size_hint()), (y.len(), y.size_hint()));
    assert_eq!(format!("{x:?}"), format!("Verified{y:?}"));
}

/// The values of `model` in the window `a..b`, as the specifications
/// have it: none if it starts at the end of `model` or past it, and up
/// to the end of `model` if it ends past that.
fn window<T>(model: &[T], a: usize, b: usize) -> &[T] {
    if a >= model.len() || b <= a {
        &[]
    } else {
        &model[a..b.min(model.len())]
    }
}

/// What a suspended iterator's state resumes to on a column: where the
/// iterator stands, where its window ends and the values it has left. Or
/// nothing, where the state does not fit the column.
fn resumed<T: Kind>(
    s: &VerifiedIterState,
    v: &VerifiedColumn<T>,
) -> Option<(usize, usize, Vec<T>)> {
    match s.try_resume(v) {
        Ok(it) => Some((it.pos(), it.end_pos(), rest_of(&it))),
        Err(e) => {
            assert!(matches!(e, PackError::InvalidResume), "{e:?}");
            None
        }
    }
}

/// The same of `Column`'s state.
fn old_resumed<T: Kind>(s: &IterState, c: &Column<T>) -> Option<(usize, usize, Vec<T>)> {
    match s.try_resume(c) {
        Ok(it) => Some((it.pos(), it.end_pos(), it.map(old).collect())),
        Err(e) => {
            assert!(matches!(e, PackError::InvalidResume), "{e:?}");
            None
        }
    }
}

/// Both iterators suspended where they stand and resumed on their own
/// columns, which are given back: they stand where the two did, with the
/// same values left, and where `Column`'s does. The window ends where it
/// did, or at the column's end where the iterator stood there. And a
/// state resumes as often as it is asked.
fn check_pause<'a, T: Kind>(
    x: &VerifiedIter<'_, T>,
    y: &hexane::Iter<'_, T>,
    v: &'a VerifiedColumn<T>,
    c: &'a Column<T>,
    slabs: bool,
) -> (VerifiedIter<'a, T>, hexane::Iter<'a, T>) {
    let (s, t) = (x.suspend(), y.suspend());
    let (x2, y2) = (s.try_resume(v).unwrap(), t.try_resume(c).unwrap());
    stand(&x2, &y2, slabs);
    assert_eq!(x2.pos(), x.pos());
    assert!(x2.end_pos() == x.end_pos() || (x.pos() == v.len() && x2.end_pos() == x.pos()));
    assert_eq!(rest_of(&x2), rest_of(x));
    assert_eq!(rest_of(&x2), y2.clone().map(old).collect::<Vec<T>>());
    let again = s.try_resume(v).unwrap();
    assert_eq!((again.pos(), again.end_pos()), (x2.pos(), x2.end_pos()));
    (x2, y2)
}

/// Where the specifications say an iterator stands that stood at `p`
/// and was moved to a window with no value, ending at `to`: where the
/// window ends or the column does, whichever comes first, if that is
/// ahead of it.
fn stays(p: usize, to: usize, n: usize) -> usize {
    if to <= p {
        p
    } else {
        to.min(n)
    }
}

/// The iterators of both APIs over the values `a..b`, moved about in
/// every way they have, to places in the column, at its end, past it and
/// behind them, and read by values and by runs: after each move they
/// stand at the same place with the same window and give the same
/// values, and these are what the verified operation's specification
/// says, in every case.
fn check_moves<T: Kind>(
    v: &VerifiedColumn<T>,
    c: &Column<T>,
    model: &[T],
    (a, b): (usize, usize),
    rng: &mut Rng,
) {
    let n = model.len();
    // the columns lie in the same slabs, but that a `VerifiedColumn`
    // emptied by `clear` has no slab where a `Column` has an empty one
    let slabs = v.slab_lens() == c.slab_lens();
    assert!(slabs || (n == 0 && v.slab_count() == 0));
    let left = |x: &VerifiedIter<'_, T>| x.clone().map(T::keep).collect::<Vec<T>>();
    // a place: most often a little ahead of `from`
    let place = |rng: &mut Rng, from: usize| match rng.below(14) {
        0 => n,
        1 => n + 1 + rng.below(5),
        2 => usize::MAX - rng.below(2),
        3 | 4 => rng.below(n + 1),
        5 => from,
        6 => from.saturating_sub(1 + rng.below(3)),
        7 => from.saturating_add(rng.below(n + 2)),
        _ => from.saturating_add(rng.below(n.saturating_sub(from).min(40) + 2)),
    };
    let (mut x, mut y) = (v.iter_range(a, b), c.iter_range(a..b));
    assert_eq!((x.pos(), x.end_pos()), (a, b));
    for step in 0..50 {
        stand(&x, &y, slabs);
        let (x2, y2) = check_pause(&x, &y, v, c, slabs);
        if step % 5 == 4 {
            // and the moves go on with the iterators resumed
            (x, y) = (x2, y2);
        }
        let (p, e) = (x.pos(), x.end_pos());
        assert!(p <= e && p <= n);
        let had = left(&x);
        // with nothing left, the iterator stands at the end of all it gave
        let spent = (p + had.len(), p + had.len());
        assert_eq!(had, window(model, p, e));
        assert_eq!(had, y.clone().map(old).collect::<Vec<T>>());
        match rng.below(12) {
            0 => {
                let r = x.next().map(T::keep);
                assert_eq!(r, y.next().map(old));
                assert_eq!(r.as_ref(), had.first());
                match r {
                    Some(_) => assert_eq!((x.pos(), x.end_pos()), (p + 1, e)),
                    None => assert_eq!((x.pos(), x.end_pos()), (p, p)),
                }
                assert_eq!(left(&x), had.get(1..).unwrap_or_default());
            }
            1 => {
                let k = match rng.below(4) {
                    0 => had.len() + rng.below(3),
                    1 => usize::MAX - rng.below(2),
                    _ => rng.below(had.len().min(60) + 1),
                };
                let r = x.nth(k).map(T::keep);
                assert_eq!(r, y.nth(k).map(old));
                assert_eq!(r.as_ref(), had.get(k));
                if k < had.len() {
                    assert_eq!((x.pos(), x.end_pos()), (p + k + 1, e));
                    assert_eq!(left(&x), &had[k + 1..]);
                } else {
                    assert!(left(&x).is_empty());
                    assert_eq!((x.pos(), x.end_pos()), spent);
                }
            }
            2 | 3 => {
                let end = place(rng, p);
                x.set_max(end);
                y.set_max(end);
                assert_eq!((x.pos(), x.end_pos()), (p, end.max(p)));
                assert_eq!(left(&x), window(model, p, end));
            }
            4 | 5 => {
                let to = place(rng, p);
                x.advance_to(to);
                y.advance_to(to);
                // what was left, `to - p` places on
                let k = to.saturating_sub(p);
                assert_eq!(left(&x), had.get(k..).unwrap_or_default());
                if to <= p {
                    assert_eq!((x.pos(), x.end_pos()), (p, e));
                } else if k <= had.len() {
                    assert_eq!((x.pos(), x.end_pos()), (to, e));
                } else {
                    assert_eq!((x.pos(), x.end_pos()), spent);
                }
            }
            6 => {
                let k = match rng.below(6) {
                    0 => usize::MAX - rng.below(3),
                    1 => had.len(),
                    2 => had.len() + 1 + rng.below(4),
                    _ => rng.below(had.len().min(50) + 2),
                };
                x.advance_by(k);
                y.advance_by(k);
                assert_eq!(left(&x), had.get(k..).unwrap_or_default());
                if k <= had.len() {
                    assert_eq!((x.pos(), x.end_pos()), (p + k, e));
                } else {
                    assert_eq!((x.pos(), x.end_pos()), spent);
                }
            }
            7 | 8 => {
                let from = place(rng, p);
                let to = place(rng, from);
                x.shift(from..to);
                y.shift(from..to);
                assert_eq!(left(&x), window(model, from.max(p), to));
                if from <= p {
                    assert_eq!((x.pos(), x.end_pos()), (p, to.max(p)));
                } else if from <= to && from <= n {
                    assert_eq!((x.pos(), x.end_pos()), (from, to));
                } else {
                    let at = stays(p, to, n);
                    assert_eq!((x.pos(), x.end_pos()), (at, at));
                }
            }
            9 => {
                // the window is to start at the iterator or after it
                let from = place(rng, p).max(p);
                let to = place(rng, from);
                let r = x.shift_next(from..to).map(T::keep);
                assert_eq!(r, y.shift_next(from..to).map(old));
                let w = window(model, from, to);
                assert_eq!(r.as_ref(), w.first());
                if w.is_empty() {
                    assert!(left(&x).is_empty());
                    let at = stays(p, to, n);
                    assert_eq!((x.pos(), x.end_pos()), (at, at));
                } else {
                    assert_eq!(left(&x), &w[1..]);
                    assert_eq!((x.pos(), x.end_pos()), (from + 1, to));
                }
            }
            _ => {
                let r = x.next_run().map(|r| (r.count, T::keep(r.value)));
                assert_eq!(r, y.next_run().map(|r| (r.count, old(r.value))));
                match r {
                    // the first value left, as often as it stood at the
                    // front of what was left
                    Some((count, value)) => {
                        assert_eq!(Some(&value), had.first());
                        assert_eq!(count, had.iter().take_while(|h| **h == value).count());
                        assert_eq!((x.pos(), x.end_pos()), (p + count, e));
                        assert_eq!(left(&x), &had[count..]);
                    }
                    None => {
                        assert!(had.is_empty());
                        assert_eq!((x.pos(), x.end_pos()), (p, p));
                    }
                }
            }
        }
        if step % 8 == 7 {
            // a copy stands where the iterator does
            stand(&x.clone(), &y, slabs);
        }
    }
    stand(&x, &y, slabs);
}

/// Columns in one slab and in many, as built and as loaded: an iterator
/// moved about, by steps and with its window, stands where `Column`'s
/// does after every move and gives what the specifications say.
#[test]
fn an_iterator_moves_as_columns_does() {
    fn moves<T: Kind>() {
        let mut rng = Rng(0x30fe5);
        let (p, q) = (T::fresh(&mut rng), T::fresh(&mut rng));
        let models: Vec<Vec<T>> = vec![
            vec![],
            vec![p.clone()],
            vec![p.clone(); 300],
            (0..400).map(|i| [&p, &q][i / 9 % 2].clone()).collect(),
            (0..700).map(|_| T::fresh(&mut rng)).collect(),
            rng.values(300),
            rng.values(900),
        ];
        for model in &models {
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
                let mut ws = vec![(0, n), (0, 0), (n, n), (n / 3, n - n / 3)];
                for _ in 0..6 {
                    let a = rng.below(n + 1);
                    ws.push((a, a + rng.below(n - a + 1)));
                }
                for w in ws {
                    check_moves(&v, &c, model, w, &mut rng);
                }
            }
        }
    }
    for_each_kind!(moves);
}

/// An iterator of no column moved about: its window is what it is set
/// to, and it stays where it is with nothing to give.
#[test]
fn an_iterator_of_no_column_moves_nowhere() {
    fn none<T: Kind>() {
        let (mut x, mut y) = (VerifiedIter::<T>::default(), hexane::Iter::<T>::default());
        stand(&x, &y, true);
        assert_eq!((x.pos(), x.end_pos(), x.get_slab()), (0, 0, 0));
        x.set_max(5);
        y.set_max(5);
        stand(&x, &y, true);
        assert_eq!((x.pos(), x.end_pos(), x.len()), (0, 5, 0));
        // past all it has, which is nothing: its window ends where it stands
        x.advance_to(3);
        y.advance_to(3);
        stand(&x, &y, true);
        assert_eq!((x.pos(), x.end_pos()), (0, 0));
        x.advance_by(2);
        y.advance_by(2);
        stand(&x, &y, true);
        x.advance_by(usize::MAX);
        y.advance_by(usize::MAX);
        stand(&x, &y, true);
        x.set_max(7);
        y.set_max(7);
        x.shift(1..4);
        y.shift(1..4);
        stand(&x, &y, true);
        assert_eq!((x.pos(), x.end_pos()), (0, 0));
        x.shift(usize::MAX..usize::MAX);
        y.shift(usize::MAX..usize::MAX);
        stand(&x, &y, true);
        let from = x.pos();
        assert!(x.shift_next(from..from + 9).is_none());
        assert!(y.shift_next(from..from + 9).is_none());
        stand(&x, &y, true);
        assert_eq!(x.len(), 0);
        assert!(x.next().is_none() && x.nth(2).is_none());
        stand(&x, &y, true);
        assert_eq!((x.pos(), x.end_pos(), x.get_slab()), (0, 0, 0));
        // and it has no run, with a window set or with none
        x.set_max(6);
        y.set_max(6);
        assert!(x.next_run().is_none() && y.next_run().is_none());
        stand(&x, &y, true);
        assert_eq!((x.pos(), x.end_pos()), (0, 0));
        assert!(x.next_run().is_none());
        assert!(x.runs().next().is_none() && y.runs().next().is_none());
    }
    for_each_kind!(none);
}

/// The runs of `vs`: each value with how often it stands there together.
fn model_runs<T: Kind>(vs: &[T]) -> Vec<(usize, T)> {
    let mut out: Vec<(usize, T)> = Vec::new();
    for v in vs {
        match out.last_mut() {
            Some((count, value)) if value == v => *count += 1,
            _ => out.push((1, v.clone())),
        }
    }
    out
}

/// All the runs an iterator of runs gives.
fn taken<T: Kind>(runs: VerifiedRuns<'_, T>) -> Vec<(usize, T)> {
    runs.map(|r| (r.count, T::keep(r.value))).collect()
}

/// Columns in one slab and in many, as built and as loaded: the runs of
/// a window are the runs of its values, whatever slabs they lie in, as
/// `next_run` gives them and as the iterator `runs()` does, and as
/// `Column` gives them; each run leaves the iterator past it; and the
/// runs spliced into a column make the window's values.
#[test]
fn runs_as_columns_does() {
    fn runs<T: Kind>() {
        let mut rng = Rng(0x4b2);
        let (p, q) = (T::fresh(&mut rng), T::fresh(&mut rng));
        let models: Vec<Vec<T>> = vec![
            vec![],
            vec![p.clone()],
            vec![p.clone(); 300],
            (0..400).map(|i| [&p, &q][i / 9 % 2].clone()).collect(),
            (0..600).map(|i| [&p, &q][i / 150 % 2].clone()).collect(),
            (0..300).map(|_| T::fresh(&mut rng)).collect(),
            rng.values(300),
            rng.values(900),
        ];
        for model in &models {
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
                let slabs = v.slab_lens() == c.slab_lens();
                let mut ws = vec![(0, n), (0, 0), (n, n), (n / 3, n - n / 3)];
                for _ in 0..8 {
                    let a = rng.below(n + 1);
                    ws.push((a, a + rng.below(n - a + 1)));
                }
                for (a, b) in ws {
                    let want = model_runs(&model[a..b]);
                    // run by run, and where the iterator stands after each
                    let (mut x, mut y) = (v.iter_range(a, b), c.iter_range(a..b));
                    let mut got = Vec::new();
                    let mut at = a;
                    loop {
                        let r = x.next_run().map(|r| (r.count, T::keep(r.value)));
                        assert_eq!(r, y.next_run().map(|r| (r.count, old(r.value))));
                        stand(&x, &y, slabs);
                        let Some(run) = r else {
                            assert_eq!((x.pos(), x.end_pos()), (at, at));
                            break;
                        };
                        at += run.0;
                        assert_eq!((x.pos(), x.end_pos()), (at, b));
                        got.push(run);
                    }
                    assert_eq!(got, want);
                    assert_eq!(at, b);
                    assert!(x.next_run().is_none() && x.next().is_none());

                    // the iterator of runs, and `Column`'s
                    let rs = v.iter_range(a, b).runs();
                    let old_rs = c.iter_range(a..b).runs();
                    assert_eq!(
                        format!("{rs:?}"),
                        format!("Verified{old_rs:?}").replace("(Iter", "(VerifiedIter")
                    );
                    assert_eq!(taken(rs), want);
                    let old_runs: Vec<(usize, T)> =
                        old_rs.map(|r| (r.count, old(r.value))).collect();
                    assert_eq!(old_runs, want);

                    // a window that ends past the column: the runs up to
                    // the column's end
                    let mut x = v.iter_range(a, b);
                    x.set_max(usize::MAX - rng.below(2));
                    assert_eq!(taken(x.runs()), model_runs(&model[a..]));

                    // from inside a run: the rest of that run comes first
                    let k = rng.below(b - a + 1);
                    let mut x = v.iter_range(a, b);
                    x.advance_by(k);
                    assert_eq!(taken(x.runs()), model_runs(&model[a + k..b]));

                    // the runs make the values, in a column of their own
                    let mut w = VerifiedColumn::<T>::new();
                    w.splice_runs(0, 0, v.iter_range(a, b).runs());
                    assert_eq!(new_values(&w), &model[a..b]);
                }
            }
        }
    }
    for_each_kind!(runs);
}

/// What an iterator has left to give.
fn rest_of<T: Kind>(x: &VerifiedIter<'_, T>) -> Vec<T> {
    x.clone().map(T::keep).collect()
}

/// Ranges to search with an iterator that stands at `p` in a window
/// ending at `e`, of a column of `n` values: all the iterator has left,
/// its window, and `count` ranges between places behind it, in its
/// window, past that and past the column, the wrong way round among them.
fn seek_ranges(rng: &mut Rng, p: usize, e: usize, n: usize, count: usize) -> Vec<Range<usize>> {
    let place = |rng: &mut Rng| match rng.below(12) {
        0 => p,
        1 => e,
        2 => n + rng.below(3),
        3 => p.saturating_sub(1 + rng.below(3)),
        4 => e + 1 + rng.below(3),
        5 => usize::MAX - rng.below(2),
        6 => rng.below(n + 1),
        _ => p + rng.below(e - p + 1),
    };
    let mut ranges = vec![0..usize::MAX, p..e];
    for _ in 0..count {
        let start = place(rng);
        ranges.push(start..place(rng));
    }
    ranges
}

/// A search for `x` in `range` by the iterators of both APIs, which
/// stand `k` values into the window `a..b`. They give the same range and
/// stand at the same place after, with the same window; and these are
/// what the specification of the verified one says: the range lies in
/// what was searched, the iterator stands at its start with the values
/// from there on left, and its window ends where it did, or where the
/// iterator stands if `range` starts past it. Where the values searched
/// are in order as far as `x` can tell, which the result says, the range
/// is that of the places of `x`, or the empty one where `x` would stand,
/// and `x` comes next if it is there.
fn check_seek<T: Kind>(
    v: &VerifiedColumn<T>,
    c: &Column<T>,
    model: &[T],
    x: &T,
    (a, b, k): (usize, usize, usize),
    range: Range<usize>,
) -> bool {
    let slabs = v.slab_lens() == c.slab_lens();
    let (mut it, mut old_it) = (v.iter_range(a, b), c.iter_range(a..b));
    it.advance_by(k);
    old_it.advance_by(k);
    let (p, e) = (it.pos(), it.end_pos());
    assert_eq!((p, e), (a + k, b));
    let had = &model[p..e];
    let r = it.seek_to_value(x.lend(), range.clone());
    let case = format!("{x:?} in {range:?} from {p} of {a}..{b}");
    assert_eq!(r, x.old_seek(&mut old_it, range.clone()), "{case}");
    stand(&it, &old_it, slabs);
    // how far the iterator moves before it searches, and what it searches
    let skip = if range.start <= p {
        0
    } else {
        (range.start - p).min(had.len())
    };
    let searched = window(had, skip, range.end.saturating_sub(p));
    let from = p + skip;
    assert!(
        from <= r.start && r.start <= r.end && r.end <= from + searched.len(),
        "{r:?}: {case}"
    );
    assert_eq!(it.pos(), r.start, "{case}");
    assert_eq!(it.end_pos(), if range.start <= e { e } else { r.start });
    assert_eq!(rest_of(&it), had[r.start - p..], "{case}");
    let sides: Vec<Ordering> = searched.iter().map(|m| m.order(x)).collect();
    let in_order = sides.windows(2).all(|w| w[0] <= w[1]);
    if in_order {
        let before = sides.iter().filter(|s| **s == Ordering::Less).count();
        let after = sides.iter().filter(|s| **s == Ordering::Greater).count();
        assert_eq!(r, from + before..from + searched.len() - after, "{case}");
        assert!(model[r.clone()].iter().all(|m| m == x));
        assert!(r.is_empty() || it.next().map(T::keep).as_ref() == Some(x));
    }
    in_order
}

/// Scans for `x` by the iterators of both APIs, which stand `k` values
/// into the window `a..b`, one after another until `x` is found no more.
/// Each gives the same place in both, the next one where the model has
/// `x`, and leaves the iterators past it with the window they had; the
/// last finds none and leaves them with nothing, their windows ending
/// where they stand. The number of places found is given.
fn check_scan<T: Kind>(
    v: &VerifiedColumn<T>,
    c: &Column<T>,
    model: &[T],
    x: &T,
    (a, b, k): (usize, usize, usize),
) -> usize {
    let slabs = v.slab_lens() == c.slab_lens();
    let (mut it, mut old_it) = (v.iter_range(a, b), c.iter_range(a..b));
    it.advance_by(k);
    old_it.advance_by(k);
    let mut found = 0;
    loop {
        let p = it.pos();
        let had = &model[p..b];
        let r = it.scan_to_value(x.lend());
        assert_eq!(r, x.old_scan(&mut old_it), "{x:?} from {p} of {a}..{b}");
        stand(&it, &old_it, slabs);
        let Some(at) = r else {
            assert!(!had.contains(x), "{x:?} from {p} of {a}..{b}");
            assert_eq!((it.pos(), it.end_pos()), (b, b));
            assert!(it.next().is_none());
            return found;
        };
        assert_eq!(had.iter().position(|m| m == x), Some(at - p));
        assert_eq!((it.pos(), it.end_pos()), (at + 1, b));
        if found < 3 {
            assert_eq!(rest_of(&it), model[at + 1..b]);
        }
        found += 1;
    }
}

/// Columns of each API with the values `model`: built under the budgets
/// 3 and 16, in many slabs, and under 64; and those of 3 and 64 loaded
/// from their bytes too.
fn columns_of<T: Kind>(rng: &mut Rng, model: &[T]) -> Vec<(VerifiedColumn<T>, Column<T>)> {
    let mut out = Vec::new();
    for (budget, loaded) in [(3, false), (3, true), (16, false), (64, true)] {
        let (mut v, mut c) = if budget < 64 {
            both_in_slabs(rng, model, budget)
        } else {
            both_with(model, budget)
        };
        if loaded {
            let bytes = v.save();
            let opts = LoadOpts::new().with_max_segments(budget);
            v = VerifiedColumn::<T>::load_with(&bytes, opts).unwrap();
            c = Column::<T>::load_with(&bytes, opts).unwrap();
        }
        out.push((v, c));
    }
    out
}

/// The window of all `n` values, from its start, and `count` windows at
/// random with the iterator some way into each.
fn windows_into(rng: &mut Rng, n: usize, count: usize) -> Vec<(usize, usize, usize)> {
    let mut ws = vec![(0, n, 0)];
    for _ in 0..count {
        let a = rng.below(n + 1);
        let b = a + rng.below(n - a + 1);
        ws.push((a, b, rng.below(b - a + 1)));
    }
    ws
}

/// The iterator's two searches, in columns of one slab and of many, as
/// built and as loaded, from any place in any window. `seek_to_value`
/// gives what `Column`'s does for every range, in the window and out of
/// it; in values in order, and in values in order only as far as the
/// one looked for can tell, it finds that one's places; and in values in
/// no order it still keeps what its specification says of every case.
/// `scan_to_value` finds each place of a value in turn, as `Column`'s
/// does, in values in any order.
#[test]
fn searches_as_columns_does() {
    fn seeks<T: Kind>() {
        let mut rng = Rng(0x4c);
        for n in [0, 1, 2, 3, 50, 400] {
            let mut model: Vec<T> = (0..n).map(|_| T::fresh(&mut rng)).collect();
            if n == 400 {
                // few values, each many times over
                model = (0..n).map(|i| model[i % 5].clone()).collect();
            }
            model.sort_by(T::order);
            let mut most = 0;
            for (v, c) in columns_of(&mut rng, &model) {
                most = most.max(v.slab_count());
                let ws = windows_into(&mut rng, n, 4);
                for x in targets(&mut rng, &model).iter().step_by(2) {
                    for &w in &ws {
                        for range in seek_ranges(&mut rng, w.0 + w.2, w.1, n, 5) {
                            assert!(check_seek(&v, &c, &model, x, w, range));
                        }
                    }
                }
            }
            assert!(n < 400 || most > 30);
        }
    }
    for_each_kind!(seeks);

    fn seeks_out_of_order<T: Kind>() {
        let mut rng = Rng(0x0dd4c);
        let mut exact = 0;
        for round in 0..20 {
            let n = [0, 1, 7, 60, 300][round % 5];
            let model: Vec<T> = if round % 2 == 0 {
                (0..n).map(|_| T::fresh(&mut rng)).collect()
            } else {
                rng.values(n)
            };
            let n = model.len();
            let budget = [3, 64][round / 5 % 2];
            let (v, c) = both_with(&model, budget);
            let ws = windows_into(&mut rng, n, 3);
            for x in targets(&mut rng, &model).iter().step_by(3) {
                for &w in &ws {
                    for range in seek_ranges(&mut rng, w.0 + w.2, w.1, n, 4) {
                        check_seek(&v, &c, &model, x, w, range);
                    }
                }
                // the same values, those before `x` first and those after
                // it last, each part in the order it had
                let mut parted: Vec<T> = vec![];
                for side in [Ordering::Less, Ordering::Equal, Ordering::Greater] {
                    parted.extend(model.iter().filter(|m| m.order(x) == side).cloned());
                }
                let (v, c) = both_with(&parted, budget);
                for &w in &ws {
                    for range in seek_ranges(&mut rng, w.0 + w.2, w.1, n, 4) {
                        assert!(check_seek(&v, &c, &parted, x, w, range));
                        exact += 1;
                    }
                }
            }
        }
        assert!(exact > 300);
    }
    for_each_kind!(seeks_out_of_order);

    fn scans<T: Kind>() {
        let mut rng = Rng(0x5ca);
        let (p, q) = (T::fresh(&mut rng), T::fresh(&mut rng));
        let models: Vec<Vec<T>> = vec![
            vec![],
            vec![p.clone()],
            vec![p.clone(); 200],
            (0..300).map(|i| [&p, &q][i / 9 % 2].clone()).collect(),
            (0..400).map(|i| [&p, &q][i / 150 % 2].clone()).collect(),
            (0..200).map(|_| T::fresh(&mut rng)).collect(),
            rng.values(300),
            rng.values(600),
        ];
        let mut found = 0;
        for model in &models {
            for (v, c) in columns_of(&mut rng, model) {
                let ws = windows_into(&mut rng, model.len(), 4);
                let mut xs = targets(&mut rng, model);
                xs.truncate(14);
                xs.extend([p.clone(), q.clone()]);
                for x in &xs {
                    for &w in &ws {
                        found += check_scan(&v, &c, model, x, w);
                    }
                }
            }
        }
        assert!(found > 1000);
    }
    for_each_kind!(scans);

    // an iterator of no column finds nothing and stays where it is, and
    // so does one of a column with no values
    fn none<T: Kind>() {
        let mut rng = Rng(0x4c0);
        let x = T::fresh(&mut rng);
        let (mut it, mut old_it) = (VerifiedIter::<T>::default(), hexane::Iter::<T>::default());
        assert_eq!(it.scan_to_value(x.lend()), None);
        assert_eq!(x.old_scan(&mut old_it), None);
        stand(&it, &old_it, true);
        // one of them the wrong way round
        #[allow(clippy::reversed_empty_ranges)]
        let ranges = [0..usize::MAX, 0..0, 3..9, 9..3, usize::MAX..usize::MAX];
        for range in ranges.clone() {
            assert_eq!(it.seek_to_value(x.lend(), range.clone()), 0..0);
            assert_eq!(x.old_seek(&mut old_it, range), 0..0);
            stand(&it, &old_it, true);
            assert_eq!((it.pos(), it.end_pos()), (0, 0));
        }
        let (mut v, mut c) = (VerifiedColumn::<T>::new(), Column::<T>::new());
        for emptied in [false, true] {
            if emptied {
                v.push(x.lend());
                c.push(x.clone());
                v.clear();
                c.clear();
            }
            assert_eq!(check_scan(&v, &c, &[], &x, (0, 0, 0)), 0);
            for range in ranges.clone() {
                assert!(check_seek(&v, &c, &[], &x, (0, 0, 0), range));
            }
        }
    }
    for_each_kind!(none);
}

/// A search with the iterator in the hands of a caller: what it gave
/// before and what it gives after are the column's values, with none
/// lost and none given twice, for values of a type of the client's own
/// and for byte strings, which are lent.
#[test]
fn a_search_then_a_read() {
    // the numbers 0, 3, 6, ... each three times over, in many slabs
    let model: Vec<u64> = (0..600).map(|i| i / 3 * 3).collect();
    let (v, c) = both_with(&model, 3);
    assert!(v.slab_count() > 30);
    let mut it = v.iter();
    assert_eq!(it.by_ref().take(10).collect::<Vec<u64>>(), model[..10]);
    // a value that is there, searched for in all that is left
    assert_eq!(it.seek_to_value(300, 0..usize::MAX), 300..303);
    assert_eq!((it.pos(), it.end_pos()), (300, 600));
    assert_eq!(it.next(), Some(300));
    // one that is not: the iterator stands where it would be
    assert_eq!(it.seek_to_value(400, 0..usize::MAX), 402..402);
    assert_eq!(it.next(), Some(402));
    // a range that starts behind the iterator is searched from where the
    // iterator stands, and one that ends behind it not at all
    assert_eq!(it.seek_to_value(402, 0..500), 403..405);
    assert_eq!(it.seek_to_value(0, 0..5), 403..403);
    // a range ahead: the iterator moves there first, though the value
    // lies before it
    assert_eq!(it.seek_to_value(402, 450..460), 450..450);
    assert_eq!((it.pos(), it.end_pos()), (450, 600));
    // a scan goes on from there, past the value it finds
    assert_eq!(it.scan_to_value(501), Some(501));
    assert_eq!((it.pos(), it.next()), (502, Some(501)));
    // one for a value behind the iterator finds none, and spends it
    assert_eq!(it.scan_to_value(300), None);
    assert_eq!((it.pos(), it.end_pos(), it.next()), (600, 600, None));
    // a range that starts past the window: so too
    let mut it = v.iter_range(10, 20);
    assert_eq!(it.seek_to_value(9, 21..30), 20..20);
    assert_eq!((it.pos(), it.end_pos(), it.next()), (20, 20, None));
    let mut old_it = c.iter_range(10..20);
    assert_eq!(old_it.seek_to_value(9, 21..30), 20..20);
    assert_eq!((old_it.pos(), old_it.end_pos()), (20, 20));

    // byte strings: the target is lent for the call alone, and the value
    // found is lent by the column
    let words: Vec<Vec<u8>> = ["ant", "ant", "bee", "cat", "cat", "cat", "dog"]
        .iter()
        .map(|w| w.as_bytes().to_vec())
        .collect();
    let (v, _) = both_with(&words, 3);
    let mut it = v.iter();
    let found = {
        let target = b"cat".to_vec();
        it.seek_to_value(&target[..], 0..usize::MAX)
    };
    assert_eq!(found, 3..6);
    let cat: &[u8] = it.next().unwrap();
    let at = {
        let target = b"dog".to_vec();
        it.scan_to_value(&target[..])
    };
    assert_eq!((cat, at, it.next()), (&b"cat"[..], Some(6), None));
}

/// A column read in pages: an iterator suspended after each page, and
/// resumed for the next, gives every value once and in order. A state
/// holds no borrow of its column, which is read between the pages.
#[test]
fn a_column_read_in_pages() {
    fn pages<T: Kind>() {
        let mut rng = Rng(0x9a6e5);
        for n in [0, 1, 50, 400] {
            let model: Vec<T> = (0..n).map(|_| T::fresh(&mut rng)).collect();
            let model: Vec<T> = (0..n).map(|i| model[i - i % 3].clone()).collect();
            for (v, c) in columns_of(&mut rng, &model) {
                let slabs = v.slab_lens() == c.slab_lens();
                for page in [1, 7, 64, 1000] {
                    let (mut s, mut t) = (v.iter().suspend(), c.iter().suspend());
                    let (mut got, mut old_got) = (Vec::<T>::new(), Vec::<T>::new());
                    loop {
                        assert_eq!(v.len(), n);
                        let (mut x, mut y) = (s.try_resume(&v).unwrap(), t.try_resume(&c).unwrap());
                        stand(&x, &y, slabs);
                        assert_eq!(x.pos(), got.len());
                        let before = got.len();
                        got.extend(x.by_ref().take(page).map(T::keep));
                        old_got.extend(y.by_ref().take(page).map(old));
                        (s, t) = (x.suspend(), y.suspend());
                        if got.len() == before {
                            break;
                        }
                    }
                    assert_eq!(got, model);
                    assert_eq!(old_got, model);
                }
            }
        }
    }
    for_each_kind!(pages);
}

/// One of the edits of a column, made of both columns and of the model.
/// Says whether it changed the values.
fn an_edit<T: Kind>(
    rng: &mut Rng,
    which: usize,
    v: &mut VerifiedColumn<T>,
    c: &mut Column<T>,
    model: &mut Vec<T>,
) -> bool {
    let n = model.len();
    let at = rng.below(n + 1);
    let del = rng.below((n - at).min(5) + 1);
    let before = model.clone();
    match which {
        0 => {
            let ins: Vec<T> = rng.values(3);
            v.splice(at, del, &lent(&ins));
            c.splice(at, del, ins.iter().cloned());
            model.splice(at..at + del, ins);
        }
        1 => {
            let ins: Vec<T> = rng.values(3);
            v.splice_iter(at, del, ins.iter().map(T::lend));
            c.splice(at, del, ins.iter().cloned());
            model.splice(at..at + del, ins);
        }
        2 => {
            let runs = some_runs::<T>(rng, 2);
            v.splice_runs(
                at,
                del,
                runs.iter().map(|(n, x)| Run {
                    count: *n,
                    value: x.lend(),
                }),
            );
            c.splice_runs(
                at,
                del,
                runs.iter().map(|(n, x)| Run {
                    count: *n,
                    value: x.clone(),
                }),
            );
            model.splice(at..at + del, run_model(&runs));
        }
        3 => {
            let x = T::fresh(rng);
            v.insert(at, x.lend());
            c.insert(at, x.clone());
            model.insert(at, x);
        }
        4 => {
            let x = T::fresh(rng);
            v.push(x.lend());
            c.push(x.clone());
            model.push(x);
        }
        5 => {
            if at < n {
                v.remove(at);
                c.remove(at);
                model.remove(at);
            }
        }
        6 => {
            // sometimes past the end, which changes nothing
            let len = rng.below(n + 3);
            v.truncate(len);
            c.truncate(len);
            model.truncate(len);
        }
        7 => {
            v.remove_n(at, del);
            c.remove_n(at, del);
            model.drain(at..at + del);
        }
        8 => {
            v.clear();
            c.clear();
            model.clear();
        }
        9 => {
            // each value to itself, or each to one value
            let to = match rng.below(2) {
                0 => None,
                _ => Some(T::fresh(rng)),
            };
            v.remap(|x: T| to.clone().unwrap_or(x));
            c.remap(|x: T| to.clone().unwrap_or(x));
            if let Some(to) = to {
                model.fill(to);
            }
        }
        _ => {
            let more: Vec<T> = rng.values(3);
            v.extend(more.iter().map(T::lend));
            c.extend(more.iter().cloned());
            model.extend(more);
        }
    }
    *model != before
}

/// An edit of a column ends what was suspended of it. A state taken
/// before any edit that changes the column's values resumes no more, as
/// `Column`'s does not, and stays so through later edits, one that puts
/// the old values back among them. After a call that changes no value,
/// the state resumes or not as `Column`'s does. A state taken after the
/// edits resumes. Columns built and loaded, in one slab and in many, so
/// that some of the edits leave the count of slabs as it was.
///
/// That a state taken before a call which deletes a value or inserts
/// one, or before a remap, resumes no more is proved as well (each such
/// call says that the column counts an edit more, and `try_resume`
/// refuses a state that counts another number). What is tested here and
/// not proved is that those are the calls after which `Column`'s state
/// resumes no more.
#[test]
fn an_edit_ends_what_was_suspended() {
    fn ends<T: Kind>() {
        let mut rng = Rng(0x4d5e);
        let (mut ended, mut kept) = (0, 0);
        for round in 0..220 {
            let which = round % 11;
            let budget = [3, 16, 64][round / 11 % 3];
            let mut model: Vec<T> = rng.values(if round / 33 % 2 == 0 { 8 } else { 250 });
            let (mut v, mut c) = both_with(&model, budget);
            if round / 66 % 2 == 1 {
                let bytes = v.save();
                let opts = LoadOpts::new().with_max_segments(budget);
                v = VerifiedColumn::<T>::load_with(&bytes, opts).unwrap();
                c = Column::<T>::load_with(&bytes, opts).unwrap();
            }
            let n = model.len();
            let (a, b, k) = windows_into(&mut rng, n, 1)[1];
            let (s, t) = {
                let (mut x, mut y) = (v.iter_range(a, b), c.iter_range(a..b));
                if k > 0 {
                    x.nth(k - 1);
                    y.nth(k - 1);
                }
                (x.suspend(), y.suspend())
            };
            let first = resumed(&s, &v);
            assert_eq!(first, old_resumed(&t, &c));
            assert_eq!(first.as_ref().map(|r| r.0), Some(a + k));
            assert_eq!(
                first.as_ref().map(|r| &r.2[..]),
                Some(window(&model, a + k, b))
            );
            // a copy of the column is as much the state's column
            let (v2, c2) = (v.clone(), c.clone());
            assert_eq!(resumed(&s, &v2), first);
            assert_eq!(old_resumed(&t, &c2), first);

            let slabs = v.slab_count();
            let changed = an_edit(&mut rng, which, &mut v, &mut c, &mut model);
            let (new, old) = (resumed(&s, &v), old_resumed(&t, &c));
            assert_eq!(new, old, "after edit {which}");
            if changed {
                assert!(new.is_none(), "edit {which} at {slabs} slabs");
                ended += (v.slab_count() == slabs) as usize;
                // nor after further edits, the first undone among them
                if which == 4 {
                    v.remove(n);
                    c.remove(n);
                    model.pop();
                } else {
                    let next = rng.below(11);
                    an_edit(&mut rng, next, &mut v, &mut c, &mut model);
                }
                assert!(resumed(&s, &v).is_none() && old_resumed(&t, &c).is_none());
            } else {
                kept += new.is_some() as usize;
            }
            // the copy took none of it
            assert_eq!(resumed(&s, &v2), first);
            assert_eq!(old_resumed(&t, &c2), first);
            // and a state of the column as it now is resumes (some of the
            // columns were loaded, and a remap leaves slabs as a load does)
            check_loaded(&v, &c, &model);
            let (s, t) = (v.iter().suspend(), c.iter().suspend());
            let all = Some((0, model.len(), model.clone()));
            assert_eq!(resumed(&s, &v), all);
            assert_eq!(old_resumed(&t, &c), all);
        }
        // edits that left the count of slabs alone, and calls that left
        // the state resuming
        assert!(ended > 40 && kept > 3, "{ended} {kept}");
    }
    for_each_kind!(ends);
}

/// A column cleared of its values refuses what was suspended of it, and
/// does so still once it has a slab again. `clear` leaves a column of
/// this API with no slab, and a call that changes no value then gives it
/// its first: a column that had one slab has one again, and a state that
/// stood at its start is told from a state of the column as it now is
/// by the count of edits alone. (`Column` keeps a slab through its
/// `clear`, and refuses its state as well.)
#[test]
fn a_cleared_column_refuses_what_was_suspended() {
    fn cleared<T: Kind>() {
        let mut rng = Rng(0xc1ea5);
        let mut one_slab = 0;
        for round in 0..48 {
            let n = [1, 8, 250][round % 3];
            let model: Vec<T> = (0..n).map(|_| T::fresh(&mut rng)).collect();
            let (mut v, mut c) = both_with(&model, 64);
            if round / 24 == 1 {
                let bytes = v.save();
                v = VerifiedColumn::<T>::load(&bytes).unwrap();
                c = Column::<T>::load(&bytes).unwrap();
            }
            let (a, b) = [(0, 0), (0, n), (0, 1), (n, n)][round / 3 % 4];
            let (s, t) = (v.iter_range(a, b).suspend(), c.iter_range(a..b).suspend());
            let first = Some((a, b, window(&model, a, b).to_vec()));
            assert_eq!(resumed(&s, &v), first);
            assert_eq!(old_resumed(&t, &c), first);
            one_slab += (v.slab_count() == 1) as usize;

            v.clear();
            c.clear();
            assert_eq!(v.slab_count(), 0);
            assert!(resumed(&s, &v).is_none() && old_resumed(&t, &c).is_none());
            // the first slab again, by a call that changes no value
            let none = &model[..0];
            if round / 12 % 2 == 0 {
                v.splice(0, 0, &lent(none));
                c.splice(0, 0, none.iter().cloned());
            } else {
                v.extend(none.iter().map(T::lend));
                c.extend(none.iter().cloned());
            }
            assert_eq!(v.slab_count(), 1);
            assert!(
                resumed(&s, &v).is_none() && old_resumed(&t, &c).is_none(),
                "round {round}"
            );
            // and with values in it again, the old ones
            v.splice(0, 0, &lent(&model));
            c.splice(0, 0, model.iter().cloned());
            assert!(resumed(&s, &v).is_none() && old_resumed(&t, &c).is_none());
            check_loaded(&v, &c, &model);
        }
        assert!(one_slab > 20, "{one_slab}");
    }
    for_each_kind!(cleared);
}

/// Calls that change no value of a column: a splice of nothing for
/// nothing, in each of its three forms and as runs that are all of
/// count 0, a truncate at the end or past it, a delete of no value, an
/// extend by nothing, a clear of a column of no value, a remap of each
/// value to itself, and a value put in the place of its equal. After
/// each, what was suspended resumes or does not as `Column`'s does. The
/// first eight are no edits (the column is specified to count what it
/// did), and the state resumes after each to what it resumed to before,
/// with one exception: a column that has no slab yet (a new one, and one
/// loaded from no bytes) is given its first by a splice or an extend, of
/// nothing too, and that ends what was suspended of it, as it does in
/// `Column`. The last two are edits, though they change no value (the
/// column is specified to count one more): the state resumes after
/// neither.
#[test]
fn a_call_that_changes_no_value() {
    fn quiet<T: Kind>() {
        let mut rng = Rng(0x9017e);
        let mut kept = [0usize; 10];
        for round in 0..240 {
            let which = round % 10;
            let n = [0, 1, 8, 250][round / 10 % 4];
            let budget = [3, 64][round / 40 % 2];
            let model: Vec<T> = (0..n).map(|_| T::fresh(&mut rng)).collect();
            let model: Vec<T> = (0..n).map(|i| model[i - i % 3].clone()).collect();
            let (mut v, mut c) = both_with(&model, budget);
            if round / 80 % 3 == 1 {
                let bytes = v.save();
                let opts = LoadOpts::new().with_max_segments(budget);
                v = VerifiedColumn::<T>::load_with(&bytes, opts).unwrap();
                c = Column::<T>::load_with(&bytes, opts).unwrap();
            } else if round / 80 % 3 == 2 && n > 0 {
                // a column that has counted edits
                let x = T::fresh(&mut rng);
                v.push(x.lend());
                c.push(x.clone());
                v.remove(n);
                c.remove(n);
            }
            let (a, b, k) = windows_into(&mut rng, n, 1)[1];
            let (s, t) = {
                let (mut x, mut y) = (v.iter_range(a, b), c.iter_range(a..b));
                if k > 0 {
                    x.nth(k - 1);
                    y.nth(k - 1);
                }
                (x.suspend(), y.suspend())
            };
            let first = resumed(&s, &v);
            assert_eq!(first, Some((a + k, b, window(&model, a + k, b).to_vec())));
            assert_eq!(first, old_resumed(&t, &c));
            let at = rng.below(n + 1);
            let slabs = v.slab_count();
            let none = &model[..0];
            let mut no_runs: Vec<(usize, T)> = Vec::new();
            if which == 3 {
                // runs that give no value
                no_runs = (0..1 + rng.below(3))
                    .map(|_| (0, T::fresh(&mut rng)))
                    .collect();
            }
            match which {
                0 => {
                    v.splice(at, 0, &lent(none));
                    c.splice(at, 0, none.iter().cloned());
                }
                1 => {
                    v.splice_iter(at, 0, none.iter().map(T::lend));
                    c.splice(at, 0, none.iter().cloned());
                }
                2 | 3 => {
                    v.splice_runs(
                        at,
                        0,
                        no_runs.iter().map(|(n, x)| Run {
                            count: *n,
                            value: x.lend(),
                        }),
                    );
                    c.splice_runs(
                        at,
                        0,
                        no_runs.iter().map(|(n, x)| Run {
                            count: *n,
                            value: x.clone(),
                        }),
                    );
                }
                4 => {
                    let len = n + rng.below(3);
                    v.truncate(len);
                    c.truncate(len);
                }
                5 => {
                    v.remove_n(at, 0);
                    c.remove_n(at, 0);
                }
                6 => {
                    v.extend(none.iter().map(T::lend));
                    c.extend(none.iter().cloned());
                }
                7 => {
                    if n > 0 {
                        continue;
                    }
                    v.clear();
                    c.clear();
                }
                8 => {
                    v.remap(|x: T| x);
                    c.remap(|x: T| x);
                }
                _ => {
                    if at == n {
                        continue;
                    }
                    v.splice(at, 1, &lent(&model[at..at + 1]));
                    c.splice(at, 1, model[at..at + 1].iter().cloned());
                }
            }
            let (new, old) = (resumed(&s, &v), old_resumed(&t, &c));
            assert_eq!(new, old, "after call {which} of {n} values");
            if which < 8 {
                let first_slab = slabs == 0 && v.slab_count() == 1;
                assert!(v.slab_count() == slabs || first_slab);
                assert!(!first_slab || [0, 1, 2, 3, 6].contains(&which));
                let same = if first_slab { None } else { first };
                assert_eq!(new, same, "after call {which} of {n} values");
            } else {
                assert!(new.is_none(), "after call {which} of {n} values");
            }
            kept[which] += new.is_some() as usize;
            check_loaded(&v, &c, &model);
        }
        assert!(kept[..8].iter().all(|&k| k > 0), "{kept:?}");
    }
    for_each_kind!(quiet);
}

/// A state resumed on a column other than its own, among columns as
/// loaded, which count no edits. Where `Column`'s state does not resume,
/// nor does this one. Where `Column`'s resumes, this one resumes to the
/// same place, window and values, with one exception: a state that stood
/// at the end of its own column, on a column that ends before that place.
/// `Column` resumes it as an iterator with nothing left that stands past
/// the column's end. The verified API refuses it, since its iterator
/// stands in its column.
#[test]
fn a_state_on_another_column() {
    fn others<T: Kind>() {
        let mut rng = Rng(0x57a7e);
        let mut cols = Vec::new();
        for n in [0, 1, 2, 5, 5, 9, 40, 40, 41, 300] {
            let model: Vec<T> = (0..n).map(|_| T::fresh(&mut rng)).collect();
            let model: Vec<T> = (0..n).map(|i| model[i - i % 2].clone()).collect();
            for budget in [3, 64] {
                let bytes = both_with(&model, 64).0.save();
                let opts = LoadOpts::new().with_max_segments(budget);
                cols.push((
                    VerifiedColumn::<T>::load_with(&bytes, opts).unwrap(),
                    Column::<T>::load_with(&bytes, opts).unwrap(),
                ));
            }
        }
        let (mut on_another, mut past) = (0, 0);
        for (i, (va, ca)) in cols.iter().enumerate() {
            let n = va.len();
            let mut states = Vec::new();
            for (a, b, k) in windows_into(&mut rng, n, 5) {
                let (mut x, mut y) = (va.iter_range(a, b), ca.iter_range(a..b));
                if k > 0 {
                    x.nth(k - 1);
                    y.nth(k - 1);
                }
                states.push((x.suspend(), y.suspend()));
                // and with all of the window read
                assert_eq!(x.by_ref().count(), y.by_ref().count());
                states.push((x.suspend(), y.suspend()));
            }
            // at the column's end, by several roads
            let (mut x, mut y) = (va.iter(), ca.iter());
            x.advance_to(n);
            y.advance_to(n);
            states.push((x.suspend(), y.suspend()));
            x.set_max(usize::MAX);
            y.set_max(usize::MAX);
            states.push((x.suspend(), y.suspend()));
            assert!(x.next().is_none() && y.next().is_none());
            states.push((x.suspend(), y.suspend()));
            let (mut x, mut y) = (va.iter(), ca.iter());
            assert!(x.nth(usize::MAX).is_none() && y.nth(usize::MAX).is_none());
            states.push((x.suspend(), y.suspend()));
            states.push((va.iter_range(n, n).suspend(), ca.iter_range(n..n).suspend()));

            for (s, t) in &states {
                for (j, (vb, cb)) in cols.iter().enumerate() {
                    let (new, old) = (resumed(s, vb), old_resumed(t, cb));
                    if new == old {
                        on_another += (i != j && new.is_some()) as usize;
                        assert!(i != j || new.is_some());
                        continue;
                    }
                    // the one exception
                    let (p, e, rest) = old.unwrap();
                    assert!(new.is_none() && i != j);
                    assert!(p > vb.len() && p == n && e == p && rest.is_empty());
                    past += 1;
                }
            }
        }
        assert!(on_another > 50 && past > 10, "{on_another} {past}");
    }
    for_each_kind!(others);
}

/// The state of an iterator of no column, which counts no edits and no
/// slabs: it resumes on a column that has neither, which a new column is
/// and one loaded from no bytes, to an iterator with nothing to give.
/// On any other column it does not resume. `Column`'s does the same. And
/// a state is plain data, with no borrow in it.
#[test]
fn the_state_of_an_iterator_of_no_column() {
    fn plain<S: Send + Sync + 'static>() {}
    plain::<VerifiedIterState>();

    fn none<T: Kind>() {
        let mut rng = Rng(0xd0);
        let s = VerifiedIter::<T>::default().suspend();
        let t = hexane::Iter::<T>::default().suspend();
        let model: Vec<T> = rng.values(30);
        // what each column gives, and whether it has no slab and no edit
        let mut seen = Vec::new();
        for model in [&model[..], &[]] {
            // built by an edit of a new column
            let (mut v, mut c) = both(model);
            seen.push((resumed(&s, &v), old_resumed(&t, &c), false));
            let bytes = v.save();
            let v2 = VerifiedColumn::<T>::load(&bytes).unwrap();
            let c2 = Column::<T>::load(&bytes).unwrap();
            assert_eq!(v2.slab_count() == 0, model.is_empty());
            seen.push((resumed(&s, &v2), old_resumed(&t, &c2), model.is_empty()));
            // a cleared column has counted an edit, or has a slab still
            v.clear();
            c.clear();
            seen.push((resumed(&s, &v), old_resumed(&t, &c), false));
        }
        let (v, c) = (VerifiedColumn::<T>::new(), Column::<T>::new());
        assert_eq!((v.slab_count(), c.slab_count()), (0, 0));
        seen.push((resumed(&s, &v), old_resumed(&t, &c), true));
        let (mut v, mut c) = (v, c);
        v.clear();
        c.clear();
        seen.push((resumed(&s, &v), old_resumed(&t, &c), true));
        for (new, old, bare) in seen {
            assert_eq!(new, old);
            assert_eq!(new, bare.then(|| (0, 0, Vec::new())));
        }
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
/// the rest with the same error. So do the two checked decoders
/// (`check_same_decoder`), which read what loads.
fn check_same_load<T: Kind>(bytes: &[u8]) -> bool {
    let v = VerifiedColumn::<T>::load(bytes);
    let c = Column::<T>::load(bytes);
    let decodes = check_same_decoder::<T>(bytes);
    match (v, c) {
        (Ok(v), Ok(c)) => {
            assert!(decodes, "no decoder of {bytes:?}, which load");
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
    refuses::<Tag>(&bad);
    refuses::<Option<Tag>>(&bad);
    refuses::<Vec<u8>>(&bad);
    refuses::<Option<Vec<u8>>>(&bad);
    refuses::<String>(&bad);
    refuses::<Option<String>>(&bad);

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
        let all: [Vec<u8>; 10] = [
            both(&rng.values::<u64>(30)).0.save(),
            both(&rng.values::<i64>(30)).0.save(),
            both(&rng.values::<Option<u32>>(30)).0.save(),
            both(&rng.values::<Option<i32>>(30)).0.save(),
            both(&rng.values::<bool>(30)).0.save(),
            both(&rng.values::<Option<Action>>(30)).0.save(),
            both(&rng.values::<Vec<u8>>(30)).0.save(),
            both(&rng.values::<Option<Vec<u8>>>(30)).0.save(),
            both(&rng.values::<String>(30)).0.save(),
            both(&rng.values::<Option<String>>(30)).0.save(),
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
            cross::<Tag>(bytes);
            cross::<Option<Tag>>(bytes);
            cross::<Vec<u8>>(bytes);
            cross::<Option<Vec<u8>>>(bytes);
            cross::<String>(bytes);
            cross::<Option<String>>(bytes);
        }
    }
}

/// What a column of strings gives is a borrow of the column, as a `&str`:
/// it lives as long as the column is not changed, across other reads, and
/// it is the column's own bytes and no copy of them. A value to search
/// for is lent for the call alone. What a clone lends is the clone's.
#[test]
fn strings_are_lent() {
    let last = "größer: 日本語 🦀";
    let mut v = VerifiedColumn::<String>::new();
    v.splice(0, 0, &["ant", "bee", "bee"]);
    v.push(last);
    let first: &str = v.get(0).unwrap();
    let all: Vec<&str> = v.to_vec();
    let mut it = v.iter_range(1, 3);
    let (x, y) = (it.next().unwrap(), it.next().unwrap());
    assert!(it.next().is_none());
    assert_eq!(first, "ant");
    assert_eq!(all, ["ant", "bee", "bee", last]);
    assert_eq!((x, y), ("bee", "bee"));
    // one read and another give the same bytes, in the same place
    assert!(std::ptr::eq(first, all[0]));
    assert!(std::ptr::eq(v.get(3).unwrap(), all[3]));

    let mut it = v.iter();
    let found = {
        let target = String::from("bee");
        it.seek_to_value(target.as_str(), 0..usize::MAX)
    };
    assert_eq!(found, 1..3);
    let bee: &str = it.next().unwrap();
    let at = {
        let target = last.to_owned();
        it.scan_to_value(target.as_str())
    };
    assert_eq!((bee, at, it.next()), ("bee", Some(3), None));

    let w = v.clone();
    let lent = w.get(3);
    v.remove(3);
    v.push("cat");
    drop(v);
    assert_eq!(lent, Some(last));
    assert_eq!(w.to_vec(), ["ant", "bee", "bee", last]);
}

/// How two strings stand in the order of the values is how they stand in
/// Rust, which is how their UTF-8 bytes stand: wherever the first
/// difference falls, in characters of however many bytes, and however
/// long the two are.
#[test]
fn the_order_of_strings_is_the_order_of_str() {
    fn check(a: &str, b: &str) {
        assert_eq!(<String as VValue>::compare(a, b), a.cmp(b), "{a:?} {b:?}");
        assert_eq!(<String as VValue>::compare(b, a), b.cmp(a), "{b:?} {a:?}");
        assert_eq!(<String as VValue>::same(a, b), a == b);
        // as their bytes stand, for a column of byte strings
        assert_eq!(
            <Vec<u8> as VValue>::compare(a.as_bytes(), b.as_bytes()),
            a.cmp(b)
        );
    }
    let text = |cs: &[char]| cs.iter().collect::<String>();
    let mut rng = Rng(0x57a1);
    let base: Vec<char> = (0..90).map(|_| a_char(&mut rng)).collect();
    for len in 0..=base.len() {
        let a = text(&base[..len]);
        check(&a, &a);
        // what `a` starts, and what starts with it
        for more in [1, 2, 7, 30] {
            let mut b = a.clone();
            b.extend((0..more).map(|_| a_char(&mut rng)));
            check(&a, &b);
            check(&text(&base[..len.saturating_sub(more)]), &a);
        }
        // a first difference at the character `d`: the next character
        // there or the one before, or any other, of as many bytes or not;
        // after it the same characters, others, fewer or more
        for d in 0..len {
            let others = [char_after(base[d]), char_before(base[d]), a_char(&mut rng)];
            for other in others {
                let mut b = base[..len].to_vec();
                b[d] = other;
                check(&a, &text(&b));
                let mut c = b.clone();
                for x in &mut c[d + 1..] {
                    *x = a_char(&mut rng);
                }
                check(&a, &text(&c));
                check(&text(&b), &text(&c));
                let cut = d + 1 + rng.below(len - d);
                check(&a, &text(&b[..cut]));
                check(&text(&base[..cut]), &text(&b));
                c.extend((0..rng.below(20)).map(|_| a_char(&mut rng)));
                check(&a, &text(&c));
            }
        }
    }
    // the ends of the order, and of each length of a character
    let ends = [
        "",
        "\0",
        "\u{7f}",
        "\u{80}",
        "\u{7ff}",
        "\u{800}",
        "\u{d7ff}",
        "\u{e000}",
        "\u{ffff}",
        "\u{10000}",
        "\u{10ffff}",
    ];
    for (i, a) in ends.iter().enumerate() {
        for (j, b) in ends.iter().enumerate() {
            assert_eq!(<String as VValue>::compare(a, b), i.cmp(&j), "{a:?} {b:?}");
        }
    }
}

/// The order of strings is that of their bytes, a string before every
/// longer one that starts with it: for strings that are long and differ
/// late, or only in their lengths, in characters of every length.
#[test]
fn a_search_of_long_strings() {
    let mut rng = Rng(0x7e47);
    let stem: Vec<char> = (0..200).map(|_| a_char(&mut rng)).collect();
    let text = |cs: &[char]| cs.iter().collect::<String>();
    let mut strings: Vec<String> = ["", "\0", "\0\0", "\u{1}", "\u{7f}", "\u{80}", "\u{10ffff}"]
        .map(String::from)
        .to_vec();
    for cut in [1, 2, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 199, 200] {
        // the stem's first `cut` characters; then with one more that is
        // less than the stem's next, the same or greater
        let s = text(&stem[..cut]);
        strings.push(s.clone());
        for last in ['\0', 'a', '\u{80}', '語', '\u{10ffff}'] {
            let mut t = s.clone();
            t.push(last);
            strings.push(t);
        }
        if cut < 200 {
            let mut t = stem.clone();
            t[cut] = char_after(stem[cut]);
            strings.push(text(&t));
            t[cut] = char_before(stem[cut]);
            strings.push(text(&t));
        }
    }
    strings.sort();
    strings.dedup();
    // some of them more than once
    let model: Vec<String> = strings
        .iter()
        .enumerate()
        .flat_map(|(i, s)| vec![s.clone(); 1 + i % 3])
        .collect();
    for budget in [3, 64] {
        let (v, c) = both_with(&model, budget);
        let ws = windows(&mut rng, &v.slab_lens(), model.len());
        for x in &strings {
            for &(start, end) in &ws {
                assert!(check_scope(&v, &c, &model, x, start, end));
            }
        }
        // a string that is not there stands between its neighbours
        let mut absent = text(&stem[..150]);
        absent.push_str("\0\0\0");
        assert!(!model.contains(&absent));
        let at = model.iter().filter(|m| **m < absent).count();
        assert_eq!(v.scope_to_value(absent.as_str(), 0, model.len()), at..at);
        assert!(0 < at && at < model.len());
    }
}

/// Strings made longer, shorter, of other characters and empty: the
/// function is given a `String` of its own for each run, and what it
/// hands back is copied into the column.
#[test]
fn a_remap_of_strings() {
    let mut rng = Rng(0x57b3);
    let letters = ['a', 'é', '語', '🦀', '\0'];
    let model: Vec<String> = (0..2000)
        .map(|i| {
            vec![letters[i / 3 % 5]; rng.below(40)]
                .into_iter()
                .collect()
        })
        .collect();
    let (mut v, mut c) = both_with(&model, 16);
    assert!(v.slab_count() > 10);
    let longer = check_remap(&mut v, &mut c, &model, |_, x| {
        let mut y = x.clone();
        y.push_str(&"語".repeat(100));
        y
    });
    assert!(longer.iter().all(|x| x.len() >= 300));
    let shorter = check_remap(&mut v, &mut c, &longer, |_, x| {
        x[..x.len() - 297].to_owned()
    });
    assert!(shorter
        .iter()
        .zip(&model)
        .all(|(s, m)| s.strip_prefix(m.as_str()) == Some("語")));
    let upper = check_remap(&mut v, &mut c, &shorter, |_, x| x.to_uppercase());
    assert!(upper.iter().any(|x| x.starts_with('É')));
    let empty = check_remap(&mut v, &mut c, &upper, |_, _| String::new());
    assert_eq!(empty, vec![String::new(); 2000]);
    assert!(v.is_only(""));
}

/// Strings: a column of them is a column of their UTF-8 bytes, and bytes
/// that are not UTF-8 are no string. Both loaders say so, of bytes that
/// load as byte strings.
#[test]
fn loads_refuse_bad_strings() {
    let invalid = PackError::InvalidUtf8.to_string();
    let bad_format = PackError::BadFormat.to_string();
    assert_ne!(invalid, bad_format);
    // a byte that starts no character; a character that its string's end
    // cuts short; one in more bytes than it needs; half of a surrogate
    // pair; one past the last character; a byte from the middle of a
    // character; such a string in a run, and second in a literal run; a
    // character with a byte in each of two strings
    let not_utf8: [&[u8]; 9] = [
        &[0x7f, 1, 0xff],
        &[0x7f, 2, 0xe6, 0x97],
        &[0x7f, 2, 0xc0, 0x80],
        &[0x7f, 3, 0xed, 0xa0, 0x80],
        &[0x7f, 4, 0xf4, 0x90, 0x80, 0x80],
        &[0x7f, 1, 0x80],
        &[2, 1, 0xff],
        &[0x7e, 1, b'a', 1, 0xff],
        &[0x7e, 1, 0xc3, 1, 0xa9],
    ];
    for b in not_utf8 {
        assert_eq!(refusal::<String>(b), invalid, "{b:?}");
        assert_eq!(refusal::<Option<String>>(b), invalid, "{b:?}");
        assert!(check_same_load::<Vec<u8>>(b), "refused {b:?}");
        assert!(check_same_load::<Option<Vec<u8>>>(b), "refused {b:?}");
    }
    // a null and then a string that is not UTF-8
    assert_eq!(refusal::<Option<String>>(&[0, 1, 0x7f, 1, 0xff]), invalid);
    // bytes with two faults: the loaders name the same one
    let twice: [&[u8]; 5] = [
        &[0x7f, 1, 0xff, 0x7f, 3, 1],
        &[0x7f, 3, 1, 0xff],
        &[0x7f, 1, 0xff, 0x7f, 1, 0xff],
        &[0x7e, 1, 0xff, 1, 0xff],
        &[2, 1, 0xff, 2, 1, 0xff],
    ];
    for b in twice {
        assert!(!check_same_load::<String>(b), "loaded {b:?}");
        assert!(!check_same_load::<Option<String>>(b), "loaded {b:?}");
    }
    // the faults of byte strings are faults of strings: a length past
    // the data's end
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
        assert_eq!(refusal::<String>(b), bad_format, "{b:?}");
    }
    // a truncated length; an overlong one; two runs of one string; one
    // string twice in a literal run; a run with no string; a literal run
    // that ends early; a null run, which strings do not have
    let bad: [&[u8]; 7] = [
        &[2, 0x81],
        &[2, 0x81, 0x00, 7],
        &[2, 1, 7, 2, 1, 7],
        &[0x7e, 1, 7, 1, 7],
        &[2, 1, 7, 9],
        &[0x7d, 1, 7, 0],
        &[2, 1, 7, 0, 2],
    ];
    for b in bad {
        assert!(!check_same_load::<String>(b), "loaded {b:?}");
    }
    // these are columns: the empty string in a run; one string after
    // another that it is the start of; characters of two bytes, of three
    // and of four, the last of all characters among them
    let good: [&[u8]; 7] = [
        &[3, 0],
        &[0x7e, 1, 7, 2, 7, 7],
        &[2, 1, 7, 0x7f, 0],
        &[0x7f, 2, 0xc3, 0xa9],
        &[2, 3, 0xe8, 0xaa, 0x9e],
        &[0x7f, 4, 0xf0, 0x9f, 0xa6, 0x80],
        &[0x7f, 4, 0xf4, 0x8f, 0xbf, 0xbf],
    ];
    for b in good {
        assert!(check_same_load::<String>(b), "refused {b:?}");
    }
    let two = [0x7e, 2, 0xc3, 0xa9, 4, 0xf0, 0x9f, 0xa6, 0x80];
    let v = VerifiedColumn::<String>::load(&two).unwrap();
    assert_eq!(v.to_vec(), ["é", "🦀"]);
    // strings save as their bytes do, and load as them
    let model: Vec<String> = Rng(0x57b17e).values(200);
    let as_bytes: Vec<Vec<u8>> = model.iter().map(|s| s.as_bytes().to_vec()).collect();
    let saved = both(&model).0.save();
    assert_eq!(saved, both(&as_bytes).0.save());
    let v = VerifiedColumn::<Vec<u8>>::load(&saved).unwrap();
    assert_eq!(new_values(&v), as_bytes);
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
    longest::<String>(most, "größer".to_owned());
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
        // the decoders count no total: they read the runs, or refuse alike
        check_same_decoder::<T>(bytes);
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
    assert_eq!(columns, 21 * 17);
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
    // a state that counts no edits and no slabs would fit it, and to
    // resume it asks for the column's parts: both APIs panic
    let (s, t) = (v.iter().suspend(), c.iter().suspend());
    let new = panic_of(std::panic::AssertUnwindSafe(|| s.try_resume(&v).is_ok())).unwrap();
    let old = panic_of(std::panic::AssertUnwindSafe(|| t.try_resume(&c).is_ok())).unwrap();
    assert!(new.contains("after a panic"), "{new}");
    assert_eq!(new, old);
    // and neither takes an edit any more
    let new = panic_of(std::panic::AssertUnwindSafe(|| v.push(7))).unwrap();
    let old = panic_of(std::panic::AssertUnwindSafe(|| c.push(7))).unwrap();
    assert!(new.contains("after a panic"), "{new}");
    assert_eq!(new, old);
}

// ── `VerifiedEncoder` ───────────────────────────────────────────────────
//
// The verified API's encoder against `Column`'s (`RleEncoder`, and for
// `bool` `BoolEncoder`): given the same values in the same steps, the two
// count the same and save the same bytes, which are the bytes a column of
// the values saves.

/// `Column`'s encoder for the value type.
fn old_encoder<'a, T: Kind>() -> Encoder<'a, T> {
    <<T as ColumnValueRef>::Encoding<Leb128> as ColumnEncoding>::encoder()
}

/// An encoder of each API, given the same values in the same steps. After
/// each step both have counted the values given so far.
struct Encoders<'a, T: Kind> {
    new: VerifiedEncoder<'a, T>,
    old: Encoder<'a, T>,
    len: usize,
}

impl<'a, T: Kind> Encoders<'a, T> {
    /// Encoders with the segment budget `max`, or with none.
    fn with(max: Option<usize>) -> Self {
        let mut old = old_encoder::<T>();
        let new = match max {
            Some(max) => {
                old.max_segments(max);
                VerifiedEncoder::with_max_segments(max)
            }
            None => VerifiedEncoder::new(),
        };
        let mut e = Encoders { new, old, len: 0 };
        e.counted(0);
        e
    }

    /// Encoders with the default segment budget: what `with_segments`
    /// gives in each API.
    fn with_segments() -> Self {
        let new = VerifiedEncoder::with_segments();
        let old = old_encoder::<T>().with_segments();
        let mut e = Encoders { new, old, len: 0 };
        e.counted(0);
        e
    }

    fn counted(&mut self, added: usize) {
        self.len += added;
        assert_eq!(self.new.len(), self.len);
        assert_eq!(self.old.len(), self.len);
        assert_eq!(self.new.is_empty(), self.len == 0);
        assert_eq!(self.old.is_empty(), self.len == 0);
    }

    fn append(&mut self, x: &'a T) {
        self.new.append(x.lend());
        self.old.append(x.as_column_ref());
        self.counted(1);
    }

    fn append_owned(&mut self, x: &T) {
        self.new.append_owned(x.clone());
        self.old.append_owned(x.clone());
        self.counted(1);
    }

    fn append_n(&mut self, x: &'a T, n: usize) {
        self.new.append_n(x.lend(), n);
        self.old.append_n(x.as_column_ref(), n);
        self.counted(n);
    }

    fn append_n_owned(&mut self, x: &T, n: usize) {
        self.new.append_n_owned(x.clone(), n);
        self.old.append_n_owned(x.clone(), n);
        self.counted(n);
    }

    fn extend(&mut self, xs: &'a [T]) {
        self.new.extend(xs.iter().map(T::lend));
        self.old.extend(xs.iter().map(|x| x.as_column_ref()));
        self.counted(xs.len());
    }

    /// The values `model`, given in one of seven ways: one by one,
    /// borrowed (0) or owned (1); run by run, borrowed (2) or owned (3);
    /// run by run, each run in two parts with a run of no values of
    /// another value between them (4); in pieces from an iterator, some
    /// of them empty (5); or by all of these in turn (6).
    fn given(model: &'a [T], other: &'a T, how: usize, rng: &mut Rng) -> Self {
        Self::given_with(None, model, other, how, rng)
    }

    /// [`given`](Self::given), to encoders with the segment budget `max`
    /// or with none.
    fn given_with(
        max: Option<usize>,
        model: &'a [T],
        other: &'a T,
        how: usize,
        rng: &mut Rng,
    ) -> Self {
        Self::with(max).give(model, other, how, rng)
    }

    /// These encoders, given `model` as [`given`](Self::given) gives it.
    fn give(self, model: &'a [T], other: &'a T, how: usize, rng: &mut Rng) -> Self {
        let mut e = self;
        let mut at = 0;
        while at < model.len() {
            let x = &model[at];
            let run = model[at..].iter().take_while(|y| *y == x).count();
            let step = if how == 6 { rng.below(6) } else { how };
            at += match step {
                0 => {
                    e.append(x);
                    1
                }
                1 => {
                    e.append_owned(x);
                    1
                }
                2 => {
                    e.append_n(x, run);
                    run
                }
                3 => {
                    e.append_n_owned(x, run);
                    run
                }
                4 => {
                    let first = rng.below(run + 1);
                    e.append_n(x, first);
                    e.append_n(other, 0);
                    e.append_n_owned(other, 0);
                    e.append_n_owned(x, run - first);
                    run
                }
                _ => {
                    let n = rng.below((model.len() - at).min(70) + 1);
                    e.extend(&model[at..at + n]);
                    n
                }
            };
        }
        assert_eq!(e.len, model.len());
        e
    }

    /// What both save: the same bytes.
    fn save(self) -> Vec<u8> {
        assert_eq!(
            format!("{:?}", self.new),
            format!("VerifiedEncoder {{ len: {} }}", self.len)
        );
        let bytes = self.new.save();
        assert_eq!(bytes, self.old.save());
        bytes
    }

    /// What both save after what a buffer holds: the same bytes in the
    /// same place, and what the buffer held before them.
    fn save_to(self, held: &[u8]) -> Vec<u8> {
        let (mut a, mut b) = (held.to_vec(), held.to_vec());
        let range = self.new.save_to(&mut a);
        assert_eq!(range, self.old.save_to(&mut b));
        assert_eq!(a, b);
        assert_eq!(range, held.len()..a.len());
        assert_eq!(a[..held.len()], *held);
        a.split_off(held.len())
    }

    /// What both write after what a buffer holds when asked to save unless
    /// every value is `x`: the same bytes in the same place, or both
    /// nothing (`None`), which the range returned tells.
    fn save_to_unless(self, held: &[u8], x: &'a T) -> Option<Vec<u8>> {
        let (mut a, mut b) = (held.to_vec(), held.to_vec());
        let range = self.new.save_to_unless(&mut a, x.lend());
        let old = self.old.save_to_unless(&mut b, x.as_column_ref());
        assert_eq!(range, old);
        assert_eq!(a, b);
        assert_eq!(range, held.len()..a.len());
        assert_eq!(a[..held.len()], *held);
        (!range.is_empty()).then(|| a.split_off(held.len()))
    }

    /// The column each becomes.
    fn into_column(self) -> (VerifiedColumn<T>, Column<T>) {
        (self.new.into_column(), self.old.into_column())
    }
}

/// Encoders given `model` in each way save what a column of `model`
/// saves, and both APIs load it as `model`.
fn check_encoders<T: Kind>(model: &[T], other: &T, rng: &mut Rng) {
    let (v, c) = both(model);
    let bytes = v.save();
    assert_eq!(bytes, c.save());
    for how in 0..7 {
        let salt = rng.next() | 1;
        let saved = Encoders::given(model, other, how, &mut Rng(salt)).save();
        assert_eq!(saved, bytes, "how {how}");
        let held = [vec![], vec![9u8; 3]][rng.below(2)].clone();
        let saved = Encoders::given(model, other, how, &mut Rng(salt)).save_to(&held);
        assert_eq!(saved, bytes, "how {how}");
    }
    assert_eq!(
        new_values(&VerifiedColumn::<T>::load(&bytes).unwrap()),
        model
    );
    assert_eq!(old_values(&Column::<T>::load(&bytes).unwrap()), model);

    // from what a column's iterator gives: the column's bytes again
    let mut e = VerifiedEncoder::<T>::new();
    e.extend(v.iter());
    assert_eq!(e.len(), model.len());
    assert_eq!(e.save(), bytes);
    // and in two parts, with a value of the model's own between
    let cut = rng.below(model.len() + 1);
    let mut e = VerifiedEncoder::<T>::default();
    e.extend(v.iter_range(0, cut));
    if let Some(x) = model.get(cut) {
        e.append_owned(x.clone());
        e.extend(v.iter_range(cut + 1, model.len()));
    }
    let mut out = vec![1u8, 2];
    assert_eq!(e.save_to(&mut out), 2..2 + bytes.len());
    assert_eq!(out[2..], bytes[..]);
}

fn encoders<T: Kind>() {
    let mut rng = Rng(0xe7c0de);
    let (a, b, c) = (T::fresh(&mut rng), T::fresh(&mut rng), T::fresh(&mut rng));
    let pick = |i: usize| [&a, &b, &c][i].clone();
    let mut shapes: Vec<Vec<T>> = vec![
        vec![],
        vec![pick(0)],
        vec![pick(0); 2],
        vec![pick(0); 1000],
        (0..1000).map(|i| pick(i / 7 % 2)).collect(),
        (0..1000).map(|i| pick(i % 3)).collect(),
        (0..3000).map(|_| T::fresh(&mut rng)).collect(),
        [0, 0, 1, 2, 2, 2, 0, 1, 1, 0].map(pick).to_vec(),
    ];
    for _ in 0..24 {
        shapes.push(rng.values(300));
    }
    for model in shapes {
        let other = T::fresh(&mut rng);
        check_encoders(&model, &other, &mut rng);
    }
}

#[test]
fn encoders_agree() {
    for_each_kind!(encoders);
}

/// Values with no two alike in a row are written as one run with their
/// count before them, which the encoder puts there when the run ends. The
/// count takes one byte up to 64 values, two up to 8192, and three from
/// there: runs of every length about those, alone and between runs of
/// equal values.
fn literal_runs<T: Kind>() {
    let mut rng = Rng(0x117e7a1);
    let mut pool: Vec<T> = (0..9000).map(|_| T::fresh(&mut rng)).collect();
    pool.dedup();
    let lens = (0..=140).chain([255, 256, 257, 8190, 8191, 8192, 8193, 8194]);
    for len in lens.filter(|&n| n <= pool.len()) {
        let lit = &pool[pool.len() - len..];
        let other = T::fresh(&mut rng);
        check_encoders(lit, &other, &mut rng);
        // a run of equal values before and after, each of a value other
        // than its neighbour's
        let Some((first, last)) = lit.first().zip(lit.last()) else {
            continue;
        };
        let before = pool.iter().find(|x| *x != first).unwrap_or(first);
        let after = pool.iter().find(|x| *x != last).unwrap_or(last);
        let mut model = vec![before.clone(); 3];
        model.extend_from_slice(lit);
        model.extend(std::iter::repeat_n(after.clone(), 2));
        check_encoders(&model, &other, &mut rng);
    }
}

#[test]
fn encoders_agree_on_literal_runs() {
    for_each_kind!(literal_runs);
}

/// An encoder given nothing saves nothing, as `Column`'s does and as a
/// column of no values does, however it was made and whatever runs of no
/// values it was given.
fn empty_encoders<T: Kind>() {
    let mut rng = Rng(0xe0);
    let x = T::fresh(&mut rng);
    let none = VerifiedColumn::<T>::new().save();
    assert_eq!(none, Column::<T>::new().save());
    assert_eq!(none, old_encoder::<T>().save());
    assert_eq!(VerifiedEncoder::<T>::new().save(), none);
    assert_eq!(VerifiedEncoder::<T>::default().save(), none);
    let mut e = VerifiedEncoder::<T>::new();
    e.append_n(x.lend(), 0);
    e.append_n_owned(x.clone(), 0);
    e.extend(std::iter::empty());
    assert!(e.is_empty());
    assert_eq!(format!("{e:?}"), "VerifiedEncoder { len: 0 }");
    let mut out = vec![5u8];
    assert_eq!(e.save_to(&mut out), 1..1 + none.len());
    assert_eq!(out[1..], none[..]);
    let mut old = vec![5u8];
    assert_eq!(old_encoder::<T>().save_to(&mut old), 1..1 + none.len());
    assert_eq!(old, out);
}

#[test]
fn encoders_of_nothing() {
    for_each_kind!(empty_encoders);
}

/// An encoder counts its values in a `usize`, and runs make many of them
/// cheap to give. Up to `usize::MAX - 2` values are counted (of `bool`,
/// one fewer), and saved as `Column`'s encoder saves them; given more,
/// both APIs panic with the same words, whether the last values come in a
/// run or one by one.
#[cfg(panic = "unwind")]
#[test]
fn encoders_count_to_the_end() {
    // each run fits the format's count on any target
    let half = usize::MAX / 2;

    let numbers = |first: usize, then: &'static [(u64, usize)]| {
        let new = panic_of(move || {
            let mut e = VerifiedEncoder::<u64>::new();
            e.append_n(1, first);
            e.append_n(2, first);
            for &(x, n) in then {
                match n {
                    1 => e.append(x),
                    _ => e.append_n(x, n),
                }
            }
            (e.len(), e.save())
        });
        let old = panic_of(move || {
            let mut e = old_encoder::<u64>();
            e.append_n(1, first);
            e.append_n(2, first);
            for &(x, n) in then {
                match n {
                    1 => e.append(x),
                    _ => e.append_n(x, n),
                }
            }
            (e.len(), e.save())
        });
        assert_eq!(new, old);
        new
    };
    assert_eq!(numbers(half - 1, &[]), None);
    assert_eq!(numbers(half - 1, &[(3, 1)]), None);
    assert_eq!(numbers(half - 1, &[(2, 1), (2, 0)]), None);
    let said = numbers(half - 1, &[(3, 2)]).unwrap();
    assert!(said.contains("encoder too long to append to"), "{said}");
    assert_eq!(numbers(half - 1, &[(3, 1), (4, 1)]), Some(said.clone()));
    assert_eq!(numbers(half - 1, &[(2, 2)]), Some(said.clone()));
    assert_eq!(numbers(half, &[]), Some(said.clone()));
    assert_eq!(
        numbers(half - 1, &[(3, 0), (3, usize::MAX)]),
        Some(said.clone())
    );

    let mut e = VerifiedEncoder::<u64>::new();
    e.append_n(1, half - 1);
    e.append_n_owned(2, half - 1);
    assert_eq!(e.len(), usize::MAX - 3);
    assert_eq!(
        format!("{e:?}"),
        format!("VerifiedEncoder {{ len: {} }}", usize::MAX - 3)
    );

    let bools = |first: usize, then: &'static [(bool, usize)]| {
        let new = panic_of(move || {
            let mut e = VerifiedEncoder::<bool>::new();
            e.append_n(true, first);
            e.append_n(false, first);
            for &(x, n) in then {
                match n {
                    1 => e.append(x),
                    _ => e.append_n(x, n),
                }
            }
            (e.len(), e.save())
        });
        let old = panic_of(move || {
            let mut e = old_encoder::<bool>();
            e.append_n(true, first);
            e.append_n(false, first);
            for &(x, n) in then {
                match n {
                    1 => e.append(x),
                    _ => e.append_n(x, n),
                }
            }
            (e.len(), e.save())
        });
        assert_eq!(new, old);
        new
    };
    assert_eq!(bools(half - 1, &[]), None);
    assert_eq!(bools(half - 1, &[(true, 0), (false, 0)]), None);
    assert_eq!(bools(half - 1, &[(true, 1)]), Some(said.clone()));
    assert_eq!(bools(half - 1, &[(false, 1)]), Some(said.clone()));
    assert_eq!(bools(half - 1, &[(false, 2)]), Some(said.clone()));
    assert_eq!(bools(half, &[]), Some(said.clone()));

    // nulls, whose count has no sign either
    let nulls = |n: usize, more: usize| {
        let new = panic_of(move || {
            let mut e = VerifiedEncoder::<Option<u64>>::new();
            e.append_n(None, n);
            e.append_n(None, more);
            (e.len(), e.save())
        });
        let old = panic_of(move || {
            let mut e = old_encoder::<Option<u64>>();
            e.append_n(None, n);
            e.append_n(None, more);
            (e.len(), e.save())
        });
        assert_eq!(new, old);
        new
    };
    assert_eq!(nulls(half, half - 1), None);
    assert_eq!(nulls(usize::MAX - 2, 0), None);
    assert_eq!(nulls(usize::MAX - 2, 1), Some(said.clone()));
    assert_eq!(nulls(usize::MAX, 0), Some(said));
}

/// The longest run of equal numbers the format writes is of `i64::MAX`.
/// An encoder given one more of the value panics in both APIs, with the
/// same words. (Where a `usize` has 64 bits: one of 32 bits counts less.)
#[cfg(target_pointer_width = "64")]
#[test]
fn encoders_of_the_longest_runs() {
    let most = i64::MAX as usize;
    let run = |count: usize, then: Option<u64>| {
        let new = panic_of(move || {
            let mut e = VerifiedEncoder::<u64>::new();
            e.append_n(7, count);
            if let Some(x) = then {
                e.append(x);
            }
            (e.len(), e.save())
        });
        let old = panic_of(move || {
            let mut e = old_encoder::<u64>();
            e.append_n(7, count);
            if let Some(x) = then {
                e.append(x);
            }
            (e.len(), e.save())
        });
        assert_eq!(new, old);
        new
    };
    assert_eq!(run(most, None), None);
    assert_eq!(run(most, Some(8)), None);
    let said = run(most, Some(7)).unwrap();
    assert!(said.contains("is not encodable"), "{said}");
    assert_eq!(run(most + 1, None), Some(said));

    let mut e = VerifiedEncoder::<u64>::new();
    e.append_n(7, most);
    assert_eq!(e.save(), VerifiedColumn::<u64>::fill(most, 7).save());
}

// ── `VerifiedEncoder`: a segment budget ─────────────────────────────────
//
// An encoder made with a segment budget against `Column`'s encoder set to
// the same one: what they save, what they write when asked to save unless
// every value is a given one, and the column each becomes.

/// The budgets an encoder is tried with: none, the two smallest (with
/// which it cuts the most), and larger ones.
const BUDGETS: [Option<usize>; 8] = [
    None,
    Some(2),
    Some(3),
    Some(4),
    Some(5),
    Some(8),
    Some(16),
    Some(1000),
];

/// Encoders with the segment budget `max` (or none), given `model` in
/// each way. They save what a column of `model` saves. Asked to save
/// unless every value is `x`, both write those bytes or both nothing:
/// nothing only if every value is `x`, and always then if there is no
/// budget. And the columns they become hold `model` in the same slabs,
/// and take an edit alike.
fn check_budgeted<T: Kind>(max: Option<usize>, model: &[T], other: &T, rng: &mut Rng) {
    let (v, _) = both(model);
    let bytes = v.save();
    for how in 0..7 {
        let salt = rng.next() | 1;
        let given = || Encoders::given_with(max, model, other, how, &mut Rng(salt));
        assert_eq!(given().save(), bytes, "{max:?}, how {how}");
        assert_eq!(given().save_to(&[7]), bytes, "{max:?}, how {how}");

        let held = [vec![], vec![9u8; 3]][rng.below(2)].clone();
        for x in model.first().into_iter().chain(model.last()).chain([other]) {
            let all = model.iter().all(|y| y == x);
            let written = given().save_to_unless(&held, x);
            if written.is_none() {
                assert!(all, "{max:?}, how {how}");
            } else {
                assert_eq!(written.as_ref(), Some(&bytes), "{max:?}, how {how}");
            }
            if max.is_none() {
                assert_eq!(written.is_none(), all, "how {how}");
            }
        }

        let (mut nv, mut nc) = given().into_column();
        check_loaded(&nv, &nc, model);
        let at = rng.below(model.len() + 1);
        let del = rng.below((model.len() - at).min(5) + 1);
        nv.splice(at, del, &[other.lend(), other.lend()]);
        nc.splice(at, del, [other.clone(), other.clone()]);
        let mut edited = model.to_vec();
        edited.splice(at..at + del, [other.clone(), other.clone()]);
        check_loaded(&nv, &nc, &edited);
    }
}

fn budgeted<T: Kind>() {
    let mut rng = Rng(0xb0d6e7);
    let (a, b, c) = (T::fresh(&mut rng), T::fresh(&mut rng), T::fresh(&mut rng));
    let pick = |i: usize| [&a, &b, &c][i].clone();
    let mut shapes: Vec<Vec<T>> = vec![
        vec![],
        vec![pick(0)],
        vec![pick(0); 2],
        vec![pick(0); 300],
        (0..600).map(|i| pick(i / 7 % 2)).collect(),
        (0..600).map(|i| pick(i % 3)).collect(),
        (0..1500).map(|_| T::fresh(&mut rng)).collect(),
        [0, 0, 1, 2, 2, 2, 0, 1, 1, 0].map(pick).to_vec(),
    ];
    for _ in 0..16 {
        shapes.push(rng.values(200));
    }
    for (i, model) in shapes.iter().enumerate() {
        let other = T::fresh(&mut rng);
        // the small shapes with every budget, the others with three each
        let some = [0, 1, 2].map(|k| BUDGETS[(i + 3 * k) % BUDGETS.len()]);
        let budgets = if model.len() <= 10 {
            &BUDGETS[..]
        } else {
            &some[..]
        };
        for &max in budgets {
            check_budgeted(max, model, &other, &mut rng);
        }
    }
}

#[test]
fn budgeted_encoders_agree() {
    for_each_kind!(budgeted);
}

/// Encoders with the default segment budget (`with_segments`) against
/// `Column`'s: they save the same bytes and become columns of the same
/// slabs. The slabs are those a budget of 64 gives, and where the values
/// are many enough to be cut they are not those a budget of 32 gives.
fn default_budget<T: Kind>() {
    let mut rng = Rng(0x5e6de7a);
    let layout = |v: &VerifiedColumn<T>| (v.slab_lens(), v.slab_segments());
    let models: Vec<Vec<T>> = vec![
        vec![],
        vec![T::fresh(&mut rng); 3],
        rng.values(400),
        (0..900).map(|_| T::fresh(&mut rng)).collect(),
    ];
    for (i, model) in models.iter().enumerate() {
        let other = T::fresh(&mut rng);
        let (v, _) = both(model);
        let bytes = v.save();
        for how in 0..7 {
            let salt = rng.next() | 1;
            let given = || Encoders::with_segments().give(model, &other, how, &mut Rng(salt));
            assert_eq!(given().save(), bytes, "how {how}");
            let (nv, nc) = given().into_column();
            check_loaded(&nv, &nc, model);
            let with = |max| {
                let e = Encoders::given_with(Some(max), model, &other, how, &mut Rng(salt));
                e.into_column().0
            };
            assert_eq!(layout(&nv), layout(&with(64)), "how {how}");
            if i == 3 {
                assert!(nv.slab_count() > 1, "how {how}");
                assert_ne!(layout(&nv), layout(&with(32)), "how {how}");
            }
        }
    }
}

#[test]
fn default_budget_encoders_agree() {
    for_each_kind!(default_budget);
}

/// When an encoder with a budget cuts equal values apart, and so writes
/// them though every one is the value it is to save unless. No
/// specification says when: this holds both APIs to what `Column`'s
/// encoder was seen to do. With a budget of 2 or 3, equal values of a
/// type other than `bool` given in more than one call are written. Given
/// in one call, or of `bool`, or with a budget of 4 or more, they are not.
fn equal_values_cut<T: Kind>() {
    let mut rng = Rng(0xc07);
    let is_bool = std::any::type_name::<T>() == "bool";
    for x in [T::fresh(&mut rng), T::fresh(&mut rng)] {
        for max in [2, 3, 4, 5, 8, 16, 1000] {
            for n in [0, 1, 2, 3, 10, 300] {
                // in one call, in two, or in a call for each value
                for calls in 0..3 {
                    let mut e = Encoders::<T>::with(Some(max));
                    match calls {
                        0 => e.append_n(&x, n),
                        1 => {
                            e.append_n(&x, n / 2);
                            e.append_n(&x, n - n / 2);
                        }
                        _ => (0..n).for_each(|_| e.append(&x)),
                    }
                    let written = e.save_to_unless(&[5], &x).is_some();
                    let cut = max < 4 && !is_bool && n >= 2 && calls != 0;
                    assert_eq!(written, cut, "{max}, {n} of {x:?}, {calls}");
                }
            }
        }
    }
}

#[test]
fn budgeted_encoders_of_equal_values() {
    for_each_kind!(equal_values_cut);
}

/// An encoder becomes a column of up to `usize::MAX - 3` values, the most
/// a column counts, with a budget or without. An encoder of numbers or of
/// nulls counts one more: of that both APIs make no column and panic, with
/// the same words (those of the column's bound with a budget, and of the
/// load that fails without one).
#[cfg(panic = "unwind")]
#[test]
fn encoders_become_columns_to_the_end() {
    // each run fits the format's count on any target
    let half = usize::MAX / 2;

    fn made<T>(max: Option<usize>, runs: [(T, usize); 3]) -> Option<String>
    where
        T: Kind + std::panic::UnwindSafe,
    {
        let (r, s) = (runs.clone(), runs);
        let new = panic_of(move || {
            let mut e = match max {
                Some(max) => VerifiedEncoder::<T>::with_max_segments(max),
                None => VerifiedEncoder::new(),
            };
            for (x, n) in &r {
                e.append_n_owned(x.clone(), *n);
            }
            let c = e.into_column();
            let n = c.len();
            let ends = [c.get(0), c.get(n - 1), c.get(n)].map(|x| x.map(T::keep));
            (n, ends, c.save())
        });
        let old = panic_of(move || {
            let mut e = old_encoder::<T>();
            if let Some(max) = max {
                e.max_segments(max);
            }
            for (x, n) in &s {
                e.append_n_owned(x.clone(), *n);
            }
            let c: Column<T> = e.into_column();
            let n = c.len();
            let ends = [c.get(0), c.get(n - 1), c.get(n)].map(|x| x.map(self::old::<T>));
            (n, ends, c.save())
        });
        assert_eq!(new, old, "{max:?}");
        new
    }

    for max in [None, Some(2), Some(3), Some(16)] {
        let fits = |n: usize| [(1u64, half - 1), (2, half - 1), (3, n)];
        assert_eq!(made(max, fits(0)), None);
        let said = made(max, fits(1)).unwrap();
        match max {
            Some(_) => assert!(said.contains("more than usize::MAX - 3 items"), "{said}"),
            None => assert!(said.contains("`Result::unwrap()` on an `Err`"), "{said}"),
        }

        let bools = [(true, half - 1), (false, half - 1), (true, 0)];
        assert_eq!(made(max, bools), None);

        let nulls = |n: usize| [(None::<u64>, half), (None, half - 2), (None, n)];
        assert_eq!(made(max, nulls(0)), None);
        assert_eq!(made(max, nulls(1)), Some(said));
    }
}

// ── The encoder's remap forms ───────────────────────────────────────────
//
// `save_to_and_remap` and `save_to_unless_and_remap` against
// `RleEncoder`'s. `BoolEncoder` has neither: an encoder of `bool` is
// checked against the values alone, as every other is as well.

impl<'a, T: Kind> Encoders<'a, T> {
    /// What both write after what a buffer holds when asked to save what
    /// `f` gives for their values, and the values `f` was called with.
    /// `f` is given how many calls came before and the value. Both call
    /// it with the same values in the same order, and write the same
    /// bytes in the same place. Of `bool` only the new encoder is asked.
    fn save_to_and_remap(self, held: &[u8], f: impl Fn(usize, &T) -> T) -> (Vec<u8>, Vec<T>) {
        let (mut a, mut b) = (held.to_vec(), held.to_vec());
        let (seen_new, seen_old) = (RefCell::new(vec![]), RefCell::new(vec![]));
        let range = self.new.save_to_and_remap(&mut a, |x: T| {
            let mut seen = seen_new.borrow_mut();
            seen.push(x.clone());
            f(seen.len() - 1, &x)
        });
        let g = |x: T| {
            let mut seen = seen_old.borrow_mut();
            seen.push(x.clone());
            f(seen.len() - 1, &x)
        };
        let seen = seen_new.into_inner();
        if let Some(old) = T::old_remap(self.old, &mut b, &g) {
            assert_eq!(range, old);
            assert_eq!(a, b);
            assert_eq!(seen, seen_old.into_inner());
        }
        assert_eq!(range, held.len()..a.len());
        assert_eq!(a[..held.len()], *held);
        (a.split_off(held.len()), seen)
    }

    /// The same when asked to save what `f` gives unless all of it is
    /// `x`: the same bytes in the same place, or both nothing (`None`),
    /// which the range returned tells.
    fn save_to_unless_and_remap(
        self,
        held: &[u8],
        x: &'a T,
        f: impl Fn(usize, &T) -> T,
    ) -> (Option<Vec<u8>>, Vec<T>) {
        let (mut a, mut b) = (held.to_vec(), held.to_vec());
        let (seen_new, seen_old) = (RefCell::new(vec![]), RefCell::new(vec![]));
        let range = self.new.save_to_unless_and_remap(&mut a, x.lend(), |x: T| {
            let mut seen = seen_new.borrow_mut();
            seen.push(x.clone());
            f(seen.len() - 1, &x)
        });
        let g = |x: T| {
            let mut seen = seen_old.borrow_mut();
            seen.push(x.clone());
            f(seen.len() - 1, &x)
        };
        let seen = seen_new.into_inner();
        if let Some(old) = T::old_remap_unless(self.old, &mut b, x, &g) {
            assert_eq!(range, old);
            assert_eq!(a, b);
            assert_eq!(seen, seen_old.into_inner());
        }
        assert_eq!(range, held.len()..a.len());
        assert_eq!(a[..held.len()], *held);
        let written = (!range.is_empty()).then(|| a.split_off(held.len()));
        (written, seen)
    }
}

/// The runs a remap of an encoder calls its function for: the runs of
/// equal values of each slab the encoder has cut, and of what it holds
/// after them. `lens` are the lengths of those. A run that a cut divides
/// is two.
fn runs_in<T: Kind>(model: &[T], lens: &[usize]) -> Vec<(usize, T)> {
    let mut at = 0;
    let mut runs = vec![];
    for n in lens {
        runs.extend(runs_of(&model[at..at + n]));
        at += n;
    }
    assert_eq!(at, model.len());
    runs
}

/// Encoders with the segment budget `max` (or none), given `model` in
/// each way and asked to save what a function gives for their values,
/// by functions of several kinds: one that changes nothing, one that
/// gives one value for all, ones that give few values or many for the
/// encoder's values, one that turns the values round among themselves,
/// and one that gives another value at each call. Both call the function
/// once for each run of each slab the encoder has cut, in order, which
/// for an encoder without a budget is once for each run of `model`. Both
/// write what a column of the values the calls gave saves. And asked to
/// save unless all of those is `x`, both write those bytes or both
/// nothing: nothing exactly if all of them is `x`, whatever the budget.
fn check_remapping<T: Kind>(max: Option<usize>, model: &[T], other: &T, rng: &mut Rng) {
    let mut keys: Vec<T> = vec![];
    for x in model {
        if !keys.contains(x) {
            keys.push(x.clone());
        }
    }
    let key = |x: &T| keys.iter().position(|k| k == x).unwrap();
    let few: Vec<T> = (0..3).map(|_| T::fresh(rng)).collect();
    let many: Vec<T> = (0..keys.len() + 7).map(|_| T::fresh(rng)).collect();
    for how in 0..7 {
        let salt = rng.next() | 1;
        // where the encoder has cut: the slabs of the column that the old
        // encoder becomes when given the same
        let lens = match max {
            Some(_) => {
                let e = Encoders::given_with(max, model, other, how, &mut Rng(salt));
                e.into_column().1.slab_lens()
            }
            None => vec![model.len()],
        };
        let runs = runs_in(model, &lens);
        let calls: Vec<T> = runs.iter().map(|(_, x)| x.clone()).collect();
        if max.is_none() {
            assert_eq!(runs, runs_of(model));
        }
        for kind in 0..6 {
            let f = |n: usize, x: &T| -> T {
                match kind {
                    0 => x.clone(),
                    1 => other.clone(),
                    2 => few[key(x) % 3].clone(),
                    3 => many[key(x)].clone(),
                    4 => keys[(key(x) + 1) % keys.len()].clone(),
                    _ => many[n % many.len()].clone(),
                }
            };
            let mut want: Vec<T> = vec![];
            for (n, (count, x)) in runs.iter().enumerate() {
                let y = f(n, x);
                want.extend((0..*count).map(|_| y.clone()));
            }
            if kind == 0 {
                assert_eq!(want, model);
            }
            let bytes = both(&want).0.save();
            let held = [vec![], vec![9u8; 3]][rng.below(2)].clone();
            let given = || Encoders::given_with(max, model, other, how, &mut Rng(salt));

            let (written, seen) = given().save_to_and_remap(&held, f);
            assert_eq!(seen, calls, "{max:?}, how {how}, kind {kind}");
            assert_eq!(written, bytes, "{max:?}, how {how}, kind {kind}");

            for x in want.first().into_iter().chain(want.last()).chain([other]) {
                let all = want.iter().all(|y| y == x);
                let (written, seen) = given().save_to_unless_and_remap(&held, x, f);
                assert_eq!(seen, calls, "{max:?}, how {how}, kind {kind}");
                let wrote = (!all).then(|| bytes.clone());
                assert_eq!(written, wrote, "{max:?}, how {how}, kind {kind}");
            }
        }
    }
}

fn remapping<T: Kind>() {
    let mut rng = Rng(0x7e3a9);
    let (a, b, c) = (T::fresh(&mut rng), T::fresh(&mut rng), T::fresh(&mut rng));
    let pick = |i: usize| [&a, &b, &c][i].clone();
    let mut shapes: Vec<Vec<T>> = vec![
        vec![],
        vec![pick(0)],
        vec![pick(0); 2],
        vec![pick(0); 300],
        (0..600).map(|i| pick(i / 7 % 2)).collect(),
        (0..600).map(|i| pick(i % 3)).collect(),
        (0..900).map(|_| T::fresh(&mut rng)).collect(),
        [0, 0, 1, 2, 2, 2, 0, 1, 1, 0].map(pick).to_vec(),
    ];
    for _ in 0..8 {
        shapes.push(rng.values(200));
    }
    for (i, model) in shapes.iter().enumerate() {
        let other = T::fresh(&mut rng);
        // the small shapes with every budget, the others with three each
        let some = [0, 1, 2].map(|k| BUDGETS[(i + 3 * k) % BUDGETS.len()]);
        let budgets = if model.len() <= 10 {
            &BUDGETS[..]
        } else {
            &some[..]
        };
        for &max in budgets {
            check_remapping(max, model, &other, &mut rng);
        }
    }
}

#[test]
fn remapping_encoders_agree() {
    for_each_kind!(remapping);
}

/// An encoder with a small budget cuts a run of equal values that it is
/// given in parts, and a remap then calls its function for each part:
/// more often than a column's remap would, in both APIs. What the calls
/// give goes to an encoder without a budget, where equal values are one
/// run again: they are not written when all of them is the value to save
/// without, and they are written as a column of them saves.
fn remapping_cut_runs<T: Kind>() {
    let mut rng = Rng(0xc07e);
    let is_bool = std::any::type_name::<T>() == "bool";
    let (a, b) = (T::fresh(&mut rng), T::fresh(&mut rng));
    let model = vec![a.clone(); 40];
    for max in [2, 3] {
        let given = || {
            let mut e = Encoders::<T>::with(Some(max));
            for part in model.chunks(4) {
                e.extend(part);
            }
            e
        };
        let lens = given().into_column().1.slab_lens();
        // of `bool` the encoder cuts nothing here
        assert_eq!(lens.len() > 1, !is_bool, "{lens:?}");
        let calls = vec![a.clone(); lens.len()];

        let (written, seen) = given().save_to_unless_and_remap(&[5], &b, |_, _| b.clone());
        assert_eq!(seen, calls);
        assert_eq!(written, None);

        let (written, seen) = given().save_to_unless_and_remap(&[5], &a, |_, _| b.clone());
        assert_eq!(seen, calls);
        let bytes = both(&vec![b.clone(); 40]).0.save();
        assert_eq!(written, (a != b).then(|| bytes.clone()));

        let (written, seen) = given().save_to_and_remap(&[], |_, _| b.clone());
        assert_eq!(seen, calls);
        assert_eq!(written, bytes);
    }
}

#[test]
fn remapping_encoders_of_cut_runs() {
    for_each_kind!(remapping_cut_runs);
}

/// A remap of an encoder that was given nothing does not call its
/// function. `save_to_and_remap` writes what a column of nothing saves,
/// and `save_to_unless_and_remap` nothing, whatever the value.
fn remapping_nothing<T: Kind>() {
    let mut rng = Rng(0x3e);
    let x = T::fresh(&mut rng);
    let nothing = both::<T>(&[]).0.save();
    for max in BUDGETS {
        let never = |_: usize, _: &T| -> T { panic!("called for no value") };
        let (written, seen) = Encoders::<T>::with(max).save_to_and_remap(&[1, 2], never);
        assert_eq!(seen, []);
        assert_eq!(written, nothing);
        let e = Encoders::<T>::with(max);
        let (written, seen) = e.save_to_unless_and_remap(&[1, 2], &x, never);
        assert_eq!(seen, []);
        assert_eq!(written, None);
    }
}

#[test]
fn remapping_encoders_of_nothing() {
    for_each_kind!(remapping_nothing);
}

/// A function that panics, at its first call, at one in the middle or at
/// its last: the panic is the caller's, and the buffer is as it was. So
/// with the old encoder, with the same words.
#[cfg(panic = "unwind")]
fn remapping_panics<T: Kind>() {
    use std::panic::AssertUnwindSafe;
    let mut rng = Rng(0x9a41c);
    let mut model: Vec<T> = rng.values(300);
    model.push(T::fresh(&mut rng));
    let other = T::fresh(&mut rng);
    for max in [None, Some(2), Some(5), Some(64)] {
        let given = || Encoders::given_with(max, &model, &other, 6, &mut Rng(77));
        let (_, seen) = given().save_to_and_remap(&[], |_, x| x.clone());
        for at in [0, seen.len() / 2, seen.len() - 1] {
            for unless in [false, true] {
                let count = Cell::new(0);
                let f = |x: T| -> T {
                    if count.get() == at {
                        panic!("no value for call {at}");
                    }
                    count.set(count.get() + 1);
                    x
                };
                let Encoders { new, old, .. } = given();
                let (mut a, mut b) = (vec![1u8, 2, 3], vec![1u8, 2, 3]);
                let said = panic_of(AssertUnwindSafe(|| match unless {
                    false => new.save_to_and_remap(&mut a, &f),
                    true => new.save_to_unless_and_remap(&mut a, other.lend(), &f),
                }));
                assert_eq!(said, Some(format!("no value for call {at}")));
                assert_eq!(a, [1, 2, 3]);
                assert_eq!(count.get(), at);

                count.set(0);
                let said_old = panic_of(AssertUnwindSafe(|| match unless {
                    false => T::old_remap(old, &mut b, &f),
                    true => T::old_remap_unless(old, &mut b, &other, &f),
                }));
                // no old encoder of `bool` has these
                if std::any::type_name::<T>() != "bool" {
                    assert_eq!(said_old, said);
                    assert_eq!(count.get(), at);
                }
                assert_eq!(b, [1, 2, 3]);
            }
        }
    }
}

#[cfg(panic = "unwind")]
#[test]
fn remapping_encoders_leave_the_buffer_at_a_panic() {
    for_each_kind!(remapping_panics);
}

// ── `VerifiedEncoderState` and the static forms ─────────────────────────
//
// The verified API's state that writes into a buffer of its caller's
// against the old API's (`RleEncoderState`, and for `bool`
// `BoolEncoderState`), and `encode`, `encode_to` and `encode_to_unless`
// against those of `Column`'s encoder.

/// The old API's state for a value type, and the static forms of its
/// encoder. Its two state types have no trait in common, and no bound
/// tells that `Column`'s encoder has a default: this is the trait the
/// tests reach both through. It takes the values as a test holds them.
trait OldStream<'a, T: Kind>: Default {
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool;

    fn append(&mut self, buf: &mut Vec<u8>, x: &'a T);

    fn append_owned(&mut self, buf: &mut Vec<u8>, x: T);

    fn append_n(&mut self, buf: &mut Vec<u8>, x: &'a T, n: usize);

    fn append_n_owned(&mut self, buf: &mut Vec<u8>, x: T, n: usize);

    fn extend(&mut self, buf: &mut Vec<u8>, xs: &'a [T]);

    fn finish(&mut self, buf: &mut Vec<u8>);

    /// Whether every value given is `x`: `is_single_run_of`, and for
    /// `bool` `all_equal_pre_finish`.
    fn single(&self, x: &'a T) -> bool;

    fn encode(xs: &'a [T]) -> Vec<u8>;

    fn encode_to(buf: &mut Vec<u8>, xs: &'a [T]) -> Range<usize>;

    fn encode_to_unless(buf: &mut Vec<u8>, xs: &'a [T], x: &'a T) -> Range<usize>;
}

impl<'a, T> OldStream<'a, T> for RleEncoderState<'a, T>
where
    T: Kind + RleValue + ColumnValueRef<Encoding<Leb128> = RleEncoding<T, Leb128>>,
{
    fn len(&self) -> usize {
        RleEncoderState::len(self)
    }

    fn is_empty(&self) -> bool {
        RleEncoderState::is_empty(self)
    }

    fn append(&mut self, buf: &mut Vec<u8>, x: &'a T) {
        RleEncoderState::append(self, buf, x.as_column_ref());
    }

    fn append_owned(&mut self, buf: &mut Vec<u8>, x: T) {
        RleEncoderState::append_owned(self, buf, x);
    }

    fn append_n(&mut self, buf: &mut Vec<u8>, x: &'a T, n: usize) {
        RleEncoderState::append_n(self, buf, x.as_column_ref(), n);
    }

    fn append_n_owned(&mut self, buf: &mut Vec<u8>, x: T, n: usize) {
        RleEncoderState::append_n_owned(self, buf, x, n);
    }

    fn extend(&mut self, buf: &mut Vec<u8>, xs: &'a [T]) {
        RleEncoderState::extend(self, buf, xs.iter().map(|x| x.as_column_ref()));
    }

    fn finish(&mut self, buf: &mut Vec<u8>) {
        RleEncoderState::finish(self, buf);
    }

    fn single(&self, x: &'a T) -> bool {
        self.is_single_run_of(x.as_column_ref())
    }

    fn encode(xs: &'a [T]) -> Vec<u8> {
        <RleEncoder<'a, T> as EncoderApi<'a, T>>::encode(xs.iter().map(|x| x.as_column_ref()))
    }

    fn encode_to(buf: &mut Vec<u8>, xs: &'a [T]) -> Range<usize> {
        <RleEncoder<'a, T> as EncoderApi<'a, T>>::encode_to(
            buf,
            xs.iter().map(|x| x.as_column_ref()),
        )
    }

    fn encode_to_unless(buf: &mut Vec<u8>, xs: &'a [T], x: &'a T) -> Range<usize> {
        <RleEncoder<'a, T> as EncoderApi<'a, T>>::encode_to_unless(
            buf,
            xs.iter().map(|x| x.as_column_ref()),
            x.as_column_ref(),
        )
    }
}

/// The state of `bool` has no owned forms: a `bool` is given as it is.
impl<'a> OldStream<'a, bool> for BoolEncoderState {
    fn len(&self) -> usize {
        BoolEncoderState::len(self)
    }

    fn is_empty(&self) -> bool {
        BoolEncoderState::is_empty(self)
    }

    fn append(&mut self, buf: &mut Vec<u8>, x: &'a bool) {
        BoolEncoderState::append(self, buf, *x);
    }

    fn append_owned(&mut self, buf: &mut Vec<u8>, x: bool) {
        BoolEncoderState::append(self, buf, x);
    }

    fn append_n(&mut self, buf: &mut Vec<u8>, x: &'a bool, n: usize) {
        BoolEncoderState::append_n(self, buf, *x, n);
    }

    fn append_n_owned(&mut self, buf: &mut Vec<u8>, x: bool, n: usize) {
        BoolEncoderState::append_n(self, buf, x, n);
    }

    fn extend(&mut self, buf: &mut Vec<u8>, xs: &'a [bool]) {
        BoolEncoderState::extend(self, buf, xs.iter().copied());
    }

    fn finish(&mut self, buf: &mut Vec<u8>) {
        BoolEncoderState::finish(self, buf);
    }

    fn single(&self, x: &'a bool) -> bool {
        self.all_equal_pre_finish(*x)
    }

    fn encode(xs: &'a [bool]) -> Vec<u8> {
        <BoolEncoder as EncoderApi<'a, bool>>::encode(xs.iter().copied())
    }

    fn encode_to(buf: &mut Vec<u8>, xs: &'a [bool]) -> Range<usize> {
        <BoolEncoder as EncoderApi<'a, bool>>::encode_to(buf, xs.iter().copied())
    }

    fn encode_to_unless(buf: &mut Vec<u8>, xs: &'a [bool], x: &'a bool) -> Range<usize> {
        <BoolEncoder as EncoderApi<'a, bool>>::encode_to_unless(buf, xs.iter().copied(), *x)
    }
}

/// A state of each API, each on a buffer of its own that held the same
/// bytes, given the values of `model` in the same steps. After each step
/// both have counted the values given so far, both say of a value
/// whether every value given so far is that one, and say it rightly, and
/// each buffer still begins with what it held.
struct States<'a, T: Kind> {
    new: VerifiedEncoderState<'a, T>,
    old: T::OldState<'a>,
    a: Vec<u8>,
    b: Vec<u8>,
    held: &'a [u8],
    model: &'a [T],
    other: &'a T,
    len: usize,
    /// whether a value given is not the first of the model
    mixed: bool,
}

impl<'a, T: Kind> States<'a, T> {
    /// States that were given nothing, the old one made by `new` and the
    /// verified one by `new` or by `default`.
    fn on(held: &'a [u8], model: &'a [T], other: &'a T, default: bool) -> Self {
        let mut s = States {
            new: match default {
                false => VerifiedEncoderState::new(),
                true => VerifiedEncoderState::default(),
            },
            old: Default::default(),
            a: held.to_vec(),
            b: held.to_vec(),
            held,
            model,
            other,
            len: 0,
            mixed: false,
        };
        s.counted(0);
        s
    }

    fn counted(&mut self, added: usize) {
        let model = self.model;
        self.mixed |= model[self.len..self.len + added]
            .iter()
            .any(|x| *x != model[0]);
        self.len += added;
        assert_eq!(self.new.len(), self.len);
        assert_eq!(self.old.len(), self.len);
        assert_eq!(self.new.is_empty(), self.len == 0);
        assert_eq!(self.old.is_empty(), self.len == 0);
        let last = self.len.checked_sub(1).map(|i| &model[i]);
        for x in [Some(self.other), model.first(), last]
            .into_iter()
            .flatten()
        {
            let all = self.len == 0 || (!self.mixed && model[0] == *x);
            assert_eq!(self.new.is_single_run_of(x.lend()), all);
            assert_eq!(self.old.single(x), all);
        }
        assert_eq!(self.a[..self.held.len()], *self.held);
        assert_eq!(self.b[..self.held.len()], *self.held);
    }

    /// These states, given the model in one of the seven ways an encoder
    /// is given values (see [`Encoders::given`]).
    fn give(mut self, how: usize, rng: &mut Rng) -> Self {
        let (model, other) = (self.model, self.other);
        while self.len < model.len() {
            let at = self.len;
            let x = &model[at];
            let run = model[at..].iter().take_while(|y| *y == x).count();
            let step = if how == 6 { rng.below(6) } else { how };
            match step {
                0 => {
                    self.new.append(&mut self.a, x.lend());
                    self.old.append(&mut self.b, x);
                    self.counted(1);
                }
                1 => {
                    self.new.append_owned(&mut self.a, x.clone());
                    self.old.append_owned(&mut self.b, x.clone());
                    self.counted(1);
                }
                2 => {
                    self.new.append_n(&mut self.a, x.lend(), run);
                    self.old.append_n(&mut self.b, x, run);
                    self.counted(run);
                }
                3 => {
                    self.new.append_n_owned(&mut self.a, x.clone(), run);
                    self.old.append_n_owned(&mut self.b, x.clone(), run);
                    self.counted(run);
                }
                4 => {
                    let first = rng.below(run + 1);
                    self.new.append_n(&mut self.a, x.lend(), first);
                    self.old.append_n(&mut self.b, x, first);
                    self.counted(first);
                    self.new.append_n(&mut self.a, other.lend(), 0);
                    self.old.append_n(&mut self.b, other, 0);
                    self.new.append_n_owned(&mut self.a, other.clone(), 0);
                    self.old.append_n_owned(&mut self.b, other.clone(), 0);
                    self.counted(0);
                    self.new.append_n_owned(&mut self.a, x.clone(), run - first);
                    self.old.append_n_owned(&mut self.b, x.clone(), run - first);
                    self.counted(run - first);
                }
                _ => {
                    let n = rng.below((model.len() - at).min(70) + 1);
                    let xs = &model[at..at + n];
                    self.new.extend(&mut self.a, xs.iter().map(T::lend));
                    self.old.extend(&mut self.b, xs);
                    self.counted(n);
                }
            }
        }
        self
    }

    /// Both finish. Each buffer is then what it held and after that the
    /// same bytes, which are returned.
    fn finish(mut self) -> Vec<u8> {
        self.new.finish(&mut self.a);
        self.old.finish(&mut self.b);
        assert_eq!(self.a, self.b);
        assert_eq!(self.a[..self.held.len()], *self.held);
        self.a.split_off(self.held.len())
    }
}

fn old_encode<T: Kind>(xs: &[T]) -> Vec<u8> {
    <T::OldState<'_> as OldStream<'_, T>>::encode(xs)
}

fn old_encode_to<T: Kind>(buf: &mut Vec<u8>, xs: &[T]) -> Range<usize> {
    <T::OldState<'_> as OldStream<'_, T>>::encode_to(buf, xs)
}

fn old_encode_to_unless<'a, T: Kind>(buf: &mut Vec<u8>, xs: &'a [T], x: &'a T) -> Range<usize> {
    <T::OldState<'a> as OldStream<'a, T>>::encode_to_unless(buf, xs, x)
}

/// States given `model` in each way write what a column of `model`
/// saves, after what the buffer held. So does `encode_to`, and `encode`
/// gives it. `encode_to_unless` writes it too, unless every value of
/// `model` is the one named: then it writes nothing. All as the old API
/// does, to the same range of the buffer.
fn check_streams<T: Kind>(model: &[T], other: &T, rng: &mut Rng) {
    let (v, c) = both(model);
    let bytes = v.save();
    assert_eq!(bytes, c.save());
    let holds = [vec![], vec![9u8; 3], bytes.clone()];
    for how in 0..7 {
        let held = &holds[rng.below(3)];
        let s = States::on(held, model, other, how % 2 == 1);
        assert_eq!(s.give(how, rng).finish(), bytes, "how {how}");
    }

    let new = VerifiedEncoder::<T>::encode(model.iter().map(T::lend));
    assert_eq!(new, old_encode(model));
    assert_eq!(new, bytes);
    assert_eq!(VerifiedEncoder::<T>::encode(lent(model)), bytes);
    assert_eq!(VerifiedEncoder::<T>::encode(v.iter()), bytes);

    let held = &holds[rng.below(3)];
    let (mut a, mut b) = (held.clone(), held.clone());
    let range = VerifiedEncoder::<T>::encode_to(&mut a, model.iter().map(T::lend));
    assert_eq!(range, old_encode_to(&mut b, model));
    assert_eq!(a, b);
    assert_eq!(range, held.len()..a.len());
    assert_eq!(a[..held.len()], held[..]);
    assert_eq!(a[held.len()..], bytes[..]);
    // and a second after the first, from a column's iterator
    let range = VerifiedEncoder::<T>::encode_to(&mut a, v.iter());
    assert_eq!(range, held.len() + bytes.len()..a.len());
    assert_eq!(a[range], bytes[..]);

    for x in [Some(other), model.first(), model.last()]
        .into_iter()
        .flatten()
    {
        let (mut a, mut b) = (held.clone(), held.clone());
        let range =
            VerifiedEncoder::<T>::encode_to_unless(&mut a, model.iter().map(T::lend), x.lend());
        assert_eq!(range, old_encode_to_unless(&mut b, model, x));
        assert_eq!(a, b);
        assert_eq!(range, held.len()..a.len());
        if model.iter().all(|y| y == x) {
            assert_eq!(a, *held);
        } else {
            assert!(!range.is_empty());
            assert_eq!(a[..held.len()], held[..]);
            assert_eq!(a[held.len()..], bytes[..]);
        }
    }
}

fn streams<T: Kind>() {
    let mut rng = Rng(0x57a7e5);
    let (a, b, c) = (T::fresh(&mut rng), T::fresh(&mut rng), T::fresh(&mut rng));
    let pick = |i: usize| [&a, &b, &c][i].clone();
    let mut shapes: Vec<Vec<T>> = vec![
        vec![],
        vec![pick(0)],
        vec![pick(0); 2],
        vec![pick(0); 1000],
        (0..1000).map(|i| pick(i / 7 % 2)).collect(),
        (0..1000).map(|i| pick(i % 3)).collect(),
        (0..3000).map(|_| T::fresh(&mut rng)).collect(),
        [0, 0, 1, 2, 2, 2, 0, 1, 1, 0].map(pick).to_vec(),
    ];
    for _ in 0..24 {
        shapes.push(rng.values(300));
    }
    for model in shapes {
        let other = T::fresh(&mut rng);
        check_streams(&model, &other, &mut rng);
        // and with a value named that the model begins with
        if let Some(first) = model.first() {
            check_streams(&model, first, &mut rng);
        }
    }
}

#[test]
fn states_and_static_forms_agree() {
    for_each_kind!(streams);
}

/// Values with no two alike in a row, whose count the state writes
/// before them when the run ends, into the buffer it does not own: runs
/// of the lengths at which the count takes one byte more, alone and
/// between runs of equal values.
fn literal_streams<T: Kind>() {
    let mut rng = Rng(0x117e7a2);
    let mut pool: Vec<T> = (0..9000).map(|_| T::fresh(&mut rng)).collect();
    pool.dedup();
    let lens = (0..=3).chain([62, 63, 64, 65, 66, 8190, 8191, 8192, 8193, 8194]);
    for len in lens.filter(|&n| n <= pool.len()) {
        let lit = &pool[pool.len() - len..];
        let other = T::fresh(&mut rng);
        check_streams(lit, &other, &mut rng);
        let Some((first, last)) = lit.first().zip(lit.last()) else {
            continue;
        };
        let before = pool.iter().find(|x| *x != first).unwrap_or(first);
        let after = pool.iter().find(|x| *x != last).unwrap_or(last);
        let mut model = vec![before.clone(); 3];
        model.extend_from_slice(lit);
        model.extend(std::iter::repeat_n(after.clone(), 2));
        check_streams(&model, &other, &mut rng);
    }
}

#[test]
fn states_agree_on_literal_runs() {
    for_each_kind!(literal_streams);
}

/// A state given nothing writes nothing, as the old one does and as a
/// column of no values saves nothing: whatever runs of no values it was
/// given, and whatever the buffer held. Every value is the only one it
/// was given. And it may be made before the buffer it is to write into
/// holds what it holds then.
fn empty_streams<T: Kind>() {
    let mut rng = Rng(0x57a7e0);
    let (x, y) = (T::fresh(&mut rng), T::fresh(&mut rng));
    assert!(VerifiedColumn::<T>::new().save().is_empty());
    assert!(VerifiedEncoder::<T>::encode(std::iter::empty()).is_empty());
    assert!(old_encode::<T>(&[]).is_empty());
    for held in [vec![], vec![5u8], vec![0u8; 70]] {
        let mut buf = held.clone();
        let mut s = VerifiedEncoderState::<T>::new();
        s.append_n(&mut buf, x.lend(), 0);
        s.append_n_owned(&mut buf, y.clone(), 0);
        s.extend(&mut buf, std::iter::empty());
        assert_eq!((s.len(), s.is_empty()), (0, true));
        assert!(s.is_single_run_of(x.lend()) && s.is_single_run_of(y.lend()));
        s.finish(&mut buf);
        assert_eq!(buf, held);

        let mut old = held.clone();
        let mut s = <T::OldState<'_>>::default();
        s.append_n(&mut old, &x, 0);
        s.append_n_owned(&mut old, y.clone(), 0);
        s.extend(&mut old, &[]);
        assert_eq!((s.len(), s.is_empty()), (0, true));
        assert!(s.single(&x) && s.single(&y));
        s.finish(&mut old);
        assert_eq!(old, held);

        assert_eq!(
            VerifiedEncoder::<T>::encode_to(&mut buf, std::iter::empty()),
            held.len()..held.len()
        );
        assert_eq!(
            VerifiedEncoder::<T>::encode_to_unless(&mut buf, std::iter::empty(), x.lend()),
            held.len()..held.len()
        );
        assert_eq!(old_encode_to::<T>(&mut old, &[]), held.len()..held.len());
        assert_eq!(
            old_encode_to_unless(&mut old, &[], &x),
            held.len()..held.len()
        );
        assert_eq!((&buf, &old), (&held, &held));

        // a state made before the buffer holds what it is to follow
        let mut s = VerifiedEncoderState::<T>::default();
        let mut buf = held.clone();
        buf.push(1);
        s.append(&mut buf, x.lend());
        s.append_owned(&mut buf, y.clone());
        s.finish(&mut buf);
        let two = [x.clone(), y.clone()];
        assert_eq!(buf[..held.len()], held[..]);
        assert_eq!(buf[held.len()], 1);
        assert_eq!(buf[held.len() + 1..], both(&two).0.save()[..]);
    }
}

#[test]
fn states_of_nothing() {
    for_each_kind!(empty_streams);
}

/// States that follow one another in one buffer, each started where the
/// last finished: the buffer holds what it held and then what a column of
/// each state's values saves, one after another, as with the old API.
/// What each wrote loads as its values.
#[test]
fn states_follow_one_another() {
    let mut rng = Rng(0xf0110);
    for round in 0..60 {
        let ids: Vec<u64> = rng.values(300);
        let flags: Vec<bool> = rng.values(300);
        let names: Vec<Option<Vec<u8>>> = rng.values(40);
        let held = vec![7u8; round % 4];

        let mut a = held.clone();
        let mut s = VerifiedEncoderState::<u64>::new();
        s.extend(&mut a, ids.iter().copied());
        s.finish(&mut a);
        let first = a.len();
        let mut s = VerifiedEncoderState::<bool>::default();
        for x in &flags {
            s.append(&mut a, *x);
        }
        s.finish(&mut a);
        let second = a.len();
        let mut s = VerifiedEncoderState::<Option<Vec<u8>>>::new();
        s.extend(&mut a, names.iter().map(|x| x.as_deref()));
        s.finish(&mut a);

        let mut b = held.clone();
        let mut s = RleEncoderState::<u64>::new();
        s.extend(&mut b, ids.iter().copied());
        s.finish(&mut b);
        let mut s = BoolEncoderState::<Leb128>::new();
        for x in &flags {
            s.append(&mut b, *x);
        }
        s.finish(&mut b);
        let mut s = RleEncoderState::<Option<Vec<u8>>>::new();
        s.extend(&mut b, names.iter().map(|x| x.as_deref()));
        s.finish(&mut b);
        assert_eq!(a, b);

        let mut c = held.clone();
        c.extend(both(&ids).0.save());
        c.extend(both(&flags).0.save());
        c.extend(both(&names).0.save());
        assert_eq!(a, c);

        let h = held.len();
        let load = VerifiedColumn::<u64>::load(&a[h..first]).unwrap();
        assert_eq!(new_values(&load), ids);
        let load = VerifiedColumn::<bool>::load(&a[first..second]).unwrap();
        assert_eq!(new_values(&load), flags);
        let load = VerifiedColumn::<Option<Vec<u8>>>::load(&a[second..]).unwrap();
        assert_eq!(new_values(&load), names);
        assert_eq!(old_values(&Column::<u64>::load(&a[h..first]).unwrap()), ids);
    }
}

/// A state counts its values in a `usize`, as an encoder does, and stops
/// where an encoder stops: up to `usize::MAX - 2` values are counted (of
/// `bool`, one fewer) and written as the old state writes them; given
/// more, both APIs panic with the words an encoder panics with.
#[cfg(panic = "unwind")]
#[test]
fn states_count_to_the_end() {
    // each run fits the format's count on any target
    let half = usize::MAX / 2;

    let numbers = |first: usize, then: &'static [(u64, usize)]| {
        let new = panic_of(move || {
            let mut buf = vec![3u8];
            let mut s = VerifiedEncoderState::<u64>::new();
            s.append_n(&mut buf, 1, first);
            s.append_n(&mut buf, 2, first);
            for &(x, n) in then {
                match n {
                    1 => s.append(&mut buf, x),
                    _ => s.append_n(&mut buf, x, n),
                }
            }
            let len = s.len();
            s.finish(&mut buf);
            (len, buf)
        });
        let old = panic_of(move || {
            let mut buf = vec![3u8];
            let mut s = RleEncoderState::<u64>::new();
            s.append_n(&mut buf, 1, first);
            s.append_n(&mut buf, 2, first);
            for &(x, n) in then {
                match n {
                    1 => s.append(&mut buf, x),
                    _ => s.append_n(&mut buf, x, n),
                }
            }
            let len = s.len();
            s.finish(&mut buf);
            (len, buf)
        });
        assert_eq!(new, old);
        new
    };
    assert_eq!(numbers(half - 1, &[]), None);
    assert_eq!(numbers(half - 1, &[(3, 1)]), None);
    assert_eq!(numbers(half - 1, &[(2, 1), (2, 0)]), None);
    let said = numbers(half - 1, &[(3, 2)]).unwrap();
    assert!(said.contains("encoder too long to append to"), "{said}");
    assert_eq!(numbers(half - 1, &[(3, 1), (4, 1)]), Some(said.clone()));
    assert_eq!(numbers(half - 1, &[(2, 2)]), Some(said.clone()));
    assert_eq!(numbers(half, &[]), Some(said.clone()));
    assert_eq!(
        numbers(half - 1, &[(3, 0), (3, usize::MAX)]),
        Some(said.clone())
    );

    // what the states that count to the end write is what the encoders
    // save
    let mut buf = vec![];
    let mut s = VerifiedEncoderState::<u64>::new();
    s.append_n(&mut buf, 1, half - 1);
    s.append_n_owned(&mut buf, 2, half - 1);
    s.append_owned(&mut buf, 3);
    assert_eq!(s.len(), usize::MAX - 2);
    assert!(!s.is_single_run_of(1));
    s.finish(&mut buf);
    let mut e = VerifiedEncoder::<u64>::new();
    e.append_n(1, half - 1);
    e.append_n(2, half - 1);
    e.append(3);
    assert_eq!(buf, e.save());

    let bools = |first: usize, then: &'static [(bool, usize)]| {
        let new = panic_of(move || {
            let mut buf = vec![3u8];
            let mut s = VerifiedEncoderState::<bool>::new();
            s.append_n(&mut buf, true, first);
            s.append_n(&mut buf, false, first);
            for &(x, n) in then {
                match n {
                    1 => s.append(&mut buf, x),
                    _ => s.append_n(&mut buf, x, n),
                }
            }
            let len = s.len();
            s.finish(&mut buf);
            (len, buf)
        });
        let old = panic_of(move || {
            let mut buf = vec![3u8];
            let mut s = BoolEncoderState::<Leb128>::new();
            s.append_n(&mut buf, true, first);
            s.append_n(&mut buf, false, first);
            for &(x, n) in then {
                match n {
                    1 => s.append(&mut buf, x),
                    _ => s.append_n(&mut buf, x, n),
                }
            }
            let len = s.len();
            s.finish(&mut buf);
            (len, buf)
        });
        assert_eq!(new, old);
        new
    };
    assert_eq!(bools(half - 1, &[]), None);
    assert_eq!(bools(half - 1, &[(true, 0), (false, 0)]), None);
    assert_eq!(bools(half - 1, &[(true, 1)]), Some(said.clone()));
    assert_eq!(bools(half - 1, &[(false, 1)]), Some(said.clone()));
    assert_eq!(bools(half - 1, &[(false, 2)]), Some(said.clone()));
    assert_eq!(bools(half, &[]), Some(said.clone()));

    // nulls, whose count has no sign either
    let nulls = |n: usize, more: usize| {
        let new = panic_of(move || {
            let mut buf = vec![];
            let mut s = VerifiedEncoderState::<Option<u64>>::new();
            s.append_n(&mut buf, None, n);
            s.append_n(&mut buf, None, more);
            let (len, all) = (s.len(), s.is_single_run_of(None));
            s.finish(&mut buf);
            (len, all, buf)
        });
        let old = panic_of(move || {
            let mut buf = vec![];
            let mut s = RleEncoderState::<Option<u64>>::new();
            s.append_n(&mut buf, None, n);
            s.append_n(&mut buf, None, more);
            let (len, all) = (s.len(), s.is_single_run_of(None));
            s.finish(&mut buf);
            (len, all, buf)
        });
        assert_eq!(new, old);
        new
    };
    assert_eq!(nulls(half, half - 1), None);
    assert_eq!(nulls(usize::MAX - 2, 0), None);
    assert_eq!(nulls(usize::MAX - 2, 1), Some(said.clone()));
    assert_eq!(nulls(usize::MAX, 0), Some(said));
}

/// The static forms take what any iterator gives, and read it once and in
/// order: one that counts what it gave has given every value by the time
/// the form returns, and no more, whether or not anything was written.
#[test]
fn static_forms_read_an_iterator_once() {
    let mut rng = Rng(0x17e2);
    for _ in 0..200 {
        let model: Vec<u64> = rng.values(120);
        let bytes = both(&model).0.save();
        let given = Cell::new(0);
        let counting = || {
            given.set(0);
            model.iter().map(|x| {
                given.set(given.get() + 1);
                *x
            })
        };
        assert_eq!(VerifiedEncoder::<u64>::encode(counting()), bytes);
        assert_eq!(given.get(), model.len());
        let mut out = vec![4u8];
        VerifiedEncoder::<u64>::encode_to(&mut out, counting());
        assert_eq!(given.get(), model.len());
        assert_eq!(out[1..], bytes[..]);
        let unless = model.first().copied().unwrap_or(0);
        let mut out = vec![4u8];
        let range = VerifiedEncoder::<u64>::encode_to_unless(&mut out, counting(), unless);
        assert_eq!(given.get(), model.len());
        assert_eq!(range.is_empty(), model.iter().all(|x| *x == unless));
        assert_eq!(out.len(), range.end);
    }
}

/// A run a `VerifiedDecoder` gave.
fn new_run<T: Kind>(r: Option<Run<Get<'_, T>>>) -> Option<(usize, T)> {
    r.map(|r| (r.count, T::keep(r.value)))
}

/// A run the old API's decoder gave.
fn old_run<T: Kind>(r: Option<Run<<T as ColumnValueRef>::Get<'_>>>) -> Option<(usize, T)> {
    r.map(|r| (r.count, old(r.value)))
}

/// The two decoders have the same values left; how many.
fn check_left<T: Kind>(x: &VerifiedDecoder<'_, T>, y: &hexane::Decoder<'_, T>) -> usize {
    let a: Vec<T> = x.clone().map(T::keep).collect();
    let b: Vec<T> = y.clone().map(old).collect();
    assert_eq!(a, b);
    a.len()
}

/// How many values at the head of `vs` are equal, counted to `max`.
fn head_of<T: Kind>(vs: &[T], max: usize) -> usize {
    vs.iter().take(max).take_while(|v| **v == vs[0]).count()
}

/// What a scan for `x` of at most `max` of the values `vs` returns, and
/// how many values it passes: it reads run after run, each cut where
/// `max` is reached, to the first that is not less than `x`, and passes
/// that one too.
fn model_scan<T: Kind>(vs: &[T], x: &T, max: usize) -> ((usize, usize), usize) {
    let mut skipped = 0;
    while skipped < max && skipped < vs.len() {
        let n = head_of(&vs[skipped..], max - skipped);
        match vs[skipped].order(x) {
            Ordering::Less => skipped += n,
            Ordering::Equal => return ((skipped, n), skipped + n),
            Ordering::Greater => return ((skipped, 0), skipped + n),
        }
    }
    ((skipped, 0), skipped)
}

/// A `VerifiedDecoder` and the old API's decoder of the bytes `model`
/// saves to, each stepped the same way, at random: what the two give at
/// each step is the same, it is what `model` has there, and they have
/// the same values left. So do the copies taken of them on the way, when
/// the two have moved on. At the end each call gives nothing, and
/// leaves the decoder at the end.
fn check_decoders<T: Kind>(model: &[T], rng: &mut Rng) {
    // (before the decoders: the old one keeps what it is asked to find)
    let ts = targets(rng, model);
    let bytes = both(model).0.save();
    let mut x = VerifiedDecoder::<T>::new(&bytes);
    let mut y = hexane::decoder::<T>(&bytes);
    assert_eq!(check_left(&x, &y), model.len());
    assert_eq!(new_values_of(&x), model);
    let tried = VerifiedDecoder::<T>::try_new(&bytes).unwrap();
    let old_tried = hexane::try_decoder::<T>(&bytes).unwrap();
    assert_eq!(check_left(&tried, &old_tried), model.len());

    let mut copies = Vec::new();
    let mut at = 0;
    while at < model.len() {
        let left = &model[at..];
        let k = match rng.below(4) {
            0 => 0,
            1 => 1 + rng.below(3),
            2 => rng.below(left.len() + 2),
            _ => rng.below(40),
        };
        match rng.below(9) {
            0 | 1 => {
                let (a, b) = (x.next().map(T::keep), y.next().map(old));
                assert_eq!(a, b);
                assert_eq!(a.as_ref(), Some(&left[0]));
                at += 1;
            }
            2 => {
                let (a, b) = (x.nth(k).map(T::keep), y.nth(k).map(old));
                assert_eq!(a, b);
                assert_eq!(a.as_ref(), left.get(k));
                at += left.len().min(k + 1);
            }
            3 => {
                x.advance_by(k);
                RunDecoder::advance_by(&mut y, k);
                at += left.len().min(k);
            }
            4 => {
                let (a, b) = (new_run::<T>(x.next_run()), old_run::<T>(y.next_run()));
                assert_eq!(a, b);
                let n = head_of(left, usize::MAX);
                assert_eq!(a, Some((n, left[0].clone())));
                at += n;
            }
            5 | 6 => {
                let a = new_run::<T>(x.next_run_max(k));
                let b = old_run::<T>(y.next_run_max(k));
                assert_eq!(a, b);
                let n = head_of(left, k);
                assert_eq!(a, (k > 0).then(|| (n, left[0].clone())));
                at += n;
            }
            7 => {
                let t = &ts[rng.below(ts.len())];
                let max = [usize::MAX, usize::MAX, k, left.len()][rng.below(4)];
                let a = x.scan_for(t.lend(), max);
                assert_eq!(a, t.old_scan_for(&mut y, max));
                let (found, passed) = model_scan(left, t, max);
                assert_eq!(a, found, "a scan of at most {max} for {t:?} in {left:?}");
                at += passed;
            }
            _ => copies.push((x.clone(), y.clone(), at)),
        }
        assert_eq!(check_left(&x, &y), model.len() - at);
    }

    let t = &ts[0];
    assert_eq!(x.next().map(T::keep), None);
    assert_eq!(y.next().map(old::<T>), None);
    assert_eq!(x.nth(0).map(T::keep), None);
    assert_eq!(x.nth(5).map(T::keep), None);
    assert_eq!(y.nth(5).map(old::<T>), None);
    x.advance_by(3);
    RunDecoder::advance_by(&mut y, 3);
    assert_eq!(new_run::<T>(x.next_run()), None);
    assert_eq!(old_run::<T>(y.next_run()), None);
    assert_eq!(new_run::<T>(x.next_run_max(4)), None);
    assert_eq!(new_run::<T>(x.next_run_max(0)), None);
    assert_eq!(old_run::<T>(y.next_run_max(4)), None);
    assert_eq!(x.scan_for(t.lend(), usize::MAX), (0, 0));
    assert_eq!(t.old_scan_for(&mut y, usize::MAX), (0, 0));
    assert_eq!(x.next().map(T::keep), None);
    assert_eq!(check_left(&x, &y), 0);
    for (x, y, at) in copies {
        assert_eq!(check_left(&x, &y), model.len() - at);
        assert_eq!(new_values_of(&x), model[at..]);
    }
}

/// What a `VerifiedDecoder` has left.
fn new_values_of<T: Kind>(x: &VerifiedDecoder<'_, T>) -> Vec<T> {
    x.clone().map(T::keep).collect()
}

fn decoders<T: Kind>() {
    let mut rng = Rng(0xdec0);
    let (a, b, c) = (T::fresh(&mut rng), T::fresh(&mut rng), T::fresh(&mut rng));
    let pick = |i: usize| [&a, &b, &c][i].clone();
    let mut shapes: Vec<Vec<T>> = vec![
        vec![],
        vec![pick(0)],
        vec![pick(0); 300],
        (0..300).map(|i| pick(i / 7 % 2)).collect(),
        (0..300).map(|i| pick(i % 3)).collect(),
        (0..300).map(|_| T::fresh(&mut rng)).collect(),
        [0, 0, 1, 2, 2, 2, 0, 1, 1, 0].map(pick).to_vec(),
    ];
    for i in 0..40 {
        let mut model: Vec<T> = rng.values(200);
        // every other one in order, for the scans
        if i % 2 == 0 {
            model.sort_by(T::order);
        }
        shapes.push(model);
    }
    for model in shapes {
        for _ in 0..3 {
            check_decoders(&model, &mut rng);
        }
    }
}

/// A `VerifiedDecoder` reads saved bytes as the old API's decoder does:
/// value by value, in steps, run by run, a run in parts, and in scans
/// for a value, for every value type.
#[test]
fn decoders_agree() {
    for_each_kind!(decoders);
}

/// The two checked decoders take the same bytes, read them alike, and
/// refuse the rest with the same error.
fn check_same_decoder<T: Kind>(bytes: &[u8]) -> bool {
    let x = VerifiedDecoder::<T>::try_new(bytes);
    let y = hexane::try_decoder::<T>(bytes);
    match (x, y) {
        (Ok(mut x), Ok(mut y)) => {
            for k in [0, 7, usize::MAX] {
                let a = x.clone().nth(k).map(T::keep);
                assert_eq!(a, y.clone().nth(k).map(old));
            }
            // run by run: a few bytes can be a run of billions
            loop {
                let (a, b) = (new_run::<T>(x.next_run()), old_run::<T>(y.next_run()));
                assert_eq!(a, b, "the decoders read {bytes:?} differently");
                if a.is_none() {
                    return true;
                }
            }
        }
        (Err(x), Err(y)) => {
            assert_eq!(
                x.to_string(),
                y.to_string(),
                "the decoders refuse {bytes:?} differently"
            );
            false
        }
        (x, y) => panic!(
            "the decoders disagree on {bytes:?}: {}, {}",
            x.is_ok(),
            y.is_ok()
        ),
    }
}

/// What a decoder gives is lent by the bytes, not by the decoder: it is
/// there when the decoder is gone.
#[test]
fn a_decoder_lends_what_the_bytes_hold() {
    let words = ["pear".to_string(), String::new(), "größer".to_string()];
    let bytes = both(&words).0.save();
    let (first, run) = {
        let mut d = VerifiedDecoder::<String>::new(&bytes);
        (d.next(), d.next_run())
    };
    assert_eq!(first, Some("pear"));
    assert_eq!(run.map(|r| (r.count, r.value)), Some((1, "")));
    let blobs = [Some(b"ab".to_vec()), None, None];
    let bytes = both(&blobs).0.save();
    let (first, second) = {
        let mut d = VerifiedDecoder::<Option<Vec<u8>>>::new(&bytes);
        (d.next(), d.nth(1))
    };
    assert_eq!(first, Some(Some(&b"ab"[..])));
    assert_eq!(second, Some(None));
    // the bytes of the string are bytes of the saved column
    let at = bytes.as_ptr_range();
    let lent = first.unwrap().unwrap().as_ptr_range();
    assert!(at.start <= lent.start && lent.end <= at.end);
}

/// Runs as long as a run can be: the decoders count them alike, pass
/// them in one step, and give them in parts.
#[cfg(target_pointer_width = "64")]
#[test]
fn decoders_of_the_longest_runs() {
    // a run of `i64::MAX` sevens, one of eights, a nine
    let mut bytes = vec![0xff; 9];
    bytes.extend([0, 7]);
    bytes.extend([0xff; 9]);
    bytes.extend([0, 8, 0x7f, 9]);
    assert!(check_same_decoder::<u64>(&bytes));
    assert!(check_same_decoder::<Option<u64>>(&bytes));
    let long = i64::MAX as usize;
    let mut x = VerifiedDecoder::<u64>::try_new(&bytes).unwrap();
    let mut y = hexane::try_decoder::<u64>(&bytes).unwrap();
    assert_eq!(x.next_run_max(5), Some(Run { count: 5, value: 7 }));
    assert_eq!(y.next_run_max(5), Some(Run { count: 5, value: 7 }));
    let rest = Some(Run {
        count: long - 5,
        value: 7,
    });
    assert_eq!(x.clone().next_run(), rest);
    assert_eq!(y.clone().next_run(), rest);
    assert_eq!(x.clone().scan_for(8, usize::MAX), (long - 5, long));
    assert_eq!(y.clone().scan_for(8, usize::MAX), (long - 5, long));
    assert_eq!(x.clone().scan_for(8, long), (long - 5, 5));
    assert_eq!(y.clone().scan_for(8, long), (long - 5, 5));
    assert_eq!(x.clone().scan_for(9, usize::MAX), (2 * long - 5, 1));
    assert_eq!(y.clone().scan_for(9, usize::MAX), (2 * long - 5, 1));
    x.advance_by(2 * long - 6);
    RunDecoder::advance_by(&mut y, 2 * long - 6);
    assert_eq!(x.next(), Some(8));
    assert_eq!(y.next(), Some(8));
    assert_eq!(x.nth(0), Some(9));
    assert_eq!(y.nth(0), Some(9));
    assert_eq!(x.next(), None);
    assert_eq!(y.next(), None);

    // nulls, and `bool`s, are counted with no sign
    let mut nulls = vec![0];
    nulls.extend([0xff; 9]);
    nulls.push(1);
    assert!(check_same_decoder::<Option<u64>>(&nulls));
    assert!(check_same_decoder::<Option<Vec<u8>>>(&nulls));
    let mut x = VerifiedDecoder::<Option<u64>>::try_new(&nulls).unwrap();
    let all = Some(Run {
        count: usize::MAX,
        value: None,
    });
    assert_eq!(x.clone().next_run(), all);
    assert_eq!(x.nth(usize::MAX - 1), Some(None));
    assert_eq!(x.next(), None);
    // `usize::MAX - 2` times `false`, and `true` twice
    let mut flags = vec![0xfd];
    flags.extend([0xff; 8]);
    flags.extend([1, 2]);
    assert!(check_same_decoder::<bool>(&flags));
    let mut x = VerifiedDecoder::<bool>::try_new(&flags).unwrap();
    let mut y = hexane::try_decoder::<bool>(&flags).unwrap();
    assert_eq!(x.scan_for(true, usize::MAX - 1), (usize::MAX - 2, 1));
    assert_eq!(y.scan_for(true, usize::MAX - 1), (usize::MAX - 2, 1));
    assert_eq!((x.next(), x.next()), (Some(true), None));
    assert_eq!((y.next(), y.next()), (Some(true), None));
}

/// The kind of index a column is kept with is a third parameter of its
/// type, with a default: a plain column is one type, the kind named or
/// left out, and each constructor and each load makes a column of the
/// kind its type says.
#[test]
fn the_kind_of_a_plain_column() {
    let vs: Vec<u64> = (0..2000).map(|i| i / 7).collect();
    let named = VerifiedColumn::<u64, Leb128, LenIndex>::from_values(&vs);
    let short: VerifiedColumn<u64> = named.clone();
    let collected: VerifiedColumn<u64, Leb128, LenIndex> = vs.iter().copied().collect();
    let loaded = VerifiedColumn::<u64, Leb128, LenIndex>::load(&short.save()).unwrap();
    let with: VerifiedColumn<u64> = VerifiedColumn::load_with(
        &short.save(),
        LoadOpts::new().with_length(vs.len()).with_max_segments(4),
    )
    .unwrap();
    let none: VerifiedColumn<u64, Leb128, LenIndex> = Default::default();
    let mut budget = VerifiedColumn::<u64, Leb128, LenIndex>::with_max_segments(4);
    budget.extend(vs.iter().copied());
    let fill = VerifiedColumn::<u64, Leb128, LenIndex>::fill(5, 7);

    let old: Column<u64> = vs.iter().copied().collect();
    for c in [&named, &short, &collected, &loaded, &with, &budget] {
        assert_eq!(c.to_vec(), vs);
        assert_eq!(c.save(), old.save());
    }
    assert!(none.is_empty() && none.save().is_empty());
    assert_eq!(fill.to_vec(), vec![7; 5]);
    let it: VerifiedIter<'_, u64, Leb128, LenIndex> = short.iter();
    assert_eq!(it.count(), vs.len());
    let it = VerifiedIter::<u64, Leb128, LenIndex>::default();
    assert_eq!(it.count(), 0);
}

/// A value type a prefix column holds, in both APIs.
trait Summed: Kind + VSummed + PrefixValue {}

impl<T: Kind + VSummed + PrefixValue> Summed for T {}

/// The same checks for each value type of a prefix column.
macro_rules! for_each_sum {
    ($f:ident) => {
        $f::<u64>();
        $f::<i64>();
        $f::<u32>();
        $f::<NonZeroU32>();
        $f::<bool>();
        $f::<Option<u64>>();
        $f::<Option<i64>>();
        $f::<Option<u32>>();
        $f::<Option<NonZeroU32>>();
    };
}

/// What a prefix column of the verified API holds.
fn sum_values<T: Summed>(p: &VerifiedPrefixColumn<T>) -> Vec<T> {
    p.to_vec().into_iter().map(T::keep).collect()
}

/// What a `PrefixColumn` holds.
fn old_sum_values<T: Summed>(c: &PrefixColumn<T>) -> Vec<T> {
    c.to_vec().into_iter().map(old).collect()
}

/// `check_invariants` says of a prefix column what it says of the plain
/// column of the same edits: nothing, or the same words. (Where a panic
/// can be caught: not on wasm32.)
#[cfg(panic = "unwind")]
fn check_sum_invariants<T: Summed>(p: &VerifiedPrefixColumn<T>, v: &VerifiedColumn<T>) {
    let sums = panic_of(std::panic::AssertUnwindSafe(|| p.check_invariants()));
    let plain = panic_of(std::panic::AssertUnwindSafe(|| v.check_invariants()));
    assert_eq!(sums, plain);
}

#[cfg(not(panic = "unwind"))]
fn check_sum_invariants<T: Summed>(_p: &VerifiedPrefixColumn<T>, _v: &VerifiedColumn<T>) {}

/// A prefix column of the verified API is a column: it holds `model` and
/// reads as the plain column `v` of the same edits does, it lies in that
/// column's slabs, and it saves the bytes that column and `PrefixColumn`
/// save. `built` says that edits made the columns of values, which the
/// checks of `check_invariants` then pass.
fn check_sums<T: Summed>(
    p: &VerifiedPrefixColumn<T>,
    v: &VerifiedColumn<T>,
    c: &PrefixColumn<T>,
    model: &[T],
    built: bool,
) {
    assert_eq!(p.len(), model.len());
    assert_eq!(p.is_empty(), model.is_empty());
    assert_eq!(sum_values(p), model);
    assert_eq!(new_values(v), model);
    assert_eq!(old_sum_values(c), model);
    let mut it = p.iter();
    for m in model {
        assert_eq!(it.next().map(T::keep).as_ref(), Some(m));
    }
    assert!(it.next().is_none());
    assert!(it.next().is_none());
    assert!(p.get(model.len()).is_none());
    assert!(p.get(usize::MAX).is_none());
    let bytes = p.save();
    assert_eq!(bytes, v.save());
    assert_eq!(bytes, c.save());

    assert_eq!(p.slab_count(), v.slab_count());
    assert_eq!(p.slab_lens(), v.slab_lens());
    assert_eq!(p.slab_segments(), v.slab_segments());
    assert_eq!(format!("{p:?}"), format!("{v:?}"));
    if !model.is_empty() {
        assert_eq!(p.slab_count(), c.slab_count());
    }
    if built {
        p.check_invariants();
    } else {
        check_sum_invariants(p, v);
    }
    assert!(p.validate_encoding().is_ok());

    let (mut a, mut b) = (vec![9u8; 3], vec![9u8; 3]);
    let range = p.save_to(&mut a);
    assert_eq!(range, c.save_to(&mut b));
    assert_eq!(a, b);
    assert_eq!(a[range], bytes[..]);
    for x in model.first().into_iter().chain(model.last()) {
        let only = model.iter().all(|m| m == x);
        assert_eq!(p.is_only(x.lend()), only);
        let (mut a, mut b) = (vec![9u8; 3], vec![9u8; 3]);
        let range = p.save_to_unless(&mut a, x.lend());
        assert_eq!(range, c.save_to_unless(&mut b, x.as_column_ref()));
        assert_eq!(a, b);
    }
}

/// Each API's prefix column loads the bytes the other saved, as the same
/// values, and the verified one into the slabs a plain column loads them
/// into.
fn check_sum_loads<T: Summed>(p: &VerifiedPrefixColumn<T>, c: &PrefixColumn<T>, model: &[T]) {
    let bytes = p.save();
    assert_eq!(bytes, c.save());
    let p2 = VerifiedPrefixColumn::<T>::load(&c.save()).unwrap();
    let c2 = PrefixColumn::<T>::load(&bytes).unwrap();
    let v2 = VerifiedColumn::<T>::load(&bytes).unwrap();
    check_sums(&p2, &v2, &c2, model, false);
    assert_eq!(p2.save(), bytes);
    assert_eq!(c2.save(), bytes);
}

/// The reads of a plain column, of a prefix column: a value by its
/// place, a range of values, a search for a value, and a clone, which is
/// a column of its own.
fn check_sum_reads<T: Summed>(
    rng: &mut Rng,
    p: &VerifiedPrefixColumn<T>,
    v: &VerifiedColumn<T>,
    model: &[T],
) {
    for _ in 0..4 {
        if !model.is_empty() {
            let i = rng.below(model.len());
            assert_eq!(p.get(i).map(T::keep).as_ref(), Some(&model[i]));
        }
        let start = rng.below(model.len() + 1);
        let end = start + rng.below(model.len() - start + 1);
        let mut it = p.iter_range(start, end);
        assert_eq!(it.len(), end - start);
        for m in &model[start..end] {
            assert_eq!(it.next().map(T::keep).as_ref(), Some(m));
        }
        assert!(it.next().is_none());
        let x = match model.get(start) {
            Some(x) => x.clone(),
            None => T::fresh(rng),
        };
        assert_eq!(
            p.scope_to_value(x.lend(), start, end),
            v.scope_to_value(x.lend(), start, end)
        );
        let runs: Vec<(usize, T)> = p
            .iter_range(start, end)
            .runs()
            .map(|r| (r.count, T::keep(r.value)))
            .collect();
        assert_eq!(runs, runs_of(&model[start..end]));
    }
    let mut p2 = p.clone();
    assert_eq!(p2.slab_lens(), p.slab_lens());
    let x = T::fresh(rng);
    p2.insert(model.len() / 2, x.lend());
    assert_eq!(p2.len(), model.len() + 1);
    assert_eq!(p2.get(model.len() / 2).map(T::keep), Some(x));
    assert_eq!(sum_values(p), model);
}

fn sum_edits<T: Totalled>() {
    for seed in 1..=8u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0x50f5);
        let budget = [2, 3, 4, 8, 64][rng.below(5)];
        let mut p = VerifiedPrefixColumn::<T>::with_max_segments(budget);
        let mut v = VerifiedColumn::<T>::with_max_segments(budget);
        let mut c = PrefixColumn::<T>::with_max_segments(budget);
        let mut model: Vec<T> = vec![];
        for step in 0..120 {
            match rng.below(14) {
                0..=3 => {
                    let at = rng.below(model.len() + 1);
                    let del = rng.below((model.len() - at).min(12) + 1);
                    let ins: Vec<T> = rng.values(40);
                    if rng.below(2) == 0 {
                        p.splice(at, del, &lent(&ins));
                    } else {
                        p.splice_iter(at, del, ins.iter().map(T::lend));
                    }
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
                    p.insert(at, x.lend());
                    v.insert(at, x.lend());
                    c.insert(at, x.clone());
                    model.insert(at, x);
                }
                6 => {
                    let x = match model.last() {
                        Some(last) => rng.value(last),
                        None => T::fresh(&mut rng),
                    };
                    p.push(x.lend());
                    v.push(x.lend());
                    c.push(x.clone());
                    model.push(x);
                }
                7 | 8 => {
                    if !model.is_empty() {
                        let at = rng.below(model.len());
                        p.remove(at);
                        v.remove(at);
                        c.remove(at);
                        model.remove(at);
                    }
                }
                9 => {
                    // sometimes past the end, which changes nothing
                    let len = rng.below(model.len() + 3);
                    p.truncate(len);
                    v.truncate(len);
                    c.truncate(len);
                    model.truncate(len);
                }
                10 => {
                    let at = rng.below(model.len() + 1);
                    let n = match rng.below(8) {
                        0 => 0,
                        1 => model.len() - at,
                        _ => rng.below((model.len() - at).min(40) + 1),
                    };
                    p.remove_n(at, n);
                    v.remove_n(at, n);
                    c.remove_n(at, n);
                    model.drain(at..at + n);
                }
                11 | 12 => {
                    let at = rng.below(model.len() + 1);
                    let del = rng.below((model.len() - at).min(30) + 1);
                    let runs = some_runs::<T>(&mut rng, 6);
                    let lent_runs = || {
                        runs.iter().map(|(n, x)| Run {
                            count: *n,
                            value: x.lend(),
                        })
                    };
                    p.splice_runs(at, del, lent_runs());
                    v.splice_runs(at, del, lent_runs());
                    let owned_runs = runs.iter().map(|(n, x)| Run {
                        count: *n,
                        value: x.clone(),
                    });
                    c.splice_runs(at, del, owned_runs);
                    model.splice(at..at + del, run_model(&runs));
                }
                _ => {
                    if rng.below(3) == 0 {
                        p.clear();
                        v.clear();
                        c.clear();
                        model.clear();
                    } else {
                        let more: Vec<T> = rng.values(40);
                        p.extend(more.iter().map(T::lend));
                        v.extend(more.iter().map(T::lend));
                        c.extend(more.iter().cloned());
                        model.extend(more);
                    }
                }
            }
            check_sums(&p, &v, &c, &model, true);
            check_sum_reads(&mut rng, &p, &v, &model);
            check_prefix_reads(&mut rng, &p, &c, &model);
            if step % 10 == 9 {
                check_sum_loads(&p, &c, &model);
            }
        }
        check_sum_loads(&p, &c, &model);
        // loaded: an index made at once of all the slabs
        let p2 = VerifiedPrefixColumn::<T>::load(&p.save()).unwrap();
        check_prefix_reads(&mut rng, &p2, &c, &model);
    }
}

/// A prefix column of the verified API ([`VerifiedPrefixColumn`]) is a
/// column kept with another index. Under the same edits it holds what a
/// plain column holds, in the same slabs, reads the same, and saves the
/// same bytes, which are the bytes `PrefixColumn` saves; and each API's
/// prefix column loads what the other saved.
#[test]
fn a_prefix_column_is_a_column() {
    for_each_sum!(sum_edits);
}

fn sum_forms<T: Summed>() {
    for seed in 1..=6u64 {
        let mut rng = Rng(seed.wrapping_mul(0x2545_f491_4f6c_dd1d) ^ 0x50f6);
        let budget = [2, 3, 4, 8, 64][rng.below(5)];
        let model: Vec<T> = rng.values(300);
        let plain = VerifiedColumn::<T>::from_values(&lent(&model));
        let old = PrefixColumn::<T>::from_values(model.clone());

        let p = VerifiedPrefixColumn::<T>::from_values(&lent(&model));
        check_sums(&p, &plain, &old, &model, true);
        let p: VerifiedPrefixColumn<T> = model.iter().map(T::lend).collect();
        check_sums(&p, &plain, &old, &model, true);
        let p = VerifiedColumn::<T, Leb128, SumIndex>::from_iter(lent(&model));
        check_sums(&p, &plain, &old, &model, true);
        let mut p = VerifiedPrefixColumn::<T>::new();
        p.extend(lent(&model));
        check_sums(&p, &plain, &old, &model, true);
        let none = VerifiedPrefixColumn::<T>::default();
        assert!(none.is_empty() && none.save().is_empty());
        assert_eq!(none.slab_count(), 0);

        // in slabs of a budget, and loaded with one
        let mut p = VerifiedPrefixColumn::<T>::from_values_with_max_segments(&lent(&model), budget);
        let mut v = VerifiedColumn::<T>::from_values_with_max_segments(&lent(&model), budget);
        let mut inner = Column::<T, Leb128, PrefixWeightFn<T>>::with_max_segments(budget);
        inner.splice(0, 0, model.iter().cloned());
        let c = PrefixColumn::from_column(inner);
        check_sums(&p, &v, &c, &model, true);
        check_sum_loads(&p, &c, &model);
        let opts = LoadOpts::new()
            .with_length(model.len())
            .with_max_segments(budget);
        let p2 = VerifiedPrefixColumn::<T>::load_with(&p.save(), opts).unwrap();
        let v2 = VerifiedColumn::<T>::load_with(&p.save(), opts).unwrap();
        let c2 = PrefixColumn::<T>::load_with(&p.save(), opts).unwrap();
        check_sums(&p2, &v2, &c2, &model, false);
        // of a length the bytes do not have: the same error
        let long = LoadOpts::new().with_length(model.len() + 1);
        assert_eq!(
            VerifiedPrefixColumn::<T>::load_with(&p.save(), long)
                .map(|p| p.len())
                .map_err(|e| e.to_string()),
            PrefixColumn::<T>::load_with(&p.save(), long)
                .map(|c| c.len())
                .map_err(|e| e.to_string()),
        );

        // one value for each of a run's, as `Column::remap` gives them
        let (a, b) = (T::fresh(&mut rng), T::fresh(&mut rng));
        let f = |x: T| if not_above(&x, &a) { b.clone() } else { x };
        p.remap(f);
        v.remap(f);
        let mut inner = c.values().clone();
        inner.remap(f);
        let c = PrefixColumn::from_column(inner);
        let want: Vec<T> = model.iter().cloned().map(f).collect();
        check_sums(&p, &v, &c, &want, false);
        check_sum_loads(&p, &c, &want);

        // a column of one value
        let n = rng.below(500);
        let p = VerifiedPrefixColumn::<T>::fill(n, a.lend());
        let v = VerifiedColumn::<T>::fill(n, a.lend());
        let c = PrefixColumn::from_column(Column::fill(n, a.as_column_ref()));
        check_sums(&p, &v, &c, &vec![a.clone(); n], true);
    }
}

/// Whether `x` is not above `a`, as the type orders its values: the
/// values a remap of these tests gives another value for.
fn not_above<T: Kind>(x: &T, a: &T) -> bool {
    x.order(a) != Ordering::Greater
}

/// The other ways to make a prefix column of the verified API make what
/// they make of a plain column, and what `PrefixColumn` holds: a slice,
/// an iterator, `extend`, a segment budget, a load with options, a
/// remap, and `fill`.
#[test]
fn the_other_forms_of_a_prefix_column() {
    for_each_sum!(sum_forms);
}

/// The kind of a prefix column is the third parameter of the column's
/// type: `VerifiedPrefixColumn<V, C>` is `VerifiedColumn<V, C, SumIndex>`
/// written short, and its iterators are the column's.
#[test]
fn the_kind_of_a_prefix_column() {
    let vs: Vec<u32> = (0..2000).map(|i| i / 7).collect();
    let short = VerifiedPrefixColumn::<u32>::from_values(&vs);
    let named: VerifiedColumn<u32, Leb128, SumIndex> = short.clone();
    let coded: VerifiedPrefixColumn<u32, Leb128> = named.clone();
    assert_eq!(coded.to_vec(), vs);
    let it: VerifiedIter<'_, u32, Leb128, SumIndex> = short.iter();
    assert_eq!(it.count(), vs.len());
    let it = VerifiedIter::<u32, Leb128, SumIndex>::default();
    assert_eq!(it.count(), 0);
    let state: VerifiedIterState = short.iter_range(5, 900).suspend();
    let mut it = state.try_resume(&named).unwrap();
    assert_eq!((it.pos(), it.next()), (5, Some(0)));
    // a plain column of the same values is another type, with the same
    // bytes
    let plain = VerifiedColumn::<u32>::from_values(&vs);
    assert_eq!(plain.save(), short.save());
}

/// A prefix column is kept only of values whose sums fit the type they
/// are kept in: the sums of `u32`s, in a `u64`. Bytes whose values add up
/// past it are a plain column's, which loads them, and no prefix column's:
/// the load refuses them, with the error of `PrefixColumn`'s. The sums of
/// a column of `u64`s, of `i64`s or of `bool`s fit whatever it holds.
///
/// (The columns here are longer than a 32-bit `usize` counts.)
#[cfg(target_pointer_width = "64")]
#[test]
fn a_load_refuses_sums_that_do_not_fit() {
    fn loads<T: Summed>(bytes: &[u8]) -> Result<usize, String> {
        let new = VerifiedPrefixColumn::<T>::load(bytes).map(|p| (p.len(), p.save()));
        let old = PrefixColumn::<T>::load(bytes).map(|c| (c.len(), c.save()));
        let (new, old) = (
            new.map_err(|e| e.to_string()),
            old.map_err(|e| e.to_string()),
        );
        assert_eq!(new, old);
        // with options: the same
        let with =
            VerifiedPrefixColumn::<T>::load_with(bytes, LoadOpts::new().with_max_segments(4));
        assert_eq!(
            with.map(|p| p.len()).map_err(|e| e.to_string()),
            new.clone().map(|x| x.0)
        );
        // a plain column loads them either way
        let plain = VerifiedColumn::<T>::load(bytes).unwrap();
        assert_eq!(plain.save(), bytes);
        new.map(|(len, saved)| {
            assert_eq!(saved, bytes);
            assert_eq!(len, plain.len());
            len
        })
    }
    let refused = Err("invalid value: prefix sum overflows its type".to_string());

    // `u32::MAX` 2^32 + 1 times adds up to `u64::MAX`
    let most = (1usize << 32) + 1;
    let mut v = VerifiedColumn::<u32>::fill(most, u32::MAX);
    assert_eq!(loads::<u32>(&v.save()), Ok(most));
    v.push(0);
    assert_eq!(loads::<u32>(&v.save()), Ok(most + 1));
    v.push(1);
    assert_eq!(loads::<u32>(&v.save()), refused);
    v.remove(0);
    assert_eq!(loads::<u32>(&v.save()), Ok(most + 1));
    let v = VerifiedColumn::<u32>::fill(most + 1, u32::MAX);
    assert_eq!(loads::<u32>(&v.save()), refused);
    let v = VerifiedColumn::<u32>::fill(1 << 33, u32::MAX);
    assert_eq!(loads::<u32>(&v.save()), refused);

    // in slabs whose sums each fit, while their total does not
    let run = |value: u32, count: usize| Run { count, value };
    let mut v = VerifiedColumn::<u32>::new();
    let runs = [
        run(u32::MAX, 1 << 32),
        run(0, 1),
        run(u32::MAX - 1, 1 << 32),
    ];
    v.splice_runs(0, 0, runs);
    let bytes = v.save();
    assert_eq!(loads::<u32>(&bytes), refused);
    let opts = LoadOpts::new().with_max_segments(2);
    let new = VerifiedPrefixColumn::<u32>::load_with(&bytes, opts);
    let old = PrefixColumn::<u32>::load_with(&bytes, opts);
    assert_eq!(
        new.err().unwrap().to_string(),
        old.err().unwrap().to_string()
    );
    assert_eq!(
        VerifiedColumn::<u32>::load_with(&bytes, opts)
            .unwrap()
            .slab_count(),
        3
    );

    // with nulls, which add nothing, and of numbers that are not zero
    let mut v = VerifiedColumn::<Option<u32>>::fill(most, Some(u32::MAX));
    v.splice_runs(
        7,
        0,
        [Run {
            count: 1 << 40,
            value: None,
        }],
    );
    assert_eq!(loads::<Option<u32>>(&v.save()), Ok(most + (1 << 40)));
    v.push(Some(1));
    assert_eq!(loads::<Option<u32>>(&v.save()), refused);
    let v = VerifiedColumn::<NonZeroU32>::fill(most, NonZeroU32::MAX);
    assert_eq!(loads::<NonZeroU32>(&v.save()), Ok(most));
    let v = VerifiedColumn::<NonZeroU32>::fill(most + 1, NonZeroU32::MAX);
    assert_eq!(loads::<NonZeroU32>(&v.save()), refused);
    let v = VerifiedColumn::<Option<NonZeroU32>>::fill(1 << 33, Some(NonZeroU32::MAX));
    assert_eq!(loads::<Option<NonZeroU32>>(&v.save()), refused);

    // the longest columns of the wider types
    let long = i64::MAX as usize;
    let v = VerifiedColumn::<u64>::fill(long, u64::MAX);
    assert_eq!(loads::<u64>(&v.save()), Ok(long));
    let v = VerifiedColumn::<i64>::fill(long, i64::MIN);
    assert_eq!(loads::<i64>(&v.save()), Ok(long));
    let v = VerifiedColumn::<Option<i64>>::fill(long, Some(i64::MAX));
    assert_eq!(loads::<Option<i64>>(&v.save()), Ok(long));
    let v = VerifiedColumn::<bool>::fill(long, true);
    assert_eq!(loads::<bool>(&v.save()), Ok(long));
}

/// An edit that would leave a prefix column of values whose sums do not
/// fit panics, as `PrefixColumn`'s does and with its words, and so do a
/// `fill` and a remap; one whose sums fit goes through, also where the
/// sum is the largest of its type.
///
/// (The columns here are longer than a 32-bit `usize` counts.)
#[cfg(target_pointer_width = "64")]
#[test]
fn an_edit_past_the_sums_panics() {
    type P<T> = VerifiedPrefixColumn<T>;
    let run = |value: u32, count: usize| Run { count, value };
    let words = Some("prefix sum overflows its type".to_string());
    let most = (1usize << 32) + 1;
    let panics = |f: &dyn Fn()| panic_of(std::panic::AssertUnwindSafe(f));

    // one run; and two edits of one slab
    let new = panics(&|| P::<u32>::new().splice_runs(0, 0, [run(u32::MAX, 1 << 33)]));
    let old = panics(&|| PrefixColumn::<u32>::new().splice_runs(0, 0, [run(u32::MAX, 1 << 33)]));
    assert_eq!((&new, &old), (&words, &words));
    let new = panics(&|| {
        let mut p = P::<u32>::new();
        p.splice_runs(0, 0, [run(u32::MAX, 1 << 32)]);
        p.splice_runs(0, 0, [run(u32::MAX - 1, 1 << 32)]);
    });
    assert_eq!(new, words);

    // slabs of at most two runs: the sums of each fit, and their total
    // does not
    let new = panics(&|| {
        let mut p = P::<u32>::with_max_segments(2);
        p.splice_runs(0, 0, [run(u32::MAX, 1 << 32)]);
        assert_eq!(p.len(), 1 << 32);
        p.splice_runs(1 << 32, 0, [run(0, 1), run(u32::MAX - 1, 1 << 32)]);
    });
    assert_eq!(new, words);
    let new = panics(&|| {
        let mut p = P::<u32>::with_max_segments(2);
        let big = run(u32::MAX, 1 << 32);
        p.splice_runs(0, 0, [big, run(0, 1), run(u32::MAX - 1, 1 << 32)]);
    });
    assert_eq!(new, words);
    let new = panics(&|| {
        let mut p = P::<u32>::with_max_segments(2);
        let big = run(u32::MAX, 1 << 32);
        p.splice_runs(0, 0, [big, run(0, 1), run(u32::MAX - 1, 1)]);
        // lengthens the last run, which keeps its slab
        p.splice_runs(p.len(), 0, [run(u32::MAX - 1, 1 << 32)]);
    });
    assert_eq!(new, words);

    // with nulls, and of numbers that are not zero
    let new = panics(&|| {
        let run = |value: Option<u32>, count: usize| Run { count, value };
        let mut p = P::<Option<u32>>::with_max_segments(2);
        p.splice_runs(0, 0, [run(Some(u32::MAX), 1 << 32)]);
        p.splice_runs(0, 0, [run(Some(u32::MAX - 1), 1 << 32), run(None, 5)]);
    });
    assert_eq!(new, words);
    let new = panics(&|| {
        let big = Run {
            count: 1 << 33,
            value: NonZeroU32::MAX,
        };
        P::<NonZeroU32>::new().splice_runs(0, 0, [big]);
    });
    assert_eq!(new, words);

    // a column filled with one value
    assert_eq!(panics(&|| drop(P::<u32>::fill(1 << 33, u32::MAX))), words);
    assert_eq!(panics(&|| drop(P::<u32>::fill(most + 1, u32::MAX))), words);
    assert_eq!(
        panics(&|| drop(P::<Option<NonZeroU32>>::fill(
            most + 1,
            Some(NonZeroU32::MAX)
        ))),
        words
    );

    // the largest sum, and one more: by each edit of one value
    let full = P::<u32>::fill(most, u32::MAX);
    assert_eq!(full.len(), most);
    let old_full = PrefixColumn::<u32>::from_column(Column::fill(most, u32::MAX));
    assert_eq!(full.save(), old_full.save());
    assert_eq!(panics(&|| full.clone().push(1)), words);
    assert_eq!(panics(&|| old_full.clone().push(1u32)), words);
    assert_eq!(panics(&|| full.clone().insert(5, 1)), words);
    assert_eq!(panics(&|| full.clone().splice(most, 0, &[0, 0, 1])), words);
    assert_eq!(panics(&|| full.clone().splice_iter(0, 0, [0, 1])), words);
    assert_eq!(panics(&|| full.clone().extend([0, 0, 1])), words);
    let (mut p, mut c) = (full.clone(), old_full.clone());
    p.push(0);
    c.push(0u32);
    p.insert(5, 0);
    c.insert(5, 0u32);
    p.extend([0, 0]);
    c.extend([0u32, 0]);
    p.splice(3, 2, &[u32::MAX, u32::MAX]);
    c.splice(3, 2, [u32::MAX, u32::MAX]);
    assert_eq!(p.len(), most + 4);
    // and room made by a deletion can be filled again
    p.remove(0);
    c.remove(0);
    p.push(u32::MAX);
    c.push(u32::MAX);
    p.remove_n(0, 2);
    c.remove_n(0, 2);
    p.splice(0, 0, &[u32::MAX - 1, u32::MAX, 1]);
    c.splice(0, 0, [u32::MAX - 1, u32::MAX, 1]);
    assert_eq!(p.save(), c.save());
    assert_eq!(panics(&|| p.clone().push(1)), words);
    assert_eq!(panics(&|| c.clone().push(1u32)), words);
    let loaded = P::<u32>::load(&p.save()).unwrap();
    assert_eq!(loaded.len(), most + 5);
    assert_eq!(panics(&|| loaded.clone().push(1)), words);

    // sums that fit still edit: u32::MAX * (2^32 + 1) == u64::MAX
    let mut p = P::<u32>::with_max_segments(2);
    p.splice_runs(0, 0, [run(u32::MAX, 1 << 32)]);
    p.splice_runs(0, 0, [run(0, 1 << 40)]);
    // three runs: two slabs
    p.insert(0, u32::MAX);
    let mut c = PrefixColumn::<u32>::with_max_segments(2);
    c.splice_runs(0, 0, [run(u32::MAX, 1 << 32)]);
    c.splice_runs(0, 0, [run(0, 1 << 40)]);
    c.insert(0, u32::MAX);
    assert_eq!((p.slab_count(), c.slab_count()), (2, 2));
    assert_eq!(p.save(), c.save());
    assert_eq!(p.len(), (1 << 40) + most);
    assert_eq!(panics(&|| p.clone().push(1)), words);
    assert_eq!(panics(&|| p.clone().insert(1 << 39, 1)), words);

    // a remap: 2^33 ones add up to 2^33, and as `u32::MAX` past `u64::MAX`
    let ones = P::<u32>::fill(1 << 33, 1);
    assert_eq!(panics(&|| ones.clone().remap(|_| u32::MAX)), words);
    let mut twos = ones.clone();
    twos.remap(|x| x + 1);
    assert_eq!((twos.len(), twos.get(77)), (1 << 33, Some(2)));
    // across slabs: a segment budget of 2 cuts the remapped column into a
    // slab for each run
    let new = panics(&|| {
        let mut p = P::<u32>::with_max_segments(2);
        p.splice_runs(0, 0, [run(1, 1 << 32), run(0, 1), run(1, 1 << 32)]);
        p.remap(|x| if x == 0 { 0 } else { u32::MAX });
    });
    assert_eq!(new, words);

    // a plain column of the same edits takes them all
    let mut v = VerifiedColumn::<u32>::fill(1 << 33, u32::MAX);
    v.push(1);
    v.remap(|_| u32::MAX);
    assert_eq!(v.len(), (1 << 33) + 1);
}

/// A sum of either API, as one type of integer. (The sums of these tests
/// are far from its bounds.)
trait Wide {
    fn wide(&self) -> i128;
}

impl Wide for u64 {
    fn wide(&self) -> i128 {
        i128::from(*self)
    }
}

impl Wide for u128 {
    fn wide(&self) -> i128 {
        i128::try_from(*self).unwrap()
    }
}

impl Wide for usize {
    fn wide(&self) -> i128 {
        i128::try_from(*self).unwrap()
    }
}

impl Wide for i128 {
    fn wide(&self) -> i128 {
        *self
    }
}

/// What a value adds to the sums of a prefix column: the model of both
/// APIs' sums.
trait Adds {
    fn adds(&self) -> i128;
}

impl Adds for u64 {
    fn adds(&self) -> i128 {
        i128::from(*self)
    }
}

impl Adds for i64 {
    fn adds(&self) -> i128 {
        i128::from(*self)
    }
}

impl Adds for u32 {
    fn adds(&self) -> i128 {
        i128::from(*self)
    }
}

impl Adds for NonZeroU32 {
    fn adds(&self) -> i128 {
        i128::from(self.get())
    }
}

impl Adds for bool {
    fn adds(&self) -> i128 {
        i128::from(*self)
    }
}

impl<T: Adds> Adds for Option<T> {
    fn adds(&self) -> i128 {
        self.as_ref().map_or(0, T::adds)
    }
}

/// A value type a prefix column holds whose sums, of either API, these
/// tests read as integers.
trait Totalled: Summed + Adds {
    fn new_sum(s: <Self as VSummed>::Sum) -> i128;

    fn old_sum(s: <Self as PrefixValue>::Prefix) -> i128;
}

impl<T: Summed + Adds> Totalled for T
where
    <T as VSummed>::Sum: Wide,
    <T as PrefixValue>::Prefix: Wide,
{
    fn new_sum(s: <T as VSummed>::Sum) -> i128 {
        s.wide()
    }

    fn old_sum(s: <T as PrefixValue>::Prefix) -> i128 {
        s.wide()
    }
}

/// The sums before each position of `model`, and before its end.
fn sums_before<T: Adds>(model: &[T]) -> Vec<i128> {
    let mut before = vec![0];
    for m in model {
        before.push(before[before.len() - 1] + m.adds());
    }
    before
}

/// Positions to ask a column of `len` values for: its ends, some within
/// it and some past it.
fn some_places(rng: &mut Rng, len: usize) -> Vec<usize> {
    let mut places = vec![
        0,
        1,
        len / 2,
        len.saturating_sub(1),
        len,
        len + 1,
        len + 7,
        usize::MAX - 1,
        usize::MAX,
    ];
    for _ in 0..4 {
        places.push(rng.below(len + 1));
    }
    places
}

/// What a prefix column answers from its sums is what its values add
/// up to, and what `PrefixColumn` answers: the sum before a position
/// and through one, at any position, and the sum of a range, whatever
/// its ends.
fn check_prefix_reads<T: Totalled>(
    rng: &mut Rng,
    p: &VerifiedPrefixColumn<T>,
    c: &PrefixColumn<T>,
    model: &[T],
) {
    let before = sums_before(model);
    let at = |i: usize| before[i.min(model.len())];
    let places = some_places(rng, model.len());
    for &i in &places {
        assert_eq!(T::new_sum(p.get_prefix(i)), at(i));
        assert_eq!(T::old_sum(c.get_prefix(i)), at(i));
        assert_eq!(T::new_sum(p.get_total(i)), at(i.saturating_add(1)));
        assert_eq!(T::old_sum(c.get_total(i)), at(i.saturating_add(1)));
    }
    for (n, &start) in places.iter().enumerate() {
        // each end with some of the starts
        for &end in places.iter().skip(n % 3).step_by(3) {
            let want = if start < end { at(end) - at(start) } else { 0 };
            assert_eq!(T::new_sum(p.sum_range(start, end)), want);
            assert_eq!(T::old_sum(c.sum_range(start..end)), want);
        }
    }
    assert_eq!(T::new_sum(p.sum_range(0, model.len())), at(model.len()));
}

/// A sum that is never negative, made of a number of these tests.
trait Target {
    fn target(x: u128) -> Self;
}

impl Target for u64 {
    fn target(x: u128) -> u64 {
        u64::try_from(x).unwrap()
    }
}

impl Target for u128 {
    fn target(x: u128) -> u128 {
        x
    }
}

impl Target for usize {
    fn target(x: u128) -> usize {
        usize::try_from(x).unwrap()
    }
}

/// A value type whose sums are never negative, in both APIs: a prefix
/// column of it is searched by sum. The searches give the place for a
/// sum before it, and the place for a sum through it.
trait Counted: Totalled {
    fn new_search(p: &VerifiedPrefixColumn<Self>, target: u128) -> (usize, usize);

    fn old_search(c: &PrefixColumn<Self>, target: u128) -> (usize, usize);
}

macro_rules! counted {
    ($($t:ty => $s:ty),*) => {
        $(
            impl Counted for $t {
                fn new_search(p: &VerifiedPrefixColumn<$t>, target: u128) -> (usize, usize) {
                    let t = <$s as Target>::target(target);
                    (p.get_index_for_prefix(t), p.get_index_for_total(t))
                }

                fn old_search(c: &PrefixColumn<$t>, target: u128) -> (usize, usize) {
                    let t = <$s as Target>::target(target);
                    (c.get_index_for_prefix(t), c.get_index_for_total(t))
                }
            }
        )*
    };
}

counted!(
    u64 => u128,
    u32 => u64,
    NonZeroU32 => u64,
    bool => usize,
    Option<u64> => u128,
    Option<u32> => u64,
    Option<NonZeroU32> => u64
);

/// The searches of a prefix column by sum find what a walk of the sums
/// finds, and what `PrefixColumn`'s find: the first position that the
/// values before it add up to a target, which is one past the end where
/// none does, and the position ahead of it.
fn check_sum_searches<T: Counted>(
    rng: &mut Rng,
    p: &VerifiedPrefixColumn<T>,
    c: &PrefixColumn<T>,
    model: &[T],
) {
    let before: Vec<u128> = sums_before(model)
        .into_iter()
        .map(|b| u128::try_from(b).unwrap())
        .collect();
    let all = before[model.len()];
    let mut targets = vec![
        0,
        1,
        2,
        all / 2,
        all.saturating_sub(1),
        all,
        all + 1,
        all + 3,
    ];
    for _ in 0..6 {
        // a sum the column has, and those beside it
        let b = before[rng.below(before.len())];
        targets.extend([b.saturating_sub(1), b, b + 1]);
    }
    for t in targets {
        let place = before
            .iter()
            .position(|b| *b >= t)
            .unwrap_or(model.len() + 1);
        let want = (place, place.saturating_sub(1));
        assert_eq!(T::new_search(p, t), want);
        assert_eq!(T::old_search(c, t), want);
    }
}

fn sum_searches<T: Counted>() {
    for seed in 1..=8u64 {
        let mut rng = Rng(seed.wrapping_mul(0xd6e8_feb8_6659_fd93) ^ 0x50f7);
        let budget = [2, 3, 4, 8, 64][rng.below(5)];
        let mut p = VerifiedPrefixColumn::<T>::with_max_segments(budget);
        let mut c = PrefixColumn::<T>::with_max_segments(budget);
        let mut model: Vec<T> = vec![];
        check_sum_searches(&mut rng, &p, &c, &model);
        for _ in 0..60 {
            let at = rng.below(model.len() + 1);
            let del = rng.below((model.len() - at).min(20) + 1);
            if rng.below(3) == 0 {
                let runs = some_runs::<T>(&mut rng, 6);
                let lent_runs = runs.iter().map(|(n, x)| Run {
                    count: *n,
                    value: x.lend(),
                });
                p.splice_runs(at, del, lent_runs);
                let owned_runs = runs.iter().map(|(n, x)| Run {
                    count: *n,
                    value: x.clone(),
                });
                c.splice_runs(at, del, owned_runs);
                model.splice(at..at + del, run_model(&runs));
            } else {
                let ins: Vec<T> = rng.values(40);
                p.splice(at, del, &lent(&ins));
                c.splice(at, del, ins.iter().cloned());
                model.splice(at..at + del, ins);
            }
            check_sum_searches(&mut rng, &p, &c, &model);
            check_prefix_reads(&mut rng, &p, &c, &model);
        }
        // loaded: an index made at once of all the slabs
        let p2 = VerifiedPrefixColumn::<T>::load(&p.save()).unwrap();
        let c2 = PrefixColumn::<T>::load(&c.save()).unwrap();
        check_sum_searches(&mut rng, &p2, &c2, &model);
        check_prefix_reads(&mut rng, &p2, &c2, &model);
    }
}

/// A prefix column whose sums are never negative is searched by sum
/// ([`VerifiedColumn::get_index_for_prefix`],
/// [`VerifiedColumn::get_index_for_total`]): under the same edits the
/// searches find the places a walk of the sums finds, and
/// `PrefixColumn`'s searches find.
#[test]
fn a_prefix_column_is_searched_by_sum() {
    sum_searches::<u64>();
    sum_searches::<u32>();
    sum_searches::<NonZeroU32>();
    sum_searches::<bool>();
    sum_searches::<Option<u64>>();
    sum_searches::<Option<u32>>();
    sum_searches::<Option<NonZeroU32>>();
}

/// The sums of a short column, by hand: before a position, through one,
/// of a range, and the places of sums.
#[test]
fn the_sums_of_a_prefix_column() {
    let p = VerifiedPrefixColumn::<u64>::from_values(&[3, 0, 4, 4, 10]);
    let sums: Vec<u128> = (0..7).map(|i| p.get_prefix(i)).collect();
    assert_eq!(sums, [0, 3, 3, 7, 11, 21, 21]);
    let sums: Vec<u128> = (0..6).map(|i| p.get_total(i)).collect();
    assert_eq!(sums, [3, 3, 7, 11, 21, 21]);
    assert_eq!(p.get_total(usize::MAX), 21);
    assert_eq!(p.sum_range(1, 4), 8);
    assert_eq!(p.sum_range(2, 2), 0);
    assert_eq!(p.sum_range(4, 2), 0);
    assert_eq!(p.sum_range(3, 99), 14);
    assert_eq!(p.sum_range(5, 99), 0);
    assert_eq!(p.sum_range(usize::MAX, usize::MAX), 0);
    // the first position the values before it add up to the target
    let places: Vec<usize> = [0, 1, 3, 4, 7, 8, 11, 12, 21, 22]
        .into_iter()
        .map(|t| p.get_index_for_prefix(t))
        .collect();
    assert_eq!(places, [0, 1, 1, 3, 3, 4, 4, 5, 5, 6]);
    // the first position the values through it add up to the target
    let places: Vec<usize> = [0, 1, 3, 4, 7, 8, 11, 12, 21, 22]
        .into_iter()
        .map(|t| p.get_index_for_total(t))
        .collect();
    assert_eq!(places, [0, 0, 0, 2, 2, 3, 3, 4, 4, 5]);

    // signed values: the sums go down too
    let p = VerifiedPrefixColumn::<Option<i64>>::from_values(&[
        Some(5),
        None,
        Some(-9),
        Some(i64::MIN),
    ]);
    assert_eq!(p.get_prefix(3), -4);
    assert_eq!(p.get_total(3), -4 + i128::from(i64::MIN));
    assert_eq!(p.sum_range(1, 3), -9);
    assert_eq!(p.sum_range(3, 4), i128::from(i64::MIN));

    // flags: the sums count those that are set
    let flags = [false, true, true, false, false, true];
    let p = VerifiedPrefixColumn::<bool>::from_values(&flags);
    assert_eq!(p.get_prefix(6), 3);
    assert_eq!(p.sum_range(2, 5), 1);
    // the place of the `k`th that is set
    let set: Vec<usize> = (0..4).map(|k| p.get_index_for_total(k + 1)).collect();
    assert_eq!(set, [1, 2, 5, 6]);

    // no values: no sums, and a place for no sum but zero
    let none = VerifiedPrefixColumn::<u32>::new();
    assert_eq!((none.get_prefix(0), none.get_prefix(9)), (0, 0));
    assert_eq!((none.get_total(0), none.sum_range(0, 9)), (0, 0));
    assert_eq!(none.get_index_for_prefix(0), 0);
    assert_eq!(none.get_index_for_prefix(1), 1);
    assert_eq!(none.get_index_for_total(0), 0);
    assert_eq!(none.get_index_for_total(1), 0);
    let old = PrefixColumn::<u32>::new();
    assert_eq!(old.get_index_for_prefix(1), 1);
    assert_eq!(old.get_index_for_total(1), 0);
}

/// The sums of columns of long runs, up to the largest a type holds:
/// what the runs add up to, and what `PrefixColumn` answers.
///
/// (The columns here are longer than a 32-bit `usize` counts.)
#[cfg(target_pointer_width = "64")]
#[test]
fn the_sums_of_long_runs() {
    // `u32::MAX` 2^32 + 1 times adds up to `u64::MAX`
    let most = (1usize << 32) + 1;
    let p = VerifiedPrefixColumn::<u32>::fill(most, u32::MAX);
    let c = PrefixColumn::<u32>::from_column(Column::fill(most, u32::MAX));
    let each = u64::from(u32::MAX);
    assert_eq!(p.get_prefix(most), u64::MAX);
    assert_eq!(p.get_prefix(usize::MAX), u64::MAX);
    assert_eq!(p.get_total(most - 1), u64::MAX);
    assert_eq!(p.get_prefix(most - 1), u64::MAX - each);
    assert_eq!(p.sum_range(1, most), u64::MAX - each);
    assert_eq!(p.sum_range(0, usize::MAX), u64::MAX);
    assert_eq!(p.sum_range(77, 1 << 32), ((1 << 32) - 77) * each);
    for t in [
        1,
        each,
        each + 1,
        5 * each,
        u64::MAX - each,
        u64::MAX - each + 1,
        u64::MAX,
    ] {
        let place = usize::try_from(t.div_ceil(each)).unwrap();
        assert_eq!(p.get_index_for_prefix(t), place);
        assert_eq!(c.get_index_for_prefix(t), place);
        assert_eq!(p.get_index_for_total(t), place - 1);
        assert_eq!(c.get_index_for_total(t), place - 1);
    }

    // in slabs of two runs
    let run = |value: u32, count: usize| Run { count, value };
    let mut p = VerifiedPrefixColumn::<u32>::with_max_segments(2);
    let mut c = PrefixColumn::<u32>::with_max_segments(2);
    for (at, runs) in [
        (0, [run(u32::MAX, 1 << 30), run(7, 1 << 40)]),
        (1 << 20, [run(0, 1 << 50), run(u32::MAX - 1, 1 << 30)]),
        (5, [run(1, 3), run(u32::MAX, 1)]),
    ] {
        p.splice_runs(at, 0, runs);
        c.splice_runs(at, 0, runs);
    }
    assert_eq!(p.save(), c.save());
    assert!(p.slab_count() > 2);
    let len = p.len();
    let mut rng = Rng(0x50f8);
    let mut places = some_places(&mut rng, len);
    places.extend([5, 9, 1 << 20, (1 << 20) + (1 << 50), (1 << 30) + 4, len - 1]);
    for &i in &places {
        assert_eq!(p.get_prefix(i), c.get_prefix(i));
        assert_eq!(p.get_total(i), c.get_total(i));
        for &j in &places {
            assert_eq!(p.sum_range(i, j), c.sum_range(i..j));
        }
        // the sum before a place, and those beside it, as targets
        let sum = p.get_prefix(i);
        for t in [sum.saturating_sub(1), sum, sum.saturating_add(1)] {
            let place = p.get_index_for_prefix(t);
            assert_eq!(place, c.get_index_for_prefix(t));
            assert_eq!(p.get_index_for_total(t), c.get_index_for_total(t));
            if place <= len {
                assert!(p.get_prefix(place) >= t);
                assert!(place == 0 || p.get_prefix(place - 1) < t);
            } else {
                assert!(p.get_prefix(len) < t);
            }
        }
    }

    // the longest columns: of the widest values, of signed ones, of flags
    let long = i64::MAX as usize;
    let p = VerifiedPrefixColumn::<u64>::fill(long, u64::MAX);
    let all = u128::from(u64::MAX) * (long as u128);
    assert_eq!(p.get_prefix(long), all);
    assert_eq!(p.sum_range(1, long - 1), all - 2 * u128::from(u64::MAX));
    assert_eq!(p.get_index_for_prefix(all), long);
    assert_eq!(p.get_index_for_prefix(all + 1), long + 1);
    assert_eq!(p.get_index_for_total(all + 1), long);
    assert_eq!(p.get_index_for_prefix(u128::MAX), long + 1);
    let p = VerifiedPrefixColumn::<i64>::fill(long, i64::MIN);
    let all = i128::from(i64::MIN) * (long as i128);
    assert_eq!(p.get_prefix(long), all);
    assert_eq!(p.get_total(usize::MAX), all);
    assert_eq!(p.sum_range(long - 3, usize::MAX), 3 * i128::from(i64::MIN));
    let p = VerifiedPrefixColumn::<bool>::fill(long, true);
    assert_eq!(p.get_prefix(long), long);
    assert_eq!(p.sum_range(12, long), long - 12);
    assert_eq!(p.get_index_for_prefix(long), long);
    assert_eq!(p.get_index_for_prefix(long + 1), long + 1);
    assert_eq!(p.get_index_for_total(1 << 40), (1 << 40) - 1);
    assert_eq!(p.get_index_for_total(usize::MAX), long);
}

/// Where a prefix iterator stands, how far its window reaches, how many
/// values it has left, and its running sum (under both its names).
type Place = (usize, usize, usize, usize, i128, i128);

fn new_place<T: Totalled>(it: &VerifiedPrefixIter<'_, T>) -> Place {
    (
        it.pos(),
        it.end_pos(),
        it.items_left(),
        it.len(),
        T::new_sum(it.total()),
        T::new_sum(it.prefix()),
    )
}

fn old_place<T: Totalled>(it: &PrefixIter<'_, T>) -> Place {
    (
        it.pos(),
        it.end_pos(),
        it.items_left(),
        it.len(),
        T::old_sum(it.total()),
        T::old_sum(it.prefix()),
    )
}

/// A value with the sums before it and through it.
type Item<T> = Option<(T, i128, i128)>;

fn new_item<T: Totalled>(pv: Option<VerifiedPrefixedValue<'_, T>>) -> Item<T> {
    pv.map(|pv| {
        (
            T::keep(pv.value),
            T::new_sum(pv.prefix()),
            T::new_sum(pv.total()),
        )
    })
}

fn old_item<T: Totalled>(pv: Option<PrefixedValue<'_, T>>) -> Item<T> {
    pv.map(|pv| {
        (
            old(pv.value),
            T::old_sum(pv.prefix()),
            T::old_sum(pv.total()),
        )
    })
}

/// A prefix iterator as its specification has it: where it stands,
/// where its window ends, and where its sum counts from.
struct SumIter<'m, T> {
    model: &'m [T],
    before: &'m [i128],
    pos: usize,
    end: usize,
    base: usize,
}

impl<T: Totalled> SumIter<'_, T> {
    fn sum(&self, to: usize) -> i128 {
        self.before[to] - self.before[self.base]
    }

    /// The values left: the window can run past the column.
    fn left(&self) -> usize {
        self.end.min(self.model.len()) - self.pos
    }

    fn place(&self) -> Place {
        let sum = self.sum(self.pos);
        (
            self.pos,
            self.end,
            self.end - self.pos,
            self.left(),
            sum,
            sum,
        )
    }

    fn nth(&mut self, n: usize) -> Item<T> {
        if n < self.left() {
            let i = self.pos + n;
            self.pos = i + 1;
            Some((self.model[i].clone(), self.sum(i), self.sum(i + 1)))
        } else {
            // all it had is passed, and its window ends there
            self.pos += self.left();
            self.end = self.pos;
            None
        }
    }

    fn set_max(&mut self, end: usize) {
        self.end = end.max(self.pos);
    }

    fn advance_to(&mut self, target: usize) {
        if target > self.pos {
            self.nth(target - self.pos - 1);
        }
    }

    fn advance_by(&mut self, amount: usize) {
        if amount > 0 {
            self.nth(amount - 1);
        }
    }
}

/// A position to move an iterator to: mostly in the column, sometimes
/// near its ends, sometimes far past it.
fn a_place(rng: &mut Rng, len: usize) -> usize {
    match rng.below(8) {
        0 => rng.below(3),
        1 => len + rng.below(3),
        2 => usize::MAX - rng.below(2),
        _ => rng.below(len + 1),
    }
}

/// A prefix column's iterator gives each value with the sums before it
/// and through it, and keeps its running sum under every move: as its
/// specification says, and as `PrefixColumn`'s iterator does, the two
/// being asked the same at each step and standing in the same place
/// after it.
fn check_prefix_iters<T: Totalled>(
    rng: &mut Rng,
    p: &VerifiedPrefixColumn<T>,
    c: &PrefixColumn<T>,
    model: &[T],
) {
    let len = model.len();
    let before = sums_before(model);
    let item = |i: usize| (i < len).then(|| (model[i].clone(), before[i], before[i + 1]));
    // all of it, in a `for` loop and as an iterator of the standard
    // library
    for (n, pv) in p.prefix_iter().enumerate() {
        assert_eq!(new_item(Some(pv)), item(n));
    }
    assert_eq!(p.prefix_iter().count(), len);
    let all: Vec<Item<T>> = (0..len).map(item).collect();
    let new: Vec<Item<T>> = p.prefix_iter().map(|pv| new_item(Some(pv))).collect();
    let old: Vec<Item<T>> = c.iter().map(|pv| old_item(Some(pv))).collect();
    assert_eq!(new, all);
    assert_eq!(old, all);
    // one value by its place, which can be past the column
    for i in some_places(rng, len) {
        assert_eq!(new_item(p.get_prefixed(i)), item(i));
        assert_eq!(old_item(c.get(i)), item(i));
    }
    // an iterator of a range, moved about
    for _ in 0..4 {
        let start = rng.below(len + 1);
        let end = start + rng.below(len - start + 1);
        let mut x = p.prefix_iter_range(start, end);
        let mut y = c.iter_range(start..end);
        let mut m = SumIter {
            model,
            before: &before,
            pos: start,
            end,
            base: 0,
        };
        assert_eq!(new_place(&x), m.place());
        assert_eq!(old_place(&y), m.place());
        for _ in 0..16 {
            match rng.below(12) {
                0..=2 => {
                    let want = m.nth(0);
                    assert_eq!(new_item(x.next()), want);
                    assert_eq!(old_item(y.next()), want);
                }
                3 | 4 => {
                    let n = match rng.below(4) {
                        0 => a_place(rng, len),
                        _ => rng.below(m.left() + 2),
                    };
                    let want = m.nth(n);
                    assert_eq!(new_item(x.nth(n)), want);
                    assert_eq!(old_item(y.nth(n)), want);
                }
                5 => {
                    let end = a_place(rng, len);
                    x.set_max(end);
                    y.set_max(end);
                    m.set_max(end);
                }
                6 => {
                    let amount = a_place(rng, len - m.pos);
                    x.advance_by(amount);
                    y.advance_by(amount);
                    m.advance_by(amount);
                }
                7 => {
                    let target = a_place(rng, len);
                    x.advance_to(target);
                    y.advance_to(target);
                    m.advance_to(target);
                }
                8 => {
                    // any range: one that starts behind the iterator, one
                    // that ends before it starts
                    let range = a_place(rng, len)..a_place(rng, len);
                    x.shift(range.clone());
                    y.shift(range.clone());
                    m.set_max(range.end);
                    m.advance_to(range.start);
                }
                9 | 10 => {
                    // a range that starts at the iterator or after it
                    let start = m.pos + a_place(rng, len - m.pos).min(usize::MAX - m.pos);
                    let range = start..a_place(rng, len);
                    m.set_max(range.end);
                    let want = m.nth(range.start - m.pos);
                    assert_eq!(new_item(x.shift_next(range.clone())), want);
                    assert_eq!(old_item(y.shift_next(range)), want);
                }
                _ => {
                    x.reset_prefix();
                    y.reset_prefix();
                    m.base = m.pos;
                }
            }
            assert_eq!(new_place(&x), m.place());
            assert_eq!(old_place(&y), m.place());
            assert_eq!(x.size_hint(), (m.left(), Some(m.left())));
        }
        // what it has left
        let rest: Vec<Item<T>> = x.map(|pv| new_item(Some(pv))).collect();
        let old_rest: Vec<Item<T>> = y.map(|pv| old_item(Some(pv))).collect();
        let mut want = vec![];
        while let Some(v) = m.nth(0) {
            want.push(Some(v));
        }
        assert_eq!(rest, want);
        assert_eq!(old_rest, want);
    }
}

fn sum_iters<T: Totalled>() {
    for seed in 1..=8u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0x50f8);
        let budget = [2, 3, 4, 8, 64][rng.below(5)];
        let mut p = VerifiedPrefixColumn::<T>::with_max_segments(budget);
        let mut c = PrefixColumn::<T>::with_max_segments(budget);
        let mut model: Vec<T> = vec![];
        check_prefix_iters(&mut rng, &p, &c, &model);
        for _ in 0..40 {
            let at = rng.below(model.len() + 1);
            let del = rng.below((model.len() - at).min(12) + 1);
            let ins: Vec<T> = rng.values(40);
            p.splice(at, del, &lent(&ins));
            c.splice(at, del, ins.iter().cloned());
            model.splice(at..at + del, ins);
            check_prefix_iters(&mut rng, &p, &c, &model);
        }
        // loaded: an index made at once of all the slabs
        let p2 = VerifiedPrefixColumn::<T>::load(&p.save()).unwrap();
        let c2 = PrefixColumn::<T>::load(&c.save()).unwrap();
        check_prefix_iters(&mut rng, &p2, &c2, &model);
    }
}

/// A prefix column of the verified API is read with its sums
/// ([`VerifiedPrefixIter`]) as its specification says and as
/// `PrefixColumn` is: each value comes with what the values before it
/// and through it add up to, and the iterator's running sum is of the
/// values it has passed since its sum was last started, whether it read
/// them or stepped over them, in a window that is moved, widened past
/// the column, or closed. Of every summed type, after edits and after a
/// load.
#[test]
fn a_prefix_column_is_read_with_its_sums() {
    for_each_sum!(sum_iters);
}

/// The iterator of a short prefix column, by hand.
// (`clone` is here for what it does)
#[allow(clippy::clone_on_copy)]
#[test]
fn the_iterator_of_a_prefix_column() {
    let p = VerifiedPrefixColumn::<u64>::from_values(&[3, 0, 4, 4, 10]);
    let got: Vec<(u128, u64, u128)> = p
        .prefix_iter()
        .map(|pv| (pv.prefix(), pv.value, pv.total()))
        .collect();
    assert_eq!(
        got,
        [(0, 3, 3), (3, 0, 3), (3, 4, 7), (7, 4, 11), (11, 10, 21)]
    );
    // a range: the sum counts from the column's start
    let mut it = p.prefix_iter_range(1, 4);
    assert_eq!((it.pos(), it.end_pos(), it.len(), it.total()), (1, 4, 3, 3));
    let pv = it.next().unwrap();
    assert_eq!(pv, VerifiedPrefixedValue { value: 0, total: 3 });
    assert_eq!(
        format!("{pv:?}"),
        "VerifiedPrefixedValue { value: 0, total: 3 }"
    );
    let copy = pv;
    assert_eq!((copy.prefix(), copy.total(), pv.clone().total()), (3, 3, 3));
    // started again, it counts from where the iterator stands
    it.reset_prefix();
    assert_eq!((it.pos(), it.total(), it.prefix()), (2, 0, 0));
    let pv = it.nth(1).unwrap();
    assert_eq!((pv.prefix(), pv.value, pv.total()), (4, 4, 8));
    assert!(it.next().is_none());
    assert_eq!((it.pos(), it.end_pos(), it.total()), (4, 4, 8));
    // a window past the column closes at the column's end
    let mut it = p.prefix_iter();
    it.set_max(9);
    assert_eq!((it.end_pos(), it.items_left(), it.len()), (9, 9, 5));
    it.advance_by(7);
    assert_eq!((it.pos(), it.end_pos(), it.total()), (5, 5, 21));
    // moved to a window, and to one that starts behind it
    let mut it = p.prefix_iter();
    it.shift(2..4);
    assert_eq!((it.pos(), it.end_pos(), it.total()), (2, 4, 3));
    it.shift(0..5);
    assert_eq!((it.pos(), it.end_pos(), it.total()), (2, 5, 3));
    let pv = it.shift_next(3..5).unwrap();
    assert_eq!((pv.prefix(), pv.value, pv.total()), (7, 4, 11));
    assert_eq!((it.pos(), it.end_pos(), it.total()), (4, 5, 11));
    assert!(it.shift_next(5..5).is_none());
    assert_eq!((it.pos(), it.end_pos(), it.total()), (5, 5, 21));
    // one value by its place
    let pv = p.get_prefixed(4).unwrap();
    assert_eq!((pv.prefix(), pv.value, pv.total()), (11, 10, 21));
    assert!(p.get_prefixed(5).is_none());
    // signed sums, and nulls, which add nothing
    let p = VerifiedPrefixColumn::<Option<i64>>::from_values(&[Some(4), None, Some(-9)]);
    let got: Vec<(i128, Option<i64>, i128)> = p
        .prefix_iter()
        .map(|pv| (pv.prefix(), pv.value, pv.total()))
        .collect();
    assert_eq!(got, [(0, Some(4), 4), (4, None, 4), (4, Some(-9), -5)]);
    // flags: how many are set
    let p = VerifiedPrefixColumn::<bool>::from_values(&[true, false, true]);
    let got: Vec<(bool, usize)> = p.prefix_iter().map(|pv| (pv.value, pv.total())).collect();
    assert_eq!(got, [(true, 1), (false, 1), (true, 2)]);
    // a column of nothing
    let none = VerifiedPrefixColumn::<u32>::new();
    let mut it = none.prefix_iter();
    assert!(it.next().is_none() && it.nth(3).is_none());
    assert_eq!((it.pos(), it.end_pos(), it.total()), (0, 0, 0));
    assert!(none.get_prefixed(0).is_none());
}

/// Sums as far as a type's go, read by the iterator: a long run is
/// stepped over by its count, and the sum of what is passed is exact.
///
/// (The columns here are longer than a 32-bit `usize` counts.)
#[cfg(target_pointer_width = "64")]
#[test]
fn the_iterator_over_long_runs() {
    let long = i64::MAX as usize;
    let each = u128::from(u64::MAX);
    let p = VerifiedPrefixColumn::<u64>::fill(long, u64::MAX);
    let mut it = p.prefix_iter();
    let pv = it.nth(long - 2).unwrap();
    assert_eq!(pv.prefix(), (long as u128 - 2) * each);
    assert_eq!(pv.total(), (long as u128 - 1) * each);
    let pv = it.next().unwrap();
    assert_eq!(pv.total(), long as u128 * each);
    assert!(it.next().is_none());
    assert_eq!((it.pos(), it.total()), (long, long as u128 * each));
    // the widest sums of the signed type, from the middle on
    let p = VerifiedPrefixColumn::<i64>::fill(long, i64::MIN);
    let half = long / 2;
    let mut it = p.prefix_iter_range(half, long);
    assert_eq!(it.total(), half as i128 * i128::from(i64::MIN));
    it.reset_prefix();
    it.advance_to(long);
    assert_eq!(it.total(), (long - half) as i128 * i128::from(i64::MIN));

    // in slabs of two runs, against `PrefixColumn`'s iterator
    let run = |value: u32, count: usize| Run { count, value };
    let mut p = VerifiedPrefixColumn::<u32>::with_max_segments(2);
    let mut c = PrefixColumn::<u32>::with_max_segments(2);
    for (at, runs) in [
        (0, [run(u32::MAX, 1 << 30), run(7, 1 << 40)]),
        (1 << 20, [run(0, 1 << 50), run(u32::MAX - 1, 1 << 30)]),
        (5, [run(1, 3), run(u32::MAX, 1)]),
    ] {
        p.splice_runs(at, 0, runs);
        c.splice_runs(at, 0, runs);
    }
    let len = p.len();
    let mut rng = Rng(0x50f9);
    let mut places = some_places(&mut rng, len);
    places.extend([5, 9, 1 << 20, (1 << 20) + (1 << 50), (1 << 30) + 4, len - 1]);
    places.sort();
    places.dedup();
    let mut x = p.prefix_iter();
    let mut y = c.iter();
    for &i in &places {
        x.advance_to(i);
        y.advance_to(i);
        assert_eq!(new_place(&x), old_place(&y));
        assert_eq!(x.total(), p.get_prefix(i));
        assert_eq!(new_item(x.next()), old_item(y.next()));
    }
}

/// A run with the sums of its last value: before that value, and
/// through it.
type RunOf<T> = Option<(usize, T, i128, i128)>;

fn new_sum_run<T: Totalled>(run: Option<Run<VerifiedPrefixedValue<'_, T>>>) -> RunOf<T> {
    run.map(|run| {
        (
            run.count,
            T::keep(run.value.value),
            T::new_sum(run.value.prefix()),
            T::new_sum(run.value.total()),
        )
    })
}

fn old_sum_run<T: Totalled>(run: Option<Run<PrefixedValue<'_, T>>>) -> RunOf<T> {
    run.map(|run| {
        (
            run.count,
            old(run.value.value),
            T::old_sum(run.value.prefix()),
            T::old_sum(run.value.total()),
        )
    })
}

/// A value found: where it is, what the values passed over before it add
/// up to, and the value with its sums.
type Found<T> = Option<(usize, i128, (T, i128, i128))>;

fn new_found<T: Totalled>(seek: Option<VerifiedPrefixSeek<'_, T>>) -> Found<T> {
    seek.map(|seek| {
        (
            seek.pos,
            T::new_sum(seek.delta),
            new_item(Some(seek.pv)).unwrap(),
        )
    })
}

fn old_found<T: Totalled>(seek: Option<PrefixSeek<'_, T>>) -> Found<T> {
    seek.map(|seek| {
        (
            seek.pos,
            T::old_sum(seek.delta),
            old_item(Some(seek.pv)).unwrap(),
        )
    })
}

impl<T: Totalled> SumIter<'_, T> {
    fn next_run(&mut self) -> RunOf<T> {
        let left = self.left();
        if left == 0 {
            // its window ends where it stands
            self.end = self.pos;
            return None;
        }
        let v = self.model[self.pos].clone();
        let n = self.model[self.pos..self.pos + left]
            .iter()
            .take_while(|x| **x == v)
            .count();
        self.pos += n;
        Some((n, v, self.sum(self.pos - 1), self.sum(self.pos)))
    }

    fn delta_nth(&mut self, n: usize) -> Found<T> {
        let from = self.pos;
        let item = self.nth(n)?;
        let at = self.pos - 1;
        Some((at, self.before[at] - self.before[from], item))
    }

    /// On to the first value with which the values from here on add up
    /// to more than `n`.
    fn advance_prefix(&mut self, n: i128) -> Found<T> {
        let from = self.pos;
        let over = (0..self.left()).find(|k| self.before[from + k + 1] - self.before[from] > n);
        self.delta_nth(over.unwrap_or(usize::MAX))
    }
}

/// A prefix column's iterator read by runs, moved by a count with what
/// it passes over summed (`delta_nth`), copied, and stopped and started
/// again, and the column asked for the sum of a range before a value
/// (`delta`): as the specifications say, and as `PrefixColumn`'s
/// iterator does, the two being asked the same at each step and standing
/// in the same place after it.
fn check_prefix_runs<T: Totalled>(
    rng: &mut Rng,
    p: &VerifiedPrefixColumn<T>,
    c: &PrefixColumn<T>,
    model: &[T],
) {
    let len = model.len();
    let before = sums_before(model);
    // all of it by runs, each with the sums of its last value
    let (mut x, mut y) = (p.prefix_iter(), c.iter());
    let mut m = SumIter {
        model,
        before: &before,
        pos: 0,
        end: len,
        base: 0,
    };
    loop {
        let want = m.next_run();
        assert_eq!(new_sum_run(x.next_run()), want);
        assert_eq!(old_sum_run(y.next_run()), want);
        assert_eq!(new_place(&x), m.place());
        assert_eq!(old_place(&y), m.place());
        if want.is_none() {
            break;
        }
    }
    for _ in 0..3 {
        // the runs of a range without their sums: what `splice_runs`
        // takes
        let start = rng.below(len + 1);
        let end = start + rng.below(len - start + 1);
        let runs: Vec<(usize, T)> = p
            .prefix_iter_range(start, end)
            .runs()
            .map(|run| (run.count, T::keep(run.value)))
            .collect();
        let old_runs: Vec<(usize, T)> = c
            .iter_range(start..end)
            .runs()
            .map(|run| (run.count, old(run.value)))
            .collect();
        assert_eq!(run_model(&runs), model[start..end]);
        assert_eq!(runs, old_runs);
        assert!(runs.windows(2).all(|w| w[0].1 != w[1].1));
        let mut copy = VerifiedColumn::<T>::new();
        copy.splice_runs(0, 0, p.prefix_iter_range(start, end).runs());
        assert_eq!(new_values(&copy), model[start..end]);
    }
    // the value at a place, with the sum of a range before it
    for _ in 0..6 {
        let to = a_place(rng, len);
        let from = rng.below(to.min(len) + 1);
        let want = (to < len).then(|| (to, before[to] - before[from], item_at(model, &before, to)));
        assert_eq!(new_found(p.delta(from, to)), want);
        assert_eq!(old_found(c.delta(from, to)), want);
    }
    // an iterator of a range, moved about
    for _ in 0..4 {
        let start = rng.below(len + 1);
        let end = start + rng.below(len - start + 1);
        let mut x = p.prefix_iter_range(start, end);
        let mut y = c.iter_range(start..end);
        let mut m = SumIter {
            model,
            before: &before,
            pos: start,
            end,
            base: 0,
        };
        for _ in 0..16 {
            match rng.below(11) {
                0 | 1 => {
                    let want = m.next_run();
                    assert_eq!(new_sum_run(x.next_run()), want);
                    assert_eq!(old_sum_run(y.next_run()), want);
                }
                2 | 3 => {
                    let n = match rng.below(4) {
                        0 => a_place(rng, len),
                        _ => rng.below(m.left() + 2),
                    };
                    let want = m.delta_nth(n);
                    assert_eq!(new_found(x.delta_nth(n)), want);
                    assert_eq!(old_found(y.delta_nth(n)), want);
                }
                4 => {
                    let want = m.nth(0);
                    assert_eq!(new_item(x.next()), want);
                    assert_eq!(old_item(y.next()), want);
                }
                5 => {
                    let end = a_place(rng, len);
                    x.set_max(end);
                    y.set_max(end);
                    m.set_max(end);
                }
                6 => {
                    x.reset_prefix();
                    y.reset_prefix();
                    m.base = m.pos;
                }
                7 | 8 => {
                    // stopped, and started again on the same column: at
                    // the column's end the window can come back closed
                    let (s, t) = (x.suspend(), y.suspend());
                    x = s.try_resume(p).unwrap();
                    y = t.try_resume(c).unwrap();
                    if m.pos == len && x.end_pos() == len {
                        m.end = len;
                    }
                    // and a state starts as often as it is asked
                    assert_eq!(new_place(&s.try_resume(p).unwrap()), new_place(&x));
                }
                9 => {
                    // a copy is an iterator of its own
                    let (mut x2, mut y2) = (x.clone(), y.clone());
                    assert_eq!(new_place(&x2), m.place());
                    assert_eq!(new_item(x2.nth(1)), old_item(y2.nth(1)));
                    assert_eq!(new_place(&x2), old_place(&y2));
                }
                _ => {
                    let target = a_place(rng, len);
                    x.advance_to(target);
                    y.advance_to(target);
                    m.advance_to(target);
                }
            }
            assert_eq!(new_place(&x), m.place());
            assert_eq!(old_place(&y), m.place());
        }
    }
}

/// The value at `i` with the sums before it and through it.
fn item_at<T: Totalled>(model: &[T], before: &[i128], i: usize) -> (T, i128, i128) {
    (model[i].clone(), before[i], before[i + 1])
}

/// A prefix column of the two APIs, of the same values in the same
/// slabs, under edits that put in single values and runs: `check` is
/// asked of them after each edit, and of the two loaded from their
/// bytes.
fn under_sum_edits<T: Totalled>(
    salt: u64,
    check: fn(&mut Rng, &VerifiedPrefixColumn<T>, &PrefixColumn<T>, &[T]),
) {
    for seed in 1..=8u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ salt);
        let budget = [2, 3, 4, 8, 64][rng.below(5)];
        let mut p = VerifiedPrefixColumn::<T>::with_max_segments(budget);
        let mut c = PrefixColumn::<T>::with_max_segments(budget);
        let mut model: Vec<T> = vec![];
        check(&mut rng, &p, &c, &model);
        for _ in 0..40 {
            let at = rng.below(model.len() + 1);
            let del = rng.below((model.len() - at).min(12) + 1);
            if rng.below(3) == 0 {
                let runs = some_runs::<T>(&mut rng, 6);
                let lent_runs = runs.iter().map(|(n, x)| Run {
                    count: *n,
                    value: x.lend(),
                });
                p.splice_runs(at, del, lent_runs);
                let owned_runs = runs.iter().map(|(n, x)| Run {
                    count: *n,
                    value: x.clone(),
                });
                c.splice_runs(at, del, owned_runs);
                model.splice(at..at + del, run_model(&runs));
            } else {
                let ins: Vec<T> = rng.values(40);
                p.splice(at, del, &lent(&ins));
                c.splice(at, del, ins.iter().cloned());
                model.splice(at..at + del, ins);
            }
            check(&mut rng, &p, &c, &model);
        }
        // loaded: an index made at once of all the slabs
        let p2 = VerifiedPrefixColumn::<T>::load(&p.save()).unwrap();
        let c2 = PrefixColumn::<T>::load(&c.save()).unwrap();
        check(&mut rng, &p2, &c2, &model);
    }
}

fn sum_runs<T: Totalled>() {
    under_sum_edits::<T>(0x50fa, check_prefix_runs::<T>);
}

/// A prefix column of the verified API is read by runs with their sums
/// ([`VerifiedPrefixIter::next_run`]), moved by a count with what is
/// passed over summed ([`VerifiedPrefixIter::delta_nth`],
/// [`VerifiedColumn::delta`]), copied, and stopped and started again
/// ([`VerifiedPrefixIter::suspend`],
/// [`VerifiedPrefixIterState::try_resume`]) as its specification says
/// and as `PrefixColumn` is. Of every summed type, after edits and after
/// a load.
#[test]
fn a_prefix_column_is_read_by_runs_with_its_sums() {
    for_each_sum!(sum_runs);
}

/// A value type whose prefix iterators, of both APIs, read on until
/// their sum is past a limit.
trait Sought: Counted {
    fn new_sought(it: &mut VerifiedPrefixIter<'_, Self>, n: u128) -> Found<Self>;

    fn old_sought(it: &mut PrefixIter<'_, Self>, n: u128) -> Found<Self>;
}

macro_rules! sought {
    ($($t:ty => $s:ty),*) => {
        $(
            impl Sought for $t {
                fn new_sought(it: &mut VerifiedPrefixIter<'_, $t>, n: u128) -> Found<$t> {
                    new_found(it.advance_prefix(<$s as Target>::target(n)))
                }

                fn old_sought(it: &mut PrefixIter<'_, $t>, n: u128) -> Found<$t> {
                    old_found(it.advance_prefix(<$s as Target>::target(n)))
                }
            }
        )*
    };
}

sought!(
    u64 => u128,
    u32 => u64,
    NonZeroU32 => u64,
    bool => usize,
    Option<u64> => u128,
    Option<u32> => u64,
    Option<NonZeroU32> => u64
);

/// A prefix column's iterator reads on until the values it has given
/// from where it stood add up to more than a limit
/// (`advance_prefix`), among its other moves: it finds what a walk of
/// the sums finds, and what `PrefixColumn`'s iterator finds, and the
/// two stand in the same place after each step.
fn check_prefix_seeks<T: Sought>(
    rng: &mut Rng,
    p: &VerifiedPrefixColumn<T>,
    c: &PrefixColumn<T>,
    model: &[T],
) {
    let len = model.len();
    let before = sums_before(model);
    for _ in 0..4 {
        let start = rng.below(len + 1);
        let end = start + rng.below(len - start + 1);
        let mut x = p.prefix_iter_range(start, end);
        let mut y = c.iter_range(start..end);
        let mut m = SumIter {
            model,
            before: &before,
            pos: start,
            end,
            base: 0,
        };
        for _ in 0..12 {
            match rng.below(8) {
                0..=3 => {
                    // a sum the values left reach, those beside it, and
                    // one past them all
                    let reach = before[m.pos + rng.below(m.left() + 1)] - before[m.pos];
                    let all = before[m.pos + m.left()] - before[m.pos];
                    let n = [0, (reach - 1).max(0), reach, reach + 1, all + 3][rng.below(5)];
                    let want = m.advance_prefix(n);
                    let n = u128::try_from(n).unwrap();
                    assert_eq!(T::new_sought(&mut x, n), want);
                    assert_eq!(T::old_sought(&mut y, n), want);
                }
                4 => {
                    let want = m.nth(0);
                    assert_eq!(new_item(x.next()), want);
                    assert_eq!(old_item(y.next()), want);
                }
                5 => {
                    x.reset_prefix();
                    y.reset_prefix();
                    m.base = m.pos;
                }
                6 => {
                    let want = m.next_run();
                    assert_eq!(new_sum_run(x.next_run()), want);
                    assert_eq!(old_sum_run(y.next_run()), want);
                }
                _ => {
                    let end = a_place(rng, len);
                    x.set_max(end);
                    y.set_max(end);
                    m.set_max(end);
                }
            }
            assert_eq!(new_place(&x), m.place());
            assert_eq!(old_place(&y), m.place());
        }
    }
}

fn sum_seeks<T: Sought>() {
    under_sum_edits::<T>(0x50fb, check_prefix_seeks::<T>);
}

/// A prefix column of the verified API whose sums are counts is read on
/// to a sum ([`VerifiedPrefixIter::advance_prefix`]) as its
/// specification says and as `PrefixColumn` is: the value found is the
/// first with which the values from where the iterator stood add up to
/// more than the limit, and with none found the iterator has given all
/// it had.
#[test]
fn a_prefix_iterator_reads_on_to_a_sum() {
    sum_seeks::<u64>();
    sum_seeks::<u32>();
    sum_seeks::<NonZeroU32>();
    sum_seeks::<bool>();
    sum_seeks::<Option<u64>>();
    sum_seeks::<Option<u32>>();
    sum_seeks::<Option<NonZeroU32>>();
}

/// The runs, seeks and states of a short prefix column's iterator, by
/// hand.
// (`clone` is here for what it does)
#[allow(clippy::clone_on_copy)]
#[test]
fn the_seeks_of_a_prefix_iterator() {
    let mut p = VerifiedPrefixColumn::<u64>::from_values(&[3, 0, 4, 4, 10]);
    // by runs: the sums are those of each run's last value
    let mut it = p.prefix_iter();
    let mut runs = vec![];
    while let Some(run) = it.next_run() {
        runs.push((
            run.count,
            run.value.value,
            run.value.prefix(),
            run.value.total(),
        ));
    }
    assert_eq!(
        runs,
        [(1, 3, 0, 3), (1, 0, 3, 3), (2, 4, 7, 11), (1, 10, 11, 21)]
    );
    assert_eq!(
        format!("{it:?}"),
        "VerifiedPrefixIter { total: 21, pos: 5 }"
    );
    // the runs of a window, without sums
    let runs: Vec<(usize, u64)> = p
        .prefix_iter_range(1, 4)
        .runs()
        .map(|run| (run.count, run.value))
        .collect();
    assert_eq!(runs, [(1, 0), (2, 4)]);
    // a value by its count from the iterator, with what is passed over
    let mut it = p.prefix_iter_range(1, 5);
    let seek = it.delta_nth(2).unwrap();
    let found = VerifiedPrefixSeek {
        pos: 3,
        delta: 4,
        pv: VerifiedPrefixedValue {
            value: 4,
            total: 11,
        },
    };
    assert_eq!(seek, found);
    assert_eq!(
        format!("{seek:?}"),
        "VerifiedPrefixSeek { pos: 3, delta: 4, pv: VerifiedPrefixedValue { value: 4, total: 11 } }"
    );
    let copy = seek;
    assert_eq!((copy.clone(), seek.pv.prefix()), (found, 7));
    assert!(it.delta_nth(1).is_none());
    assert_eq!((it.pos(), it.end_pos(), it.total()), (5, 5, 21));
    // the same of the column: the value at a place, and the sum of a
    // range before it
    assert_eq!(p.delta(1, 3), Some(found));
    let seek = p.delta(2, 2).unwrap();
    assert_eq!((seek.pos, seek.delta, seek.pv.total()), (2, 0, 7));
    assert!(p.delta(0, 5).is_none());
    // on to a sum: the first value with which the sum is past the limit
    let mut it = p.prefix_iter();
    let seek = it.advance_prefix(3).unwrap();
    assert_eq!(
        (seek.pos, seek.delta, seek.pv.value, seek.pv.total()),
        (2, 3, 4, 7)
    );
    // from where the iterator stands, whatever its sum counts from
    let seek = it.advance_prefix(4).unwrap();
    assert_eq!(
        (seek.pos, seek.delta, seek.pv.value, seek.pv.total()),
        (4, 4, 10, 21)
    );
    assert!(it.advance_prefix(0).is_none());
    assert_eq!((it.pos(), it.end_pos(), it.total()), (5, 5, 21));
    // values of one unit each: a limit of `n` units finds the value
    // that holds unit `n + 1`
    let units = VerifiedPrefixColumn::<u64>::from_values(&[1, 1, 1]);
    for n in 0..3 {
        let seek = units.prefix_iter().advance_prefix(n as u128).unwrap();
        assert_eq!((seek.pos, seek.delta), (n, n as u128));
    }
    assert!(units.prefix_iter().advance_prefix(3).is_none());
    // within a window, all of whose values add up to the limit or less
    let mut it = p.prefix_iter_range(0, 3);
    assert!(it.advance_prefix(7).is_none());
    assert_eq!((it.pos(), it.end_pos(), it.total()), (3, 3, 7));
    // flags: on to the second that is set
    let flags = VerifiedPrefixColumn::<bool>::from_values(&[false, true, false, true, true]);
    let seek = flags.prefix_iter().advance_prefix(1).unwrap();
    assert_eq!((seek.pos, seek.delta, seek.pv.total()), (3, 1, 2));

    // a copy is an iterator of its own
    let mut it = p.prefix_iter_range(1, 4);
    it.next();
    it.reset_prefix();
    let mut twin = it.clone();
    assert_eq!(twin.next().map(|pv| pv.total()), Some(4));
    assert_eq!((it.pos(), it.total(), twin.pos()), (2, 0, 3));
    // stopped and started again: its place, its window, and where its
    // sum counts from
    it.next();
    let state: VerifiedPrefixIterState<u64> = it.suspend();
    let mut on = state.try_resume(&p).unwrap();
    assert_eq!((on.pos(), on.end_pos(), on.total()), (3, 4, 4));
    let pv = on.next().unwrap();
    assert_eq!((pv.prefix(), pv.value, pv.total()), (4, 4, 8));
    // on another column of that many edits, with that column's sums
    let other = VerifiedPrefixColumn::<u64>::from_values(&[1, 2, 3, 4, 5, 6]);
    let on = state.try_resume(&other).unwrap();
    assert_eq!((on.pos(), on.end_pos(), on.total()), (3, 4, 3));
    // and not on a column edited since
    p.push(1);
    assert!(matches!(
        state.try_resume(&p),
        Err(PackError::InvalidResume)
    ));
    // a state is plain data, with no borrow in it
    fn plain<S: Send + Sync + 'static>() {}
    plain::<VerifiedPrefixIterState<Option<i64>>>();

    // an iterator of no column
    let mut it = VerifiedPrefixIter::<u64>::default();
    assert_eq!((it.pos(), it.end_pos(), it.len(), it.total()), (0, 0, 0, 0));
    assert!(it.next_run().is_none() && it.delta_nth(0).is_none() && it.next().is_none());
    assert_eq!(it.clone().runs().count(), 0);
    assert_eq!(format!("{it:?}"), "VerifiedPrefixIter { total: 0, pos: 0 }");
    // it has no index to ask: it finds nothing, and its window is
    // closed (that of `PrefixColumn`'s is left as it was)
    it.set_max(5);
    assert!(it.advance_prefix(0).is_none());
    assert_eq!((it.pos(), it.end_pos()), (0, 0));
    let mut old = PrefixIter::<u64>::default();
    old.set_max(5);
    assert!(old.advance_prefix(0).is_none());
    assert_eq!((old.pos(), old.end_pos()), (0, 5));
    // its state starts on a new column
    let state = VerifiedPrefixIter::<u64>::default().suspend();
    let none = VerifiedPrefixColumn::<u64>::new();
    let on = state.try_resume(&none).unwrap();
    assert_eq!((on.pos(), on.end_pos(), on.total()), (0, 0, 0));
    assert!(state.try_resume(&p).is_err());
}

/// A value type of a client's own that a prefix column adds up, as a
/// client compiled without Verus writes it: one number that says what a
/// value is, in its low four bits, and how long it is, in the bits above
/// them. A prefix column of them adds up the lengths in a `u64`, which
/// sixteen lengths cannot overflow: a column of no more values is not
/// checked, and a longer one is.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Meta(u64);

impl Meta {
    /// The longest a value can be.
    const LONGEST: u64 = (1 << 60) - 1;

    fn of(kind: u64, len: u64) -> Meta {
        assert!(kind < 16 && len <= Meta::LONGEST);
        Meta(len << 4 | kind)
    }

    fn length(self) -> u64 {
        self.0 >> 4
    }
}

impl VNewtype for Meta {
    type Base = u64;

    fn to_base(self) -> u64 {
        self.0
    }

    fn from_base(b: u64) -> Result<Meta, PackError> {
        Ok(Meta(b))
    }
}

impl VAmount for Meta {
    type Sum = u64;

    fn room_len() -> usize {
        16
    }

    fn part_of(v: Meta) -> u64 {
        v.length()
    }
}

// the same type for `Column`
impl ColumnValue for Meta {
    type Encoding<C: Codec> = RleEncoding<Meta, C>;
}

impl RleValue for Meta {
    fn try_unpack<C: Codec>(data: &[u8]) -> Result<(usize, Meta), PackError> {
        let (n, b) = C::try_read_unsigned(data)?;
        Ok((n, Meta(b)))
    }

    fn pack<C: Codec>(value: Meta, out: &mut Vec<u8>) -> bool {
        out.extend(C::encode_unsigned(value.0));
        true
    }
}

/// `*target += len * count`, unless that overflows a `u64`. (`len` is
/// below 2^60, so the product fits a `u128`.)
fn try_add_lengths(target: &mut u64, len: u64, count: usize) -> bool {
    let sum = u128::from(*target) + u128::from(len) * count as u128;
    match u64::try_from(sum) {
        Ok(sum) => {
            *target = sum;
            true
        }
        Err(_) => false,
    }
}

// and for `PrefixColumn`: the same sums, checked for the same columns
impl PrefixValue for Meta {
    type Prefix = u64;

    fn accumulate(target: &mut u64, val: Meta) {
        *target += val.length();
    }

    fn accumulate_run(target: &mut u64, run: &Run<Meta>) {
        *target += run.value.length() * run.count as u64;
    }

    const MAX_UNCHECKED_LEN: usize = 16;

    fn try_accumulate_run(target: &mut u64, run: &Run<Meta>) -> bool {
        try_add_lengths(target, run.value.length(), run.count)
    }

    fn try_add_prefix(total: &mut u64, part: &u64) -> bool {
        try_add_lengths(total, *part, 1)
    }
}

// short values of every kind: the sums of these tests are far from a
// `u64`'s end
plain_kind!(Meta, |rng| {
    Meta::of(rng.below(16) as u64, rng.below(40) as u64)
});

impl Adds for Meta {
    fn adds(&self) -> i128 {
        i128::from(self.length())
    }
}

counted!(Meta => u64, Option<Meta> => u64);

sought!(Meta => u64, Option<Meta> => u64);

/// A prefix column of a client's own type is, in every respect these
/// tests look at, what a prefix column of the types hexane gives is,
/// and what `PrefixColumn` of the client's type is: under edits, in its
/// other forms, in what it answers from its sums, searched by sum, and
/// read by its iterator, by runs and on to a sum. So is one of the
/// type's nulls.
#[test]
fn a_prefix_column_of_a_clients_own_type() {
    macro_rules! each {
        ($($f:ident),*) => {
            $(
                $f::<Meta>();
                $f::<Option<Meta>>();
            )*
        };
    }
    each!(
        sum_edits,
        sum_forms,
        sum_searches,
        sum_iters,
        sum_runs,
        sum_seeks
    );
}

/// The sums of a client's type are what the client says its values add:
/// here the lengths, and nothing for what kind a value is of.
#[test]
fn a_clients_values_add_what_it_says() {
    let model = [
        Meta::of(3, 5),
        Meta::of(0, 0),
        Meta::of(15, 7),
        Meta::of(15, 7),
        Meta::of(1, 1 << 40),
    ];
    let p = VerifiedPrefixColumn::<Meta>::from_values(&model);
    let c: PrefixColumn<Meta> = model.iter().copied().collect();
    assert_eq!(p.save(), c.save());
    assert_eq!(p.to_vec(), model);
    let sums = [0, 5, 5, 12, 19, 19 + (1 << 40)];
    for (i, &sum) in sums.iter().enumerate() {
        assert_eq!(p.get_prefix(i), sum);
        assert_eq!(c.get_prefix(i), sum);
    }
    assert_eq!(p.get_total(2), 12);
    assert_eq!(p.sum_range(1, 4), 14);
    assert_eq!(c.sum_range(1..4), 14);
    // the value in which a place of the whole lies
    assert_eq!(p.get_index_for_total(1), 0);
    assert_eq!(p.get_index_for_total(5), 0);
    assert_eq!(p.get_index_for_total(6), 2);
    assert_eq!(p.get_index_for_total(13), 3);
    assert_eq!(p.get_index_for_total(20), 4);
    assert_eq!(p.get_index_for_total(20 + (1 << 40)), 5);
    for target in [0, 1, 5, 6, 12, 13, 19, 20, 19 + (1 << 40), 20 + (1 << 40)] {
        assert_eq!(p.get_index_for_total(target), c.get_index_for_total(target));
        assert_eq!(
            p.get_index_for_prefix(target),
            c.get_index_for_prefix(target)
        );
    }
    let mut it = p.prefix_iter();
    let found = it.advance_prefix(12).unwrap();
    assert_eq!((found.pos, found.delta, found.pv.value), (3, 12, model[3]));
    assert_eq!((found.pv.prefix(), found.pv.total()), (12, 19));

    // nulls add nothing
    let nulls = [Some(model[0]), None, Some(model[2]), None, None];
    let p = VerifiedPrefixColumn::<Option<Meta>>::from_values(&nulls);
    let c: PrefixColumn<Option<Meta>> = nulls.iter().copied().collect();
    assert_eq!(p.save(), c.save());
    for (i, sum) in [0, 5, 5, 12, 12, 12].into_iter().enumerate() {
        assert_eq!(p.get_prefix(i), sum);
        assert_eq!(c.get_prefix(i), sum);
    }
}

/// A prefix column of a client's type is kept only of values whose sums
/// fit the type the client keeps them in: sixteen of the longest values
/// add up to sixteen short of a `u64`'s end. Bytes whose values add up
/// past it are a plain column's and no prefix column's, and the load
/// refuses them with the error of `PrefixColumn`'s.
#[test]
fn a_clients_sums_that_do_not_fit_are_refused() {
    fn loads<T: Summed>(bytes: &[u8]) -> Result<usize, String> {
        let new = VerifiedPrefixColumn::<T>::load(bytes).map(|p| (p.len(), p.save()));
        let old = PrefixColumn::<T>::load(bytes).map(|c| (c.len(), c.save()));
        let (new, old) = (
            new.map_err(|e| e.to_string()),
            old.map_err(|e| e.to_string()),
        );
        assert_eq!(new, old);
        // a plain column loads them either way
        let plain = VerifiedColumn::<T>::load(bytes).unwrap();
        assert_eq!(plain.save(), bytes);
        new.map(|(len, saved)| {
            assert_eq!(saved, bytes);
            assert_eq!(len, plain.len());
            len
        })
    }
    let refused = Err("invalid value: prefix sum overflows its type".to_string());
    let longest = Meta::of(9, Meta::LONGEST);

    let mut v = VerifiedColumn::<Meta>::fill(16, longest);
    assert_eq!(loads::<Meta>(&v.save()), Ok(16));
    v.push(Meta::of(0, 15));
    assert_eq!(loads::<Meta>(&v.save()), Ok(17));
    v.push(Meta::of(2, 0));
    assert_eq!(loads::<Meta>(&v.save()), Ok(18));
    v.push(Meta::of(0, 1));
    assert_eq!(loads::<Meta>(&v.save()), refused);
    v.remove(0);
    assert_eq!(loads::<Meta>(&v.save()), Ok(18));
    let v = VerifiedColumn::<Meta>::fill(17, longest);
    assert_eq!(loads::<Meta>(&v.save()), refused);
    let v = VerifiedColumn::<Meta>::fill(1 << 20, longest);
    assert_eq!(loads::<Meta>(&v.save()), refused);

    // in slabs whose sums each fit, while their total does not
    let run = |value: Meta, count: usize| Run { count, value };
    let mut v = VerifiedColumn::<Meta>::new();
    let runs = [
        run(longest, 9),
        run(Meta::of(0, 0), 1),
        run(Meta::of(9, Meta::LONGEST - 1), 8),
    ];
    v.splice_runs(0, 0, runs);
    let bytes = v.save();
    assert_eq!(loads::<Meta>(&bytes), refused);
    let opts = LoadOpts::new().with_max_segments(2);
    let new = VerifiedPrefixColumn::<Meta>::load_with(&bytes, opts);
    let old = PrefixColumn::<Meta>::load_with(&bytes, opts);
    assert_eq!(
        new.err().unwrap().to_string(),
        old.err().unwrap().to_string()
    );
    assert_eq!(
        VerifiedColumn::<Meta>::load_with(&bytes, opts)
            .unwrap()
            .slab_count(),
        3
    );

    // with nulls, which add nothing
    let mut v = VerifiedColumn::<Option<Meta>>::fill(16, Some(longest));
    v.splice_runs(
        7,
        0,
        [Run {
            count: 1000,
            value: None,
        }],
    );
    assert_eq!(loads::<Option<Meta>>(&v.save()), Ok(1016));
    v.push(Some(Meta::of(0, 16)));
    assert_eq!(loads::<Option<Meta>>(&v.save()), refused);
}

/// An edit that would leave a prefix column of a client's type with
/// sums that do not fit panics, as `PrefixColumn`'s does and with its
/// words; one whose sums fit goes through, also where the sum is the
/// largest of its type. (Where a panic can be caught: not on wasm32.)
#[cfg(panic = "unwind")]
#[test]
fn an_edit_past_a_clients_sums_panics() {
    type P<T> = VerifiedPrefixColumn<T>;
    let words = Some("prefix sum overflows its type".to_string());
    let panics = |f: &dyn Fn()| panic_of(std::panic::AssertUnwindSafe(f));
    let longest = Meta::of(9, Meta::LONGEST);
    let run = |value: Meta, count: usize| Run { count, value };

    let new = panics(&|| P::<Meta>::new().splice_runs(0, 0, [run(longest, 17)]));
    let old = panics(&|| PrefixColumn::<Meta>::new().splice_runs(0, 0, [run(longest, 17)]));
    assert_eq!((&new, &old), (&words, &words));
    assert_eq!(panics(&|| drop(P::<Meta>::fill(17, longest))), words);
    assert_eq!(
        panics(&|| drop(P::<Option<Meta>>::fill(1 << 20, Some(longest)))),
        words
    );

    // the largest sum, and one more: by each edit of one value
    let mut full = P::<Meta>::fill(16, longest);
    let mut old_full = PrefixColumn::<Meta>::from_column(Column::fill(16, longest));
    full.push(Meta::of(0, 15));
    old_full.push(Meta::of(0, 15));
    assert_eq!(full.get_prefix(17), u64::MAX);
    assert_eq!(old_full.get_prefix(17), u64::MAX);
    assert_eq!(full.save(), old_full.save());
    let one = Meta::of(4, 1);
    let none = Meta::of(4, 0);
    assert_eq!(panics(&|| full.clone().push(one)), words);
    assert_eq!(panics(&|| old_full.clone().push(one)), words);
    assert_eq!(panics(&|| full.clone().insert(5, one)), words);
    assert_eq!(panics(&|| old_full.clone().insert(5, one)), words);
    assert_eq!(
        panics(&|| full.clone().splice(17, 0, &[none, none, one])),
        words
    );
    assert_eq!(panics(&|| full.clone().extend([none, one])), words);
    assert_eq!(panics(&|| full.clone().remap(|_| longest)), words);
    let (mut p, mut c) = (full.clone(), old_full.clone());
    p.push(none);
    c.push(none);
    p.insert(5, none);
    c.insert(5, none);
    // room made by a deletion can be filled again
    p.remove(0);
    c.remove(0);
    p.push(longest);
    c.push(longest);
    assert_eq!(p.save(), c.save());
    assert_eq!(p.get_prefix(p.len()), u64::MAX);
    assert_eq!(panics(&|| p.clone().push(one)), words);
    assert_eq!(panics(&|| c.clone().push(one)), words);
    let loaded = P::<Meta>::load(&p.save()).unwrap();
    assert_eq!(panics(&|| loaded.clone().push(one)), words);

    // a plain column of the same edits takes them all
    let mut v = VerifiedColumn::<Meta>::fill(17, longest);
    v.push(one);
    assert_eq!(v.len(), 18);
}

/// An accumulator of a client's own, as a client compiled without Verus
/// writes it: which of 64 kinds of mark are open. A mark opens its kind,
/// or closes it where it is open, so adding is `^`, and so is taking
/// away.
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
struct Flags(u64);

// the laws are Verus's to check: a client compiled without it has them
// to keep by itself
impl VGroup for Flags {}

impl VAccum for Flags {
    fn new_zero() -> Flags {
        Flags(0)
    }

    fn add_acc(&mut self, b: &Flags) {
        self.0 ^= b.0;
    }

    fn sub_acc(&mut self, b: &Flags) {
        self.0 ^= b.0;
    }

    fn dup(&self) -> Flags {
        *self
    }

    // in a way of its own: twice over adds nothing
    fn add_times(&mut self, b: &Flags, n: usize) {
        if n % 2 == 1 {
            self.0 ^= b.0;
        }
    }
}

/// A value type of a client's own for an accumulator column to add up:
/// a mark, of the kind its number's last six bits say.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Mark(u64);

impl Mark {
    /// The flag of the mark's kind.
    fn flag(self) -> u64 {
        1 << (self.0 % 64)
    }
}

impl VNewtype for Mark {
    type Base = u64;

    fn to_base(self) -> u64 {
        self.0
    }

    fn from_base(b: u64) -> Result<Mark, PackError> {
        Ok(Mark(b))
    }
}

impl VAdds for Mark {
    type Acc = Flags;

    fn acc_of(v: Mark) -> Flags {
        Flags(v.flag())
    }
}

// the same type for `Column`
impl ColumnValue for Mark {
    type Encoding<C: Codec> = RleEncoding<Mark, C>;
}

impl RleValue for Mark {
    fn try_unpack<C: Codec>(data: &[u8]) -> Result<(usize, Mark), PackError> {
        let (n, b) = C::try_read_unsigned(data)?;
        Ok((n, Mark(b)))
    }

    fn pack<C: Codec>(value: Mark, out: &mut Vec<u8>) -> bool {
        out.extend(C::encode_unsigned(value.0));
        true
    }
}

// and for `PrefixColumn`, which asks std's operators of what it adds
// up in, and takes their laws on trust
#[allow(clippy::suspicious_arithmetic_impl)]
impl Add for Flags {
    type Output = Flags;

    fn add(self, b: Flags) -> Flags {
        Flags(self.0 ^ b.0)
    }
}

#[allow(clippy::suspicious_arithmetic_impl)]
impl Sub for Flags {
    type Output = Flags;

    fn sub(self, b: Flags) -> Flags {
        Flags(self.0 ^ b.0)
    }
}

#[allow(clippy::suspicious_op_assign_impl)]
impl AddAssign for Flags {
    fn add_assign(&mut self, b: Flags) {
        self.0 ^= b.0;
    }
}

#[allow(clippy::suspicious_op_assign_impl)]
impl SubAssign for Flags {
    fn sub_assign(&mut self, b: Flags) {
        self.0 ^= b.0;
    }
}

#[allow(clippy::suspicious_op_assign_impl)]
impl AddAssign<&Flags> for Flags {
    fn add_assign(&mut self, b: &Flags) {
        self.0 ^= b.0;
    }
}

impl PrefixValue for Mark {
    type Prefix = Flags;

    fn accumulate(target: &mut Flags, val: Mark) {
        target.0 ^= val.flag();
    }

    fn accumulate_run(target: &mut Flags, run: &Run<Mark>) {
        if run.count % 2 == 1 {
            target.0 ^= run.value.flag();
        }
    }
}

// two hundred marks of the 64 kinds, so that marks of a kind meet
plain_kind!(Mark, |rng| Mark(rng.below(200) as u64));

/// An accumulator in which the order matters: the lamps of a board, and
/// whether the board is turned round. A step changes the lamps of its
/// number as the board then lies, and an odd one turns the board round,
/// so two steps leave a board differently in either order.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Board {
    lit: u64,
    turned: bool,
}

/// The lamps `x` as they lie on a board that is turned round, or is
/// not.
fn across(turned: bool, x: u64) -> u64 {
    if turned {
        x.rotate_left(32)
    } else {
        x
    }
}

impl VGroup for Board {}

// `add_times` is left as the trait gives it
impl VAccum for Board {
    fn new_zero() -> Board {
        Board {
            lit: 0,
            turned: false,
        }
    }

    fn add_acc(&mut self, b: &Board) {
        self.lit ^= across(self.turned, b.lit);
        self.turned ^= b.turned;
    }

    // what is to be added to `b` for the board
    fn sub_acc(&mut self, b: &Board) {
        self.lit = across(b.turned, self.lit ^ b.lit);
        self.turned ^= b.turned;
    }

    fn dup(&self) -> Board {
        *self
    }
}

/// A value type for a column that adds up in a [`Board`]: a step, which
/// changes the lamps that half its number says and is odd where it
/// turns the board round.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Step(u64);

impl VNewtype for Step {
    type Base = u64;

    fn to_base(self) -> u64 {
        self.0
    }

    fn from_base(b: u64) -> Result<Step, PackError> {
        Ok(Step(b))
    }
}

impl VAdds for Step {
    type Acc = Board;

    fn acc_of(v: Step) -> Board {
        Board {
            lit: v.0 >> 1,
            turned: v.0 & 1 == 1,
        }
    }
}

impl ColumnValue for Step {
    type Encoding<C: Codec> = RleEncoding<Step, C>;
}

impl RleValue for Step {
    fn try_unpack<C: Codec>(data: &[u8]) -> Result<(usize, Step), PackError> {
        let (n, b) = C::try_read_unsigned(data)?;
        Ok((n, Step(b)))
    }

    fn pack<C: Codec>(value: Step, out: &mut Vec<u8>) -> bool {
        out.extend(C::encode_unsigned(value.0));
        true
    }
}

// lamps on both halves of the board, and every other step turning it
// round
plain_kind!(Step, |rng| {
    Step((rng.below(7) as u64) << 31 | rng.below(4) as u64)
});

/// What an accumulator column of `T` adds its values up in.
type Added<T> = <T as VAccumulated>::Acc;

/// A value type an accumulator column holds. The tests reckon by
/// themselves what values of it add up to, one value after another.
trait Accd: Kind + VAccumulated<Acc: PartialEq> {
    /// What the values add up to, in the order they are in.
    fn added(vs: &[Self]) -> Added<Self>;

    /// What runs of values add up to, for a type whose accumulator adds
    /// a run in a way of its own, so that a run of billions is added at
    /// once.
    fn added_runs(_runs: &[(usize, Self)]) -> Option<Added<Self>> {
        None
    }

    /// What `PrefixColumn` answers of the values, for a type the tests
    /// have a `PrefixColumn` of.
    fn check_old(_rng: &mut Rng, _p: &VerifiedAccColumn<Self>, _model: &[Self]) {}
}

impl Accd for Mark {
    fn added(vs: &[Mark]) -> Flags {
        Flags(vs.iter().fold(0, |open, m| open ^ m.flag()))
    }

    fn added_runs(runs: &[(usize, Mark)]) -> Option<Flags> {
        let odd = runs.iter().filter(|(n, _)| n % 2 == 1);
        Some(Flags(odd.fold(0, |open, (_, m)| open ^ m.flag())))
    }

    fn check_old(rng: &mut Rng, p: &VerifiedAccColumn<Mark>, model: &[Mark]) {
        check_old_acc(rng, p, model);
    }
}

impl Accd for Option<Mark> {
    fn added(vs: &[Option<Mark>]) -> Flags {
        Flags(vs.iter().flatten().fold(0, |open, m| open ^ m.flag()))
    }

    fn added_runs(runs: &[(usize, Option<Mark>)]) -> Option<Flags> {
        let odd = runs.iter().filter(|(n, _)| n % 2 == 1);
        let marks = odd.flat_map(|(_, m)| m);
        Some(Flags(marks.fold(0, |open, m| open ^ m.flag())))
    }

    fn check_old(rng: &mut Rng, p: &VerifiedAccColumn<Option<Mark>>, model: &[Option<Mark>]) {
        check_old_acc(rng, p, model);
    }
}

/// The board after the steps, taken one after another.
fn board_after<'a>(steps: impl Iterator<Item = &'a Step>) -> Board {
    let (mut lit, mut turned) = (0u64, false);
    for s in steps {
        let lamps = s.0 >> 1;
        lit ^= if turned { lamps.rotate_left(32) } else { lamps };
        turned ^= s.0 % 2 == 1;
    }
    Board { lit, turned }
}

impl Accd for Step {
    fn added(vs: &[Step]) -> Board {
        board_after(vs.iter())
    }
}

impl Accd for Option<Step> {
    fn added(vs: &[Option<Step>]) -> Board {
        board_after(vs.iter().flatten())
    }
}

/// The same checks for each value type of an accumulator column.
macro_rules! for_each_acc {
    ($f:ident) => {
        $f::<Mark>();
        $f::<Option<Mark>>();
        $f::<Step>();
        $f::<Option<Step>>();
    };
}

/// What an accumulator column holds.
fn acc_values<T: Accd>(p: &VerifiedAccColumn<T>) -> Vec<T> {
    p.to_vec().into_iter().map(T::keep).collect()
}

/// `check_invariants` says of an accumulator column what it says of the
/// plain column of the same edits: nothing, or the same words. (Where a
/// panic can be caught: not on wasm32.)
#[cfg(panic = "unwind")]
fn check_acc_invariants<T: Accd>(p: &VerifiedAccColumn<T>, v: &VerifiedColumn<T>) {
    let acc = panic_of(std::panic::AssertUnwindSafe(|| p.check_invariants()));
    let plain = panic_of(std::panic::AssertUnwindSafe(|| v.check_invariants()));
    assert_eq!(acc, plain);
}

#[cfg(not(panic = "unwind"))]
fn check_acc_invariants<T: Accd>(_p: &VerifiedAccColumn<T>, _v: &VerifiedColumn<T>) {}

/// An accumulator column is a column: it holds `model` and reads as the
/// plain column `v` of the same edits does, it lies in that column's
/// slabs, and it saves that column's bytes. `built` says that edits made
/// the columns of values, which the checks of `check_invariants` then
/// pass.
fn check_acc<T: Accd>(p: &VerifiedAccColumn<T>, v: &VerifiedColumn<T>, model: &[T], built: bool) {
    assert_eq!(p.len(), model.len());
    assert_eq!(p.is_empty(), model.is_empty());
    assert_eq!(acc_values(p), model);
    assert_eq!(new_values(v), model);
    let mut it = p.iter();
    for m in model {
        assert_eq!(it.next().map(T::keep).as_ref(), Some(m));
    }
    assert!(it.next().is_none());
    assert!(it.next().is_none());
    assert!(p.get(model.len()).is_none());
    assert!(p.get(usize::MAX).is_none());
    let bytes = p.save();
    assert_eq!(bytes, v.save());

    assert_eq!(p.slab_count(), v.slab_count());
    assert_eq!(p.slab_lens(), v.slab_lens());
    assert_eq!(p.slab_segments(), v.slab_segments());
    assert_eq!(format!("{p:?}"), format!("{v:?}"));
    if built {
        p.check_invariants();
    } else {
        check_acc_invariants(p, v);
    }
    assert!(p.validate_encoding().is_ok());

    let (mut a, mut b) = (vec![9u8; 3], vec![9u8; 3]);
    let range = p.save_to(&mut a);
    assert_eq!(range, v.save_to(&mut b));
    assert_eq!(a, b);
    assert_eq!(a[range], bytes[..]);
    for x in model.first().into_iter().chain(model.last()) {
        let only = model.iter().all(|m| m == x);
        assert_eq!(p.is_only(x.lend()), only);
        let (mut a, mut b) = (vec![9u8; 3], vec![9u8; 3]);
        let range = p.save_to_unless(&mut a, x.lend());
        assert_eq!(range, v.save_to_unless(&mut b, x.lend()));
        assert_eq!(a, b);
    }
}

/// What an accumulator column answers from its index is what its values
/// add up to in the order it holds them: those before a position and
/// those through one, at any position, and those of a range, whatever
/// its ends.
fn check_acc_reads<T: Accd>(rng: &mut Rng, p: &VerifiedAccColumn<T>, model: &[T]) {
    let len = model.len();
    let places = some_places(rng, len);
    for &i in &places {
        assert_eq!(p.get_prefix(i), T::added(&model[..i.min(len)]));
        let through = i.saturating_add(1).min(len);
        assert_eq!(p.get_total(i), T::added(&model[..through]));
    }
    for (n, &start) in places.iter().enumerate() {
        // each end with some of the starts
        for &end in places.iter().skip(n % 3).step_by(3) {
            let range = if start < end.min(len) {
                start..end.min(len)
            } else {
                0..0
            };
            assert_eq!(p.sum_range(start, end), T::added(&model[range]));
        }
    }
    assert_eq!(p.sum_range(0, len), T::added(model));
}

/// `PrefixColumn` of the same values, with an accumulator that has
/// std's operators, saves the same bytes and answers the same.
fn check_old_acc<T>(rng: &mut Rng, p: &VerifiedAccColumn<T>, model: &[T])
where
    T: Accd + PrefixValue<Prefix = Added<T>>,
{
    let c = PrefixColumn::<T>::from_values(model.to_vec());
    assert_eq!(p.save(), c.save());
    let places = some_places(rng, model.len());
    for (n, &i) in places.iter().enumerate() {
        assert_eq!(p.get_prefix(i), c.get_prefix(i));
        assert_eq!(p.get_total(i), c.get_total(i));
        for &end in places.iter().skip(n % 3).step_by(3) {
            assert_eq!(p.sum_range(i, end), c.sum_range(i..end));
        }
    }
}

/// The reads of a plain column, of an accumulator column: a value by
/// its place, a range of values, a search for a value, and a clone,
/// which is a column of its own with an index of its own.
fn check_acc_plain_reads<T: Accd>(
    rng: &mut Rng,
    p: &VerifiedAccColumn<T>,
    v: &VerifiedColumn<T>,
    model: &[T],
) {
    for _ in 0..4 {
        if !model.is_empty() {
            let i = rng.below(model.len());
            assert_eq!(p.get(i).map(T::keep).as_ref(), Some(&model[i]));
        }
        let start = rng.below(model.len() + 1);
        let end = start + rng.below(model.len() - start + 1);
        let mut it = p.iter_range(start, end);
        assert_eq!(it.len(), end - start);
        for m in &model[start..end] {
            assert_eq!(it.next().map(T::keep).as_ref(), Some(m));
        }
        assert!(it.next().is_none());
        let x = match model.get(start) {
            Some(x) => x.clone(),
            None => T::fresh(rng),
        };
        assert_eq!(
            p.scope_to_value(x.lend(), start, end),
            v.scope_to_value(x.lend(), start, end)
        );
        let runs: Vec<(usize, T)> = p
            .iter_range(start, end)
            .runs()
            .map(|r| (r.count, T::keep(r.value)))
            .collect();
        assert_eq!(runs, runs_of(&model[start..end]));
    }
    let mut p2 = p.clone();
    assert_eq!(p2.slab_lens(), p.slab_lens());
    let (at, x) = (model.len() / 2, T::fresh(rng));
    p2.insert(at, x.lend());
    let mut with_it = model.to_vec();
    with_it.insert(at, x);
    assert_eq!(acc_values(&p2), with_it);
    assert_eq!(p2.get_total(at), T::added(&with_it[..at + 1]));
    assert_eq!(p2.get_prefix(usize::MAX), T::added(&with_it));
    assert_eq!(acc_values(p), model);
    assert_eq!(p.get_prefix(usize::MAX), T::added(model));
}

/// An accumulator column loads the bytes it saved, which are a plain
/// column's, as the same values and into the slabs a plain column loads
/// them into, and answers of them what it answered.
fn check_acc_loads<T: Accd>(rng: &mut Rng, p: &VerifiedAccColumn<T>, model: &[T]) {
    let bytes = p.save();
    let p2 = VerifiedAccColumn::<T>::load(&bytes).unwrap();
    let v2 = VerifiedColumn::<T>::load(&bytes).unwrap();
    check_acc(&p2, &v2, model, false);
    assert_eq!(p2.save(), bytes);
    // of an index made at once of all the slabs
    check_acc_reads(rng, &p2, model);
}

fn acc_edits<T: Accd>() {
    for seed in 1..=8u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0xacc5);
        let budget = [2, 3, 4, 8, 64][rng.below(5)];
        let mut p = VerifiedAccColumn::<T>::with_max_segments(budget);
        let mut v = VerifiedColumn::<T>::with_max_segments(budget);
        let mut model: Vec<T> = vec![];
        // one edit, of the accumulator column and of the plain one
        macro_rules! each {
            ($col:ident => $edit:expr) => {{
                let $col = &mut p;
                $edit;
                let $col = &mut v;
                $edit;
            }};
        }
        for step in 0..120 {
            match rng.below(14) {
                0..=3 => {
                    let at = rng.below(model.len() + 1);
                    let del = rng.below((model.len() - at).min(12) + 1);
                    let ins: Vec<T> = rng.values(40);
                    if rng.below(2) == 0 {
                        each!(col => col.splice(at, del, &lent(&ins)));
                    } else {
                        each!(col => col.splice_iter(at, del, ins.iter().map(T::lend)));
                    }
                    model.splice(at..at + del, ins);
                }
                4 | 5 => {
                    let at = rng.below(model.len() + 1);
                    let x = match at.checked_sub(1) {
                        Some(i) => rng.value(&model[i]),
                        None => T::fresh(&mut rng),
                    };
                    each!(col => col.insert(at, x.lend()));
                    model.insert(at, x);
                }
                6 => {
                    let x = match model.last() {
                        Some(last) => rng.value(last),
                        None => T::fresh(&mut rng),
                    };
                    each!(col => col.push(x.lend()));
                    model.push(x);
                }
                7 | 8 => {
                    if !model.is_empty() {
                        let at = rng.below(model.len());
                        each!(col => col.remove(at));
                        model.remove(at);
                    }
                }
                9 => {
                    // sometimes past the end, which changes nothing
                    let len = rng.below(model.len() + 3);
                    each!(col => col.truncate(len));
                    model.truncate(len);
                }
                10 => {
                    let at = rng.below(model.len() + 1);
                    let n = match rng.below(8) {
                        0 => 0,
                        1 => model.len() - at,
                        _ => rng.below((model.len() - at).min(40) + 1),
                    };
                    each!(col => col.remove_n(at, n));
                    model.drain(at..at + n);
                }
                11 | 12 => {
                    let at = rng.below(model.len() + 1);
                    let del = rng.below((model.len() - at).min(30) + 1);
                    let runs = some_runs::<T>(&mut rng, 6);
                    let lent_runs = || {
                        runs.iter().map(|(n, x)| Run {
                            count: *n,
                            value: x.lend(),
                        })
                    };
                    each!(col => col.splice_runs(at, del, lent_runs()));
                    model.splice(at..at + del, run_model(&runs));
                }
                _ => {
                    if rng.below(3) == 0 {
                        each!(col => col.clear());
                        model.clear();
                    } else {
                        let more: Vec<T> = rng.values(40);
                        each!(col => col.extend(more.iter().map(T::lend)));
                        model.extend(more);
                    }
                }
            }
            check_acc(&p, &v, &model, true);
            check_acc_reads(&mut rng, &p, &model);
            check_acc_plain_reads(&mut rng, &p, &v, &model);
            T::check_old(&mut rng, &p, &model);
            if step % 10 == 9 {
                check_acc_loads(&mut rng, &p, &model);
            }
        }
        check_acc_loads(&mut rng, &p, &model);
    }
}

/// An accumulator column of the verified API ([`VerifiedAccColumn`]) is
/// a column kept with another index. Under the same edits it holds what
/// a plain column holds, in the same slabs, reads the same, and saves
/// the same bytes; and what it answers from its index after each edit
/// is what its values then add up to in their order, for an accumulator
/// in which the order does not matter and for one in which it does.
/// Where the old API has the accumulator too, `PrefixColumn` of the
/// values saves the same bytes and answers the same.
#[test]
fn an_accumulator_column_is_a_column() {
    for_each_acc!(acc_edits);
}

fn acc_forms<T: Accd>() {
    for seed in 1..=6u64 {
        let mut rng = Rng(seed.wrapping_mul(0x2545_f491_4f6c_dd1d) ^ 0xacc6);
        let budget = [2, 3, 4, 8, 64][rng.below(5)];
        let model: Vec<T> = rng.values(300);
        let plain = VerifiedColumn::<T>::from_values(&lent(&model));

        let p = VerifiedAccColumn::<T>::from_values(&lent(&model));
        check_acc(&p, &plain, &model, true);
        check_acc_reads(&mut rng, &p, &model);
        T::check_old(&mut rng, &p, &model);
        let p: VerifiedAccColumn<T> = model.iter().map(T::lend).collect();
        check_acc(&p, &plain, &model, true);
        check_acc_reads(&mut rng, &p, &model);
        let p = VerifiedColumn::<T, Leb128, AccIndex>::from_iter(lent(&model));
        check_acc(&p, &plain, &model, true);
        check_acc_reads(&mut rng, &p, &model);
        let mut p = VerifiedAccColumn::<T>::new();
        p.extend(lent(&model));
        check_acc(&p, &plain, &model, true);
        check_acc_reads(&mut rng, &p, &model);
        let none = VerifiedAccColumn::<T>::default();
        assert!(none.is_empty() && none.save().is_empty());
        assert_eq!(none.slab_count(), 0);
        assert_eq!(none.get_prefix(3), T::added(&[]));
        assert_eq!(none.get_total(0), T::added(&[]));

        // in slabs of a budget, and loaded with one
        let mut p = VerifiedAccColumn::<T>::from_values_with_max_segments(&lent(&model), budget);
        let mut v = VerifiedColumn::<T>::from_values_with_max_segments(&lent(&model), budget);
        check_acc(&p, &v, &model, true);
        check_acc_reads(&mut rng, &p, &model);
        check_acc_loads(&mut rng, &p, &model);
        let opts = LoadOpts::new()
            .with_length(model.len())
            .with_max_segments(budget);
        let p2 = VerifiedAccColumn::<T>::load_with(&p.save(), opts).unwrap();
        let v2 = VerifiedColumn::<T>::load_with(&p.save(), opts).unwrap();
        check_acc(&p2, &v2, &model, false);
        check_acc_reads(&mut rng, &p2, &model);
        // of a length the bytes do not have: the same error
        let long = LoadOpts::new().with_length(model.len() + 1);
        assert_eq!(
            VerifiedAccColumn::<T>::load_with(&p.save(), long)
                .map(|p| p.len())
                .map_err(|e| e.to_string()),
            VerifiedColumn::<T>::load_with(&p.save(), long)
                .map(|v| v.len())
                .map_err(|e| e.to_string()),
        );

        // one value for each of a run's
        let (a, b) = (T::fresh(&mut rng), T::fresh(&mut rng));
        let f = |x: T| if not_above(&x, &a) { b.clone() } else { x };
        p.remap(f);
        v.remap(f);
        let want: Vec<T> = model.iter().cloned().map(f).collect();
        check_acc(&p, &v, &want, false);
        check_acc_reads(&mut rng, &p, &want);
        T::check_old(&mut rng, &p, &want);
        check_acc_loads(&mut rng, &p, &want);

        // a column of one value
        let n = rng.below(500);
        let p = VerifiedAccColumn::<T>::fill(n, a.lend());
        let v = VerifiedColumn::<T>::fill(n, a.lend());
        check_acc(&p, &v, &vec![a.clone(); n], true);
        check_acc_reads(&mut rng, &p, &vec![a.clone(); n]);
        T::check_old(&mut rng, &p, &vec![a.clone(); n]);
    }
}

/// The other ways to make an accumulator column make what they make of
/// a plain column, and one that answers what its values add up to: a
/// slice, an iterator, `extend`, a segment budget, a load with options,
/// a remap, and `fill`.
#[test]
fn the_other_forms_of_an_accumulator_column() {
    for_each_acc!(acc_forms);
}

/// Whether the bytes load. An accumulator column loads them where a
/// plain column does, as the same values in the same slabs, and is
/// refused them where that is, with the same error.
fn same_acc_load<T: Accd>(bytes: &[u8]) -> bool {
    let v = match VerifiedColumn::<T>::load(bytes) {
        Ok(v) => v,
        Err(e) => {
            let as_one = VerifiedAccColumn::<T>::load(bytes).map(|p| p.len());
            assert_eq!(as_one.map_err(|f| f.to_string()), Err(e.to_string()));
            return false;
        }
    };
    if v.len() <= 100_000 {
        let p = VerifiedAccColumn::<T>::load(bytes).unwrap();
        let model = new_values(&v);
        check_acc(&p, &v, &model, false);
        assert_eq!(p.get_prefix(usize::MAX), T::added(&model));
        return true;
    }
    // a few bytes can be a run of billions. That is read in places; and
    // it is not for an accumulator that adds a run one time after
    // another, which would be as long about it as the run is
    let runs = v.iter().runs().map(|r| (r.count, T::keep(r.value)));
    if let Some(all) = T::added_runs(&runs.collect::<Vec<_>>()) {
        let p = VerifiedAccColumn::<T>::load(bytes).unwrap();
        assert_eq!(p.len(), v.len());
        assert_eq!(p.save(), bytes);
        assert_eq!(p.get_prefix(usize::MAX), all);
        assert_eq!(p.sum_range(0, v.len()), all);
        for i in [0, 1, v.len() / 2, v.len() - 1] {
            assert_eq!(p.get(i).map(T::keep), v.get(i).map(T::keep));
        }
    }
    true
}

fn acc_loads<T: Accd>() {
    let mut rng = Rng(0xacc7);
    // how many were refused, and how many taken
    let mut seen = [0usize; 2];
    // bytes at random, mostly not a column
    for _ in 0..3_000 {
        let n = rng.below(12);
        let bytes: Vec<u8> = (0..n)
            .map(|_| match rng.below(4) {
                0 => rng.next() as u8,
                1 => [0x7f, 0x7e, 0x00, 0x80][rng.below(4)],
                _ => (rng.next() % 6) as u8,
            })
            .collect();
        seen[usize::from(same_acc_load::<T>(&bytes))] += 1;
    }
    // a column's bytes with one changed, taken out or put in
    for _ in 0..1_000 {
        let model: Vec<T> = rng.values(30);
        let mut bytes = VerifiedColumn::<T>::from_values(&lent(&model)).save();
        assert!(same_acc_load::<T>(&bytes));
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
        seen[usize::from(same_acc_load::<T>(&bytes))] += 1;
    }
    assert!(seen[0] > 100 && seen[1] > 100, "{seen:?}");
    // a run of billions, in a few bytes
    let x = T::fresh(&mut rng);
    let long = VerifiedColumn::<T>::fill(1 << 31, x.lend()).save();
    assert!(long.len() < 20 && same_acc_load::<T>(&long));
}

/// Nothing overflows in an accumulator, so there is an accumulator
/// column of any values: whatever bytes load as a plain column load as
/// one, and no others. (An accumulator that adds a run one time after
/// another is as long about a load as the column is, so the test loads
/// no run of billions for one.)
#[test]
fn an_accumulator_column_loads_what_a_plain_column_loads() {
    for_each_acc!(acc_loads);
}

/// The kind of an accumulator column is the third parameter of the
/// column's type: `VerifiedAccColumn<V, C>` is
/// `VerifiedColumn<V, C, AccIndex>`, and each constructor and each load
/// makes a column of the kind its type says. Its iterator, and what
/// `IntoIterator` gives of it, are a column's.
#[test]
fn the_kind_of_an_accumulator_column() {
    let vs: Vec<Mark> = (0..2000).map(|i| Mark(i / 7)).collect();
    let named = VerifiedColumn::<Mark, Leb128, AccIndex>::from_values(&vs);
    let short: VerifiedAccColumn<Mark> = named.clone();
    let collected: VerifiedAccColumn<Mark, Leb128> = vs.iter().copied().collect();
    let loaded = VerifiedAccColumn::<Mark>::load(&short.save()).unwrap();
    let plain: VerifiedColumn<Mark> = vs.iter().copied().collect();
    for c in [&named, &short, &collected, &loaded] {
        assert_eq!(c.to_vec(), vs);
        assert_eq!(c.save(), plain.save());
        assert_eq!(c.get_prefix(2000), Mark::added(&vs));
    }
    let it: VerifiedIter<'_, Mark, Leb128, AccIndex> = short.iter();
    assert_eq!(it.count(), vs.len());
    assert!((&short).into_iter().eq(vs.iter().copied()));
}

/// What an accumulator column answers is what its client says its
/// values add, added in the order the column holds them: by hand, for
/// marks, for nulls, which add nothing, and for steps, which two columns
/// of the same values in another order answer differently of.
#[test]
fn an_accumulator_adds_what_its_client_says() {
    // of the kinds 3, 5, 3, 5 and 0
    let model = [Mark(3), Mark(5), Mark(67), Mark(5), Mark(64)];
    let p = VerifiedAccColumn::<Mark>::from_values(&model);
    let c: PrefixColumn<Mark> = model.iter().copied().collect();
    assert_eq!(p.save(), c.save());
    assert_eq!(p.to_vec(), model);
    for (i, open) in [0, 8, 0x28, 0x20, 0, 1].into_iter().enumerate() {
        assert_eq!(p.get_prefix(i), Flags(open));
        assert_eq!(c.get_prefix(i), Flags(open));
    }
    assert_eq!(p.get_total(1), Flags(0x28));
    assert_eq!(p.sum_range(1, 4), Flags(8));
    assert_eq!(c.sum_range(1..4), Flags(8));
    assert_eq!(p.sum_range(3, 99), Flags(0x21));
    assert_eq!(p.sum_range(4, 1), Flags(0));

    let nulls = [Some(model[0]), None, Some(model[1]), None, None];
    let p = VerifiedAccColumn::<Option<Mark>>::from_values(&nulls);
    let c: PrefixColumn<Option<Mark>> = nulls.iter().copied().collect();
    assert_eq!(p.save(), c.save());
    for (i, open) in [0, 8, 8, 0x28, 0x28, 0x28].into_iter().enumerate() {
        assert_eq!(p.get_prefix(i), Flags(open));
        assert_eq!(c.get_prefix(i), Flags(open));
    }

    // any values at all: nothing overflows
    let p = VerifiedAccColumn::<Mark>::fill(1 << 20, Mark(u64::MAX));
    assert_eq!(p.get_prefix(1 << 20), Flags(0));
    assert_eq!(p.get_prefix((1 << 20) - 1), Flags(1 << 63));
    let loaded = VerifiedAccColumn::<Mark>::load(&p.save()).unwrap();
    assert_eq!(loaded.get_total(0), Flags(1 << 63));

    let board = |lit, turned| Board { lit, turned };
    let one_way = VerifiedAccColumn::<Step>::from_values(&[Step(3), Step(4)]);
    let the_other = VerifiedAccColumn::<Step>::from_values(&[Step(4), Step(3)]);
    assert_eq!(one_way.get_prefix(2), board(0x2_0000_0001, true));
    assert_eq!(the_other.get_prefix(2), board(3, true));
    assert_eq!(one_way.get_total(0), board(1, true));
    // a range's own steps, of a board that is not turned round
    assert_eq!(one_way.sum_range(1, 2), board(2, false));
    assert_eq!(the_other.sum_range(1, 2), board(1, true));
}

/// A run adds its value as many times over as it is long: for an
/// accumulator that adds so in a way of its own (`Flags`), and for one
/// that adds as the trait gives it, one time after another (`Board`).
#[test]
fn a_run_adds_its_value_times_over() {
    let step = Step(1 << 33 | 5);
    for n in [0usize, 1, 2, 3, 64, 1000, 65_537] {
        let mut open = Flags(0xf0);
        open.add_times(&Flags(0x3c), n);
        assert_eq!(open, Flags(if n % 2 == 1 { 0xcc } else { 0xf0 }));

        // a board of the step 15, and the step `n` times
        let mut steps = vec![Step(15)];
        steps.resize(n + 1, step);
        let mut at_once = Step::acc_of(Step(15));
        let mut one_by_one = at_once.dup();
        at_once.add_times(&Step::acc_of(step), n);
        for _ in 0..n {
            one_by_one.add_acc(&Step::acc_of(step));
        }
        assert_eq!(at_once, one_by_one);
        assert_eq!(at_once, Step::added(&steps));
        // what was added after the first step, by taking that away
        one_by_one.sub_acc(&Step::acc_of(Step(15)));
        assert_eq!(one_by_one, Step::added(&steps[1..]));
        one_by_one.sub_acc(&Step::added(&steps[1..]));
        assert_eq!(one_by_one, Board::new_zero());

        // in a column: one run, and runs put in
        let p = VerifiedAccColumn::<Step>::fill(n, step);
        for i in [0, 1, n / 2, n, n + 1] {
            assert_eq!(p.get_prefix(i), Step::added(&steps[1..][..i.min(n)]));
        }
        let mut p = VerifiedAccColumn::<Option<Mark>>::new();
        let run = |count, value| Run { count, value };
        let runs = [run(n, Some(Mark(9))), run(3, None), run(n, Some(Mark(70)))];
        p.splice_runs(0, 0, runs);
        let odd = n % 2 == 1;
        assert_eq!(p.get_prefix(n), Flags(if odd { 1 << 9 } else { 0 }));
        assert_eq!(p.get_total(2 * n + 2), Flags(if odd { 0x240 } else { 0 }));
    }
}

/// A value type of an accumulator column that `PrefixColumn` holds too,
/// with the same accumulator.
trait OldAccd: Accd + PrefixValue<Prefix = Added<Self>> {}

impl<T: Accd + PrefixValue<Prefix = Added<T>>> OldAccd for T {}

/// Where an accumulator column's iterator stands, how far its window
/// reaches, how many values it has left, and its running sum (under both
/// its names).
type AccPlace<T> = (usize, usize, usize, usize, Added<T>, Added<T>);

fn acc_place<T: Accd>(it: &VerifiedAccIter<'_, T>) -> AccPlace<T> {
    (
        it.pos(),
        it.end_pos(),
        it.items_left(),
        it.len(),
        it.total(),
        it.prefix(),
    )
}

fn old_acc_place<T: OldAccd>(it: &PrefixIter<'_, T>) -> AccPlace<T> {
    (
        it.pos(),
        it.end_pos(),
        it.items_left(),
        it.len(),
        it.total(),
        it.prefix(),
    )
}

/// A value with what the values before it and through it add up to.
type AccItem<T> = Option<(T, Added<T>, Added<T>)>;

fn acc_item<T: Accd>(pv: Option<VerifiedAccValue<'_, T>>) -> AccItem<T> {
    pv.map(|pv| (T::keep(pv.value), pv.prefix(), pv.total()))
}

fn old_acc_item<T: OldAccd>(pv: Option<PrefixedValue<'_, T>>) -> AccItem<T> {
    pv.map(|pv| (old(pv.value), pv.prefix(), pv.total()))
}

/// A run with the sums of its last value: before that value, and
/// through it.
type AccRun<T> = Option<(usize, T, Added<T>, Added<T>)>;

fn acc_run<T: Accd>(run: Option<Run<VerifiedAccValue<'_, T>>>) -> AccRun<T> {
    run.map(|run| {
        (
            run.count,
            T::keep(run.value.value),
            run.value.prefix(),
            run.value.total(),
        )
    })
}

fn old_acc_run<T: OldAccd>(run: Option<Run<PrefixedValue<'_, T>>>) -> AccRun<T> {
    run.map(|run| {
        (
            run.count,
            old(run.value.value),
            run.value.prefix(),
            run.value.total(),
        )
    })
}

/// A value found: where it is, what the values passed over before it add
/// up to, and the value with its sums.
type AccFound<T> = Option<(usize, Added<T>, (T, Added<T>, Added<T>))>;

fn acc_found<T: Accd>(seek: Option<VerifiedAccSeek<'_, T>>) -> AccFound<T> {
    seek.map(|seek| (seek.pos, seek.delta, acc_item(Some(seek.pv)).unwrap()))
}

fn old_acc_found<T: OldAccd>(seek: Option<PrefixSeek<'_, T>>) -> AccFound<T> {
    seek.map(|seek| (seek.pos, seek.delta, old_acc_item(Some(seek.pv)).unwrap()))
}

/// What the values before each place add up to, and at the last place
/// what all of them do: each sum is the one before it with the value's
/// own added after it, as the client's accumulator adds.
fn accs_before<T: Accd>(model: &[T]) -> Vec<Added<T>> {
    let mut sum = T::added(&[]);
    let mut before = vec![sum.dup()];
    for v in model {
        sum.add_acc(&T::added(std::slice::from_ref(v)));
        before.push(sum.dup());
    }
    // which is what the tests reckon of the values by themselves
    for i in [model.len() / 3, model.len() / 2, model.len()] {
        assert_eq!(before[i], T::added(&model[..i]));
    }
    before
}

/// A move of an accumulator column's iterator.
#[derive(Clone, Debug)]
enum Move {
    Next,
    Nth(usize),
    SetMax(usize),
    AdvanceBy(usize),
    AdvanceTo(usize),
    Shift(Range<usize>),
    ShiftNext(Range<usize>),
    ResetPrefix,
    NextRun,
    DeltaNth(usize),
    /// Stopped, and started again on its column.
    Resume,
    /// A copy moved on, which leaves the iterator where it is.
    Copy(usize),
}

/// What a move gives.
#[derive(PartialEq, Debug)]
enum Gave<T: Accd> {
    Nothing,
    Item(AccItem<T>),
    Run(AccRun<T>),
    Found(AccFound<T>),
}

/// An accumulator column's iterator as its specification has it: where
/// it stands, where its window ends, and where its sum counts from. Its
/// sums are reckoned of the values themselves, in their order.
#[derive(Clone)]
struct AccWalk<'m, T: Accd> {
    model: &'m [T],
    before: &'m [Added<T>],
    pos: usize,
    end: usize,
    base: usize,
}

impl<T: Accd> AccWalk<'_, T> {
    /// What the values from where the sum counts up to `to` add up to.
    fn sum(&self, to: usize) -> Added<T> {
        if self.base == 0 {
            self.before[to].dup()
        } else {
            T::added(&self.model[self.base..to])
        }
    }

    /// The values left: the window can run past the column.
    fn left(&self) -> usize {
        self.end.min(self.model.len()) - self.pos
    }

    fn place(&self) -> AccPlace<T> {
        (
            self.pos,
            self.end,
            self.end - self.pos,
            self.left(),
            self.sum(self.pos),
            self.sum(self.pos),
        )
    }

    fn nth(&mut self, n: usize) -> AccItem<T> {
        if n < self.left() {
            let i = self.pos + n;
            self.pos = i + 1;
            Some((self.model[i].clone(), self.sum(i), self.sum(i + 1)))
        } else {
            // all it had is passed, and its window ends there
            self.pos += self.left();
            self.end = self.pos;
            None
        }
    }

    fn set_max(&mut self, end: usize) {
        self.end = end.max(self.pos);
    }

    fn advance_to(&mut self, target: usize) {
        if target > self.pos {
            self.nth(target - self.pos - 1);
        }
    }

    fn next_run(&mut self) -> AccRun<T> {
        let left = self.left();
        if left == 0 {
            // its window ends where it stands
            self.end = self.pos;
            return None;
        }
        let v = self.model[self.pos].clone();
        let n = self.model[self.pos..self.pos + left]
            .iter()
            .take_while(|x| **x == v)
            .count();
        self.pos += n;
        Some((n, v, self.sum(self.pos - 1), self.sum(self.pos)))
    }

    fn delta_nth(&mut self, n: usize) -> AccFound<T> {
        let from = self.pos;
        let item = self.nth(n)?;
        let at = self.pos - 1;
        Some((at, T::added(&self.model[from..at]), item))
    }

    /// What a move gives, and where it leaves the iterator.
    fn does(&mut self, mv: &Move) -> Gave<T> {
        match mv.clone() {
            Move::Next => return Gave::Item(self.nth(0)),
            Move::Nth(n) => return Gave::Item(self.nth(n)),
            Move::SetMax(end) => self.set_max(end),
            Move::AdvanceBy(amount) => {
                if amount > 0 {
                    self.nth(amount - 1);
                }
            }
            Move::AdvanceTo(target) => self.advance_to(target),
            Move::Shift(range) => {
                self.set_max(range.end);
                self.advance_to(range.start);
            }
            Move::ShiftNext(range) => {
                self.set_max(range.end);
                return Gave::Item(self.nth(range.start - self.pos));
            }
            Move::ResetPrefix => self.base = self.pos,
            Move::NextRun => return Gave::Run(self.next_run()),
            Move::DeltaNth(n) => return Gave::Found(self.delta_nth(n)),
            // (where its window then ends is seen of the iterator)
            Move::Resume => {}
            Move::Copy(n) => return Gave::Item(self.clone().nth(n)),
        }
        Gave::Nothing
    }

    /// The window of an iterator that was stopped and started again:
    /// at the column's end it can come back closed.
    fn resumed(&mut self, mv: &Move, end_pos: usize) {
        let len = self.model.len();
        if matches!(mv, Move::Resume) && self.pos == len && end_pos == len {
            self.end = len;
        }
    }
}

/// The move made of an accumulator column's iterator, which is one of
/// `p`.
fn new_does<'a, T: Accd>(
    x: &mut VerifiedAccIter<'a, T>,
    p: &'a VerifiedAccColumn<T>,
    mv: &Move,
) -> Gave<T> {
    match mv.clone() {
        Move::Next => return Gave::Item(acc_item(x.next())),
        Move::Nth(n) => return Gave::Item(acc_item(x.nth(n))),
        Move::SetMax(end) => x.set_max(end),
        Move::AdvanceBy(amount) => x.advance_by(amount),
        Move::AdvanceTo(target) => x.advance_to(target),
        Move::Shift(range) => x.shift(range),
        Move::ShiftNext(range) => return Gave::Item(acc_item(x.shift_next(range))),
        Move::ResetPrefix => x.reset_prefix(),
        Move::NextRun => return Gave::Run(acc_run(x.next_run())),
        Move::DeltaNth(n) => return Gave::Found(acc_found(x.delta_nth(n))),
        Move::Resume => {
            let state = x.suspend();
            *x = state.try_resume(p).unwrap();
            // and a state starts as often as it is asked
            assert_eq!(acc_place(&state.try_resume(p).unwrap()), acc_place(x));
        }
        Move::Copy(n) => return Gave::Item(acc_item(x.clone().nth(n))),
    }
    Gave::Nothing
}

/// The move made of `PrefixColumn`'s iterator, which is one of `c`.
fn old_does<'a, T: OldAccd>(
    y: &mut PrefixIter<'a, T>,
    c: &'a PrefixColumn<T>,
    mv: &Move,
) -> Gave<T> {
    match mv.clone() {
        Move::Next => return Gave::Item(old_acc_item(y.next())),
        Move::Nth(n) => return Gave::Item(old_acc_item(y.nth(n))),
        Move::SetMax(end) => y.set_max(end),
        Move::AdvanceBy(amount) => y.advance_by(amount),
        Move::AdvanceTo(target) => y.advance_to(target),
        Move::Shift(range) => y.shift(range),
        Move::ShiftNext(range) => return Gave::Item(old_acc_item(y.shift_next(range))),
        Move::ResetPrefix => y.reset_prefix(),
        Move::NextRun => return Gave::Run(old_acc_run(y.next_run())),
        Move::DeltaNth(n) => return Gave::Found(old_acc_found(y.delta_nth(n))),
        Move::Resume => *y = y.suspend().try_resume(c).unwrap(),
        Move::Copy(n) => return Gave::Item(old_acc_item(y.clone().nth(n))),
    }
    Gave::Nothing
}

/// A move for an iterator that stands as `m` does: mostly within what it
/// has left, sometimes past it.
fn a_move<T: Accd>(rng: &mut Rng, m: &AccWalk<'_, T>) -> Move {
    let len = m.model.len();
    // how many values to pass over
    let count = |rng: &mut Rng| match rng.below(4) {
        0 => a_place(rng, len),
        _ => rng.below(m.left() + 2),
    };
    match rng.below(19) {
        0..=2 => Move::Next,
        3 | 4 => Move::Nth(count(rng)),
        5 => Move::SetMax(a_place(rng, len)),
        6 => Move::AdvanceBy(a_place(rng, len - m.pos)),
        7 => Move::AdvanceTo(a_place(rng, len)),
        // any range: one that starts behind the iterator, one that ends
        // before it starts
        8 => Move::Shift(a_place(rng, len)..a_place(rng, len)),
        9 | 10 => {
            // a range that starts at the iterator or after it
            let start = m.pos + a_place(rng, len - m.pos).min(usize::MAX - m.pos);
            Move::ShiftNext(start..a_place(rng, len))
        }
        11 => Move::ResetPrefix,
        12 | 13 => Move::NextRun,
        14 | 15 => Move::DeltaNth(count(rng)),
        16 | 17 => Move::Resume,
        _ => Move::Copy(rng.below(3)),
    }
}

/// An accumulator column's iterator gives each value with what the
/// values before it and through it add up to, in the order the column
/// holds them, and keeps its running sum under every move: by single
/// values and by runs, over values it does not give, into another
/// window, with its sum started again, stopped and started again, and
/// copied. As its specification says, which a walk of the values
/// follows.
fn check_acc_iters<T: Accd>(rng: &mut Rng, p: &VerifiedAccColumn<T>, model: &[T]) {
    let len = model.len();
    let before = accs_before(model);
    let item =
        |i: usize| (i < len).then(|| (model[i].clone(), before[i].dup(), before[i + 1].dup()));
    // all of it, in a `for` loop and by the standard library's functions
    for (n, pv) in p.prefix_iter().enumerate() {
        assert_eq!(acc_item(Some(pv)), item(n));
    }
    assert_eq!(p.prefix_iter().count(), len);
    let last = len.checked_sub(1).and_then(item);
    assert_eq!(acc_item(p.prefix_iter().last()), last);
    // one value by its place, which can be past the column
    for i in some_places(rng, len) {
        assert_eq!(acc_item(p.get_prefixed(i)), item(i));
    }
    // all of it by runs, each with the sums of its last value
    let mut x = p.prefix_iter();
    let mut m = AccWalk {
        model,
        before: &before,
        pos: 0,
        end: len,
        base: 0,
    };
    loop {
        let want = m.next_run();
        assert_eq!(acc_run(x.next_run()), want);
        assert_eq!(acc_place(&x), m.place());
        if want.is_none() {
            break;
        }
    }
    for _ in 0..3 {
        // the runs of a range without their sums: what `splice_runs`
        // takes
        let start = rng.below(len + 1);
        let end = start + rng.below(len - start + 1);
        let runs: Vec<(usize, T)> = p
            .prefix_iter_range(start, end)
            .runs()
            .map(|run| (run.count, T::keep(run.value)))
            .collect();
        assert_eq!(runs, runs_of(&model[start..end]));
        let mut copy = VerifiedAccColumn::<T>::new();
        copy.splice_runs(0, 0, p.prefix_iter_range(start, end).runs());
        assert_eq!(acc_values(&copy), model[start..end]);
        assert_eq!(copy.get_prefix(usize::MAX), T::added(&model[start..end]));
    }
    // the value at a place, with what a range before it adds up to
    for _ in 0..6 {
        let to = a_place(rng, len);
        let from = rng.below(to.min(len) + 1);
        let want = item(to).map(|it| (to, T::added(&model[from..to]), it));
        assert_eq!(acc_found(p.delta(from, to)), want);
    }
    // an iterator of a range, moved about
    for _ in 0..4 {
        let start = rng.below(len + 1);
        let end = start + rng.below(len - start + 1);
        let mut x = p.prefix_iter_range(start, end);
        let mut m = AccWalk {
            model,
            before: &before,
            pos: start,
            end,
            base: 0,
        };
        assert_eq!(acc_place(&x), m.place());
        for _ in 0..16 {
            let mv = a_move(rng, &m);
            let want = m.does(&mv);
            assert_eq!(new_does(&mut x, p, &mv), want, "{mv:?}");
            m.resumed(&mv, x.end_pos());
            assert_eq!(acc_place(&x), m.place(), "{mv:?}");
            assert_eq!(x.size_hint(), (m.left(), Some(m.left())));
        }
        // what it has left
        let rest: Vec<AccItem<T>> = x.map(|pv| acc_item(Some(pv))).collect();
        let mut want = vec![];
        while let Some(v) = m.nth(0) {
            want.push(Some(v));
        }
        assert_eq!(rest, want);
    }
}

/// An accumulator column under edits that put in single values and
/// runs: `check` is asked of it after each edit, and of the column
/// loaded from its bytes.
fn under_acc_edits<T: Accd>(salt: u64, check: fn(&mut Rng, &VerifiedAccColumn<T>, &[T])) {
    for seed in 1..=8u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ salt);
        let budget = [2, 3, 4, 8, 64][rng.below(5)];
        let mut p = VerifiedAccColumn::<T>::with_max_segments(budget);
        let mut model: Vec<T> = vec![];
        check(&mut rng, &p, &model);
        for _ in 0..40 {
            let at = rng.below(model.len() + 1);
            let del = rng.below((model.len() - at).min(12) + 1);
            if rng.below(3) == 0 {
                let runs = some_runs::<T>(&mut rng, 6);
                let lent_runs = runs.iter().map(|(n, x)| Run {
                    count: *n,
                    value: x.lend(),
                });
                p.splice_runs(at, del, lent_runs);
                model.splice(at..at + del, run_model(&runs));
            } else {
                let ins: Vec<T> = rng.values(40);
                p.splice(at, del, &lent(&ins));
                model.splice(at..at + del, ins);
            }
            check(&mut rng, &p, &model);
        }
        // loaded: an index made at once of all the slabs
        let p2 = VerifiedAccColumn::<T>::load(&p.save()).unwrap();
        check(&mut rng, &p2, &model);
    }
}

fn acc_iters<T: Accd>() {
    under_acc_edits::<T>(0xacc8, check_acc_iters::<T>);
}

/// An accumulator column of the verified API is read with its sums
/// ([`VerifiedAccIter`]) as its specification says: each value comes
/// with what the values before it and through it add up to, and the
/// iterator's running sum is of the values it has passed since its sum
/// was last started, in their order, whether it read them or stepped
/// over them, one at a time or a run at a time, in a window that is
/// moved, widened past the column, or closed. It finds a value further
/// on with what it passes over on the way
/// ([`VerifiedAccIter::delta_nth`], [`VerifiedColumn::delta`]), and is
/// copied, and stopped and started again ([`VerifiedAccIter::suspend`],
/// [`VerifiedAccIterState::try_resume`]). For an accumulator in which
/// the order does not matter and for one in which it does, after edits
/// and after a load.
#[test]
fn an_accumulator_column_is_read_with_its_sums() {
    for_each_acc!(acc_iters);
}

/// `PrefixColumn` of the same values, with an accumulator that has
/// std's operators: its iterator, asked what the accumulator column's
/// is at each step, gives the same and stands in the same place after
/// it.
fn check_old_acc_iters<T: OldAccd>(rng: &mut Rng, p: &VerifiedAccColumn<T>, model: &[T]) {
    let len = model.len();
    let c = PrefixColumn::<T>::from_values(model.to_vec());
    let before = accs_before(model);
    // all of it, and one value by its place
    let new: Vec<AccItem<T>> = p.prefix_iter().map(|pv| acc_item(Some(pv))).collect();
    let was: Vec<AccItem<T>> = c.iter().map(|pv| old_acc_item(Some(pv))).collect();
    assert_eq!(new, was);
    for i in some_places(rng, len) {
        assert_eq!(acc_item(p.get_prefixed(i)), old_acc_item(c.get(i)));
    }
    for _ in 0..3 {
        // the runs of a range, and the value at a place with what a
        // range before it adds up to
        let start = rng.below(len + 1);
        let end = start + rng.below(len - start + 1);
        let runs: Vec<(usize, T)> = p
            .prefix_iter_range(start, end)
            .runs()
            .map(|run| (run.count, T::keep(run.value)))
            .collect();
        let old_runs: Vec<(usize, T)> = c
            .iter_range(start..end)
            .runs()
            .map(|run| (run.count, old(run.value)))
            .collect();
        assert_eq!(runs, old_runs);
        let to = a_place(rng, len);
        let from = rng.below(to.min(len) + 1);
        assert_eq!(
            acc_found(p.delta(from, to)),
            old_acc_found(c.delta(from, to))
        );
    }
    // an iterator of a range, moved about
    for _ in 0..4 {
        let start = rng.below(len + 1);
        let end = start + rng.below(len - start + 1);
        let mut x = p.prefix_iter_range(start, end);
        let mut y = c.iter_range(start..end);
        let mut m = AccWalk {
            model,
            before: &before,
            pos: start,
            end,
            base: 0,
        };
        assert_eq!(old_acc_place(&y), m.place());
        for _ in 0..16 {
            let mv = a_move(rng, &m);
            let want = m.does(&mv);
            assert_eq!(new_does(&mut x, p, &mv), want, "{mv:?}");
            assert_eq!(old_does(&mut y, &c, &mv), want, "{mv:?}");
            m.resumed(&mv, x.end_pos());
            assert_eq!(acc_place(&x), m.place(), "{mv:?}");
            assert_eq!(old_acc_place(&y), m.place(), "{mv:?}");
        }
        // what they have left
        let rest = x.map(|pv| acc_item(Some(pv)));
        assert!(rest.eq(y.map(|pv| old_acc_item(Some(pv)))));
    }
}

fn old_acc_iters<T: OldAccd>() {
    under_acc_edits::<T>(0xacc9, check_old_acc_iters::<T>);
}

/// Where the old API has the accumulator too, the iterator of an
/// accumulator column moves as `PrefixColumn`'s does: the two, asked
/// the same at each step, give the same values with the same sums, and
/// stand in the same place with the same window after it.
#[test]
fn an_accumulator_iterator_moves_as_a_prefix_columns_does() {
    old_acc_iters::<Mark>();
    old_acc_iters::<Option<Mark>>();
}

/// The iterator of a short accumulator column, by hand.
#[test]
fn the_iterator_of_an_accumulator_column() {
    // of the kinds 3, 5, 3, 5 and 0
    let model = [Mark(3), Mark(5), Mark(67), Mark(5), Mark(64)];
    let open = [0, 8, 0x28, 0x20, 0, 1].map(Flags);
    let p = VerifiedAccColumn::<Mark>::from_values(&model);
    let got: Vec<(Flags, Mark, Flags)> = p
        .prefix_iter()
        .map(|pv| (pv.prefix(), pv.value, pv.total()))
        .collect();
    let want: Vec<(Flags, Mark, Flags)> =
        (0..5).map(|i| (open[i], model[i], open[i + 1])).collect();
    assert_eq!(got, want);
    // a range: the sum counts from the column's start
    let mut it = p.prefix_iter_range(1, 4);
    assert_eq!(
        (it.pos(), it.end_pos(), it.len(), it.total()),
        (1, 4, 3, Flags(8))
    );
    let pv = it.next().unwrap();
    let given = VerifiedAccValue {
        value: Mark(5),
        total: Flags(0x28),
    };
    assert_eq!(pv, given);
    assert_eq!(
        format!("{pv:?}"),
        "VerifiedAccValue { value: Mark(5), total: Flags(40) }"
    );
    let copy = pv.clone();
    assert_eq!(
        (copy.prefix(), copy.total(), pv.total()),
        (open[1], open[2], open[2])
    );
    // started again, it counts from where the iterator stands
    it.reset_prefix();
    assert_eq!((it.pos(), it.total(), it.prefix()), (2, Flags(0), Flags(0)));
    let pv = it.nth(1).unwrap();
    assert_eq!(
        (pv.prefix(), pv.value, pv.total()),
        (Flags(8), Mark(5), Flags(0x28))
    );
    assert!(it.next().is_none());
    assert_eq!((it.pos(), it.end_pos(), it.total()), (4, 4, Flags(0x28)));
    // a window past the column closes at the column's end
    let mut it = p.prefix_iter();
    it.set_max(9);
    assert_eq!((it.end_pos(), it.items_left(), it.len()), (9, 9, 5));
    it.advance_by(7);
    assert_eq!((it.pos(), it.end_pos(), it.total()), (5, 5, open[5]));
    // moved to a window, and to one that starts behind it
    let mut it = p.prefix_iter();
    it.shift(2..4);
    assert_eq!((it.pos(), it.end_pos(), it.total()), (2, 4, open[2]));
    it.shift(0..5);
    assert_eq!((it.pos(), it.end_pos(), it.total()), (2, 5, open[2]));
    let pv = it.shift_next(3..5).unwrap();
    assert_eq!(
        (pv.prefix(), pv.value, pv.total()),
        (open[3], Mark(5), open[4])
    );
    assert_eq!((it.pos(), it.end_pos(), it.total()), (4, 5, open[4]));
    assert!(it.shift_next(5..5).is_none());
    assert_eq!((it.pos(), it.end_pos(), it.total()), (5, 5, open[5]));
    // one value by its place
    let pv = p.get_prefixed(4).unwrap();
    assert_eq!(
        (pv.prefix(), pv.value, pv.total()),
        (open[4], Mark(64), open[5])
    );
    assert!(p.get_prefixed(5).is_none());
    // nulls, which add nothing
    let nulls = [Some(Mark(3)), None, Some(Mark(5))];
    let p = VerifiedAccColumn::<Option<Mark>>::from_values(&nulls);
    let got: Vec<(Flags, Option<Mark>, Flags)> = p
        .prefix_iter()
        .map(|pv| (pv.prefix(), pv.value, pv.total()))
        .collect();
    let want = [(0, nulls[0], 8), (8, None, 8), (8, nulls[2], 0x28)];
    assert_eq!(got, want.map(|(b, v, t)| (Flags(b), v, Flags(t))));

    // steps, whose order matters: the sum before a value is the sum
    // through it with the value's own taken off its end
    let board = |lit, turned| Board { lit, turned };
    let p = VerifiedAccColumn::<Step>::from_values(&[Step(3), Step(4), Step(3)]);
    let got: Vec<(Board, Step, Board)> = p
        .prefix_iter()
        .map(|pv| (pv.prefix(), pv.value, pv.total()))
        .collect();
    let after = [
        board(0, false),
        board(1, true),
        board(0x2_0000_0001, true),
        board(0x3_0000_0001, false),
    ];
    let steps = [Step(3), Step(4), Step(3)];
    let want: Vec<(Board, Step, Board)> =
        (0..3).map(|i| (after[i], steps[i], after[i + 1])).collect();
    assert_eq!(got, want);
    // a sum started again is of the steps since, on a board of their
    // own: what the column answers of that range
    let mut it = p.prefix_iter_range(1, 3);
    it.reset_prefix();
    let pv = it.next().unwrap();
    assert_eq!(
        (pv.prefix(), pv.total()),
        (board(0, false), board(2, false))
    );
    let pv = it.next().unwrap();
    assert_eq!((pv.prefix(), pv.total()), (board(2, false), board(3, true)));
    assert_eq!(
        (it.total(), p.sum_range(1, 3)),
        (board(3, true), board(3, true))
    );
    // a column of nothing
    let none = VerifiedAccColumn::<Step>::new();
    let mut it = none.prefix_iter();
    assert!(it.next().is_none() && it.nth(3).is_none());
    assert_eq!(
        (it.pos(), it.end_pos(), it.total()),
        (0, 0, board(0, false))
    );
    assert!(none.get_prefixed(0).is_none());
}

/// The runs, seeks and states of a short accumulator column's iterator,
/// by hand.
#[test]
fn the_seeks_of_an_accumulator_iterator() {
    // three marks of the kind 3, one of the kind 5, two of the kind 0
    let model = [Mark(3), Mark(3), Mark(3), Mark(5), Mark(64), Mark(64)];
    let mut p = VerifiedAccColumn::<Mark>::from_values(&model);
    // by runs: the sums are those of each run's last value
    let mut it = p.prefix_iter();
    let mut runs = vec![];
    while let Some(run) = it.next_run() {
        runs.push((
            run.count,
            run.value.value,
            run.value.prefix(),
            run.value.total(),
        ));
    }
    let want = [(3, 3, 0, 8), (1, 5, 8, 0x28), (2, 64, 0x29, 0x28)];
    assert_eq!(
        runs,
        want.map(|(n, v, b, t)| (n, Mark(v), Flags(b), Flags(t)))
    );
    assert_eq!(
        format!("{it:?}"),
        "VerifiedAccIter { total: Flags(40), pos: 6 }"
    );
    // the runs of a window, without sums
    let runs: Vec<(usize, Mark)> = p
        .prefix_iter_range(1, 5)
        .runs()
        .map(|run| (run.count, run.value))
        .collect();
    assert_eq!(runs, [(2, Mark(3)), (1, Mark(5)), (1, Mark(64))]);
    // a value by its count from the iterator, with what is passed over
    let mut it = p.prefix_iter_range(1, 6);
    let seek = it.delta_nth(3).unwrap();
    let found = VerifiedAccSeek {
        pos: 4,
        delta: Flags(0x20),
        pv: VerifiedAccValue {
            value: Mark(64),
            total: Flags(0x29),
        },
    };
    assert_eq!(seek, found);
    assert_eq!(
        format!("{seek:?}"),
        "VerifiedAccSeek { pos: 4, delta: Flags(32), pv: VerifiedAccValue { value: Mark(64), total: Flags(41) } }"
    );
    assert_eq!(
        (seek.clone(), seek.pv.prefix()),
        (found.clone(), Flags(0x28))
    );
    assert!(it.delta_nth(1).is_none());
    assert_eq!((it.pos(), it.end_pos(), it.total()), (6, 6, Flags(0x28)));
    // the same of the column: the value at a place, and what a range
    // before it adds up to
    assert_eq!(p.delta(1, 4), Some(found));
    let seek = p.delta(2, 2).unwrap();
    assert_eq!(
        (seek.pos, seek.delta, seek.pv.total()),
        (2, Flags(0), Flags(8))
    );
    assert!(p.delta(0, 6).is_none());

    // a copy is an iterator of its own
    let mut it = p.prefix_iter_range(1, 5);
    it.next();
    it.reset_prefix();
    let mut twin = it.clone();
    assert_eq!(twin.next().map(|pv| pv.total()), Some(Flags(8)));
    assert_eq!((it.pos(), it.total(), twin.pos()), (2, Flags(0), 3));
    // stopped and started again: its place, its window, and where its
    // sum counts from
    it.next();
    let state: VerifiedAccIterState<Mark> = it.suspend();
    let mut on = state.try_resume(&p).unwrap();
    assert_eq!((on.pos(), on.end_pos(), on.total()), (3, 5, Flags(8)));
    let pv = on.next().unwrap();
    assert_eq!(
        (pv.prefix(), pv.value, pv.total()),
        (Flags(8), Mark(5), Flags(0x28))
    );
    // on another column of that many edits, with that column's sums
    let other = VerifiedAccColumn::<Mark>::from_values(&[1, 2, 4, 7, 9, 9].map(Mark));
    let on = state.try_resume(&other).unwrap();
    assert_eq!((on.pos(), on.end_pos(), on.total()), (3, 5, Flags(0x10)));
    // and not on a column edited since
    p.push(Mark(1));
    assert!(matches!(
        state.try_resume(&p),
        Err(PackError::InvalidResume)
    ));
    // a state is plain data, with no borrow in it
    fn plain<S: Send + Sync + 'static>() {}
    plain::<VerifiedAccIterState<Option<Step>>>();

    // an iterator of no column
    let mut it = VerifiedAccIter::<Mark>::default();
    assert_eq!(
        (it.pos(), it.end_pos(), it.len(), it.total()),
        (0, 0, 0, Flags(0))
    );
    assert!(it.next_run().is_none() && it.delta_nth(0).is_none() && it.next().is_none());
    assert_eq!(it.clone().runs().count(), 0);
    assert_eq!(
        format!("{it:?}"),
        "VerifiedAccIter { total: Flags(0), pos: 0 }"
    );
    // its state starts on a new column
    let state = VerifiedAccIter::<Mark>::default().suspend();
    let none = VerifiedAccColumn::<Mark>::new();
    let on = state.try_resume(&none).unwrap();
    assert_eq!((on.pos(), on.end_pos(), on.total()), (0, 0, Flags(0)));
    assert!(state.try_resume(&p).is_err());
}

/// A long run is stepped over by its count where the accumulator adds a
/// run in a way of its own, and the sum of what is passed is the run's
/// value added as many times over.
///
/// (The columns here are longer than a 32-bit `usize` counts.)
#[cfg(target_pointer_width = "64")]
#[test]
fn the_accumulator_iterator_over_long_runs() {
    // an odd count of marks of one kind
    let long = i64::MAX as usize;
    let p = VerifiedAccColumn::<Mark>::fill(long, Mark(5));
    let mut it = p.prefix_iter();
    let pv = it.nth(long - 2).unwrap();
    assert_eq!((pv.prefix(), pv.total()), (Flags(0x20), Flags(0)));
    let pv = it.next().unwrap();
    assert_eq!(pv.total(), Flags(0x20));
    assert!(it.next().is_none());
    assert_eq!((it.pos(), it.total()), (long, Flags(0x20)));
    // from the middle on: an odd count before it, an even one after
    let half = long / 2;
    let mut it = p.prefix_iter_range(half, long);
    assert_eq!(it.total(), Flags(0x20));
    it.reset_prefix();
    it.advance_to(long);
    assert_eq!(it.total(), Flags(0));
    // one run of them all
    let run = p.prefix_iter().next_run().unwrap();
    assert_eq!(
        (run.count, run.value.prefix(), run.value.total()),
        (long, Flags(0), Flags(0x20))
    );

    // in slabs of two runs, against `PrefixColumn`'s iterator
    let run = |mark: u64, count: usize| Run {
        count,
        value: Mark(mark),
    };
    let mut p = VerifiedAccColumn::<Mark>::with_max_segments(2);
    let mut c = PrefixColumn::<Mark>::with_max_segments(2);
    for (at, runs) in [
        (0, [run(3, 1 << 30), run(7, (1 << 40) + 1)]),
        (1 << 20, [run(0, (1 << 50) + 1), run(70, 1 << 30)]),
        (5, [run(1, 3), run(67, 1)]),
    ] {
        p.splice_runs(at, 0, runs);
        c.splice_runs(at, 0, runs);
    }
    let len = p.len();
    let mut rng = Rng(0xacca);
    let mut places = some_places(&mut rng, len);
    places.extend([5, 9, 1 << 20, (1 << 20) + (1 << 50), (1 << 30) + 4, len - 1]);
    places.sort();
    places.dedup();
    let mut x = p.prefix_iter();
    let mut y = c.iter();
    for &i in &places {
        x.advance_to(i);
        y.advance_to(i);
        assert_eq!(acc_place(&x), old_acc_place(&y));
        assert_eq!(x.total(), p.get_prefix(i));
        assert_eq!(acc_item(x.next()), old_acc_item(y.next()));
    }
}

/// An accumulator is its client's code, and can panic. If it does while
/// an iterator moves, the iterator is left with nothing to give, as one
/// of no column is, and the column is as it was. (Where a panic can be
/// caught: not on wasm32.)
#[cfg(panic = "unwind")]
#[test]
fn an_accumulator_that_panics_under_an_iterator() {
    thread_local! {
        /// How many more times a `Fused` sum adds before it panics: at
        /// the most there is, any number.
        static FUSE: Cell<u32> = const { Cell::new(u32::MAX) };
    }

    /// An accumulator that panics when it is told to: a sum whose adds
    /// are counted down.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    struct Fused(u64);

    impl VGroup for Fused {}

    impl VAccum for Fused {
        fn new_zero() -> Fused {
            Fused(0)
        }

        fn add_acc(&mut self, b: &Fused) {
            let left = FUSE.get();
            if left == 0 {
                panic!("the fuse is burnt");
            }
            if left < u32::MAX {
                FUSE.set(left - 1);
            }
            self.0 = self.0.wrapping_add(b.0);
        }

        fn sub_acc(&mut self, b: &Fused) {
            self.0 = self.0.wrapping_sub(b.0);
        }

        fn dup(&self) -> Fused {
            *self
        }
    }

    /// A value for a column that adds up in a `Fused` sum.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    struct Tick(u64);

    impl VNewtype for Tick {
        type Base = u64;

        fn to_base(self) -> u64 {
            self.0
        }

        fn from_base(b: u64) -> Result<Tick, PackError> {
            Ok(Tick(b))
        }
    }

    impl VAdds for Tick {
        type Acc = Fused;

        fn acc_of(v: Tick) -> Fused {
            Fused(v.0)
        }
    }

    let model = [1, 2, 2, 3, 4].map(Tick);
    let p = VerifiedAccColumn::<Tick>::from_values(&model);
    let moves: [fn(&mut VerifiedAccIter<'_, Tick>); 7] = [
        |it| {
            let _ = it.next();
        },
        |it| {
            let _ = it.nth(2);
        },
        |it| it.advance_to(4),
        |it| {
            let _ = it.next_run();
        },
        |it| {
            let _ = it.delta_nth(3);
        },
        |it| {
            let _ = it.shift_next(3..5);
        },
        // which adds the sum so far to what came before it
        |it| it.reset_prefix(),
    ];
    for go in moves {
        let mut it = p.prefix_iter();
        assert_eq!(it.next().map(|pv| pv.total()), Some(Fused(1)));
        // the next add panics
        FUSE.set(0);
        let said = panic_of(std::panic::AssertUnwindSafe(|| go(&mut it)));
        FUSE.set(u32::MAX);
        assert_eq!(said.as_deref(), Some("the fuse is burnt"));
        assert_eq!(
            (it.pos(), it.end_pos(), it.len(), it.total()),
            (0, 0, 0, Fused(0))
        );
        assert!(it.next().is_none() && it.next_run().is_none());
        // the column is as it was
        assert_eq!(p.to_vec(), model);
        let sums: Vec<u64> = p.prefix_iter().map(|pv| pv.total().0).collect();
        assert_eq!(sums, [1, 3, 5, 8, 12]);
    }
    // and a fuse that burns later, in the middle of a move
    for burn in 1..3 {
        let mut it = p.prefix_iter();
        FUSE.set(burn);
        let said = panic_of(std::panic::AssertUnwindSafe(|| it.by_ref().count()));
        FUSE.set(u32::MAX);
        assert_eq!(said.as_deref(), Some("the fuse is burnt"));
        assert_eq!(
            (it.pos(), it.end_pos(), it.len(), it.total()),
            (0, 0, 0, Fused(0))
        );
    }
}

/// A value type of a delta column that `DeltaColumn` holds too.
trait Deltad: VDeltaValue + DeltaValue + PartialEq + Debug {
    /// A value near `last`, or (now and then) null or far away.
    fn near(rng: &mut Rng, last: i64) -> (Self, i64);
}

macro_rules! deltad {
    ($t:ty, $lo:expr, $hi:expr) => {
        impl Deltad for $t {
            fn near(rng: &mut Rng, last: i64) -> (Self, i64) {
                let step = if rng.below(8) == 0 {
                    rng.next() as i64 >> 24
                } else {
                    rng.below(7) as i64 - 3
                };
                let n = last.wrapping_add(step).clamp($lo, $hi);
                (n as $t, n)
            }
        }

        impl Deltad for Option<$t> {
            fn near(rng: &mut Rng, last: i64) -> (Self, i64) {
                if rng.below(5) == 0 {
                    (None, last)
                } else {
                    let (v, n) = <$t as Deltad>::near(rng, last);
                    (Some(v), n)
                }
            }
        }
    };
}

deltad!(u32, 0, u32::MAX as i64);
deltad!(u64, 0, i64::MAX >> 2);
deltad!(i32, i32::MIN as i64, i32::MAX as i64);
deltad!(i64, i64::MIN >> 2, i64::MAX >> 2);
deltad!(usize, 0, i64::MAX >> 2);

fn delta_agrees<T: Deltad>(new: &VerifiedDeltaColumn<T>, old: &DeltaColumn<T>) {
    assert_eq!(new.len(), old.len());
    assert_eq!(new.is_empty(), old.is_empty());
    for i in 0..old.len() + 2 {
        assert_eq!(new.get(i), old.get(i));
    }
}

fn delta_edits<T: Deltad>(seed: u64) {
    let mut rng = Rng(seed | 1);
    let mut last = 0;
    let mut some = |rng: &mut Rng, n: usize| -> Vec<T> {
        (0..n)
            .map(|_| {
                let (v, l) = T::near(rng, last);
                last = l;
                v
            })
            .collect()
    };
    let n = rng.below(300);
    let start = some(&mut rng, n);
    let mut new = VerifiedDeltaColumn::<T>::from_values(&start);
    let mut old = DeltaColumn::<T>::from_values(start);
    delta_agrees(&new, &old);
    for _ in 0..60 {
        let at = rng.below(old.len() + 1);
        let del = rng.below((old.len() - at).min(6) + 1);
        let n = rng.below(9);
        let values = some(&mut rng, n);
        new.splice(at, del, &values);
        old.splice(at, del, values);
        delta_agrees(&new, &old);
    }
    let empty = VerifiedDeltaColumn::<T>::new();
    delta_agrees(&empty, &DeltaColumn::<T>::new());
}

/// A verified delta column and the old one hold the same values after
/// the same builds and splices, for every type of value.
#[test]
fn delta_columns_agree() {
    for seed in 0..12 {
        delta_edits::<u32>(seed);
        delta_edits::<u64>(seed);
        delta_edits::<i32>(seed);
        delta_edits::<i64>(seed);
        delta_edits::<usize>(seed);
        delta_edits::<Option<u32>>(seed);
        delta_edits::<Option<u64>>(seed);
        delta_edits::<Option<i32>>(seed);
        delta_edits::<Option<i64>>(seed);
        delta_edits::<Option<usize>>(seed);
    }
}

/// The simple edits and reads of a delta column, done to a verified
/// column and to the old one: both hold the same values after each, and
/// lay them out in as many slabs.
fn delta_simple_edits<T: Deltad>(seed: u64, max_segments: Option<usize>) {
    let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9).wrapping_add(7) | 1);
    let mut last = 0;
    let (mut new, mut old) = match max_segments {
        Some(m) => (
            VerifiedDeltaColumn::<T>::with_max_segments(m),
            DeltaColumn::<T>::with_max_segments(m),
        ),
        None => (
            VerifiedDeltaColumn::<T>::default(),
            DeltaColumn::<T>::default(),
        ),
    };
    for _ in 0..300 {
        let len = old.len();
        match rng.below(16) {
            0..=5 => {
                let (v, l) = T::near(&mut rng, last);
                last = l;
                new.push(v);
                old.push(v);
            }
            6..=8 => {
                let at = rng.below(len + 1);
                let (v, l) = T::near(&mut rng, last);
                last = l;
                new.insert(at, v);
                old.insert(at, v);
            }
            9 => {
                // now and then past the end, where there is nothing
                let at = rng.below(len + 3);
                new.remove(at);
                old.remove(at);
            }
            10 => {
                let at = rng.below(len + 1);
                let n = rng.below((len - at).min(5) + 1);
                new.remove_n(at, n);
                old.remove_n(at, n);
            }
            11 => {
                let keep = rng.below(len + 4);
                new.truncate(keep);
                old.truncate(keep);
            }
            12..=13 => assert_eq!(new.pop(), old.pop()),
            14 => {
                if rng.below(8) == 0 {
                    new.clear();
                    old.clear();
                }
            }
            _ => {
                // go on with a clone, and leave the column it was made of
                let other = new.clone();
                assert_eq!(new.to_vec(), other.to_vec());
                new = other;
            }
        }
        assert_eq!(new.to_vec(), old.to_vec());
        assert_eq!(new.len(), old.len());
        assert_eq!(new.first(), old.first());
        assert_eq!(new.last(), old.last());
        // the same slabs, and so the same of what is printed: but for a
        // cleared column, which has no slab where `DeltaColumn` keeps an
        // empty one
        if new.is_empty() {
            assert!(new.slab_count() <= old.slab_count() && old.slab_count() <= 1);
        } else {
            assert_eq!(new.slab_count(), old.slab_count());
            assert_eq!(
                format!("{new:?}"),
                format!("{old:?}").replace("DeltaColumn", "VerifiedDeltaColumn")
            );
        }
    }
    delta_agrees(&new, &old);
}

/// A verified delta column and the old one agree after the same simple
/// edits (`push`, `insert`, `remove`, `remove_n`, `truncate`, `pop`,
/// `clear`), for every type of value and with small slabs too.
#[test]
fn delta_simple_edits_agree() {
    for seed in 0..6 {
        for max_segments in [None, Some(2), Some(5)] {
            delta_simple_edits::<u32>(seed, max_segments);
            delta_simple_edits::<u64>(seed, max_segments);
            delta_simple_edits::<i32>(seed, max_segments);
            delta_simple_edits::<i64>(seed, max_segments);
            delta_simple_edits::<usize>(seed, max_segments);
            delta_simple_edits::<Option<u32>>(seed, max_segments);
            delta_simple_edits::<Option<u64>>(seed, max_segments);
            delta_simple_edits::<Option<i32>>(seed, max_segments);
            delta_simple_edits::<Option<i64>>(seed, max_segments);
            delta_simple_edits::<Option<usize>>(seed, max_segments);
        }
    }
}

/// The ends of a delta column: nothing to read or to pop of an empty
/// one, and a removal past the end changes nothing.
#[test]
fn delta_column_ends() {
    let mut col = VerifiedDeltaColumn::<Option<i64>>::new();
    assert_eq!(col.first(), None);
    assert_eq!(col.last(), None);
    assert_eq!(col.pop(), None);
    assert_eq!(col.to_vec(), vec![]);
    col.remove(0);
    col.remove_n(9, 0);
    col.truncate(3);
    col.clear();
    assert!(col.is_empty());
    for v in [Some(i64::MIN / 4), None, None, Some(-1), Some(i64::MAX / 4)] {
        col.push(v);
    }
    assert_eq!(col.first(), Some(Some(i64::MIN / 4)));
    assert_eq!(col.last(), Some(Some(i64::MAX / 4)));
    col.remove(5);
    assert_eq!(col.len(), 5);
    col.remove(1);
    assert_eq!(
        col.to_vec(),
        vec![Some(i64::MIN / 4), None, Some(-1), Some(i64::MAX / 4)]
    );
    assert_eq!(col.pop(), Some(Some(i64::MAX / 4)));
    col.truncate(1);
    assert_eq!(col.to_vec(), vec![Some(i64::MIN / 4)]);
    let other = col.clone();
    col.clear();
    assert!(col.is_empty());
    assert_eq!(other.to_vec(), vec![Some(i64::MIN / 4)]);
    assert_eq!(
        format!("{other:?}"),
        "VerifiedDeltaColumn { len: 1, slabs: 1 }"
    );
}

/// Bytes loaded as a delta column by both APIs: they load in both or in
/// neither, as the same values, and refused with the same error. Whether
/// they load.
fn delta_same_load<T: Deltad>(bytes: &[u8]) -> bool {
    match (
        VerifiedDeltaColumn::<T>::load(bytes),
        DeltaColumn::<T>::load(bytes),
    ) {
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
            // and it is laid out in the same slabs: but for no values,
            // which `DeltaColumn` can keep an empty slab for
            if v.is_empty() {
                assert!(v.slab_count() <= c.slab_count() && c.slab_count() <= 1);
            } else {
                assert_eq!(v.slab_count(), c.slab_count());
            }
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

/// What both delta columns save to, whole and unless they hold one value
/// only, and whether they do: the same bytes at the same place.
fn delta_saves_agree<T: Deltad>(new: &VerifiedDeltaColumn<T>, old: &DeltaColumn<T>, others: &[T]) {
    let values = old.to_vec();
    let bytes = old.save();
    assert_eq!(new.save(), bytes);
    assert!(delta_same_load::<T>(&bytes));

    let (mut a, mut b) = (vec![1u8, 2, 3], vec![1u8, 2, 3]);
    assert_eq!(new.save_to(&mut a), old.save_to(&mut b));
    assert_eq!(a, b);
    assert_eq!(a[3..], bytes[..]);

    for v in values.iter().take(2).chain(values.last()).chain(others) {
        let only = values.iter().all(|x| x == v);
        assert_eq!(new.is_only(*v), only);
        let (ra, rb) = (
            new.save_to_unless(&mut a, *v),
            old.save_to_unless(&mut b, *v),
        );
        assert_eq!(ra, rb);
        assert_eq!(a, b);
        assert_eq!(ra.is_empty(), only || bytes.is_empty());
        assert_eq!(&a[ra], if only { &[][..] } else { &bytes[..] });
    }
}

fn delta_saves<T: Deltad>(seed: u64, max_segments: Option<usize>) {
    let mut rng = Rng(seed.wrapping_mul(0x51_7cc1).wrapping_add(11) | 1);
    let mut last = 0;
    let mut some = |rng: &mut Rng, n: usize| -> Vec<T> {
        (0..n)
            .map(|_| {
                let (v, l) = T::near(rng, last);
                last = l;
                v
            })
            .collect()
    };
    let others = some(&mut rng, 3);
    let (mut new, mut old) = match max_segments {
        Some(m) => (
            VerifiedDeltaColumn::<T>::with_max_segments(m),
            DeltaColumn::<T>::with_max_segments(m),
        ),
        None => (
            VerifiedDeltaColumn::<T>::default(),
            DeltaColumn::<T>::default(),
        ),
    };
    delta_saves_agree(&new, &old, &others);
    for _ in 0..12 {
        let at = rng.below(old.len() + 1);
        let del = rng.below((old.len() - at).min(20) + 1);
        let n = rng.below(40);
        let values = some(&mut rng, n);
        new.splice(at, del, &values);
        old.splice(at, del, values);
        delta_saves_agree(&new, &old, &others);
    }
}

/// A verified delta column and the old one save to the same bytes, which
/// load as the same values in both, for every type of value, whatever
/// the edits that made the column and however small its slabs.
#[test]
fn delta_columns_save_the_same() {
    for seed in 0..8 {
        for m in [None, Some(4), Some(16)] {
            delta_saves::<u32>(seed, m);
            delta_saves::<u64>(seed, m);
            delta_saves::<i32>(seed, m);
            delta_saves::<i64>(seed, m);
            delta_saves::<usize>(seed, m);
            delta_saves::<Option<u32>>(seed, m);
            delta_saves::<Option<u64>>(seed, m);
            delta_saves::<Option<i32>>(seed, m);
            delta_saves::<Option<i64>>(seed, m);
            delta_saves::<Option<usize>>(seed, m);
        }
    }
}

fn delta_uniform<T: Deltad>(values: &[T], others: &[T]) {
    for m in [2, 4, 64] {
        let mut new = VerifiedDeltaColumn::<T>::with_max_segments(m);
        let mut old = DeltaColumn::<T>::with_max_segments(m);
        // one by one, so that small slabs are split
        for v in values {
            new.push(*v);
            old.push(*v);
        }
        delta_saves_agree(&new, &old, others);
    }
    let new = VerifiedDeltaColumn::<T>::from_values(values);
    let old = DeltaColumn::<T>::from_values(values.to_vec());
    delta_saves_agree(&new, &old, others);
}

/// Columns of one value only, and ones that all but are: both APIs say
/// which is which, and leave the same ones unwritten.
#[test]
fn delta_columns_of_one_value() {
    delta_uniform::<i64>(&[], &[0, 7]);
    delta_uniform::<i64>(&[7], &[0, 7, 8]);
    delta_uniform::<i64>(&[0; 9], &[0, 1, -1]);
    delta_uniform::<i64>(&[7; 300], &[0, 7, 8]);
    delta_uniform::<i64>(&[-7, -7, -7, -8], &[0, -7, -8]);
    delta_uniform::<i64>(&[8, 7, 7, 7], &[0, 7, 8]);
    delta_uniform::<i64>(&[7, 7, 8, 7, 7], &[0, 7, 8]);
    // a step and a step back: the last value is the first again
    delta_uniform::<i64>(&[7, 8, 7], &[0, 1, 7, 8]);
    delta_uniform::<u32>(&[u32::MAX; 40], &[0, u32::MAX, u32::MAX - 1]);
    delta_uniform::<u64>(&[1 << 62; 40], &[0, 1 << 62]);
    delta_uniform::<i32>(&[i32::MIN; 40], &[0, i32::MIN, i32::MAX]);
    delta_uniform::<usize>(&[3; 40], &[0, 3]);

    delta_uniform::<Option<i64>>(&[], &[None, Some(0)]);
    delta_uniform::<Option<i64>>(&[None], &[None, Some(0)]);
    delta_uniform::<Option<i64>>(&[None; 300], &[None, Some(0), Some(5)]);
    delta_uniform::<Option<i64>>(&[Some(5); 300], &[None, Some(0), Some(5)]);
    delta_uniform::<Option<i64>>(&[Some(0); 30], &[None, Some(0), Some(5)]);
    // a null between two of the same number is another value
    delta_uniform::<Option<i64>>(&[Some(5), None, Some(5)], &[None, Some(0), Some(5)]);
    delta_uniform::<Option<i64>>(&[None, Some(5), Some(5)], &[None, Some(0), Some(5)]);
    delta_uniform::<Option<i64>>(&[Some(5), Some(5), None], &[None, Some(0), Some(5)]);
    delta_uniform::<Option<i64>>(&[None, None, Some(0)], &[None, Some(0), Some(5)]);
    delta_uniform::<Option<u32>>(&[Some(u32::MAX); 40], &[None, Some(u32::MAX), Some(0)]);
    delta_uniform::<Option<u64>>(&[None, None, Some(9), None], &[None, Some(9)]);
}

/// The saved bytes of `values`, as the old API saves them.
fn delta_bytes<T: Deltad>(values: &[T]) -> Vec<u8> {
    DeltaColumn::<T>::from_values(values.to_vec()).save()
}

/// Whether `bytes` load as a delta column of each type: the same in
/// both APIs.
fn delta_loads(bytes: &[u8]) -> [bool; 10] {
    [
        delta_same_load::<u32>(bytes),
        delta_same_load::<u64>(bytes),
        delta_same_load::<i32>(bytes),
        delta_same_load::<i64>(bytes),
        delta_same_load::<usize>(bytes),
        delta_same_load::<Option<u32>>(bytes),
        delta_same_load::<Option<u64>>(bytes),
        delta_same_load::<Option<i32>>(bytes),
        delta_same_load::<Option<i64>>(bytes),
        delta_same_load::<Option<usize>>(bytes),
    ]
}

/// A delta column loads only as a type that has each of its values: the
/// load of one saved from a wider type is refused where a value is not
/// one of the narrower type's, by both APIs alike.
#[test]
fn delta_loads_keep_to_the_type() {
    const Y: bool = true;
    const N: bool = false;
    // where a `usize` is as wide as a `u64`
    const W: bool = usize::BITS == 64;
    // u32, u64, i32, i64, usize, and then the options of them
    assert_eq!(delta_loads(&[]), [Y; 10]);
    assert_eq!(delta_loads(&delta_bytes::<i64>(&[3, 4, 5])), [Y; 10]);
    assert_eq!(
        delta_loads(&delta_bytes::<i64>(&[3, -1, 5])),
        [N, N, Y, Y, N, N, N, Y, Y, N]
    );
    assert_eq!(
        delta_loads(&delta_bytes::<i64>(&[-1])),
        [N, N, Y, Y, N, N, N, Y, Y, N]
    );
    let wide = delta_bytes::<i64>(&[0, u32::MAX as i64]);
    assert_eq!(delta_loads(&wide), [Y, Y, N, Y, Y, Y, Y, N, Y, Y]);
    let wider = delta_bytes::<i64>(&[0, u32::MAX as i64 + 1, 0]);
    assert_eq!(delta_loads(&wider), [N, Y, N, Y, W, N, Y, N, Y, W]);
    let low = delta_bytes::<i64>(&[i32::MIN as i64 - 1, 0]);
    assert_eq!(delta_loads(&low), [N, N, N, Y, N, N, N, N, Y, N]);
    // the ends of each type are values of it
    let ends = delta_bytes::<i64>(&[i32::MAX as i64, i32::MIN as i64, i32::MAX as i64]);
    assert_eq!(delta_loads(&ends), [N, N, Y, Y, N, N, N, Y, Y, N]);
    let top = delta_bytes::<i64>(&[i64::MAX, i64::MAX - 1, i64::MAX]);
    assert_eq!(delta_loads(&top), [N, Y, N, Y, W, N, Y, N, Y, W]);
    // a value that leaves the type and comes back within one run
    let out = delta_bytes::<i64>(&[2, 1, 0, -1, -2, -1, 0, 1, 2]);
    assert_eq!(delta_loads(&out), [N, N, Y, Y, N, N, N, Y, Y, N]);
    // a null is a value only of the options
    let nulls = delta_bytes::<Option<i64>>(&[Some(3), None, Some(4)]);
    assert_eq!(delta_loads(&nulls), [N, N, N, N, N, Y, Y, Y, Y, Y]);
    let only_nulls = delta_bytes::<Option<i64>>(&[None, None]);
    assert_eq!(delta_loads(&only_nulls), [N, N, N, N, N, Y, Y, Y, Y, Y]);
    let both = delta_bytes::<Option<i64>>(&[None, Some(-3), None]);
    assert_eq!(delta_loads(&both), [N, N, N, N, N, N, N, Y, Y, N]);
}

/// Saved delta columns with a byte changed, cut short or with bytes
/// added: both APIs load the same of them, as the same values, and
/// refuse the others with the same error.
#[test]
fn delta_loads_agree_on_any_bytes() {
    let mut loaded = 0;
    let mut refused = 0;
    for seed in 0..40u64 {
        let mut rng = Rng(seed.wrapping_mul(0x2f_9be5).wrapping_add(3) | 1);
        let mut last = 0;
        let n = rng.below(60);
        let values: Vec<Option<i64>> = (0..n)
            .map(|_| {
                let (v, l) = <Option<i64> as Deltad>::near(&mut rng, last);
                last = l;
                // nulls in some columns only
                if seed % 2 == 0 {
                    v.or(Some(l))
                } else {
                    v
                }
            })
            .collect();
        let bytes = delta_bytes(&values);
        for _ in 0..40 {
            let mut b = bytes.clone();
            match rng.below(4) {
                0 if !b.is_empty() => {
                    let i = rng.below(b.len());
                    b[i] ^= 1 << rng.below(8);
                }
                1 if !b.is_empty() => {
                    let i = rng.below(b.len());
                    b[i] = rng.next() as u8;
                }
                2 => b.truncate(rng.below(b.len() + 1)),
                _ => {
                    for _ in 0..rng.below(4) + 1 {
                        b.push(rng.next() as u8);
                    }
                }
            }
            for ok in delta_loads(&b) {
                if ok {
                    loaded += 1;
                } else {
                    refused += 1;
                }
            }
        }
    }
    // the test is of both
    assert!(loaded > 1000, "{loaded} loaded");
    assert!(refused > 1000, "{refused} refused");
}

/// Whether a run passes the check that the old API makes of a run put
/// into a column of `T`, and is no run of nulls where `T` has no null:
/// worked out here, apart from either API.
fn delta_run_passes<T: Deltad>(r: &hexane::DeltaRun) -> bool {
    if r.count == 0 {
        return true;
    }
    let Some(d) = r.delta else {
        return T::NULLABLE;
    };
    if T::MIN_I64 == i64::MIN && T::MAX_I64 == i64::MAX {
        return true;
    }
    let first = r.prefix as i128 + d as i128;
    let last = r.prefix as i128 + d as i128 * r.count as i128;
    let within = |x: i128| T::MIN_I64 as i128 <= x && x <= T::MAX_I64 as i128;
    within(first) && within(last)
}

/// Whether a run has a value outside the type: its first or its last,
/// worked out with no wrapping, where neither leaves `i64`.
fn delta_run_leaves<T: Deltad>(r: &hexane::DeltaRun) -> bool {
    let Some(d) = r.delta else {
        return false;
    };
    if r.count == 0 {
        return false;
    }
    let first = r.prefix as i128 + d as i128;
    let last = r.prefix as i128 + d as i128 * r.count as i128;
    let wide = |x: i128| i64::MIN as i128 <= x && x <= i64::MAX as i128;
    let within = |x: i128| T::MIN_I64 as i128 <= x && x <= T::MAX_I64 as i128;
    wide(first) && wide(last) && !(within(first) && within(last))
}

/// The old API panics at a run it refuses. So does the verified one at a
/// run with a value outside the type, where its `requires` is not met: a
/// caller that is not verified is stopped all the same.
#[cfg(any(target_pointer_width = "64", panic = "unwind"))]
fn delta_run_refused<T: Deltad>(run: hexane::DeltaRun) {
    let old = panic_of(move || {
        let mut old = DeltaColumn::<T>::new();
        old.splice_runs(0, 0, [run]);
    });
    assert!(old.is_some(), "the old API takes {run:?}");
    if run.count <= 9 && delta_run_leaves::<T>(&run) {
        let new = panic_of(move || {
            let mut new = VerifiedDeltaColumn::<T>::new();
            new.splice_runs(0, 0, &[run]);
        });
        assert!(new.is_some(), "the verified API takes {run:?}");
    }
}

#[cfg(not(any(target_pointer_width = "64", panic = "unwind")))]
fn delta_run_refused<T: Deltad>(_run: hexane::DeltaRun) {}

/// Whether the values of a run, put into an empty column, lie 2^63 or
/// more apart as the column sums them: each the first and so many times
/// the delta, with no wrapping.
fn delta_run_spans(r: &hexane::DeltaRun) -> bool {
    let Some(d) = r.delta else {
        return false;
    };
    if r.count == 0 {
        return false;
    }
    let first = r.prefix.wrapping_add(d) as i128;
    let last = first + d as i128 * (r.count - 1) as i128;
    let within = |x: i128| i64::MIN as i128 <= x && x <= i64::MAX as i128;
    !within(last) || (last - first).abs() > i64::MAX as i128
}

/// Both APIs panic alike at a run whose values lie 2^63 or more apart.
#[cfg(any(target_pointer_width = "64", panic = "unwind"))]
fn delta_run_too_wide<T: Deltad>(run: hexane::DeltaRun) {
    let new = panic_of(move || {
        let mut new = VerifiedDeltaColumn::<T>::new();
        new.splice_runs(0, 0, &[run]);
    });
    let old = panic_of(move || {
        let mut old = DeltaColumn::<T>::new();
        old.splice_runs(0, 0, [run]);
    });
    assert!(new.is_some(), "the verified API takes {run:?}");
    assert_eq!(new, old, "{run:?}");
}

#[cfg(not(any(target_pointer_width = "64", panic = "unwind")))]
fn delta_run_too_wide<T: Deltad>(_run: hexane::DeltaRun) {}

/// `run_fits` of a verified delta column is the old API's check, for the
/// runs of a table: what it passes both APIs take alike, or panic at
/// alike where its values lie too far apart, and what it refuses the old
/// API panics at.
fn delta_run_checks<T: Deltad>() {
    let (lo, hi) = (T::MIN_I64, T::MAX_I64);
    let prefixes = [
        0,
        1,
        -1,
        10,
        lo,
        hi,
        lo.saturating_add(3),
        hi.saturating_sub(3),
        i64::MAX,
        i64::MIN,
    ];
    let deltas = [
        None,
        Some(0),
        Some(1),
        Some(-1),
        Some(3),
        Some(-3),
        Some(1 << 31),
        Some(-(1 << 31)),
        Some(i64::MAX),
        Some(i64::MIN),
    ];
    let counts = [0, 1, 2, 3, 9, usize::MAX / 2, usize::MAX];
    let (mut passed, mut refused, mut wide) = (0, 0, 0);
    for prefix in prefixes {
        for delta in deltas {
            for count in counts {
                let run = hexane::DeltaRun {
                    prefix,
                    delta,
                    count,
                };
                let fits = VerifiedDeltaColumn::<T>::run_fits(&run);
                assert_eq!(fits, delta_run_passes::<T>(&run), "{run:?}");
                if !fits {
                    refused += 1;
                    delta_run_refused::<T>(run);
                } else if count <= 9 && delta_run_spans(&run) {
                    wide += 1;
                    delta_run_too_wide::<T>(run);
                } else if count <= 9 {
                    passed += 1;
                    let mut new = VerifiedDeltaColumn::<T>::new();
                    let mut old = DeltaColumn::<T>::new();
                    new.splice_runs(0, 0, &[run]);
                    old.splice_runs(0, 0, [run]);
                    delta_agrees(&new, &old);
                    assert_eq!(new.len(), count);
                    assert_eq!(new.save(), old.save());
                }
            }
        }
    }
    assert!(passed > 100);
    // values that lie too far apart are of the widest types alone
    assert_eq!(wide > 0, lo == i64::MIN && hi == i64::MAX);
    // a type with a null and every `i64` takes every run
    assert_eq!(
        refused == 0,
        T::NULLABLE && lo == i64::MIN && hi == i64::MAX
    );
}

/// A run is checked by a verified delta column as by the old one, for
/// every type of value.
#[test]
fn delta_run_checks_agree() {
    delta_run_checks::<u32>();
    delta_run_checks::<u64>();
    delta_run_checks::<i32>();
    delta_run_checks::<i64>();
    delta_run_checks::<usize>();
    delta_run_checks::<Option<u32>>();
    delta_run_checks::<Option<u64>>();
    delta_run_checks::<Option<i32>>();
    delta_run_checks::<Option<i64>>();
    delta_run_checks::<Option<usize>>();
}

/// Runs spliced into a verified delta column and into the old one: the
/// runs another column lays its values out in, and runs made up. Both
/// hold the same values after each splice, in as many slabs, and save to
/// the same bytes; and a splice of nothing changes neither.
fn delta_run_splices<T: Deltad>(seed: u64, max_segments: Option<usize>) {
    let mut rng = Rng(seed.wrapping_mul(0x2545_f491).wrapping_add(5) | 1);
    let mut last = 0;
    let mut some = |rng: &mut Rng, n: usize| -> Vec<T> {
        (0..n)
            .map(|_| {
                let (v, l) = T::near(rng, last);
                last = l;
                v
            })
            .collect()
    };
    let (mut new, mut old) = match max_segments {
        Some(m) => (
            VerifiedDeltaColumn::<T>::with_max_segments(m),
            DeltaColumn::<T>::with_max_segments(m),
        ),
        None => (
            VerifiedDeltaColumn::<T>::default(),
            DeltaColumn::<T>::default(),
        ),
    };
    let n = rng.below(40);
    let start = some(&mut rng, n);
    new.splice(0, 0, &start);
    old.splice(0, 0, start);
    let mut spliced = 0;
    for _ in 0..40 {
        let at = rng.below(old.len() + 1);
        let del = rng.below((old.len() - at).min(6) + 1);
        let runs: Vec<hexane::DeltaRun> = if rng.below(2) == 0 {
            let n = rng.below(30);
            let src = DeltaColumn::<T>::from_values(some(&mut rng, n));
            let runs = src.iter().runs().collect();
            runs
        } else {
            (0..rng.below(4))
                .map(|_| {
                    let (_, prefix) = T::near(&mut rng, 0);
                    let delta = match rng.below(6) {
                        0 => None,
                        _ => Some(rng.below(9) as i64 - 4),
                    };
                    let count = rng.below(6);
                    hexane::DeltaRun {
                        prefix,
                        delta,
                        count,
                    }
                })
                .filter(VerifiedDeltaColumn::<T>::run_fits)
                .collect()
        };
        for run in &runs {
            assert!(VerifiedDeltaColumn::<T>::run_fits(run), "{run:?}");
            assert!(delta_run_passes::<T>(run), "{run:?}");
        }
        let len = old.len();
        let added: usize = runs.iter().map(|r| r.count).sum();
        spliced += added;
        new.splice_runs(at, del, &runs);
        old.splice_runs(at, del, runs.iter().copied());
        delta_agrees(&new, &old);
        assert_eq!(new.len(), len - del + added);
        assert_eq!(new.save(), old.save());
        if !old.is_empty() {
            assert_eq!(new.slab_count(), old.slab_count(), "{runs:?}");
        }
        // a splice of nothing leaves both as they are
        let at = rng.below(old.len() + 1);
        let slabs = new.slab_count();
        new.splice(at, 0, &[]);
        old.splice(at, 0, Vec::<T>::new());
        assert_eq!(new.slab_count(), slabs);
        if !old.is_empty() {
            assert_eq!(new.slab_count(), old.slab_count());
        }
    }
    assert!(spliced > 0);
}

/// A verified delta column and the old one take runs alike, for every
/// type of value and in slabs of several sizes.
#[test]
fn delta_columns_take_runs() {
    for seed in 0..9 {
        let m = [None, Some(2), Some(5)][seed as usize % 3];
        delta_run_splices::<u32>(seed, m);
        delta_run_splices::<u64>(seed, m);
        delta_run_splices::<i32>(seed, m);
        delta_run_splices::<i64>(seed, m);
        delta_run_splices::<usize>(seed, m);
        delta_run_splices::<Option<u32>>(seed, m);
        delta_run_splices::<Option<u64>>(seed, m);
        delta_run_splices::<Option<i32>>(seed, m);
        delta_run_splices::<Option<i64>>(seed, m);
        delta_run_splices::<Option<usize>>(seed, m);
    }
}

/// Bytes loaded with options by both APIs, as a delta column of the
/// type: they load in both or in neither, as the same values in as many
/// slabs, and are refused with the same error. Whether they load.
macro_rules! delta_same_load_with {
    ($t:ty, $bytes:expr, $opts:expr) => {{
        let bytes: &[u8] = $bytes;
        match (
            VerifiedDeltaColumn::<$t>::load_with(bytes, $opts),
            DeltaColumn::<$t>::load_with(bytes, $opts),
        ) {
            (Ok(v), Ok(c)) => {
                assert_eq!(v.len(), c.len());
                // a fill can be a run of billions: read those in places
                if v.len() <= 10_000 {
                    assert_eq!(v.to_vec(), c.to_vec());
                } else {
                    for i in [0, 1, v.len() / 2, v.len() - 1] {
                        assert_eq!(v.get(i), c.get(i));
                    }
                }
                assert_eq!(v.save(), c.save());
                if !c.is_empty() {
                    assert_eq!(v.slab_count(), c.slab_count());
                }
                Some(v)
            }
            (Err(v), Err(c)) => {
                assert_eq!(v.to_string(), c.to_string());
                None
            }
            (v, c) => panic!(
                "only one API loads {bytes:?}: the verified one {}, the old one {}",
                v.is_ok(),
                c.is_ok()
            ),
        }
    }};
}

/// No bytes loaded with a length and a fill, and with one of the two,
/// by both APIs, in slabs of several sizes. How many of the loads load.
///
/// Each type's loads are a function of their own: all of them in one
/// are more than a WebAssembly engine takes in a function.
macro_rules! delta_fills {
    ($t:ty, $f:ty, $fills:expr) => {{
        fn fills(fills: &[$f]) -> usize {
            let lens = [
                0,
                1,
                2,
                3,
                1000,
                20_000,
                usize::MAX / 7,
                usize::MAX / 2,
                usize::MAX / 2 + 1,
                usize::MAX - 3,
                usize::MAX - 2,
                usize::MAX,
            ];
            let mut loads = 0;
            for &fill in fills {
                for len in lens {
                    for m in [2, 5, 64] {
                        let opts = || LoadOpts::new().with_max_segments(m).with_length(len);
                        if let Some(v) = delta_same_load_with!($t, &[], opts().with_fill(fill)) {
                            assert_eq!(v.len(), len);
                            loads += 1;
                        }
                        // a length alone is no fill, and a fill alone fills nothing
                        let v = delta_same_load_with!($t, &[], opts());
                        assert_eq!(v.is_some(), len == 0);
                        let v = delta_same_load_with!($t, &[], LoadOpts::new().with_fill(fill));
                        assert!(v.is_some_and(|v| v.is_empty()));
                    }
                }
            }
            loads
        }
        fills(&$fills)
    }};
}

/// A fill loads in a verified delta column as in the old one, for every
/// type of value: as the same values, or refused alike.
#[test]
fn delta_fills_agree() {
    let nums = [
        0,
        1,
        -1,
        7,
        -7,
        1 << 20,
        u32::MAX as i64,
        i64::MAX / 3,
        i64::MAX,
        i64::MIN,
    ];
    let opts = [
        None,
        Some(0),
        Some(1),
        Some(-1),
        Some(7),
        Some(1 << 20),
        Some(i64::MAX),
        Some(i64::MIN),
    ];
    let mut loads = 0;
    loads += delta_fills!(u32, i64, nums);
    loads += delta_fills!(u64, i64, nums);
    loads += delta_fills!(i32, i64, nums);
    loads += delta_fills!(i64, i64, nums);
    loads += delta_fills!(usize, i64, nums);
    loads += delta_fills!(Option<u32>, Option<i64>, opts);
    loads += delta_fills!(Option<u64>, Option<i64>, opts);
    loads += delta_fills!(Option<i32>, Option<i64>, opts);
    loads += delta_fills!(Option<i64>, Option<i64>, opts);
    loads += delta_fills!(Option<usize>, Option<i64>, opts);
    assert!(loads > 500);

    // the fill is the difference between neighbours
    let col =
        VerifiedDeltaColumn::<u64>::load_with(&[], LoadOpts::new().with_length(3).with_fill(2));
    assert_eq!(col.unwrap().to_vec(), vec![2, 4, 6]);
    let col = VerifiedDeltaColumn::<Option<i32>>::load_with(
        &[],
        LoadOpts::new().with_length(3).with_fill(Some(-5)),
    );
    assert_eq!(col.unwrap().to_vec(), vec![Some(-5), Some(-10), Some(-15)]);
    let col = VerifiedDeltaColumn::<Option<u32>>::load_with(
        &[],
        LoadOpts::new().with_length(2).with_fill(None),
    );
    assert_eq!(col.unwrap().to_vec(), vec![None, None]);
    // and a fill that leaves the type is refused
    let col =
        VerifiedDeltaColumn::<u32>::load_with(&[], LoadOpts::new().with_length(3).with_fill(-1));
    assert!(col.is_err());
    let col = VerifiedDeltaColumn::<u32>::load_with(
        &[],
        LoadOpts::new().with_length(3).with_fill(1 << 31),
    );
    assert!(col.is_err());
}

/// A saved delta column loaded with options by both APIs: with its
/// length and with another, with a fill and without, in slabs of several
/// sizes. Each type's loads are a function of their own, as a fill's.
macro_rules! delta_loads_with {
    ($t:ty, $seed:expr, $fill:expr) => {{
        fn loads(seed: u64) {
            let mut rng = Rng(seed.wrapping_mul(0x6c07_8965).wrapping_add(3) | 1);
            let mut last = 0;
            let n = 1 + rng.below(200);
            let values: Vec<$t> = (0..n)
                .map(|_| {
                    let (v, l) = <$t as Deltad>::near(&mut rng, last);
                    last = l;
                    v
                })
                .collect();
            let bytes = delta_bytes::<$t>(&values);
            for m in [2, 3, 64] {
                let opts = || LoadOpts::new().with_max_segments(m);
                let v = delta_same_load_with!($t, &bytes, opts());
                assert_eq!(v.unwrap().to_vec(), values);
                let v = delta_same_load_with!($t, &bytes, opts().with_length(n));
                assert_eq!(v.unwrap().to_vec(), values);
                // bytes are read whatever the fill
                let v = delta_same_load_with!($t, &bytes, opts().with_length(n).with_fill($fill));
                assert_eq!(v.unwrap().to_vec(), values);
                let v = delta_same_load_with!($t, &bytes, opts().with_fill($fill));
                assert_eq!(v.unwrap().to_vec(), values);
                for other in [0, n - 1, n + 1] {
                    let v = delta_same_load_with!($t, &bytes, opts().with_length(other));
                    assert!(v.is_none());
                    let v = delta_same_load_with!(
                        $t,
                        &bytes,
                        opts().with_length(other).with_fill($fill)
                    );
                    assert!(v.is_none());
                }
            }
        }
        loads($seed as u64)
    }};
}

/// A saved delta column loads with options in a verified delta column
/// as in the old one, for every type of value.
#[test]
fn delta_columns_load_with_options() {
    for seed in 0..6 {
        delta_loads_with!(u32, seed, 1);
        delta_loads_with!(u64, seed, 1);
        delta_loads_with!(i32, seed, -1);
        delta_loads_with!(i64, seed, 0);
        delta_loads_with!(usize, seed, 2);
        delta_loads_with!(Option<u32>, seed, Some(1));
        delta_loads_with!(Option<u64>, seed, None);
        delta_loads_with!(Option<i32>, seed, Some(-1));
        delta_loads_with!(Option<i64>, seed, None);
        delta_loads_with!(Option<usize>, seed, Some(0));
    }
}

// ── Delta columns: the iterator ─────────────────────────────────────────────

/// A move of a delta column's iterator.
#[derive(Clone, Debug)]
enum DeltaMove {
    Next,
    Nth(usize),
    SetMax(usize),
    AdvanceBy(usize),
    AdvanceTo(usize),
    Shift(Range<usize>),
    ShiftNext(Range<usize>),
    Copy(usize),
}

/// What the specification of `VerifiedDeltaIter` says of an iterator
/// over `model`: where it stands and where its window ends, with the
/// values of `model` between the two left to give.
struct DeltaWalk<'a, T> {
    model: &'a [T],
    pos: usize,
    end: usize,
}

impl<T: Deltad> DeltaWalk<'_, T> {
    /// How many values the iterator has left.
    fn left(&self) -> usize {
        self.end.min(self.model.len()).saturating_sub(self.pos)
    }

    /// Where the iterator stands, where its window ends and how many
    /// values it has left.
    fn place(&self) -> (usize, usize, usize) {
        (self.pos, self.end, self.left())
    }

    /// A window that ends at `end` and has no value for the iterator
    /// leaves it there, if that is ahead of it and in the column, with
    /// its window closed.
    fn closed_at(&mut self, end: usize) {
        self.pos = end.clamp(self.pos, self.model.len());
        self.end = self.pos;
    }

    fn nth(&mut self, n: usize) -> Option<T> {
        if n < self.left() {
            let v = self.model[self.pos + n];
            self.pos += n + 1;
            Some(v)
        } else {
            self.pos += self.left();
            self.end = self.pos;
            None
        }
    }

    fn advance_by(&mut self, amount: usize) {
        if amount <= self.left() {
            self.pos += amount;
        } else {
            self.pos += self.left();
            self.end = self.pos;
        }
    }

    /// What a move gives, and where it leaves the iterator.
    fn does(&mut self, mv: &DeltaMove) -> Option<T> {
        let len = self.model.len();
        match mv.clone() {
            DeltaMove::Next => return self.nth(0),
            DeltaMove::Nth(n) => return self.nth(n),
            DeltaMove::SetMax(end) => self.end = end.max(self.pos),
            DeltaMove::AdvanceBy(amount) => self.advance_by(amount),
            DeltaMove::AdvanceTo(target) => self.advance_by(target.saturating_sub(self.pos)),
            DeltaMove::Shift(range) => {
                if range.start <= self.pos {
                    self.end = range.end.max(self.pos);
                } else if range.start <= range.end && range.start <= len {
                    self.pos = range.start;
                    self.end = range.end;
                } else {
                    self.closed_at(range.end);
                }
            }
            DeltaMove::ShiftNext(range) => {
                if range.start < range.end.min(len) {
                    self.pos = range.start + 1;
                    self.end = range.end;
                    return Some(self.model[range.start]);
                }
                self.closed_at(range.end);
            }
            DeltaMove::Copy(n) => {
                let mut copy = DeltaWalk {
                    model: self.model,
                    pos: self.pos,
                    end: self.end,
                };
                return copy.nth(n);
            }
        }
        None
    }
}

/// The move made of a verified delta column's iterator.
fn new_delta_does<T: Deltad>(
    x: &mut hexane::verified::VerifiedDeltaIter<'_, T>,
    mv: &DeltaMove,
) -> Option<T> {
    match mv.clone() {
        DeltaMove::Next => return x.next(),
        DeltaMove::Nth(n) => return x.nth(n),
        DeltaMove::SetMax(end) => x.set_max(end),
        DeltaMove::AdvanceBy(amount) => x.advance_by(amount),
        DeltaMove::AdvanceTo(target) => x.advance_to(target),
        DeltaMove::Shift(range) => x.shift(range),
        DeltaMove::ShiftNext(range) => return x.shift_next(range),
        DeltaMove::Copy(n) => {
            let mut copy = x.clone();
            assert_eq!(format!("{copy:?}"), format!("{x:?}"));
            return copy.nth(n);
        }
    }
    None
}

/// The move made of `DeltaColumn`'s iterator.
fn old_delta_does<T: Deltad>(y: &mut hexane::DeltaIter<'_, T>, mv: &DeltaMove) -> Option<T> {
    match mv.clone() {
        DeltaMove::Next => return y.next(),
        DeltaMove::Nth(n) => return y.nth(n),
        DeltaMove::SetMax(end) => y.set_max(end),
        DeltaMove::AdvanceBy(amount) => y.advance_by(amount),
        DeltaMove::AdvanceTo(target) => y.advance_to(target),
        DeltaMove::Shift(range) => y.shift(range),
        DeltaMove::ShiftNext(range) => return y.shift_next(range),
        DeltaMove::Copy(n) => return y.clone().nth(n),
    }
    None
}

/// A move for an iterator that stands as `m` does: mostly within what it
/// has left, sometimes past it.
fn a_delta_move<T: Deltad>(rng: &mut Rng, m: &DeltaWalk<'_, T>) -> DeltaMove {
    let len = m.model.len();
    // how many values to pass over
    let count = |rng: &mut Rng| match rng.below(4) {
        0 => a_place(rng, len),
        _ => rng.below(m.left() + 2),
    };
    match rng.below(14) {
        0..=2 => DeltaMove::Next,
        3 | 4 => DeltaMove::Nth(count(rng)),
        5 | 6 => DeltaMove::SetMax(a_place(rng, len)),
        7 => DeltaMove::AdvanceBy(a_place(rng, len - m.pos)),
        8 => DeltaMove::AdvanceTo(a_place(rng, len)),
        // any range: one that starts behind the iterator, one that ends
        // before it starts
        9 | 10 => DeltaMove::Shift(a_place(rng, len)..a_place(rng, len)),
        11 | 12 => {
            // a range that starts at the iterator or after it
            let start = m.pos + a_place(rng, len - m.pos).min(usize::MAX - m.pos);
            DeltaMove::ShiftNext(start..a_place(rng, len))
        }
        _ => DeltaMove::Copy(rng.below(3)),
    }
}

/// Where a verified delta column's iterator stands, where its window
/// ends and how many values it has left: asked each way there is.
fn delta_place<T: Deltad>(x: &hexane::verified::VerifiedDeltaIter<'_, T>) -> (usize, usize, usize) {
    assert_eq!(x.size_hint(), (x.len(), Some(x.len())));
    (x.pos(), x.end_pos(), x.len())
}

/// The same of `DeltaColumn`'s iterator.
fn old_delta_place<T: Deltad>(y: &hexane::DeltaIter<'_, T>) -> (usize, usize, usize) {
    (y.pos(), y.end_pos(), y.len())
}

/// A verified delta column and the old one of the same `n` values, laid
/// out in the same slabs, with the values.
fn delta_pair<T: Deltad>(
    rng: &mut Rng,
    n: usize,
    max_segments: Option<usize>,
) -> (VerifiedDeltaColumn<T>, DeltaColumn<T>, Vec<T>) {
    let (mut new, mut old) = match max_segments {
        Some(m) => (
            VerifiedDeltaColumn::<T>::with_max_segments(m),
            DeltaColumn::<T>::with_max_segments(m),
        ),
        None => (
            VerifiedDeltaColumn::<T>::default(),
            DeltaColumn::<T>::default(),
        ),
    };
    let mut last = 0;
    let mut model = Vec::new();
    for _ in 0..n {
        let (v, l) = T::near(rng, last);
        last = l;
        // now and then at a place before the end, so that slabs are cut
        let at = match rng.below(6) {
            0 => rng.below(model.len() + 1),
            _ => model.len(),
        };
        new.insert(at, v);
        old.insert(at, v);
        model.insert(at, v);
    }
    assert_eq!(new.slab_count(), old.slab_count());
    (new, old, model)
}

/// A delta column's iterator gives the column's values in order and
/// stands where its specification says under every move, as
/// `DeltaColumn`'s iterator does: the two are asked the same at each
/// step, with a walk of the values that follows the specification.
fn delta_iters<T: Deltad>(seed: u64, max_segments: Option<usize>) {
    let mut rng = Rng(seed.wrapping_mul(0x51_7cc1_b727).wrapping_add(11) | 1);
    let n = match rng.below(5) {
        0 => rng.below(4),
        _ => rng.below(200),
    };
    let (new, old, model) = delta_pair::<T>(&mut rng, n, max_segments);
    let len = model.len();
    // all of it: in a `for` loop over the column and over its iterator,
    // and by the standard library's functions
    let mut seen = Vec::new();
    for v in &new {
        seen.push(v);
    }
    assert_eq!(seen, model);
    assert_eq!(new.iter().collect::<Vec<T>>(), model);
    assert_eq!(new.iter().count(), len);
    assert_eq!(new.iter().last(), model.last().copied());
    assert_eq!(new.iter().len(), old.iter().len());
    assert_eq!(
        format!("{:?}", new.iter()),
        format!("{:?}", old.iter()).replace("DeltaIter", "VerifiedDeltaIter")
    );
    // the values of a range
    for _ in 0..4 {
        let start = rng.below(len + 1);
        let end = start + rng.below(len - start + 1);
        let x = new.iter_range(start, end);
        let y = old.iter_range(start..end);
        assert_eq!(delta_place(&x), (start, end, end - start));
        assert_eq!(delta_place(&x), old_delta_place(&y));
        assert_eq!(x.collect::<Vec<T>>(), model[start..end]);
        assert_eq!(y.collect::<Vec<T>>(), model[start..end]);
    }
    // any moves
    for _ in 0..6 {
        let mut x = new.iter();
        let mut y = old.iter();
        let mut m = DeltaWalk {
            model: &model,
            pos: 0,
            end: len,
        };
        for _ in 0..40 {
            let mv = a_delta_move(&mut rng, &m);
            let want = m.does(&mv);
            assert_eq!(new_delta_does(&mut x, &mv), want, "{mv:?}");
            assert_eq!(old_delta_does(&mut y, &mv), want, "{mv:?}");
            assert_eq!(delta_place(&x), m.place(), "{mv:?}");
            assert_eq!(old_delta_place(&y), m.place(), "{mv:?}");
            assert_eq!(
                format!("{x:?}"),
                format!("{y:?}").replace("DeltaIter", "VerifiedDeltaIter"),
                "{mv:?}"
            );
        }
    }
}

/// A verified delta column's iterator and the old one's give the same
/// values and stand in the same places under the same moves, which are
/// the places the specification says: for every type of value, and with
/// small slabs too.
#[test]
fn delta_iters_agree() {
    for seed in 0..8 {
        for max_segments in [None, Some(2), Some(5)] {
            delta_iters::<u32>(seed, max_segments);
            delta_iters::<u64>(seed, max_segments);
            delta_iters::<i32>(seed, max_segments);
            delta_iters::<i64>(seed, max_segments);
            delta_iters::<usize>(seed, max_segments);
            delta_iters::<Option<u32>>(seed, max_segments);
            delta_iters::<Option<u64>>(seed, max_segments);
            delta_iters::<Option<i32>>(seed, max_segments);
            delta_iters::<Option<i64>>(seed, max_segments);
            delta_iters::<Option<usize>>(seed, max_segments);
        }
    }
}

/// The ends of a delta column's iterator: one of no column and one of an
/// empty column give nothing wherever they are moved, as `DeltaColumn`'s
/// do, and a read to the end closes the window where it stops.
#[test]
fn delta_iter_ends() {
    let mut x = hexane::verified::VerifiedDeltaIter::<Option<i64>>::default();
    let mut y = hexane::DeltaIter::<Option<i64>>::default();
    // (a range that ends before it starts, which is no slip here)
    let (from, to) = (5, 1);
    let moves = [
        DeltaMove::Next,
        DeltaMove::SetMax(4),
        DeltaMove::Nth(2),
        DeltaMove::AdvanceBy(3),
        DeltaMove::AdvanceTo(7),
        DeltaMove::Shift(2..9),
        DeltaMove::ShiftNext(0..3),
        DeltaMove::Shift(from..to),
        DeltaMove::Copy(0),
        DeltaMove::SetMax(usize::MAX),
        DeltaMove::Nth(usize::MAX),
        DeltaMove::AdvanceBy(usize::MAX),
    ];
    let mut m = DeltaWalk::<Option<i64>> {
        model: &[],
        pos: 0,
        end: 0,
    };
    assert_eq!(delta_place(&x), (0, 0, 0));
    assert_eq!(old_delta_place(&y), (0, 0, 0));
    for mv in &moves {
        assert_eq!(m.does(mv), None);
        assert_eq!(new_delta_does(&mut x, mv), None, "{mv:?}");
        assert_eq!(old_delta_does(&mut y, mv), None, "{mv:?}");
        assert_eq!(delta_place(&x), m.place(), "{mv:?}");
        assert_eq!(old_delta_place(&y), m.place(), "{mv:?}");
    }
    let new = VerifiedDeltaColumn::<Option<i64>>::new();
    let old = DeltaColumn::<Option<i64>>::new();
    let (mut x, mut y) = (new.iter(), old.iter());
    m.end = 0;
    for mv in &moves {
        assert_eq!(m.does(mv), None);
        assert_eq!(new_delta_does(&mut x, mv), None, "{mv:?}");
        assert_eq!(old_delta_does(&mut y, mv), None, "{mv:?}");
        assert_eq!(delta_place(&x), m.place(), "{mv:?}");
        assert_eq!(old_delta_place(&y), m.place(), "{mv:?}");
    }

    let new = VerifiedDeltaColumn::<u64>::from_values(&[3, 5, 5, 9, 2]);
    let mut it = new.iter_range(1, 4);
    assert_eq!(it.by_ref().collect::<Vec<u64>>(), vec![5, 5, 9]);
    assert_eq!((it.pos(), it.end_pos(), it.len()), (4, 4, 0));
    assert_eq!(it.next(), None);
    // the window opened again: on from where the iterator stands
    it.set_max(9);
    assert_eq!((it.pos(), it.end_pos(), it.len()), (4, 9, 1));
    assert_eq!(it.next(), Some(2));
    assert_eq!(it.next(), None);
    assert_eq!((it.pos(), it.end_pos(), it.len()), (5, 5, 0));
    assert_eq!(
        format!("{it:?}"),
        "VerifiedDeltaIter { pos: 5, running: 2, items_left: 0 }"
    );
}

// ── Delta columns: runs of differences, and a suspended iterator ────────────

/// A value type of a delta column, with the number kept for each value.
trait DeltaStepped: Deltad {
    /// The number of a value, if it is not null.
    fn number(self) -> Option<i64>;
    /// The value of a number.
    fn numbered(n: i64) -> Self;
    /// The null of the type, if it has one.
    fn null() -> Option<Self>;
    /// The least and the greatest number the tests give the type.
    fn bounds() -> (i64, i64);
}

macro_rules! delta_stepped {
    ($t:ty, $lo:expr, $hi:expr) => {
        impl DeltaStepped for $t {
            fn number(self) -> Option<i64> {
                Some(self as i64)
            }
            fn numbered(n: i64) -> Self {
                n as $t
            }
            fn null() -> Option<Self> {
                None
            }
            fn bounds() -> (i64, i64) {
                ($lo, $hi)
            }
        }

        impl DeltaStepped for Option<$t> {
            fn number(self) -> Option<i64> {
                self.map(|v| v as i64)
            }
            fn numbered(n: i64) -> Self {
                Some(n as $t)
            }
            fn null() -> Option<Self> {
                Some(None)
            }
            fn bounds() -> (i64, i64) {
                ($lo, $hi)
            }
        }
    };
}

delta_stepped!(u32, 0, u32::MAX as i64);
delta_stepped!(u64, 0, i64::MAX >> 2);
delta_stepped!(i32, i32::MIN as i64, i32::MAX as i64);
delta_stepped!(i64, i64::MIN >> 2, i64::MAX >> 2);
delta_stepped!(usize, 0, i64::MAX >> 2);

/// A verified delta column and the old one of the same values, laid out
/// in the same slabs, with the values: `n` of them or a few more, in
/// stretches of equal steps and of nulls among single values, so that
/// the runs of differences are of many lengths.
fn delta_run_pair<T: DeltaStepped>(
    rng: &mut Rng,
    n: usize,
    max_segments: Option<usize>,
) -> (VerifiedDeltaColumn<T>, DeltaColumn<T>, Vec<T>) {
    let (mut new, mut old) = match max_segments {
        Some(m) => (
            VerifiedDeltaColumn::<T>::with_max_segments(m),
            DeltaColumn::<T>::with_max_segments(m),
        ),
        None => (
            VerifiedDeltaColumn::<T>::default(),
            DeltaColumn::<T>::default(),
        ),
    };
    let (lo, hi) = T::bounds();
    let mut last = 0;
    let mut model = Vec::new();
    while model.len() < n {
        let mut some = Vec::new();
        let count = 1 + rng.below(6);
        match rng.below(4) {
            0 => {
                // equal steps, as far as the type's numbers go
                let step = rng.below(9) as i64 - 4;
                for _ in 0..count {
                    if (lo..=hi).contains(&(last + step)) {
                        last += step;
                        some.push(T::numbered(last));
                    }
                }
            }
            1 => {
                if let Some(null) = T::null() {
                    some.resize(count, null);
                }
            }
            _ => {
                let (v, l) = T::near(rng, last);
                last = l;
                some.push(v);
            }
        }
        // now and then at a place before the end, so that slabs are cut
        let at = match rng.below(6) {
            0 => rng.below(model.len() + 1),
            _ => model.len(),
        };
        for (i, v) in some.into_iter().enumerate() {
            new.insert(at + i, v);
            old.insert(at + i, v);
            model.insert(at + i, v);
        }
    }
    assert_eq!(new.slab_count(), old.slab_count());
    (new, old, model)
}

/// The runs of differences of `model[start..end]`, as the specification
/// has them (`delta_runs_at` of the `runs_of` that window of `deltas`):
/// the difference of a value is its number less the number before it,
/// which is the last that is not null, or 0 where there is none; a null
/// has none; a run is of the equal differences that lie together in the
/// window, with the number before its first.
fn delta_runs_model<T: DeltaStepped>(
    model: &[T],
    start: usize,
    end: usize,
) -> Vec<hexane::DeltaRun> {
    let mut before = 0i64;
    for v in &model[..start] {
        before = v.number().unwrap_or(before);
    }
    let mut runs: Vec<hexane::DeltaRun> = Vec::new();
    for v in &model[start..end] {
        let delta = v.number().map(|n| n.wrapping_sub(before));
        match runs.last_mut() {
            Some(run) if run.delta == delta => run.count += 1,
            _ => runs.push(hexane::DeltaRun {
                prefix: before,
                delta,
                count: 1,
            }),
        }
        before = v.number().unwrap_or(before);
    }
    runs
}

/// What `next_run` gives an iterator that stands as `m` does, by the
/// specification: the first run of what it has left, which it passes;
/// or nothing, with its window closed where it stands.
fn delta_walk_run<T: DeltaStepped>(m: &mut DeltaWalk<'_, T>) -> Option<hexane::DeltaRun> {
    let run = delta_runs_model(m.model, m.pos, m.pos + m.left())
        .first()
        .copied();
    match run {
        Some(run) => m.pos += run.count,
        None => m.end = m.pos,
    }
    run
}

/// A delta column's iterator gives the runs of differences that its
/// specification says, as `DeltaColumn`'s iterator does: of the whole
/// column, of a range (the runs cut where the range is), and run by run
/// among other moves, the two being asked the same at each step.
fn delta_run_reads<T: DeltaStepped>(seed: u64, max_segments: Option<usize>) {
    let mut rng = Rng(seed.wrapping_mul(0x2545_f491_4f6c).wrapping_add(13) | 1);
    let n = match rng.below(5) {
        0 => rng.below(4),
        _ => rng.below(200),
    };
    let (new, old, model) = delta_run_pair::<T>(&mut rng, n, max_segments);
    let len = model.len();
    // all the runs
    let want = delta_runs_model(&model, 0, len);
    assert_eq!(new.iter().runs().collect::<Vec<_>>(), want);
    assert_eq!(old.iter().runs().collect::<Vec<_>>(), want);
    assert_eq!(
        format!("{:?}", new.iter().runs()),
        format!("{:?}", old.iter().runs()).replace("Delta", "VerifiedDelta")
    );
    // the runs of a range, which `splice_runs` takes: they are the
    // range's values
    for _ in 0..4 {
        let start = rng.below(len + 1);
        let end = start + rng.below(len - start + 1);
        let want = delta_runs_model(&model, start, end);
        let runs = new.iter_range(start, end).runs().collect::<Vec<_>>();
        assert_eq!(runs, want);
        assert_eq!(old.iter_range(start..end).runs().collect::<Vec<_>>(), want);
        let mut copy = VerifiedDeltaColumn::<T>::new();
        copy.splice_runs(0, 0, &runs);
        assert_eq!(copy.to_vec(), model[start..end]);
    }
    // run by run, among other moves
    for _ in 0..6 {
        let start = rng.below(len + 1);
        let end = start + rng.below(len - start + 1);
        let mut x = new.iter_range(start, end);
        let mut y = old.iter_range(start..end);
        let mut m = DeltaWalk {
            model: &model,
            pos: start,
            end,
        };
        for _ in 0..40 {
            if rng.below(2) == 0 {
                let mv = a_delta_move(&mut rng, &m);
                let want = m.does(&mv);
                assert_eq!(new_delta_does(&mut x, &mv), want, "{mv:?}");
                assert_eq!(old_delta_does(&mut y, &mv), want, "{mv:?}");
            } else {
                let want = delta_walk_run(&mut m);
                assert_eq!(x.next_run(), want);
                assert_eq!(y.next_run(), want);
            }
            assert_eq!(delta_place(&x), m.place());
            assert_eq!(old_delta_place(&y), m.place());
            assert_eq!(
                format!("{x:?}"),
                format!("{y:?}").replace("DeltaIter", "VerifiedDeltaIter")
            );
        }
    }
}

/// A verified delta column's iterator and the old one's give the same
/// runs of differences, which are the runs the specification says: for
/// every type of value, and with small slabs too.
#[test]
fn delta_runs_agree() {
    for seed in 0..8 {
        for max_segments in [None, Some(2), Some(5)] {
            delta_run_reads::<u32>(seed, max_segments);
            delta_run_reads::<u64>(seed, max_segments);
            delta_run_reads::<i32>(seed, max_segments);
            delta_run_reads::<i64>(seed, max_segments);
            delta_run_reads::<usize>(seed, max_segments);
            delta_run_reads::<Option<u32>>(seed, max_segments);
            delta_run_reads::<Option<u64>>(seed, max_segments);
            delta_run_reads::<Option<i32>>(seed, max_segments);
            delta_run_reads::<Option<i64>>(seed, max_segments);
            delta_run_reads::<Option<usize>>(seed, max_segments);
        }
    }
}

/// A suspended iterator's state tried on a column, in both APIs: both
/// refuse it, or both give an iterator of that column, the two standing
/// in the same place with the same sum and the same values left, which
/// are the column's there. Says whether the state was taken.
fn delta_resumes_alike<T: DeltaStepped>(
    s: &hexane::verified::VerifiedDeltaIterState,
    t: &hexane::DeltaIterState,
    new: &VerifiedDeltaColumn<T>,
    old: &DeltaColumn<T>,
    model: &[T],
) -> bool {
    match (s.try_resume(new), t.try_resume(old)) {
        (Ok(x), Ok(y)) => {
            assert_eq!(
                format!("{x:?}"),
                format!("{y:?}").replace("DeltaIter", "VerifiedDeltaIter")
            );
            assert_eq!(delta_place(&x), old_delta_place(&y));
            let (pos, left) = (x.pos(), x.len());
            assert_eq!(x.collect::<Vec<T>>(), model[pos..pos + left]);
            assert_eq!(y.collect::<Vec<T>>(), model[pos..pos + left]);
            true
        }
        (Err(e), Err(f)) => {
            assert!(matches!(e, PackError::InvalidResume), "{e:?}");
            assert!(matches!(f, PackError::InvalidResume), "{f:?}");
            false
        }
        (x, y) => panic!("the new API: {}, the old: {}", x.is_ok(), y.is_ok()),
    }
}

/// A suspended iterator's state tried on a column other than the one it
/// was taken of. Here the two APIs need not agree whether it is taken: a
/// state holds its column's count of edits, and the two count
/// differently (the verified column one for each edit, the old one by
/// what the edit wrote), so two columns can have counted the same in one
/// API and not in the other. Where both take it or both refuse it, they
/// do so alike (`delta_resumes_alike`); where only the verified one
/// takes it, its iterator gives that column's values in the state's
/// window, as its specification says. Says whether the verified one took
/// it.
fn delta_resumes_elsewhere<T: DeltaStepped>(
    s: &hexane::verified::VerifiedDeltaIterState,
    t: &hexane::DeltaIterState,
    new: &VerifiedDeltaColumn<T>,
    old: &DeltaColumn<T>,
    model: &[T],
) -> bool {
    match (s.try_resume(new), t.try_resume(old)) {
        (Ok(x), Err(f)) => {
            assert!(matches!(f, PackError::InvalidResume), "{f:?}");
            let (pos, left) = (x.pos(), x.len());
            assert_eq!(x.collect::<Vec<T>>(), model[pos..pos + left]);
            true
        }
        (Err(e), Ok(_)) => {
            assert!(matches!(e, PackError::InvalidResume), "{e:?}");
            false
        }
        _ => delta_resumes_alike(s, t, new, old, model),
    }
}

/// A suspended iterator of a delta column is resumed where it stood,
/// with the sum and the window it had, on the column and on a copy of
/// it, and after an edit of the column its state is refused: as its
/// specification says, and as `DeltaColumn`'s iterator and its state do.
/// On another column it is refused or gives that column's values
/// (`delta_resumes_elsewhere`).
fn delta_suspends<T: DeltaStepped>(seed: u64, max_segments: Option<usize>) {
    let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a).wrapping_add(17) | 1);
    let n = match rng.below(5) {
        0 => rng.below(4),
        _ => rng.below(120),
    };
    let (mut new, mut old, mut model) = delta_run_pair::<T>(&mut rng, n, max_segments);
    let n = rng.below(40);
    let (other_new, other_old, other_model) = delta_run_pair::<T>(&mut rng, n, max_segments);
    for _ in 0..6 {
        let len = model.len();
        // an iterator moved about, and its state
        let (s, t) = {
            let mut x = new.iter();
            let mut y = old.iter();
            let mut m = DeltaWalk {
                model: &model,
                pos: 0,
                end: len,
            };
            for _ in 0..rng.below(6) {
                if rng.below(4) == 0 {
                    let want = delta_walk_run(&mut m);
                    assert_eq!(x.next_run(), want);
                    assert_eq!(y.next_run(), want);
                } else {
                    let mv = a_delta_move(&mut rng, &m);
                    let want = m.does(&mv);
                    assert_eq!(new_delta_does(&mut x, &mv), want, "{mv:?}");
                    assert_eq!(old_delta_does(&mut y, &mv), want, "{mv:?}");
                }
            }
            let (s, t) = (x.suspend(), y.suspend());
            // resumed: where the suspended one stood, with its sum and the
            // values it had left; its window ends where that one's did, or
            // at the column's end if it stood there
            let x2 = s.try_resume(&new).unwrap();
            assert_eq!(format!("{x2:?}"), format!("{x:?}"));
            let (pos, end, left) = m.place();
            assert_eq!((x2.pos(), x2.len()), (pos, left));
            assert!(x2.end_pos() == end || (pos == len && x2.end_pos() == pos));
            assert_eq!(x2.collect::<Vec<T>>(), model[pos..pos + left]);
            (s, t)
        };
        assert!(delta_resumes_alike(&s, &t, &new, &old, &model));
        // on a copy of the column too
        let (new_copy, old_copy) = (new.clone(), old.clone());
        assert!(delta_resumes_alike(&s, &t, &new_copy, &old_copy, &model));
        // on another column, and the state of an iterator of no column
        delta_resumes_elsewhere(&s, &t, &other_new, &other_old, &other_model);
        let s0 = hexane::verified::VerifiedDeltaIter::<T>::default().suspend();
        let t0 = hexane::DeltaIter::<T>::default().suspend();
        delta_resumes_elsewhere(&s0, &t0, &new, &old, &model);
        // an edit, and the column refuses the state; the copy still
        // takes it
        let copy_model = model.clone();
        if len > 0 && rng.below(3) == 0 {
            let at = rng.below(len);
            new.remove(at);
            old.remove(at);
            model.remove(at);
        } else {
            let (v, _) = T::near(&mut rng, 0);
            let at = rng.below(len + 1);
            new.insert(at, v);
            old.insert(at, v);
            model.insert(at, v);
        }
        assert!(!delta_resumes_alike(&s, &t, &new, &old, &model));
        assert!(delta_resumes_alike(
            &s,
            &t,
            &new_copy,
            &old_copy,
            &copy_model
        ));
    }
    // two columns of as many values, each pushed: where they are laid out
    // alike, a state of one is a place in the other (a verified column
    // counts one edit a push), and the iterator resumed there gives the
    // other's values, with the sum of the other's differences
    let count = 1 + rng.below(12);
    let (mut a_new, mut a_old) = (VerifiedDeltaColumn::<T>::new(), DeltaColumn::<T>::new());
    let (mut b_new, mut b_old) = (VerifiedDeltaColumn::<T>::new(), DeltaColumn::<T>::new());
    let mut b_model = Vec::new();
    let (mut a_last, mut b_last) = (0, 0);
    for _ in 0..count {
        let (v, l) = T::near(&mut rng, a_last);
        a_last = l;
        a_new.push(v);
        a_old.push(v);
        let (v, l) = T::near(&mut rng, b_last);
        b_last = l;
        b_new.push(v);
        b_old.push(v);
        b_model.push(v);
    }
    let start = rng.below(count + 1);
    let end = start + rng.below(count - start + 1);
    let s = a_new.iter_range(start, end).suspend();
    let t = a_old.iter_range(start..end).suspend();
    let taken = delta_resumes_elsewhere(&s, &t, &b_new, &b_old, &b_model);
    if a_new.slab_count() == 1 && b_new.slab_count() == 1 {
        assert!(taken);
        let x = s.try_resume(&b_new).unwrap();
        assert_eq!(x.pos(), start);
        assert_eq!(x.collect::<Vec<T>>(), b_model[start..end]);
    }
}

/// A verified delta column's suspended iterators and the old one's are
/// resumed and refused alike on their own columns, and as the
/// specification says on any: for every type of value, and with small
/// slabs too.
#[test]
fn delta_states_agree() {
    for seed in 0..8 {
        for max_segments in [None, Some(2), Some(5)] {
            delta_suspends::<u32>(seed, max_segments);
            delta_suspends::<u64>(seed, max_segments);
            delta_suspends::<i32>(seed, max_segments);
            delta_suspends::<i64>(seed, max_segments);
            delta_suspends::<usize>(seed, max_segments);
            delta_suspends::<Option<u32>>(seed, max_segments);
            delta_suspends::<Option<u64>>(seed, max_segments);
            delta_suspends::<Option<i32>>(seed, max_segments);
            delta_suspends::<Option<i64>>(seed, max_segments);
            delta_suspends::<Option<usize>>(seed, max_segments);
        }
    }
}

/// The runs of some small delta columns, and a state resumed on a column
/// of another type of value: said out in full.
#[test]
fn delta_runs_and_states_told() {
    let run = |prefix, delta, count| hexane::DeltaRun {
        prefix,
        delta,
        count,
    };
    let col = VerifiedDeltaColumn::<u64>::from_values(&[5, 7, 9, 9]);
    assert_eq!(
        col.iter().runs().collect::<Vec<_>>(),
        vec![run(0, Some(5), 1), run(5, Some(2), 2), run(9, Some(0), 1)]
    );
    // a run cut by the range: the number before it is the one before
    // the range
    assert_eq!(
        col.iter_range(2, 4).runs().collect::<Vec<_>>(),
        vec![run(7, Some(2), 1), run(9, Some(0), 1)]
    );
    assert_eq!(
        format!("{:?}", col.iter_range(1, 3).runs()),
        "VerifiedDeltaRuns(VerifiedDeltaIter { pos: 1, running: 5, items_left: 2 })"
    );
    let mut it = col.iter();
    assert_eq!(it.next(), Some(5));
    assert_eq!(it.next_run(), Some(run(5, Some(2), 2)));
    assert_eq!((it.pos(), it.end_pos(), it.len()), (3, 4, 1));
    assert_eq!(it.next_run(), Some(run(9, Some(0), 1)));
    assert_eq!(it.next_run(), None);
    assert_eq!((it.pos(), it.end_pos(), it.len()), (4, 4, 0));
    // nulls make a run of no difference, and the number before the next
    // value is the last that was not null
    let nulls =
        VerifiedDeltaColumn::<Option<u32>>::from_values(&[None, None, Some(3), None, Some(1)]);
    assert_eq!(
        nulls.iter().runs().collect::<Vec<_>>(),
        vec![
            run(0, None, 2),
            run(0, Some(3), 1),
            run(3, None, 1),
            run(3, Some(-2), 1)
        ]
    );
    // a state is of no type of value: one of a column of `u32`s is a
    // place in a column of as many `Option<i64>`s laid out alike
    let mut a = VerifiedDeltaColumn::<u32>::new();
    let mut b = VerifiedDeltaColumn::<Option<i64>>::new();
    let mut c = DeltaColumn::<u32>::new();
    let mut d = DeltaColumn::<Option<i64>>::new();
    for (v, w) in [(4, Some(-5)), (6, None), (7, Some(9))] {
        a.push(v);
        b.push(w);
        c.push(v);
        d.push(w);
    }
    let mut it = a.iter();
    assert_eq!(it.next(), Some(4));
    let s = it.suspend();
    let mut it = c.iter();
    assert_eq!(it.next(), Some(4));
    let t = it.suspend();
    let x = s.try_resume(&b).unwrap();
    assert_eq!(
        format!("{x:?}"),
        "VerifiedDeltaIter { pos: 1, running: -5, items_left: 2 }"
    );
    // the old API's iterator is the same, where its columns have counted
    // the same (its count of edits is not one for each)
    if let Ok(y) = t.try_resume(&d) {
        assert_eq!(
            format!("{x:?}"),
            format!("{y:?}").replace("DeltaIter", "VerifiedDeltaIter")
        );
        assert_eq!(y.collect::<Vec<_>>(), vec![None, Some(9)]);
    }
    assert_eq!(x.collect::<Vec<_>>(), vec![None, Some(9)]);
    // and an edit of either column ends it
    b.push(None);
    assert!(matches!(s.try_resume(&b), Err(PackError::InvalidResume)));
    assert!(s.try_resume(&a).is_ok());
    a.remove(0);
    assert!(matches!(s.try_resume(&a), Err(PackError::InvalidResume)));
    // the state of an iterator of no column is a place in a new column,
    // which has no slab and has counted no edit, in both APIs; and in no
    // column that has counted one
    let s0 = hexane::verified::VerifiedDeltaIter::<u32>::default().suspend();
    let t0 = hexane::DeltaIter::<u32>::default().suspend();
    let mut none = VerifiedDeltaColumn::<u32>::new();
    let old_none = DeltaColumn::<u32>::new();
    {
        let x = s0.try_resume(&none).unwrap();
        let y = t0.try_resume(&old_none).unwrap();
        assert_eq!((x.pos(), x.end_pos(), x.len()), (0, 0, 0));
        assert_eq!(
            format!("{x:?}"),
            format!("{y:?}").replace("DeltaIter", "VerifiedDeltaIter")
        );
    }
    none.push(1);
    none.clear();
    assert_eq!(none.slab_count(), 0);
    assert!(matches!(
        s0.try_resume(&none),
        Err(PackError::InvalidResume)
    ));
}

// ── Delta columns: values found by a scan, and by a search of sorted ones ───

/// A value type of a delta column in the order its values are searched
/// in, which is `Ord`'s: numbers as they are counted, and a null before
/// every number.
trait DeltaSought: DeltaStepped + Ord {
    /// A number of the type that no delta column holds, if it has one.
    fn unfitting(rng: &mut Rng) -> Option<Self>;
    /// Whether a delta column can hold the value.
    fn storable(&self) -> bool;
    /// The least number of the type, and the greatest that a delta
    /// column holds.
    fn ends() -> [Self; 2];
}

macro_rules! delta_sought {
    ($t:ty) => {
        impl DeltaSought for $t {
            fn unfitting(rng: &mut Rng) -> Option<Self> {
                if <$t>::BITS < 64 || <$t>::MIN != 0 {
                    return None;
                }
                let least = (i64::MAX as u64 + 1) as $t;
                Some(match rng.below(2) {
                    0 => least + rng.below(3) as $t,
                    _ => <$t>::MAX - rng.below(3) as $t,
                })
            }
            fn storable(&self) -> bool {
                (*self as i128) <= i64::MAX as i128
            }
            fn ends() -> [Self; 2] {
                if <$t>::BITS < 64 || <$t>::MIN != 0 {
                    [<$t>::MIN, <$t>::MAX]
                } else {
                    [<$t>::MIN, i64::MAX as $t]
                }
            }
        }

        impl DeltaSought for Option<$t> {
            fn unfitting(rng: &mut Rng) -> Option<Self> {
                <$t as DeltaSought>::unfitting(rng).map(Some)
            }
            fn storable(&self) -> bool {
                self.is_none_or(|v| DeltaSought::storable(&v))
            }
            fn ends() -> [Self; 2] {
                <$t as DeltaSought>::ends().map(Some)
            }
        }
    };
}

delta_sought!(u32);
delta_sought!(u64);
delta_sought!(i32);
delta_sought!(i64);
delta_sought!(usize);

/// A value to look for in a column of the values `model`: one near one
/// of them, the null, an end of the type's numbers, or a number that no
/// column holds.
fn a_delta_target<T: DeltaSought>(rng: &mut Rng, model: &[T]) -> T {
    match rng.below(12) {
        0 | 1 => {
            if let Some(null) = T::null() {
                return null;
            }
        }
        2 => {
            if let Some(v) = T::unfitting(rng) {
                return v;
            }
        }
        3 => return T::ends()[rng.below(2)],
        _ => {}
    }
    let (lo, hi) = T::bounds();
    let near = if model.is_empty() {
        0
    } else {
        model[rng.below(model.len())].number().unwrap_or(0)
    };
    T::numbered(near.saturating_add(rng.below(5) as i64 - 2).clamp(lo, hi))
}

/// Bounds for a scan of a column of the values `model`: each at such a
/// value as is looked for, or none; the upper one now and then a little
/// above the lower, so that some numbers are within them.
fn delta_bounds<T: DeltaSought>(rng: &mut Rng, model: &[T]) -> (Bound<T>, Bound<T>) {
    let (lo, hi) = T::bounds();
    let a = a_delta_target(rng, model);
    let b = match (rng.below(3), a.number()) {
        (0, Some(n)) if a.storable() => {
            T::numbered(n.saturating_add(rng.below(40) as i64).clamp(lo, hi))
        }
        _ => a_delta_target(rng, model),
    };
    let bound = |rng: &mut Rng, v: T| match rng.below(5) {
        0 => Bound::Unbounded,
        1 | 2 => Bound::Excluded(v),
        _ => Bound::Included(v),
    };
    (bound(rng, a), bound(rng, b))
}

/// Whether `v` is within the bounds, as the specification says
/// (`delta_within`): a number, not a null, at the lower bound or after
/// it and at the upper one or before it in the order of the type's
/// values.
fn delta_model_within<T: DeltaSought>(lo: &Bound<T>, hi: &Bound<T>, v: T) -> bool {
    v.number().is_some()
        && match lo {
            Bound::Included(a) => v >= *a,
            Bound::Excluded(a) => v > *a,
            Bound::Unbounded => true,
        }
        && match hi {
            Bound::Included(b) => v <= *b,
            Bound::Excluded(b) => v < *b,
            Bound::Unbounded => true,
        }
}

/// What the specification says of a scan for the values that `is`
/// holds of: the place of the first of those left and the value, the
/// walk moving past it; or nothing, the walk giving all it has.
fn delta_walk_scan<T: Deltad>(
    m: &mut DeltaWalk<'_, T>,
    is: impl Fn(T) -> bool,
) -> Option<(usize, T)> {
    let left = m.left();
    match m.model[m.pos..m.pos + left].iter().position(|v| is(*v)) {
        Some(i) => {
            let at = m.pos + i;
            m.pos = at + 1;
            Some((at, m.model[at]))
        }
        None => {
            m.pos += left;
            m.end = m.pos;
            None
        }
    }
}

/// A delta column's iterator finds by a scan what its specification
/// says, the first value left that is the one looked for or is within
/// the bounds, and stands where it says afterwards, as `DeltaColumn`'s
/// iterator does: the two are asked the same at each step, among other
/// moves, with a walk of the values that follows the specification.
fn delta_scans<T: DeltaSought>(seed: u64, max_segments: Option<usize>) {
    let mut rng = Rng(seed.wrapping_mul(0x2f_9be6_cc27).wrapping_add(13) | 1);
    let n = match rng.below(5) {
        0 => rng.below(4),
        _ => rng.below(200),
    };
    let (new, old, model) = delta_pair::<T>(&mut rng, n, max_segments);
    let len = model.len();
    for _ in 0..8 {
        // over all of the column, or over a part of it
        let (start, end) = match rng.below(2) {
            0 => (0, len),
            _ => {
                let start = rng.below(len + 1);
                (start, start + rng.below(len - start + 1))
            }
        };
        let mut x = new.iter_range(start, end);
        let mut y = old.iter_range(start..end);
        let mut m = DeltaWalk {
            model: &model,
            pos: start,
            end,
        };
        for _ in 0..24 {
            match rng.below(5) {
                0 => {
                    let mv = a_delta_move(&mut rng, &m);
                    let want = m.does(&mv);
                    assert_eq!(new_delta_does(&mut x, &mv), want, "{mv:?}");
                    assert_eq!(old_delta_does(&mut y, &mv), want, "{mv:?}");
                }
                1 | 2 => {
                    let target = a_delta_target(&mut rng, &model);
                    let want = delta_walk_scan(&mut m, |v| v == target).map(|(at, _)| at);
                    assert_eq!(x.scan_to_value(target), want, "{target:?}");
                    assert_eq!(y.scan_to_value(target), want, "{target:?}");
                }
                _ => {
                    let (lo, hi) = delta_bounds(&mut rng, &model);
                    let want = delta_walk_scan(&mut m, |v| delta_model_within(&lo, &hi, v));
                    assert_eq!(x.scan_to_range((lo, hi)), want, "{lo:?} {hi:?}");
                    assert_eq!(y.scan_to_range((lo, hi)), want, "{lo:?} {hi:?}");
                }
            }
            assert_eq!(delta_place(&x), m.place());
            assert_eq!(old_delta_place(&y), m.place());
            assert_eq!(
                format!("{x:?}"),
                format!("{y:?}").replace("DeltaIter", "VerifiedDeltaIter")
            );
        }
    }
}

/// A verified delta column's iterator and the old one's find the same
/// by a scan for a value and by a scan for a value within bounds, and
/// stand in the same places afterwards, which is what the specification
/// says: for every type of value, and with small slabs too.
#[test]
fn delta_scans_agree() {
    for seed in 0..8 {
        for max_segments in [None, Some(2), Some(5)] {
            delta_scans::<u32>(seed, max_segments);
            delta_scans::<u64>(seed, max_segments);
            delta_scans::<i32>(seed, max_segments);
            delta_scans::<i64>(seed, max_segments);
            delta_scans::<usize>(seed, max_segments);
            delta_scans::<Option<u32>>(seed, max_segments);
            delta_scans::<Option<u64>>(seed, max_segments);
            delta_scans::<Option<i32>>(seed, max_segments);
            delta_scans::<Option<i64>>(seed, max_segments);
            delta_scans::<Option<usize>>(seed, max_segments);
        }
    }
}

/// A verified delta column and the old one of the same `n` values in
/// their order, laid out in the same slabs, with the values.
fn delta_sorted_pair<T: DeltaSought>(
    rng: &mut Rng,
    n: usize,
    max_segments: Option<usize>,
) -> (VerifiedDeltaColumn<T>, DeltaColumn<T>, Vec<T>) {
    let (mut new, mut old) = match max_segments {
        Some(m) => (
            VerifiedDeltaColumn::<T>::with_max_segments(m),
            DeltaColumn::<T>::with_max_segments(m),
        ),
        None => (
            VerifiedDeltaColumn::<T>::default(),
            DeltaColumn::<T>::default(),
        ),
    };
    let mut last = 0;
    let mut model = Vec::new();
    for _ in 0..n {
        let (v, l) = T::near(rng, last);
        last = l;
        model.push(v);
    }
    model.sort();
    // most of them one after the other, and then the others at their
    // places, so that slabs are cut
    let mut held = Vec::new();
    let mut later = Vec::new();
    for v in &model {
        if rng.below(5) == 0 {
            later.push(*v);
        } else {
            new.insert(held.len(), *v);
            old.insert(held.len(), *v);
            held.push(*v);
        }
    }
    for v in later {
        let at = held.partition_point(|h| *h < v);
        new.insert(at, v);
        old.insert(at, v);
        held.insert(at, v);
    }
    assert_eq!(held, model);
    assert_eq!(new.slab_count(), old.slab_count());
    (new, old, model)
}

/// A range to search for an iterator that stands as `m` does: mostly
/// ahead of it and in the column, sometimes any range.
fn a_delta_range<T: Deltad>(rng: &mut Rng, m: &DeltaWalk<'_, T>) -> Range<usize> {
    let len = m.model.len();
    match rng.below(8) {
        0 => 0..usize::MAX,
        1 => 0..m.end,
        // any range: one behind the iterator, one that ends before it
        // starts
        2 | 3 => a_place(rng, len)..a_place(rng, len),
        _ => {
            let start = m.pos + rng.below(len - m.pos + 1);
            start..start + rng.below(len - start + 2)
        }
    }
}

/// Where the specification of `seek_to_value` leaves an iterator that
/// stands as `m` does and gives `r`, whatever the values are; `r` is
/// checked to lie in the places searched, which are given.
fn delta_walk_sought<T: DeltaSought>(
    m: &mut DeltaWalk<'_, T>,
    target: T,
    range: &Range<usize>,
    r: &Range<usize>,
) -> Range<usize> {
    let (p, len) = (m.pos, m.model.len());
    // where the window is made to end
    let e = range.end.max(range.start);
    // the places searched
    let t = if e <= p { p } else { e.min(len) };
    let s = if range.start <= p {
        p
    } else {
        range.start.min(t)
    };
    assert!(s <= r.start && r.end <= t, "{range:?} {r:?}");
    m.pos = r.start;
    m.end = if range.start > t || !target.storable() {
        r.start
    } else {
        e.max(p)
    };
    s..t
}

/// `r` is what the specifications say of a search for `value` among the
/// values of `model` at the places `searched`: a range of those places;
/// and, where the values there are in order, the places of those that
/// are `value`, or the empty range where it would stand.
fn delta_sought_rightly<T: DeltaSought>(
    model: &[T],
    value: T,
    searched: Range<usize>,
    r: &Range<usize>,
) {
    assert!(searched.start <= r.start && r.start <= r.end && r.end <= searched.end);
    let part = &model[searched.clone()];
    if part.windows(2).all(|w| w[0] <= w[1]) {
        let first = searched.start + part.partition_point(|v| *v < value);
        let after = searched.start + part.partition_point(|v| *v <= value);
        assert_eq!(*r, first..after, "{value:?} in {searched:?}");
    }
}

/// A delta column and its iterator search values for one of them as
/// their specifications say, and the iterator stands where its own
/// says afterwards, as `DeltaColumn` and its iterator do: for values in
/// their order (`in_order`), where the range found is the one of the
/// places that hold the value, and for values in no order, where it is
/// some range of the places searched.
fn delta_seeks<T: DeltaSought>(seed: u64, max_segments: Option<usize>, in_order: bool) {
    let mut rng = Rng(seed.wrapping_mul(0x63_d1a5_4e0b).wrapping_add(17) | 1);
    let n = match rng.below(5) {
        0 => rng.below(4),
        _ => rng.below(200),
    };
    let (new, old, model) = if in_order {
        delta_sorted_pair::<T>(&mut rng, n, max_segments)
    } else {
        delta_pair::<T>(&mut rng, n, max_segments)
    };
    let len = model.len();
    // the column's own search
    for _ in 0..12 {
        let start = rng.below(len + 1);
        let end = start + rng.below(len - start + 1);
        let value = a_delta_target(&mut rng, &model);
        let r = new.scope_to_value(value, start, end);
        assert_eq!(old.scope_to_value(value, start..end), r, "{value:?}");
        delta_sought_rightly(&model, value, start..end, &r);
    }
    // the iterator's, among other moves
    for _ in 0..10 {
        let mut x = new.iter();
        let mut y = old.iter();
        let mut m = DeltaWalk {
            model: &model,
            pos: 0,
            end: len,
        };
        for _ in 0..10 {
            match rng.below(3) {
                0 => {
                    let mv = a_delta_move(&mut rng, &m);
                    let want = m.does(&mv);
                    assert_eq!(new_delta_does(&mut x, &mv), want, "{mv:?}");
                    assert_eq!(old_delta_does(&mut y, &mv), want, "{mv:?}");
                }
                _ => {
                    let target = a_delta_target(&mut rng, &model);
                    let range = a_delta_range(&mut rng, &m);
                    let r = x.seek_to_value(target, range.clone());
                    assert_eq!(
                        y.seek_to_value(target, range.clone()),
                        r,
                        "{target:?} {range:?}"
                    );
                    let searched = delta_walk_sought(&mut m, target, &range, &r);
                    delta_sought_rightly(&model, target, searched, &r);
                }
            }
            assert_eq!(delta_place(&x), m.place());
            assert_eq!(old_delta_place(&y), m.place());
            assert_eq!(
                format!("{x:?}"),
                format!("{y:?}").replace("DeltaIter", "VerifiedDeltaIter")
            );
        }
    }
}

/// A verified delta column and the old one, and their iterators, find
/// the same by a search for a value, and the iterators stand in the
/// same places afterwards, which is what the specifications say: for
/// values in their order and in no order, for every type of value, and
/// with small slabs too.
#[test]
fn delta_seeks_agree() {
    for seed in 0..8 {
        for max_segments in [None, Some(2), Some(5)] {
            for in_order in [true, false] {
                delta_seeks::<u32>(seed, max_segments, in_order);
                delta_seeks::<u64>(seed, max_segments, in_order);
                delta_seeks::<i32>(seed, max_segments, in_order);
                delta_seeks::<i64>(seed, max_segments, in_order);
                delta_seeks::<usize>(seed, max_segments, in_order);
                delta_seeks::<Option<u32>>(seed, max_segments, in_order);
                delta_seeks::<Option<u64>>(seed, max_segments, in_order);
                delta_seeks::<Option<i32>>(seed, max_segments, in_order);
                delta_seeks::<Option<i64>>(seed, max_segments, in_order);
                delta_seeks::<Option<usize>>(seed, max_segments, in_order);
            }
        }
    }
}

/// The scans and the search of a delta column's iterator at their
/// edges, each written out: a value no column holds, bounds that hold
/// nothing, the null as a value and as a bound, a range that starts
/// past its end, values in no order, and an iterator of no column. The
/// old iterator is asked the same.
#[test]
fn delta_searches_told() {
    let values: [u64; 6] = [1, 3, 3, 3, 7, 9];
    let mut new = VerifiedDeltaColumn::<u64>::default();
    let mut old = DeltaColumn::<u64>::default();
    for (i, v) in values.iter().enumerate() {
        new.insert(i, *v);
        old.insert(i, *v);
    }
    let unfitting = i64::MAX as u64 + 1;

    // a scan for a value gives each place of it in turn, and then all
    // the iterator has
    let (mut x, mut y) = (new.iter(), old.iter());
    for want in [Some(1), Some(2), Some(3), None, None] {
        assert_eq!(x.scan_to_value(3), want);
        assert_eq!(y.scan_to_value(3), want);
    }
    assert_eq!(delta_place(&x), (6, 6, 0));
    assert_eq!(old_delta_place(&y), (6, 6, 0));
    // a value no column holds is found nowhere
    let (mut x, mut y) = (new.iter_range(2, 5), old.iter_range(2..5));
    assert_eq!(x.scan_to_value(unfitting), None);
    assert_eq!(y.scan_to_value(unfitting), None);
    assert_eq!(delta_place(&x), (5, 5, 0));
    assert_eq!(old_delta_place(&y), (5, 5, 0));

    // bounds of every kind
    let (mut x, mut y) = (new.iter(), old.iter());
    assert_eq!(x.scan_to_range(3..8), Some((1, 3)));
    assert_eq!(y.scan_to_range(3..8), Some((1, 3)));
    let above = (Bound::Excluded(3), Bound::Unbounded);
    assert_eq!(x.scan_to_range(above), Some((4, 7)));
    assert_eq!(y.scan_to_range(above), Some((4, 7)));
    assert_eq!(x.scan_to_range(..=unfitting), Some((5, 9)));
    assert_eq!(y.scan_to_range(..=unfitting), Some((5, 9)));
    assert_eq!(delta_place(&x), (6, 6, 0));
    assert_eq!(old_delta_place(&y), (6, 6, 0));
    // bounds that hold nothing: the upper one below the lower, the
    // lower one a value no column holds, and one that leaves out the
    // only number there could be
    let (lo, hi) = (8u64, 2u64);
    let nothing = [
        (Bound::Included(lo), Bound::Included(hi)),
        (Bound::Included(unfitting), Bound::Unbounded),
        (Bound::Excluded(i64::MAX as u64), Bound::Unbounded),
        (Bound::Unbounded, Bound::Excluded(0)),
    ];
    for bounds in nothing {
        let (mut x, mut y) = (new.iter(), old.iter());
        assert_eq!(x.scan_to_range(bounds), None, "{bounds:?}");
        assert_eq!(y.scan_to_range(bounds), None, "{bounds:?}");
        assert_eq!(delta_place(&x), (6, 6, 0), "{bounds:?}");
        assert_eq!(old_delta_place(&y), (6, 6, 0), "{bounds:?}");
    }

    // a search gives the places of the value, and the iterator the
    // first of them next; the window ends where the range does
    let (mut x, mut y) = (new.iter(), old.iter());
    assert_eq!(x.seek_to_value(3, 0..usize::MAX), 1..4);
    assert_eq!(y.seek_to_value(3, 0..usize::MAX), 1..4);
    assert_eq!(delta_place(&x), (1, usize::MAX, 5));
    assert_eq!(old_delta_place(&y), (1, usize::MAX, 5));
    assert_eq!((x.next(), y.next()), (Some(3), Some(3)));
    // a value that is not there: where it would stand
    assert_eq!(x.seek_to_value(4, 0..5), 4..4);
    assert_eq!(y.seek_to_value(4, 0..5), 4..4);
    assert_eq!(delta_place(&x), (4, 5, 1));
    assert_eq!(old_delta_place(&y), (4, 5, 1));
    assert_eq!((x.next(), y.next()), (Some(7), Some(7)));
    // a value no column holds comes after them all, and the window
    // ends there
    let (mut x, mut y) = (new.iter(), old.iter());
    assert_eq!(x.seek_to_value(unfitting, 0..usize::MAX), 6..6);
    assert_eq!(y.seek_to_value(unfitting, 0..usize::MAX), 6..6);
    assert_eq!(delta_place(&x), (6, 6, 0));
    assert_eq!(old_delta_place(&y), (6, 6, 0));
    // a range that starts past its end is the empty one at its start
    let (start, end) = (5, 2);
    let (mut x, mut y) = (new.iter(), old.iter());
    assert_eq!(x.seek_to_value(3, start..end), 5..5);
    assert_eq!(y.seek_to_value(3, start..end), 5..5);
    assert_eq!(delta_place(&x), (5, 5, 0));
    assert_eq!(old_delta_place(&y), (5, 5, 0));
    // the column's own search
    assert_eq!(new.scope_to_value(3, 0, 6), 1..4);
    assert_eq!(new.scope_to_value(3, 2, 3), 2..3);
    assert_eq!(new.scope_to_value(8, 0, 6), 5..5);
    assert_eq!(new.scope_to_value(unfitting, 0, 4), 4..4);
    assert_eq!(new.scope_to_value(3, 6, 6), 6..6);

    // nulls come before every number, are found by a scan for the
    // null, and are within no bounds; a null as a lower bound is below
    // every number, and as an upper one above none
    let values = [None, None, Some(-5i64), Some(2), Some(2)];
    let mut new = VerifiedDeltaColumn::<Option<i64>>::default();
    let mut old = DeltaColumn::<Option<i64>>::default();
    for (i, v) in values.iter().enumerate() {
        new.insert(i, *v);
        old.insert(i, *v);
    }
    let (mut x, mut y) = (new.iter(), old.iter());
    assert_eq!(x.seek_to_value(None, 0..5), 0..2);
    assert_eq!(y.seek_to_value(None, 0..5), 0..2);
    assert_eq!(x.seek_to_value(Some(2), 0..5), 3..5);
    assert_eq!(y.seek_to_value(Some(2), 0..5), 3..5);
    assert_eq!(new.scope_to_value(None, 1, 5), 1..2);
    assert_eq!(new.scope_to_value(Some(-9), 0, 5), 2..2);
    let (mut x, mut y) = (new.iter(), old.iter());
    for want in [Some(0), Some(1), None] {
        assert_eq!(x.scan_to_value(None), want);
        assert_eq!(y.scan_to_value(None), want);
    }
    let (mut x, mut y) = (new.iter(), old.iter());
    assert_eq!(x.scan_to_range(None..), Some((2, Some(-5))));
    assert_eq!(y.scan_to_range(None..), Some((2, Some(-5))));
    assert_eq!(x.scan_to_range(..), Some((3, Some(2))));
    assert_eq!(y.scan_to_range(..), Some((3, Some(2))));
    let (mut x, mut y) = (new.iter(), old.iter());
    assert_eq!(x.scan_to_range(None..=None), None);
    assert_eq!(y.scan_to_range(None..=None), None);
    assert_eq!(delta_place(&x), (5, 5, 0));
    assert_eq!(old_delta_place(&y), (5, 5, 0));
    let (mut x, mut y) = (new.iter(), old.iter());
    assert_eq!(x.scan_to_range(..=None), None);
    assert_eq!(y.scan_to_range(..=None), None);

    // values in no order: some range of the places searched, the same
    // of both
    let values: [u32; 6] = [5, 1, 5, 1, 9, 0];
    let mut new = VerifiedDeltaColumn::<u32>::default();
    let mut old = DeltaColumn::<u32>::default();
    for (i, v) in values.iter().enumerate() {
        new.insert(i, *v);
        old.insert(i, *v);
    }
    for value in [0, 1, 5, 9, 10] {
        let r = new.scope_to_value(value, 1, 6);
        assert_eq!(old.scope_to_value(value, 1..6), r);
        assert!(1 <= r.start && r.start <= r.end && r.end <= 6);
    }

    // an iterator of no column finds nothing
    let mut x = hexane::verified::VerifiedDeltaIter::<Option<i64>>::default();
    let mut y = hexane::DeltaIter::<Option<i64>>::default();
    assert_eq!(x.scan_to_value(Some(1)), None);
    assert_eq!(y.scan_to_value(Some(1)), None);
    assert_eq!(x.scan_to_value(None), None);
    assert_eq!(y.scan_to_value(None), None);
    assert_eq!(x.scan_to_range(..), None);
    assert_eq!(y.scan_to_range(..), None);
    assert_eq!(delta_place(&x), (0, 0, 0));
    assert_eq!(old_delta_place(&y), (0, 0, 0));
    assert_eq!(x.seek_to_value(Some(1), 3..9), 0..0);
    assert_eq!(y.seek_to_value(Some(1), 3..9), 0..0);
    assert_eq!(delta_place(&x), (0, 0, 0));
    assert_eq!(old_delta_place(&y), (0, 0, 0));
    assert_eq!(x.seek_to_value(None, 0..9), 0..0);
    assert_eq!(y.seek_to_value(None, 0..9), 0..0);
    assert_eq!(delta_place(&x), (0, 9, 0));
    assert_eq!(old_delta_place(&y), (0, 9, 0));
}

// ── Delta columns: the places of values, found by the column's index ────────

/// A value type of a delta column whose numbers are told as `i128`s,
/// which hold every number of every such type.
trait DeltaFound: DeltaSought {
    /// The number of a value, if it is not null.
    fn wide(self) -> Option<i128>;
}

macro_rules! delta_found {
    ($t:ty) => {
        impl DeltaFound for $t {
            fn wide(self) -> Option<i128> {
                Some(self as i128)
            }
        }

        impl DeltaFound for Option<$t> {
            fn wide(self) -> Option<i128> {
                self.map(|v| v as i128)
            }
        }
    };
}

delta_found!(u32);
delta_found!(u64);
delta_found!(i32);
delta_found!(i64);
delta_found!(usize);

/// The numbers within the bounds as a range of numbers that includes
/// its start and not its end, which is what `DeltaColumn`'s
/// `find_by_range` takes: a null is before every number.
fn delta_bounds_wide<T: DeltaFound>(lo: &Bound<T>, hi: &Bound<T>) -> Range<i128> {
    let big = 1i128 << 70;
    let start = match lo {
        Bound::Included(a) => a.wide().unwrap_or(-big),
        Bound::Excluded(a) => a.wide().map_or(-big, |n| n + 1),
        Bound::Unbounded => -big,
    };
    let end = match hi {
        Bound::Included(b) => b.wide().map_or(-big, |n| n + 1),
        Bound::Excluded(b) => b.wide().unwrap_or(-big),
        Bound::Unbounded => big,
    };
    start..end
}

/// A delta column's finds are the places its specification says
/// (`delta_finds`): those of the values that are the one looked for, or
/// are within the bounds, in order, with no null among them; and they
/// are what `DeltaColumn` finds.
fn delta_finds<T: DeltaFound>(seed: u64, max_segments: Option<usize>) {
    let mut rng = Rng(seed.wrapping_mul(0x3d_4d51_cb17).wrapping_add(17) | 1);
    let n = match rng.below(5) {
        0 => rng.below(4),
        _ => rng.below(200),
    };
    let (new, old, model) = delta_pair::<T>(&mut rng, n, max_segments);
    for _ in 0..12 {
        let target = a_delta_target(&mut rng, &model);
        // a null is found nowhere
        let want: Vec<usize> = (0..model.len())
            .filter(|&i| model[i] == target && target.number().is_some())
            .collect();
        let first = want.first().copied();
        assert_eq!(
            new.find_by_value(target).collect::<Vec<_>>(),
            want,
            "{target:?}"
        );
        assert_eq!(
            old.find_by_value(target).collect::<Vec<_>>(),
            want,
            "{target:?}"
        );
        assert_eq!(new.find_first(target), first, "{target:?}");
        assert_eq!(old.find_first(target), first, "{target:?}");
        // a finder that has given every place gives no more
        let mut finder = new.find_by_value(target);
        assert_eq!(finder.by_ref().count(), want.len());
        assert_eq!((finder.next(), finder.next()), (None, None));
    }
    for _ in 0..12 {
        let (lo, hi) = delta_bounds(&mut rng, &model);
        let want: Vec<usize> = (0..model.len())
            .filter(|&i| delta_model_within(&lo, &hi, model[i]))
            .collect();
        assert_eq!(
            new.find_by_range((lo, hi)).collect::<Vec<_>>(),
            want,
            "{lo:?} {hi:?}"
        );
        assert_eq!(
            old.find_by_range(delta_bounds_wide(&lo, &hi))
                .collect::<Vec<_>>(),
            want,
            "{lo:?} {hi:?}"
        );
        // one place at a time, each call going on from the last find
        let mut finder = new.find_by_range((lo, hi));
        for at in &want {
            assert_eq!(finder.next(), Some(*at));
        }
        assert_eq!((finder.next(), finder.next()), (None, None));
    }
}

/// A verified delta column and the old one find the same places of a
/// value and of the values within bounds, which are the places the
/// specification says: for every type of value, and with small slabs
/// too.
#[test]
fn delta_finds_agree() {
    for seed in 0..8 {
        for max_segments in [None, Some(2), Some(5)] {
            delta_finds::<u32>(seed, max_segments);
            delta_finds::<u64>(seed, max_segments);
            delta_finds::<i32>(seed, max_segments);
            delta_finds::<i64>(seed, max_segments);
            delta_finds::<usize>(seed, max_segments);
            delta_finds::<Option<u32>>(seed, max_segments);
            delta_finds::<Option<u64>>(seed, max_segments);
            delta_finds::<Option<i32>>(seed, max_segments);
            delta_finds::<Option<i64>>(seed, max_segments);
            delta_finds::<Option<usize>>(seed, max_segments);
        }
    }
}

/// The finds of a delta column in cases told one by one: numbers at the
/// ends of what a column holds, bounds beyond them and bounds the wrong
/// way round, nulls, and a finder of no column.
#[test]
fn delta_finds_told() {
    // a finder of no column
    let mut none = hexane::verified::VerifiedFindByRange::<u64>::default();
    assert_eq!((none.next(), none.next()), (None, None));

    // numbers up to the greatest a column holds, and bounds above it
    let top = i64::MAX as u64;
    let values = [1, 5, 9, top];
    let col = VerifiedDeltaColumn::<u64>::from_values(&values);
    let old = DeltaColumn::<u64>::from_values(values.to_vec());
    assert_eq!(
        col.find_by_range(5..u64::MAX).collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(
        old.find_by_range(5..u64::MAX).collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(col.find_by_range(..).collect::<Vec<_>>(), [0, 1, 2, 3]);
    assert_eq!(col.find_by_range(top..=u64::MAX).collect::<Vec<_>>(), [3]);
    assert_eq!(col.find_by_range(..top).collect::<Vec<_>>(), [0, 1, 2]);
    assert_eq!(col.find_by_range(top + 1..).count(), 0);
    let (start, end) = (u64::MAX, 5);
    assert_eq!(col.find_by_range(start..end).count(), 0);
    assert_eq!(old.find_by_range(start..end).count(), 0);
    assert_eq!(col.find_first(top), Some(3));
    assert_eq!(col.find_first(u64::MAX), None);
    assert_eq!(old.find_first(u64::MAX), None);
    assert_eq!(col.find_by_value(u64::MAX).next(), None);

    // the least numbers
    let values = [i64::MIN, i64::MIN + 1, -7, -1, -7];
    let col = VerifiedDeltaColumn::<i64>::from_values(&values);
    let old = DeltaColumn::<i64>::from_values(values.to_vec());
    assert_eq!(col.find_by_range(i64::MIN..-7).collect::<Vec<_>>(), [0, 1]);
    assert_eq!(old.find_by_range(i64::MIN..-7).collect::<Vec<_>>(), [0, 1]);
    assert_eq!(
        col.find_by_range(i64::MIN + 1..=i64::MAX)
            .collect::<Vec<_>>(),
        [1, 2, 3, 4]
    );
    let bounds = (Bound::Excluded(i64::MIN), Bound::Excluded(-1));
    assert_eq!(col.find_by_range(bounds).collect::<Vec<_>>(), [1, 2, 4]);
    assert_eq!(col.find_first(i64::MIN), Some(0));
    assert_eq!(old.find_first(i64::MIN), Some(0));
    assert_eq!(col.find_by_value(-7).collect::<Vec<_>>(), [2, 4]);
    assert_eq!(old.find_by_value(-7).collect::<Vec<_>>(), [2, 4]);
    assert_eq!(col.find_first(i64::MAX), None);

    // nulls: found nowhere and within no bounds, though a bound may be
    // one
    let values = [None, Some(3), None, Some(-2), Some(3)];
    let col = VerifiedDeltaColumn::<Option<i64>>::from_values(&values);
    let old = DeltaColumn::<Option<i64>>::from_values(values.to_vec());
    assert_eq!(col.find_first(None), None);
    assert_eq!(old.find_first(None), None);
    assert_eq!(col.find_by_value(None).count(), 0);
    assert_eq!(old.find_by_value(None).count(), 0);
    assert_eq!(col.find_by_range(..).collect::<Vec<_>>(), [1, 3, 4]);
    assert_eq!(col.find_by_range(None..).collect::<Vec<_>>(), [1, 3, 4]);
    assert_eq!(col.find_by_range(..=None).count(), 0);
    assert_eq!(col.find_by_range(None..=None).count(), 0);
    assert_eq!(col.find_by_range(None..Some(3)).collect::<Vec<_>>(), [3]);
    assert_eq!(col.find_by_value(Some(3)).collect::<Vec<_>>(), [1, 4]);
    assert_eq!(col.find_first(Some(3)), Some(1));
    assert_eq!(old.find_first(Some(3)), Some(1));

    // a finder looks no further than it is asked to: each call goes on
    // from the last find
    let mut finder = col.find_by_range(Some(-5)..Some(4));
    assert_eq!(finder.next(), Some(1));
    assert_eq!(finder.next(), Some(3));
    assert_eq!(finder.next(), Some(4));
    assert_eq!((finder.next(), finder.next()), (None, None));

    // an empty column
    let col = VerifiedDeltaColumn::<u32>::new();
    assert_eq!(col.find_by_range(..).next(), None);
    assert_eq!(col.find_first(0), None);
}

// ── Delta columns: the encoder ──────────────────────────────────────────

/// What an encoder is given, as steps: a value and how many times over.
/// Now and then a count is nought or large, a value is the one before
/// it, and one feed in four is of one value alone.
fn delta_feed<T: Deltad>(rng: &mut Rng) -> Vec<(T, usize)> {
    let steps = match rng.below(6) {
        0 => rng.below(3),
        _ => rng.below(60),
    };
    let one = rng.below(4) == 0;
    let mut last = 0;
    let mut feed: Vec<(T, usize)> = Vec::new();
    for _ in 0..steps {
        let v = match feed.last() {
            Some(&(v, _)) if one || rng.below(4) == 0 => v,
            _ => {
                let (v, l) = T::near(rng, last);
                last = l;
                v
            }
        };
        let n = match rng.below(10) {
            0 => 0,
            1 => 2 + rng.below(70),
            2 => 300,
            _ => 1,
        };
        feed.push((v, n));
    }
    feed
}

/// An encoder of each API, given a feed.
fn delta_fed<'a, T: Deltad>(
    feed: &[(T, usize)],
) -> (VerifiedDeltaEncoder<'a, T>, DeltaEncoder<'a, T>) {
    let mut new = VerifiedDeltaEncoder::<T>::new();
    let mut old = DeltaEncoder::<T>::new();
    for &(v, n) in feed {
        new.append_n(v, n);
        old.append_n(v, n);
    }
    (new, old)
}

/// `bytes` after `prefix`.
fn after(prefix: &[u8], bytes: &[u8]) -> Vec<u8> {
    [prefix, bytes].concat()
}

/// A verified delta encoder and the old one, given the same values in
/// the same way, count them alike and write the same bytes, by every
/// way of saving: the bytes a column of those values saves, which load
/// as the values. So do the two states that write into a buffer of the
/// caller's, which also keep what the buffer held and tell alike
/// whether every value is a given one.
fn delta_encodes<T: Deltad>(seed: u64) {
    let mut rng = Rng(seed.wrapping_mul(0x2f_9be6_cd31).wrapping_add(23) | 1);
    let feed = delta_feed::<T>(&mut rng);
    let values: Vec<T> = feed
        .iter()
        .flat_map(|&(v, n)| std::iter::repeat_n(v, n))
        .collect();
    let bytes = delta_bytes(&values);
    // values to compare with: some the encoder is given, some it may
    // not be
    let mut others = vec![T::near(&mut rng, 0).0, T::near(&mut rng, 0).0];
    others.extend(values.first().copied());
    others.extend(values.last().copied());
    let only = |other: T| values.iter().all(|v| *v == other);
    let prefix: Vec<u8> = (0..rng.below(4)).map(|_| rng.next() as u8).collect();

    // the encoders that own their bytes, given the values in every way
    let mut new = VerifiedDeltaEncoder::<T>::new();
    let mut old = DeltaEncoder::<T>::new();
    assert_eq!((new.len(), new.is_empty()), (0, true));
    assert_eq!(format!("{new:?}"), format!("Verified{old:?}"));
    for &(v, n) in &feed {
        match rng.below(4) {
            0 if n == 1 => {
                new.append(v);
                old.append(v);
            }
            1 if n == 1 => {
                new.append_owned(v);
                old.append_owned(v);
            }
            2 => {
                new.extend(std::iter::repeat_n(v, n));
                old.extend(std::iter::repeat_n(v, n));
            }
            _ => {
                new.append_n(v, n);
                old.append_n(v, n);
            }
        }
        assert_eq!((new.len(), new.is_empty()), (old.len(), old.is_empty()));
        assert_eq!(format!("{new:?}"), format!("Verified{old:?}"));
    }
    assert_eq!(new.len(), values.len());
    let saved = new.save();
    assert_eq!(saved, old.save());
    assert_eq!(saved, bytes);
    assert!(delta_same_load::<T>(&saved));
    let loaded = VerifiedDeltaColumn::<T>::load(&saved).unwrap();
    assert_eq!(loaded.to_vec(), values);

    // saved after what a buffer holds, or not at all
    let (new, old) = delta_fed(&feed);
    let (mut out, mut out_old) = (prefix.clone(), prefix.clone());
    let r = new.save_to(&mut out);
    assert_eq!(r, old.save_to(&mut out_old));
    assert_eq!(r, prefix.len()..prefix.len() + bytes.len());
    assert_eq!(out, out_old);
    assert_eq!(out, after(&prefix, &bytes));
    for &other in &others {
        let kept = if only(other) { &[][..] } else { &bytes[..] };
        let (new, old) = delta_fed(&feed);
        let (mut out, mut out_old) = (prefix.clone(), prefix.clone());
        let r = new.save_to_unless(&mut out, other);
        assert_eq!(r, old.save_to_unless(&mut out_old, other));
        assert_eq!(r, prefix.len()..prefix.len() + kept.len());
        assert_eq!(out, out_old);
        assert_eq!(out, after(&prefix, kept), "{other:?}");
    }

    // encoded in one call
    assert_eq!(VerifiedDeltaEncoder::<T>::encode(values.clone()), bytes);
    assert_eq!(DeltaEncoder::<T>::encode(values.clone()), bytes);
    let (mut out, mut out_old) = (prefix.clone(), prefix.clone());
    let r = VerifiedDeltaEncoder::<T>::encode_to(&mut out, values.iter().copied());
    let r_old = DeltaEncoder::<T>::encode_to(&mut out_old, values.iter().copied());
    assert_eq!(r, r_old);
    assert_eq!(r, prefix.len()..prefix.len() + bytes.len());
    assert_eq!(out, out_old);
    assert_eq!(out, after(&prefix, &bytes));
    for &other in &others {
        let kept = if only(other) { &[][..] } else { &bytes[..] };
        let (mut out, mut out_old) = (prefix.clone(), prefix.clone());
        let r = VerifiedDeltaEncoder::<T>::encode_to_unless(&mut out, values.clone(), other);
        let r_old = DeltaEncoder::<T>::encode_to_unless(&mut out_old, values.clone(), other);
        assert_eq!(r, r_old);
        assert_eq!(r, prefix.len()..prefix.len() + kept.len());
        assert_eq!(out, out_old);
        assert_eq!(out, after(&prefix, kept), "{other:?}");
    }

    // the states that write into the caller's buffer
    let mut new = VerifiedDeltaEncoderState::<T>::new();
    let mut old = DeltaEncoderState::<T>::new();
    let (mut buf, mut buf_old) = (prefix.clone(), prefix.clone());
    let mut given: Vec<T> = Vec::new();
    for &(v, n) in &feed {
        match rng.below(3) {
            0 if n == 1 => {
                new.append(&mut buf, v);
                old.append(&mut buf_old, v);
            }
            1 => {
                new.extend(&mut buf, std::iter::repeat_n(v, n));
                old.extend(&mut buf_old, std::iter::repeat_n(v, n));
            }
            _ => {
                new.append_n(&mut buf, v, n);
                old.append_n(&mut buf_old, v, n);
            }
        }
        given.extend(std::iter::repeat_n(v, n));
        assert_eq!((new.len(), new.is_empty()), (given.len(), given.is_empty()));
        assert_eq!((old.len(), old.is_empty()), (given.len(), given.is_empty()));
        // what the buffer held is kept
        assert_eq!(buf[..prefix.len()], prefix[..]);
        assert_eq!(buf, buf_old);
        for &other in &others {
            let uniform = given.iter().all(|x| *x == other);
            assert_eq!(new.is_uniform(other), uniform, "{other:?}");
            assert_eq!(old.is_uniform(other), uniform, "{other:?}");
        }
    }
    new.finish(&mut buf);
    old.finish(&mut buf_old);
    assert_eq!(buf, buf_old);
    assert_eq!(buf, after(&prefix, &bytes));
}

/// A verified delta encoder writes what the old one writes, which is
/// what a column of the same values saves: for every type of value, by
/// every way of giving it values and every way of saving, and through
/// the state that writes into a buffer of the caller's too.
#[test]
fn delta_encoders_agree() {
    for seed in 0..24 {
        delta_encodes::<u32>(seed);
        delta_encodes::<u64>(seed);
        delta_encodes::<i32>(seed);
        delta_encodes::<i64>(seed);
        delta_encodes::<usize>(seed);
        delta_encodes::<Option<u32>>(seed);
        delta_encodes::<Option<u64>>(seed);
        delta_encodes::<Option<i32>>(seed);
        delta_encodes::<Option<i64>>(seed);
        delta_encodes::<Option<usize>>(seed);
    }
}

/// What a delta encoder writes in cases told one by one: nothing for no
/// values, the bytes of a small column, values as far apart as a column
/// holds, runs of billions, values that fit no column asked after, and
/// two states writing one after the other into one buffer.
#[test]
fn delta_encoders_told() {
    // no values: no bytes
    assert!(VerifiedDeltaEncoder::<u32>::new().save().is_empty());
    assert!(VerifiedDeltaEncoder::<Option<i64>>::default()
        .save()
        .is_empty());
    assert!(VerifiedDeltaEncoder::<u64>::encode([]).is_empty());
    let mut out = vec![9];
    let none = VerifiedDeltaEncoder::<i32>::new();
    assert_eq!(none.save_to_unless(&mut out, 5), 1..1);
    assert_eq!(VerifiedDeltaEncoder::<i32>::new().save_to(&mut out), 1..1);
    assert_eq!(out, [9]);
    let state = VerifiedDeltaEncoderState::<Option<u32>>::default();
    assert_eq!((state.len(), state.is_empty()), (0, true));
    assert!(state.is_uniform(None) && state.is_uniform(Some(4)));
    state.finish(&mut out);
    assert_eq!(out, [9]);

    // a run of three steps of one: a count and the difference
    assert_eq!(VerifiedDeltaEncoder::<u32>::encode([1, 2, 3]), [3, 1]);
    assert_eq!(DeltaEncoder::<u32>::encode([1, 2, 3]), [3, 1]);
    // nulls alone: a nought and their count
    let nulls = [None::<i64>; 2];
    assert_eq!(VerifiedDeltaEncoder::<Option<i64>>::encode(nulls), [0, 2]);
    assert_eq!(DeltaEncoder::<Option<i64>>::encode(nulls), [0, 2]);

    // numbers as far apart as a column holds: 2^63 - 1
    for values in [
        [i64::MIN, -1],
        [-1, i64::MIN],
        [0, i64::MAX],
        [i64::MAX, 0],
        [i64::MIN + 1, 0],
        [-2, i64::MAX - 2],
    ] {
        let bytes = VerifiedDeltaEncoder::<i64>::encode(values);
        assert_eq!(bytes, DeltaEncoder::<i64>::encode(values), "{values:?}");
        assert_eq!(bytes, delta_bytes(&values), "{values:?}");
        assert!(delta_same_load::<i64>(&bytes), "{values:?}");
        let loaded = VerifiedDeltaColumn::<i64>::load(&bytes).unwrap();
        assert_eq!(loaded.to_vec(), values);
    }
    let top = i64::MAX as u64;
    let values = [Some(top), None, Some(0), Some(top)];
    let bytes = VerifiedDeltaEncoder::<Option<u64>>::encode(values);
    assert_eq!(bytes, DeltaEncoder::<Option<u64>>::encode(values));
    assert_eq!(bytes, delta_bytes(&values));
    assert!(delta_same_load::<Option<u64>>(&bytes));

    // runs longer than any memory holds, in a few bytes
    let long = usize::MAX / 4;
    let mut new = VerifiedDeltaEncoder::<Option<u32>>::new();
    let mut old = DeltaEncoder::<Option<u32>>::new();
    for value in [Some(7), None, Some(7)] {
        new.append_n(value, long);
        old.append_n(value, long);
        assert_eq!(new.len(), old.len());
    }
    assert_eq!(new.len(), 3 * long);
    assert_eq!(format!("{new:?}"), format!("Verified{old:?}"));
    let bytes = new.save();
    assert_eq!(bytes, old.save());
    assert!(bytes.len() < 40);
    // whether bytes of so many values load, the two APIs agree
    delta_same_load::<Option<u32>>(&bytes);

    // a number no column holds is none of an encoder's values: to ask
    // after it is no error, in either API
    let misfit = Some(u64::MAX);
    let mut new = VerifiedDeltaEncoderState::<Option<u64>>::new();
    let mut old = DeltaEncoderState::<Option<u64>>::new();
    let (mut buf, mut buf_old) = (vec![], vec![]);
    assert!(new.is_uniform(misfit) && old.is_uniform(misfit));
    new.append(&mut buf, Some(3));
    old.append(&mut buf_old, Some(3));
    assert!(new.is_uniform(Some(3)) && old.is_uniform(Some(3)));
    assert!(!new.is_uniform(misfit) && !old.is_uniform(misfit));
    assert!(!new.is_uniform(None) && !old.is_uniform(None));
    let threes = [Some(3u64); 4];
    let bytes = delta_bytes(&threes);
    let n = bytes.len();
    let mut out = vec![1, 2];
    let (new, old) = delta_fed(&[(Some(3u64), 4)]);
    assert_eq!(new.save_to_unless(&mut out, misfit), 2..2 + n);
    assert_eq!(old.save_to_unless(&mut out, misfit), 2 + n..2 + 2 * n);
    assert_eq!(out, [&[1, 2][..], &bytes[..], &bytes[..]].concat());
    type New = VerifiedDeltaEncoder<'static, Option<u64>>;
    let r = New::encode_to_unless(&mut out, threes, Some(3));
    assert_eq!(r, 2 + 2 * n..2 + 2 * n);
    let r = New::encode_to_unless(&mut out, threes, misfit);
    assert_eq!(r, 2 + 2 * n..2 + 3 * n);
    assert_eq!(out[2 + 2 * n..], bytes[..]);

    // two states, one after the other, into one buffer: each keeps what
    // is before it and writes its own column after
    let mut buf = vec![0xee];
    let mut first = VerifiedDeltaEncoderState::<u32>::new();
    first.append_n(&mut buf, 7, 2);
    first.append(&mut buf, 9);
    assert!(!first.is_uniform(7));
    first.finish(&mut buf);
    let mid = buf.len();
    let mut second = VerifiedDeltaEncoderState::<Option<i64>>::new();
    second.append(&mut buf, Some(-5));
    second.append_n(&mut buf, None, 2);
    second.extend(&mut buf, [Some(i64::MIN)]);
    assert_eq!(second.len(), 4);
    second.finish(&mut buf);
    assert_eq!(buf[0], 0xee);
    assert_eq!(buf[1..mid], delta_bytes::<u32>(&[7, 7, 9]));
    let values = [Some(-5), None, None, Some(i64::MIN)];
    assert_eq!(buf[mid..], delta_bytes(&values));
}

/// Values 2^63 or more apart are those of no column, and neither API
/// writes them: each encoder takes them and panics, with the same
/// words, when asked for the bytes. Until then nothing panics, and to
/// ask whether the values are all one is no error.
#[cfg(any(target_pointer_width = "64", panic = "unwind"))]
#[test]
fn delta_encoders_stop_at_values_too_far_apart() {
    let told = Some("delta values out of domain: they span 2^63 or more".to_string());
    for values in [
        [i64::MIN, 0],
        [0, i64::MIN],
        [-1, i64::MAX],
        [i64::MAX, -1],
        [i64::MIN, i64::MAX],
    ] {
        let new = panic_of(move || VerifiedDeltaEncoder::<i64>::encode(values));
        assert_eq!(new, told, "{values:?}");
        let old = panic_of(move || DeltaEncoder::<i64>::encode(values));
        assert_eq!(old, told, "{values:?}");

        let feed = [(values[0], 3), (values[1], 1)];
        let (new, old) = delta_fed(&feed);
        assert_eq!((new.len(), old.len()), (4, 4));
        assert_eq!(panic_of(move || new.save()), told);
        assert_eq!(panic_of(move || old.save()), told);
        let (new, old) = delta_fed(&feed);
        assert_eq!(panic_of(move || new.save_to(&mut vec![1])), told);
        assert_eq!(panic_of(move || old.save_to(&mut vec![1])), told);
        let (new, old) = delta_fed(&feed);
        assert_eq!(panic_of(move || new.save_to_unless(&mut vec![], 7)), told);
        assert_eq!(panic_of(move || old.save_to_unless(&mut vec![], 7)), told);
        let new = panic_of(move || VerifiedDeltaEncoder::<i64>::encode_to(&mut vec![], values));
        assert_eq!(new, told);
        let old = panic_of(move || DeltaEncoder::<i64>::encode_to(&mut vec![], values));
        assert_eq!(old, told);
        let new = panic_of(move || {
            VerifiedDeltaEncoder::<i64>::encode_to_unless(&mut vec![], values, values[0])
        });
        assert_eq!(new, told);
        let old =
            panic_of(move || DeltaEncoder::<i64>::encode_to_unless(&mut vec![], values, values[0]));
        assert_eq!(old, told);

        let mut new = VerifiedDeltaEncoderState::<i64>::new();
        let mut old = DeltaEncoderState::<i64>::new();
        let (mut buf, mut buf_old) = (vec![], vec![]);
        new.extend(&mut buf, values);
        old.extend(&mut buf_old, values);
        assert_eq!((new.len(), old.len()), (2, 2));
        assert!(!new.is_uniform(values[0]) && !old.is_uniform(values[0]));
        assert_eq!(panic_of(move || new.finish(&mut buf)), told);
        assert_eq!(panic_of(move || old.finish(&mut buf_old)), told);
    }

    // with nulls between, and of a type that has them
    let values = [Some(i64::MIN), None, Some(5), None];
    let new = panic_of(move || VerifiedDeltaEncoder::<Option<i64>>::encode(values));
    assert_eq!(new, told);
    let old = panic_of(move || DeltaEncoder::<Option<i64>>::encode(values));
    assert_eq!(old, told);
}

// ── Delta columns: the decoder ───────────────────────────────

/// The differences a delta column of `values` keeps: a model. A null
/// keeps none and moves no sum.
fn delta_diffs<T: Deltad>(values: &[T]) -> Vec<Option<i64>> {
    let mut last = 0i64;
    values
        .iter()
        .map(|&v| {
            let n = <T as DeltaValue>::to_i64(v)?;
            let d = n.wrapping_sub(last);
            last = n;
            Some(d)
        })
        .collect()
}

/// The run of one difference at the head of `diffs`, of `max` at most:
/// a model of what a decoder's next run is.
fn delta_head_run(diffs: &[Option<i64>], max: usize) -> Option<Run<Option<i64>>> {
    let value = *diffs.first()?;
    let count = diffs.iter().take(max).take_while(|d| **d == value).count();
    (count > 0).then_some(Run { count, value })
}

/// A decoder of each API over one saved column: read whole, and then
/// moved alike, step by step, and compared with each other and with a
/// model of the values and of their differences.
fn delta_decodes<T: Deltad>(seed: u64) {
    let mut rng = Rng(seed.wrapping_mul(0x51_7cc1_b727).wrapping_add(29) | 1);
    let feed = delta_feed::<T>(&mut rng);
    let values: Vec<T> = feed
        .iter()
        .flat_map(|&(v, n)| std::iter::repeat_n(v, n))
        .collect();
    let bytes = delta_bytes(&values);
    let diffs = delta_diffs(&values);

    // read whole, whichever way the decoder is made
    let new = VerifiedDeltaDecoder::<T>::new(&bytes);
    let old = DeltaDecoder::<T>::new(&bytes);
    assert_eq!(format!("{new:?}"), format!("Verified{old:?}"));
    assert_eq!(new.collect::<Vec<T>>(), values, "seed {seed}");
    assert_eq!(old.collect::<Vec<T>>(), values, "seed {seed}");
    let mut new = VerifiedDeltaDecoder::<T>::try_new(&bytes).unwrap();
    let mut old = DeltaDecoder::<T>::try_new(&bytes).unwrap();
    assert_eq!(new.clone().collect::<Vec<T>>(), values, "seed {seed}");

    // and moved step by step; `at` is how many values are behind
    let mut at = 0;
    for step in 0..80 {
        match rng.below(6) {
            0 | 1 => {
                let v = new.next();
                assert_eq!(v, old.next(), "seed {seed} step {step}");
                assert_eq!(v, values.get(at).copied(), "seed {seed} step {step}");
                at = (at + 1).min(values.len());
            }
            2 => {
                let n = rng.below(30);
                let v = new.nth(n);
                assert_eq!(v, old.nth(n), "seed {seed} step {step}");
                assert_eq!(v, values.get(at + n).copied(), "seed {seed} step {step}");
                at = (at + n + 1).min(values.len());
            }
            3 => {
                let n = rng.below(30);
                new.advance_by(n);
                old.advance_by(n);
                at = (at + n).min(values.len());
            }
            4 => {
                // a run of no values is no run, and moves nothing
                assert_eq!(new.next_delta_run_max(0), None);
            }
            _ => {
                let max = match rng.below(3) {
                    0 => usize::MAX,
                    _ => 1 + rng.below(40),
                };
                let run = new.next_delta_run_max(max);
                assert_eq!(run, old.next_delta_run_max(max), "seed {seed} step {step}");
                assert_eq!(
                    run,
                    delta_head_run(&diffs[at..], max),
                    "seed {seed} step {step}"
                );
                at += run.map_or(0, |r| r.count);
            }
        }
        assert_eq!(
            format!("{new:?}"),
            format!("Verified{old:?}"),
            "seed {seed} step {step}"
        );
        if rng.below(6) == 0 {
            // a copy reads on from where its decoder is, by itself
            assert_eq!(new.clone().collect::<Vec<T>>(), values[at..]);
            assert_eq!(old.clone().collect::<Vec<T>>(), values[at..]);
        }
    }
    assert_eq!(new.collect::<Vec<T>>(), values[at..], "seed {seed}");
    assert_eq!(old.collect::<Vec<T>>(), values[at..], "seed {seed}");
}

/// Decoders of both APIs read saved columns alike, for the ten types.
#[test]
fn delta_decoders_agree() {
    for seed in 0..16 {
        delta_decodes::<u32>(seed);
        delta_decodes::<u64>(seed);
        delta_decodes::<i32>(seed);
        delta_decodes::<i64>(seed);
        delta_decodes::<usize>(seed);
        delta_decodes::<Option<u32>>(seed);
        delta_decodes::<Option<u64>>(seed);
        delta_decodes::<Option<i32>>(seed);
        delta_decodes::<Option<i64>>(seed);
        delta_decodes::<Option<usize>>(seed);
    }
}

/// Whether both APIs make a checked decoder of `bytes`, which then read
/// alike; if not, both refuse them, and in the same words.
fn delta_same_try_new<T: Deltad>(bytes: &[u8]) -> bool {
    let new = VerifiedDeltaDecoder::<T>::try_new(bytes);
    let old = DeltaDecoder::<T>::try_new(bytes);
    match (new, old) {
        (Ok(new), Ok(old)) => {
            // a spoilt count can be of more values than there is time for
            assert!(new.take(5000).eq(old.take(5000)), "{bytes:?}");
            true
        }
        (Err(new), Err(old)) => {
            assert_eq!(new.to_string(), old.to_string(), "{bytes:?}");
            false
        }
        (new, old) => panic!("{} but {} of {bytes:?}", new.is_ok(), old.is_ok()),
    }
}

/// `delta_same_try_new` for the ten types.
fn delta_checked_decoders(bytes: &[u8]) -> [bool; 10] {
    [
        delta_same_try_new::<u32>(bytes),
        delta_same_try_new::<u64>(bytes),
        delta_same_try_new::<i32>(bytes),
        delta_same_try_new::<i64>(bytes),
        delta_same_try_new::<usize>(bytes),
        delta_same_try_new::<Option<u32>>(bytes),
        delta_same_try_new::<Option<u64>>(bytes),
        delta_same_try_new::<Option<i32>>(bytes),
        delta_same_try_new::<Option<i64>>(bytes),
        delta_same_try_new::<Option<usize>>(bytes),
    ]
}

/// Saved delta columns with a byte changed, cut short or with bytes
/// added: `try_new` of both APIs takes the same of them, as decoders
/// that read alike, and refuses the others with the same error. A
/// decoder is made of any bytes a column loads from.
#[test]
fn delta_checked_decoders_agree_on_any_bytes() {
    let mut taken = 0;
    let mut refused = 0;
    for seed in 0..40u64 {
        let mut rng = Rng(seed.wrapping_mul(0x2f_9be5).wrapping_add(31) | 1);
        let mut last = 0;
        let n = rng.below(60);
        let values: Vec<Option<i64>> = (0..n)
            .map(|_| {
                let (v, l) = <Option<i64> as Deltad>::near(&mut rng, last);
                last = l;
                // nulls in some columns only
                if seed % 2 == 0 {
                    v.or(Some(l))
                } else {
                    v
                }
            })
            .collect();
        let bytes = delta_bytes(&values);
        for _ in 0..40 {
            let mut b = bytes.clone();
            match rng.below(4) {
                0 if !b.is_empty() => {
                    let i = rng.below(b.len());
                    b[i] ^= 1 << rng.below(8);
                }
                1 if !b.is_empty() => {
                    let i = rng.below(b.len());
                    b[i] = rng.next() as u8;
                }
                2 => b.truncate(rng.below(b.len() + 1)),
                _ => {
                    for _ in 0..rng.below(4) + 1 {
                        b.push(rng.next() as u8);
                    }
                }
            }
            let made = delta_checked_decoders(&b);
            for (made, loaded) in made.iter().zip(delta_loads(&b)) {
                assert!(*made || !loaded, "{b:?}");
            }
            for ok in made {
                if ok {
                    taken += 1;
                } else {
                    refused += 1;
                }
            }
        }
    }
    assert!(taken > 1000, "{taken} taken");
    assert!(refused > 1000, "{refused} refused");
}

/// Small cases of the decoder, written out.
#[test]
fn delta_decoders_told() {
    fn run(count: usize, d: i64) -> Option<Run<Option<i64>>> {
        Some(Run {
            count,
            value: Some(d),
        })
    }

    // nothing saved, nothing read
    let mut dec = VerifiedDeltaDecoder::<u32>::new(&[]);
    assert_eq!(dec.next_delta_run_max(usize::MAX), None);
    assert_eq!(dec.next(), None);
    assert_eq!(dec.nth(3), None);
    dec.advance_by(2);
    assert_eq!(format!("{dec:?}"), "VerifiedDeltaDecoder { running: 0 }");

    // the differences of 3, 5, 7, 7 are 3, 2, 2 and 0: a run is of one
    // difference, and of no more values than asked for
    let bytes = delta_bytes::<u32>(&[3, 5, 7, 7]);
    let mut dec = VerifiedDeltaDecoder::<u32>::new(&bytes);
    assert_eq!(dec.next_delta_run_max(0), None);
    assert_eq!(dec.next_delta_run_max(9), run(1, 3));
    assert_eq!(dec.next_delta_run_max(1), run(1, 2));
    assert_eq!(format!("{dec:?}"), "VerifiedDeltaDecoder { running: 5 }");
    assert_eq!(dec.clone().collect::<Vec<_>>(), [7, 7]);
    assert_eq!(dec.next_delta_run_max(9), run(1, 2));
    assert_eq!(dec.next_delta_run_max(9), run(1, 0));
    assert_eq!(dec.next_delta_run_max(9), None);
    assert_eq!(format!("{dec:?}"), "VerifiedDeltaDecoder { running: 7 }");
    let mut dec = VerifiedDeltaDecoder::<u32>::new(&bytes);
    assert_eq!(dec.nth(0), Some(3));
    assert_eq!(dec.next_delta_run_max(usize::MAX), run(2, 2));
    assert_eq!(dec.nth(1), None);
    assert_eq!(dec.next(), None);

    // nulls are runs of their own, and move no sum
    let values = [None, None, Some(-4), None, Some(-4), Some(-8)];
    let bytes = delta_bytes::<Option<i64>>(&values);
    let mut dec = VerifiedDeltaDecoder::<Option<i64>>::new(&bytes);
    let nulls = Some(Run {
        count: 2,
        value: None,
    });
    assert_eq!(dec.next_delta_run_max(5), nulls);
    assert_eq!(format!("{dec:?}"), "VerifiedDeltaDecoder { running: 0 }");
    assert_eq!(dec.next(), Some(Some(-4)));
    assert_eq!(dec.nth(1), Some(Some(-4)));
    assert_eq!(format!("{dec:?}"), "VerifiedDeltaDecoder { running: -4 }");
    dec.advance_by(7);
    assert_eq!(dec.next(), None);
    assert_eq!(format!("{dec:?}"), "VerifiedDeltaDecoder { running: -8 }");
    let mut dec = VerifiedDeltaDecoder::<Option<i64>>::new(&bytes);
    dec.advance_by(3);
    assert_eq!(dec.clone().collect::<Vec<_>>(), values[3..]);
    dec.advance_by(0);
    assert_eq!(dec.collect::<Vec<_>>(), values[3..]);

    // A column of `i64` is not one of `u32` where a value is negative:
    // the checked decoder says so as the load does.
    let bytes = delta_bytes::<i64>(&[3, -1, 5]);
    assert_eq!(
        delta_checked_decoders(&bytes),
        [false, false, true, true, false, false, false, true, true, false]
    );
    let err = VerifiedDeltaDecoder::<u32>::try_new(&bytes).unwrap_err();
    assert!(
        err.to_string().contains("delta value out of domain"),
        "{err}"
    );
    // and a null is a value only of the options
    let bytes = delta_bytes::<Option<i64>>(&[Some(1), None]);
    assert_eq!(
        delta_checked_decoders(&bytes),
        [false, false, false, false, false, true, true, true, true, true]
    );

    // Bytes that no column saves: the differences `i64::MAX` and 1. The
    // exact sum of them is no `i64`, and the checked decoders of both
    // APIs refuse the bytes, as the loads do. Read with sums that wrap
    // they are the values `i64::MAX` and `i64::MIN`, and that is how
    // the specification reads them: `new` gives a decoder of those.
    // (The old `new` adds without wrapping, so it does the same only
    // where overflow is not checked.)
    let wrap = VerifiedColumn::<i64>::from_values(&[i64::MAX, 1]).save();
    assert!(!delta_same_try_new::<i64>(&wrap));
    assert!(!delta_same_load::<i64>(&wrap));
    let dec = VerifiedDeltaDecoder::<i64>::new(&wrap);
    assert_eq!(dec.collect::<Vec<_>>(), [i64::MAX, i64::MIN]);
    #[cfg(not(debug_assertions))]
    assert_eq!(
        DeltaDecoder::<i64>::new(&wrap).collect::<Vec<_>>(),
        [i64::MAX, i64::MIN]
    );
}

// ── Raw columns ──────────────────────────────────────────────

/// `hexane::verified` gives the raw column of the old API, the one type
/// the two APIs share: both paths name it, and its iterator.
#[test]
fn raw_one_type() {
    let mut col: hexane::verified::RawColumn = hexane::RawColumn::new();
    col.splice_slice(0, 0, b"abc");
    let it: hexane::verified::RawColumnIter<'_> = hexane::RawColumn::iter(&col);
    let mut it: hexane::RawColumnIter<'_> = it;
    assert_eq!(it.take(3), b"abc");
    let none: hexane::RawColumnIter<'_> = hexane::verified::RawColumnIter::default();
    assert_eq!(none.pos(), 0);
}

/// Where the slabs of a raw column meet: the places it gives no range
/// across.
fn raw_cuts(col: &RawColumn) -> Vec<usize> {
    (1..col.len())
        .filter(|&i| col.try_get(i - 1..i + 1).is_err())
        .collect()
}

/// A raw column edited at random, and a `Vec<u8>` given the same edits:
/// after each one, every way of reading the column gives the bytes of the
/// `Vec`. A clone given the edits the column is given from then on stays
/// its like, slab for slab.
fn raw_edits(seed: u64, max_segments: usize) {
    let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9).wrapping_add(31) | 1);
    let mut col = RawColumn::with_max_segments(max_segments);
    let mut shadow = col.clone();
    let mut model: Vec<u8> = Vec::new();
    for step in 0..40 {
        let ctx = format!("seed {seed} step {step}");
        let index = rng.below(model.len() + 1);
        let del = rng.below((model.len() - index).min(12) + 1);
        let n = rng.below(20);
        let bytes: Vec<u8> = (0..n).map(|_| rng.next() as u8).collect();

        // an edit the column refuses changes nothing
        if rng.below(6) == 0 {
            let past = model.len() + 1 + rng.below(3);
            assert!(col.try_splice_slice(past, 0, &bytes).is_err(), "{ctx}");
            let too_many = model.len() - index + 1 + rng.below(3);
            assert!(
                col.try_splice_slice(index, too_many, &bytes).is_err(),
                "{ctx}"
            );
            assert_eq!(col.save(), model, "{ctx}");
        }
        if rng.below(4) == 0 {
            shadow = col.clone();
        }
        if rng.below(2) == 0 {
            col.splice_slice(index, del, &bytes);
        } else {
            col.try_splice_slice(index, del, &bytes).unwrap();
        }
        shadow.splice_slice(index, del, &bytes);
        model.splice(index..index + del, bytes.iter().copied());
        let len = model.len();
        assert_eq!(shadow.save(), model, "{ctx}");
        assert_eq!(raw_cuts(&shadow), raw_cuts(&col), "{ctx}");

        assert_eq!((col.len(), col.is_empty()), (len, len == 0), "{ctx}");
        assert_eq!(col.save(), model, "{ctx}");
        let mut buf = vec![7u8; 3];
        let range = col.save_to(&mut buf);
        assert_eq!(range, 3..3 + len, "{ctx}");
        assert_eq!((&buf[..3], &buf[3..]), (&[7u8; 3][..], &model[..]), "{ctx}");

        // a range the column gives is the model's, and `get` gives it too;
        // a range of no bytes within the column is always given, and so is
        // one byte; a range that leaves the column never is
        for _ in 0..8 {
            let start = rng.below(len + 2);
            let end = start + rng.below(6);
            let got = col.try_get(start..end);
            if let Ok(s) = got {
                assert_eq!(s, &model[start..end], "{ctx}");
                assert_eq!(col.get(start..end), s, "{ctx}");
            }
            if end > len {
                assert!(got.is_err(), "{ctx}");
            } else if end - start <= 1 {
                assert!(got.is_ok(), "{ctx}");
            }
        }
        assert!(col.try_get(len + 1..len).is_err(), "{ctx}");

        // a clone is a column of its own
        let mut twin = col.clone();
        assert_eq!(twin.save(), model, "{ctx}");
        twin.splice_slice(0, 0, b"x");
        assert_eq!((col.save(), twin.len()), (model.clone(), len + 1), "{ctx}");

        // what the column saved loads, however small the slabs
        let loaded = RawColumn::load(&model).unwrap();
        assert_eq!((loaded.len(), loaded.save()), (len, model.clone()), "{ctx}");
        let loaded = RawColumn::load_with_max_segments(&model, max_segments).unwrap();
        assert_eq!((loaded.len(), loaded.save()), (len, model.clone()), "{ctx}");

        // an iterator: made at a place, moved on, and read a byte at a time
        let mut it = col.iter();
        let mut read = Vec::new();
        while it.pos() < len {
            assert!(it.take(0).is_empty(), "{ctx}");
            read.extend_from_slice(it.take(1));
        }
        assert_eq!(read, model, "{ctx}");
        let from = rng.below(len + 1);
        let mut it = col.iter_at(from);
        assert_eq!(it.pos(), from, "{ctx}");
        let k = rng.below(len - from + 1);
        it.skip(k);
        assert_eq!(it.pos(), from + k, "{ctx}");
        let to = from + k + rng.below(len - from - k + 1);
        it.seek_to(to);
        assert_eq!(it.pos(), to, "{ctx}");
        let mut other = it.clone();
        if to < len {
            assert_eq!(it.take(1), &model[to..to + 1], "{ctx}");
            assert_eq!(other.take(1), &model[to..to + 1], "{ctx}");
            assert_eq!((it.pos(), other.pos()), (to + 1, to + 1), "{ctx}");
        }
    }
}

#[test]
fn raw_columns_against_a_model() {
    for seed in 0..48 {
        raw_edits(seed, [1, 2, 4, 16, 64, 4096][seed as usize % 6]);
    }
}

/// `new`, `default` and `load` make columns that lay their bytes out
/// alike: given the same bytes and then the same edits, their slabs meet
/// at the same places.
#[test]
fn raw_made_alike() {
    for seed in 0..8u64 {
        let mut rng = Rng(seed.wrapping_mul(0xc2b2_ae35).wrapping_add(41) | 1);
        let n = if seed == 0 { 0 } else { rng.below(40) };
        let first: Vec<u8> = (0..n).map(|_| rng.next() as u8).collect();
        let mut cols = [
            RawColumn::new(),
            RawColumn::default(),
            RawColumn::load(&first).unwrap(),
        ];
        cols[0].splice_slice(0, 0, &first);
        cols[1].splice_slice(0, 0, &first);
        let mut len = first.len();
        for step in 0..60 {
            let index = rng.below(len + 1);
            let del = rng.below((len - index).min(6) + 1);
            let bytes: Vec<u8> = (0..rng.below(12)).map(|_| rng.next() as u8).collect();
            for col in &mut cols {
                col.splice_slice(index, del, &bytes);
            }
            len = len - del + bytes.len();
            let cuts = raw_cuts(&cols[0]);
            assert_eq!(raw_cuts(&cols[1]), cuts, "seed {seed} step {step}");
            assert_eq!(raw_cuts(&cols[2]), cuts, "seed {seed} step {step}");
            assert_eq!(cols[2].len(), len, "seed {seed} step {step}");
        }
    }
}

/// Values put into a raw column whole and taken out of it whole are read
/// whole, however small the slabs are asked to be: no value comes to lie
/// across two slabs, whether values go in one to a splice, as the chunks
/// of one splice, or run together in one slice.
fn raw_values(seed: u64, max_segments: usize) {
    let mut rng = Rng(seed.wrapping_mul(0x85eb_ca6b).wrapping_add(37) | 1);
    let mut col = RawColumn::with_max_segments(max_segments);
    let mut model: Vec<Vec<u8>> = Vec::new();
    for step in 0..60 {
        let ctx = format!("seed {seed} step {step}");
        let at = rng.below(model.len() + 1);
        let del = rng.below((model.len() - at).min(3) + 1);
        let new: Vec<Vec<u8>> = (0..rng.below(4))
            .map(|_| (0..rng.below(9)).map(|_| rng.next() as u8).collect())
            .collect();
        let index: usize = model[..at].iter().map(Vec::len).sum();
        let gone: usize = model[at..at + del].iter().map(Vec::len).sum();
        match rng.below(3) {
            0 => col.splice(index, gone, new.iter()),
            1 => col.splice_slice(index, gone, &new.concat()),
            _ => {
                col.splice_slice(index, gone, &[]);
                let mut place = index;
                for v in &new {
                    col.splice_slice(place, 0, v);
                    place += v.len();
                }
            }
        }
        model.splice(at..at + del, new);

        assert_eq!(col.save(), model.concat(), "{ctx}");
        let mut it = col.iter();
        let mut start = 0;
        for v in &model {
            assert_eq!(it.take(v.len()), &v[..], "{ctx}");
            assert_eq!(col.get(start..start + v.len()), &v[..], "{ctx}");
            assert_eq!(col.iter_at(start).take(v.len()), &v[..], "{ctx}");
            start += v.len();
            assert_eq!(it.pos(), start, "{ctx}");
        }
    }
}

#[test]
fn raw_values_stay_whole() {
    for seed in 0..48 {
        raw_values(seed, [1, 2, 4, 16, 64, 4096][seed as usize % 6]);
    }
}

/// `get` panics of exactly the ranges `try_get` refuses, and says why:
/// those are the ranges its `requires` has a verified caller rule out.
#[cfg(any(target_pointer_width = "64", panic = "unwind"))]
#[test]
fn raw_get_panics_where_try_get_refuses() {
    let mut col = RawColumn::with_max_segments(2);
    for i in 0..3u8 {
        col.splice_slice(col.len(), 0, &[i; 3]);
    }
    let len = col.len();
    // the three values are a slab each: no range across two is given
    assert!(col.try_get(0..len).is_err() && col.try_get(2..4).is_err());
    let (mut given, mut refused) = (0, 0);
    for start in 0..len + 2 {
        for end in 0..len + 2 {
            #[allow(clippy::reversed_empty_ranges)]
            let wanted = col.try_get(start..end).map(|s| s.to_vec());
            let got = panic_of(std::panic::AssertUnwindSafe(|| {
                col.get(start..end).to_vec()
            }));
            match (wanted, got) {
                (Ok(_), None) => given += 1,
                (Err(_), Some(text)) => {
                    assert!(text.contains("raw get: OOB or cross-slab range"), "{text}");
                    refused += 1;
                }
                (wanted, got) => panic!("{start}..{end}: {wanted:?}, {got:?}"),
            }
        }
    }
    assert!(given > 0 && refused > 0);
}

// ── Where a raw column's slabs meet ──────────────────────────

/// What `spliced_cut` says, for the tests: whether `x` may be a cut of a
/// column in which `del` bytes at `index` were replaced by `n`, and whose
/// cuts were `cuts`.
fn raw_spliced_cut(cuts: &[usize], index: usize, del: usize, n: usize, x: usize) -> bool {
    (x <= index && cuts.contains(&x))
        || (x >= index + n && cuts.contains(&(x - n + del)))
        || x == index
        || x == index + n
}

/// All the cuts of a raw column: its start, where its slabs meet, and
/// its end.
fn raw_all_cuts(col: &RawColumn) -> Vec<usize> {
    let mut cuts = vec![0];
    cuts.extend(raw_cuts(col));
    cuts.push(col.len());
    cuts
}

/// The slabs of a raw column meet only where its specification lets
/// them. After each splice every cut is one that `spliced_cut` allows of
/// the cuts before it, and a splice refused leaves them; a range is
/// given exactly when no cut is strictly inside it; a clone has the
/// column's cuts, and a loaded column none but its ends.
fn raw_cut_edits(seed: u64, max_segments: usize) {
    let mut rng = Rng(seed.wrapping_mul(0xa24b_aed4).wrapping_add(43) | 1);
    let mut col = RawColumn::with_max_segments(max_segments);
    assert_eq!(raw_all_cuts(&col), [0, 0]);
    let mut len = 0;
    for step in 0..60 {
        let ctx = format!("seed {seed} step {step}");
        let index = rng.below(len + 1);
        let del = rng.below((len - index).min(12) + 1);
        let n = rng.below(20);
        let bytes: Vec<u8> = (0..n).map(|_| rng.next() as u8).collect();
        let before = raw_all_cuts(&col);
        let too_many = len - index + 1;
        assert!(
            col.try_splice_slice(index, too_many, &bytes).is_err(),
            "{ctx}"
        );
        assert_eq!(raw_all_cuts(&col), before, "{ctx}");
        if rng.below(2) == 0 {
            col.splice_slice(index, del, &bytes);
        } else {
            col.try_splice_slice(index, del, &bytes).unwrap();
        }
        len = len - del + n;
        let after = raw_all_cuts(&col);
        for &x in &after {
            assert!(
                raw_spliced_cut(&before, index, del, n, x),
                "{ctx}: cut {x} of {after:?}, which were {before:?}"
            );
        }
        for _ in 0..12 {
            let start = rng.below(len + 1);
            let end = start + rng.below(len - start + 1);
            let whole = !after.iter().any(|&c| start < c && c < end);
            assert_eq!(
                col.try_get(start..end).is_ok(),
                whole,
                "{ctx}: {start}..{end}"
            );
        }
        assert_eq!(raw_all_cuts(&col.clone()), after, "{ctx}");
        let loaded = RawColumn::load_with_max_segments(&col.save(), max_segments).unwrap();
        assert_eq!(raw_all_cuts(&loaded), [0, len], "{ctx}");
    }
}

#[test]
fn raw_cuts_follow_splices() {
    for seed in 0..48 {
        raw_cut_edits(seed, [1, 2, 4, 16, 64, 4096][seed as usize % 6]);
    }
}

// ── The second codec, bijou64 ────────────────────────────────

/// The modules that name the codec at the import give the types of
/// `hexane::verified` themselves, with LEB128 as their codec.
#[test]
fn codec_named_at_the_import() {
    use hexane::verified::leb128 as l;
    let col: l::VerifiedColumn<u64> = VerifiedColumn::<u64>::from_values(&[1, 2]);
    let bytes = col.save();
    let _: l::VerifiedPrefixColumn<u64> = VerifiedPrefixColumn::<u64>::new();
    let _: l::VerifiedAccColumn<Mark> = VerifiedAccColumn::<Mark>::new();
    let deltas: l::VerifiedDeltaColumn<i64> = VerifiedDeltaColumn::<i64>::from_values(&[5, 6]);
    let delta_bytes = deltas.save();
    let mut e: l::VerifiedEncoder<'_, u64> = VerifiedEncoder::<u64>::new();
    e.append(1);
    e.append(2);
    assert_eq!(e.save(), bytes);
    let d: l::VerifiedDecoder<'_, u64> = VerifiedDecoder::<u64>::new(&bytes);
    assert_eq!(d.collect::<Vec<u64>>(), [1, 2]);
    let mut e: l::VerifiedDeltaEncoder<'_, i64> = VerifiedDeltaEncoder::<i64>::new();
    e.append(5);
    e.append(6);
    assert_eq!(e.save(), delta_bytes);
    let d: l::VerifiedDeltaDecoder<'_, i64> = VerifiedDeltaDecoder::<i64>::new(&delta_bytes);
    assert_eq!(d.collect::<Vec<i64>>(), [5, 6]);
}

/// The numbers of a codec of the verified API are those of the same codec
/// of the old API: written to the same bytes, read from any bytes to the
/// same value or refused in the same words, and of the same length.
fn codec_reads<C: hexane::verified::VCodec + Codec>(data: &[u8]) {
    let (new, old) = (C::read_u(data), C::try_read_unsigned(data));
    match (&new, &old) {
        (Ok(a), Ok(b)) => assert_eq!(a, b, "{data:?}"),
        (Err(a), Err(b)) => assert_eq!(a.to_string(), b.to_string(), "{data:?}"),
        _ => panic!("{new:?} but {old:?} of {data:?}"),
    }
    assert_eq!(C::read_u_opt(data), new.as_ref().ok().copied(), "{data:?}");
    assert_eq!(C::read_u_opt(data), C::read_unsigned(data), "{data:?}");
    assert_eq!(C::len_u(data), C::unsigned_len(data), "{data:?}");
    if let Ok((n, _)) = new {
        assert_eq!(C::len_u(data), Some(n), "{data:?}");
    }

    let (new, old) = (C::read_i(data), C::try_read_signed(data));
    match (&new, &old) {
        (Ok(a), Ok(b)) => assert_eq!(a, b, "{data:?}"),
        (Err(a), Err(b)) => assert_eq!(a.to_string(), b.to_string(), "{data:?}"),
        _ => panic!("{new:?} but {old:?} of {data:?}"),
    }
    assert_eq!(C::read_i_opt(data), new.as_ref().ok().copied(), "{data:?}");
    assert_eq!(C::read_i_opt(data), C::read_signed(data), "{data:?}");
    assert_eq!(C::len_i(data), C::signed_len(data), "{data:?}");
    if let Ok((n, _)) = new {
        assert_eq!(C::len_i(data), Some(n), "{data:?}");
    }
}

fn codec_numbers<C: hexane::verified::VCodec + Codec>() {
    let mut rng = Rng(0xc0_dec5);
    for round in 0..4000 {
        let n = rng.number();
        let i = rng.number() as i64;
        let rest: Vec<u8> = (0..rng.below(4)).map(|_| rng.next() as u8).collect();

        // written as the old API writes it, after what the buffer holds
        let mut unsigned = vec![0xAA];
        C::put_u(n, &mut unsigned);
        assert_eq!(unsigned[0], 0xAA);
        unsigned.remove(0);
        assert_eq!(unsigned, C::encode_unsigned(n).as_bytes(), "{n}");
        let mut signed = vec![0xAA];
        C::put_i(i, &mut signed);
        assert_eq!(signed[0], 0xAA);
        signed.remove(0);
        assert_eq!(signed, C::encode_signed(i).as_bytes(), "{i}");

        // read back, whatever comes after
        let data = [&unsigned[..], &rest[..]].concat();
        assert_eq!(C::read_u(&data).unwrap(), (unsigned.len(), n));
        assert_eq!(C::read_u_opt(&data), Some((unsigned.len(), n)));
        assert_eq!(C::len_u(&data), Some(unsigned.len()));
        codec_reads::<C>(&data);
        let data = [&signed[..], &rest[..]].concat();
        assert_eq!(C::read_i(&data).unwrap(), (signed.len(), i));
        assert_eq!(C::read_i_opt(&data), Some((signed.len(), i)));
        assert_eq!(C::len_i(&data), Some(signed.len()));
        codec_reads::<C>(&data);

        // and what is no number, or not all of one: a number cut short,
        // bytes at random, and bytes that say a number is long
        let mut data = if round % 2 == 0 { unsigned } else { signed };
        match rng.below(4) {
            0 => data.truncate(rng.below(data.len() + 1)),
            1 => data = (0..rng.below(12)).map(|_| rng.next() as u8).collect(),
            2 => data = vec![0xFF - rng.below(3) as u8; rng.below(12)],
            _ => {
                let at = rng.below(data.len());
                data[at] = rng.next() as u8;
                data.truncate(at + 1 + rng.below(3));
            }
        }
        codec_reads::<C>(&data);
    }
}

/// The count that heads a run of unlike values, rewritten in place by the
/// verified API's codec as by the old one's: the bytes before it and after
/// it stay, and the buffer grows or shrinks by what is returned.
fn codec_headers<C: hexane::verified::VCodec + Codec>() {
    let mut rng = Rng(0x04ea_de25);
    for _ in 0..1500 {
        let before: Vec<u8> = (0..rng.below(5)).map(|_| rng.next() as u8).collect();
        let after: Vec<u8> = (0..rng.below(5)).map(|_| rng.next() as u8).collect();
        let was = rng.number() as i64;
        let total = ((rng.number() >> 1) as usize).max(1);
        let mut buf = before.clone();
        C::put_i(was, &mut buf);
        buf.extend_from_slice(&after);
        let len = buf.len() as i64;
        let mut old_buf = buf.clone();
        let grew = C::rewrite_header(&mut buf, before.len(), total);
        let old_grew = C::rewrite_lit_header(&mut old_buf, before.len(), total);
        let mut want = before.clone();
        C::put_i(-(total as i64), &mut want);
        want.extend_from_slice(&after);
        assert_eq!(buf, want, "{was} to {total}");
        assert_eq!(old_buf, want, "{was} to {total}");
        assert_eq!(
            (grew, old_grew),
            (want.len() as i64 - len, want.len() as i64 - len)
        );
    }
}

#[test]
fn codecs_agree_leb128() {
    codec_numbers::<Leb128>();
    codec_headers::<Leb128>();
}

/// The verified API with bijou64 as its codec (`hexane::verified::bijou`)
/// against the old API with the same (`hexane::bijou`).
#[cfg(feature = "bijou64")]
mod bijou {
    use super::*;
    use hexane::verified::{bijou as vb, VCodec};
    use hexane::{bijou as ob, Bijou64};

    /// A value type of both APIs' columns, whichever the codec.
    trait BKind: Kind + VValue<Bijou64> {}

    impl<T: Kind + VValue<Bijou64>> BKind for T {}

    fn new_vals<T: BKind>(v: &vb::VerifiedColumn<T>) -> Vec<T> {
        v.to_vec().into_iter().map(T::keep).collect()
    }

    fn old_vals<T: BKind>(c: &ob::Column<T>) -> Vec<T> {
        c.to_vec().into_iter().map(old).collect()
    }

    #[test]
    fn codecs_agree() {
        codec_numbers::<Bijou64>();
        codec_headers::<Bijou64>();
    }

    /// A number cut short and a number too great for a `u64` are refused
    /// in the old API's words, by the codec and by a load.
    #[test]
    fn refusals_in_the_old_words() {
        let short: &[u8] = &[0xF9, 0x00];
        let over: &[u8] = &[0xFF; 9];
        for (data, why) in [
            (short, "buffer too short for bijou64 encoding"),
            (over, "bijou64 tier 8 payload overflows u64"),
        ] {
            let new = <Bijou64 as VCodec>::read_u(data).unwrap_err().to_string();
            let old = Bijou64::try_read_unsigned(data).unwrap_err().to_string();
            assert_eq!(new, old);
            assert!(new.contains(&format!("bijou64: {why}")), "{new}");
            let new = <Bijou64 as VCodec>::read_i(data).unwrap_err().to_string();
            let old = Bijou64::try_read_signed(data).unwrap_err().to_string();
            assert_eq!(new, old);
            assert!(new.contains(&format!("bijou64s: {why}")), "{new}");

            // as the count of a run, and as the value of a run of two
            assert!(!same_load::<u64>(data));
            let run = [&[0x04][..], data].concat();
            assert!(!same_load::<u64>(&run));
            let new = vb::VerifiedColumn::<u64>::load(&run)
                .unwrap_err()
                .to_string();
            assert!(new.contains(why), "{new}");
        }
    }

    /// What a column saves with bijou64, byte for byte, beside what it
    /// saves with LEB128.
    #[test]
    fn bytes_are_bijou64() {
        fn saved<T: BKind>(values: &[T]) -> (Vec<u8>, Vec<u8>) {
            let bytes = vb::VerifiedColumn::<T>::from_values(&lent(values)).save();
            assert_eq!(bytes, ob::Column::<T>::from_values(values.to_vec()).save());
            let leb = VerifiedColumn::<T>::from_values(&lent(values)).save();
            assert_eq!(leb, Column::<T>::from_values(values.to_vec()).save());
            (bytes, leb)
        }
        // one value: a run of one unlike value, counted below zero
        assert_eq!(saved(&[200u64]), (vec![0x01, 0xC8], vec![0x7F, 0xC8, 0x01]));
        assert_eq!(saved(&[-3i64]), (vec![0x01, 0x05], vec![0x7F, 0x7D]));
        // a run of three; a run of two nulls, which the codecs write alike
        assert_eq!(saved(&[7u64; 3]), (vec![0x06, 0x07], vec![0x03, 0x07]));
        assert_eq!(
            saved(&[None::<u64>; 2]),
            (vec![0x00, 0x02], vec![0x00, 0x02])
        );
        // the greatest number of one byte, and the least of two
        assert_eq!(saved(&[247u64]).0, [0x01, 0xF7]);
        assert_eq!(saved(&[248u64]).0, [0x01, 0xF8, 0x00]);
        assert_eq!(saved(&[u64::MAX]).0.len(), 10);
        assert_eq!(saved::<u64>(&[]), (vec![], vec![]));
    }

    /// Both columns hold `model`, read alike, lie in as many slabs and save
    /// the same bytes.
    fn agree<T: BKind>(v: &vb::VerifiedColumn<T>, c: &ob::Column<T>, model: &[T], ctx: &str) {
        assert_eq!(
            (v.len(), v.is_empty()),
            (model.len(), model.is_empty()),
            "{ctx}"
        );
        assert_eq!(
            (c.len(), c.is_empty()),
            (model.len(), model.is_empty()),
            "{ctx}"
        );
        assert_eq!(new_vals(v), model, "{ctx}");
        assert_eq!(old_vals(c), model, "{ctx}");
        let mut it = v.iter();
        for m in model {
            assert_eq!(it.next().map(T::keep).as_ref(), Some(m), "{ctx}");
        }
        assert!(it.next().is_none(), "{ctx}");
        assert!(v.get(model.len()).is_none(), "{ctx}");
        assert_eq!(v.save(), c.save(), "{ctx}");
        assert_eq!(v.slab_count(), c.slab_count(), "{ctx}");
    }

    /// Whether both APIs load `bytes`, into columns that are alike; if not,
    /// both refuse them, and in the same words. The checked decoders take
    /// what the loads take, and read it alike.
    fn same_load<T: BKind>(bytes: &[u8]) -> bool {
        let x = vb::VerifiedDecoder::<T>::try_new(bytes);
        let y = ob::try_decoder::<T>(bytes);
        let decodes = match (x, y) {
            (Ok(x), Ok(y)) => {
                // a spoilt count can be of more values than there is time for
                let (x, y) = (x.take(3000).map(T::keep), y.take(3000).map(old));
                assert!(x.eq(y), "the decoders read {bytes:?} differently");
                true
            }
            (Err(x), Err(y)) => {
                assert_eq!(x.to_string(), y.to_string(), "{bytes:?}");
                false
            }
            (x, y) => panic!("a decoder: {} but {} of {bytes:?}", x.is_ok(), y.is_ok()),
        };
        let v = vb::VerifiedColumn::<T>::load(bytes);
        let c = ob::Column::<T>::load(bytes);
        match (v, c) {
            (Ok(v), Ok(c)) => {
                assert!(decodes, "no decoder of {bytes:?}, which load");
                assert_eq!(v.len(), c.len());
                if v.len() <= 20_000 {
                    assert_eq!(new_vals(&v), old_vals(&c));
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
                assert_eq!(v.to_string(), c.to_string(), "{bytes:?}");
                false
            }
            (v, c) => panic!(
                "the loaders disagree on {bytes:?}: {:?}, {:?}",
                v.map(|v| v.len()),
                c.map(|c| c.len())
            ),
        }
    }

    /// What the columns save is what the verified API's encoder writes,
    /// given the values at once or one by one; both APIs load it and both
    /// decoders read it.
    fn streams<T: BKind>(v: &vb::VerifiedColumn<T>, model: &[T], ctx: &str) {
        let bytes = v.save();
        assert!(same_load::<T>(&bytes), "{ctx}");
        let loaded = vb::VerifiedColumn::<T>::load(&bytes).unwrap();
        assert_eq!(new_vals(&loaded), model, "{ctx}");
        let built = vb::VerifiedColumn::<T>::from_values(&lent(model));
        assert_eq!(built.save(), bytes, "{ctx}");
        assert_eq!(
            vb::VerifiedEncoder::<T>::encode(lent(model)),
            bytes,
            "{ctx}"
        );
        let mut e = vb::VerifiedEncoder::<T>::new();
        for m in model {
            e.append(m.lend());
        }
        assert_eq!(e.len(), model.len(), "{ctx}");
        assert_eq!(e.save(), bytes, "{ctx}");
        let read: Vec<T> = vb::VerifiedDecoder::<T>::new(&bytes).map(T::keep).collect();
        assert_eq!(read, model, "{ctx}");
        let read: Vec<T> = ob::decoder::<T>(&bytes).map(old).collect();
        assert_eq!(read, model, "{ctx}");
    }

    fn columns<T: BKind>() {
        for seed in 1..=4u64 {
            let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0xb170);
            let budget = [2, 4, 64][rng.below(3)];
            let mut v = vb::VerifiedColumn::<T>::with_max_segments(budget);
            let mut c = ob::Column::<T>::with_max_segments(budget);
            let mut model: Vec<T> = vec![];
            for step in 0..50 {
                let ctx = format!("seed {seed} step {step}");
                match rng.below(8) {
                    0..=3 => {
                        let at = rng.below(model.len() + 1);
                        let del = rng.below((model.len() - at).min(12) + 1);
                        let ins: Vec<T> = rng.values(30);
                        v.splice(at, del, &lent(&ins));
                        c.splice(at, del, ins.iter().cloned());
                        model.splice(at..at + del, ins);
                    }
                    4 => {
                        let at = rng.below(model.len() + 1);
                        let x = T::fresh(&mut rng);
                        v.insert(at, x.lend());
                        c.insert(at, x.clone());
                        model.insert(at, x);
                    }
                    5 => {
                        let x = match model.last() {
                            Some(last) => rng.value(last),
                            None => T::fresh(&mut rng),
                        };
                        v.push(x.lend());
                        c.push(x.clone());
                        model.push(x);
                    }
                    6 => {
                        if !model.is_empty() {
                            let at = rng.below(model.len());
                            v.remove(at);
                            c.remove(at);
                            model.remove(at);
                        }
                    }
                    _ => {
                        let at = rng.below(model.len() + 1);
                        let n = rng.below((model.len() - at).min(20) + 1);
                        v.remove_n(at, n);
                        c.remove_n(at, n);
                        model.drain(at..at + n);
                    }
                }
                agree(&v, &c, &model, &ctx);
                if !model.is_empty() {
                    let i = rng.below(model.len());
                    assert_eq!(v.get(i).map(T::keep).as_ref(), Some(&model[i]), "{ctx}");
                    assert_eq!(c.get(i).map(old).as_ref(), Some(&model[i]), "{ctx}");
                }
                if step % 10 == 9 {
                    streams(&v, &model, &ctx);
                }
            }
        }
    }

    /// The columns of the two APIs, with bijou64 as their codec, are alike
    /// under the same edits, for every type of value.
    #[test]
    fn columns_agree() {
        for_each_kind!(columns);
    }

    fn spoilt<T: BKind>() {
        let mut rng = Rng(0xb1_7064);
        let (mut taken, mut refused) = (0, 0);
        for _ in 0..80 {
            let model: Vec<T> = rng.values(30);
            let mut bytes = ob::Column::<T>::from_values(model).save();
            match rng.below(5) {
                0 => bytes.truncate(rng.below(bytes.len() + 1)),
                1 => bytes = (0..rng.below(12)).map(|_| rng.next() as u8).collect(),
                2 => {
                    let at = rng.below(bytes.len() + 1);
                    bytes.insert(at, rng.next() as u8);
                }
                3 => {
                    let at = rng.below(bytes.len() + 1);
                    bytes.insert(at, 0xFF - rng.below(8) as u8);
                }
                _ => {
                    if !bytes.is_empty() {
                        let at = rng.below(bytes.len());
                        bytes[at] = rng.next() as u8;
                    }
                }
            }
            if same_load::<T>(&bytes) {
                taken += 1;
            } else {
                refused += 1;
            }
        }
        assert!(taken > 0 && refused > 0, "{taken} taken, {refused} refused");
    }

    /// Bytes a column saved and then spoilt, and bytes at random: both
    /// APIs take the same of them, and refuse the rest in the same words.
    #[test]
    fn loads_agree() {
        for_each_kind!(spoilt);
    }

    /// The same values kept with either codec: what one column gives, the
    /// other is made of; and the bytes of one codec are no column of the
    /// other, or are one that both APIs load alike.
    fn across<T: BKind>() {
        let mut rng = Rng(0xac_2055);
        for _ in 0..12 {
            let model: Vec<T> = rng.values(40);
            let leb = VerifiedColumn::<T>::from_values(&lent(&model));
            let bij = vb::VerifiedColumn::<T>::from_values(&leb.to_vec());
            assert_eq!(new_vals(&bij), model);
            let back = VerifiedColumn::<T>::from_values(&bij.to_vec());
            assert_eq!(back.save(), leb.save());
            same_load::<T>(&leb.save());
            check_same_load::<T>(&bij.save());
        }
    }

    #[test]
    fn one_codec_to_the_other() {
        for_each_kind!(across);
    }

    /// The types of `hexane::verified::bijou` are those of
    /// `hexane::verified` with the codec named.
    #[test]
    fn named_at_the_import() {
        use hexane::verified as v;
        let bytes = [];
        let _: vb::VerifiedColumn<u64> = v::VerifiedColumn::<u64, Bijou64>::new();
        let _: vb::VerifiedPrefixColumn<u64> = v::VerifiedPrefixColumn::<u64, Bijou64>::new();
        let _: vb::VerifiedAccColumn<Mark> = v::VerifiedAccColumn::<Mark, Bijou64>::new();
        let _: vb::VerifiedDeltaColumn<i64> = v::VerifiedDeltaColumn::<i64, Bijou64>::new();
        let _: vb::VerifiedEncoder<'_, u64> = v::VerifiedEncoder::<u64, Bijou64>::new();
        let _: vb::VerifiedDecoder<'_, u64> = v::VerifiedDecoder::<u64, Bijou64>::new(&bytes);
        let _: vb::VerifiedDeltaEncoder<'_, i64> = v::VerifiedDeltaEncoder::<i64, Bijou64>::new();
        let _: vb::VerifiedDeltaDecoder<'_, i64> =
            v::VerifiedDeltaDecoder::<i64, Bijou64>::new(&bytes);
    }

    /// A prefix column with bijou64 as its codec answers from its sums what
    /// its values add up to and what the old API's answers, and saves the
    /// old one's bytes.
    fn sums<T: Totalled + VValue<Bijou64>>() {
        let mut rng = Rng(0x50f5_b170);
        for round in 0..6 {
            let mut model: Vec<T> = rng.values(60);
            let mut p = vb::VerifiedPrefixColumn::<T>::from_values(&lent(&model));
            let mut c = ob::PrefixColumn::<T>::from_values(model.clone());
            for step in 0..6 {
                let ctx = format!("round {round} step {step}");
                let at = rng.below(model.len() + 1);
                let del = rng.below((model.len() - at).min(8) + 1);
                let ins: Vec<T> = rng.values(12);
                p.splice(at, del, &lent(&ins));
                c.splice(at, del, ins.iter().cloned());
                model.splice(at..at + del, ins);

                assert_eq!(p.save(), c.save(), "{ctx}");
                let kept: Vec<T> = p.to_vec().into_iter().map(T::keep).collect();
                assert_eq!(kept, model, "{ctx}");
                let before = sums_before(&model);
                let at = |i: usize| before[i.min(model.len())];
                for i in 0..model.len() + 2 {
                    assert_eq!(T::new_sum(p.get_prefix(i)), at(i), "{ctx}");
                    assert_eq!(T::old_sum(c.get_prefix(i)), at(i), "{ctx}");
                    assert_eq!(T::new_sum(p.get_total(i)), at(i + 1), "{ctx}");
                    assert_eq!(T::old_sum(c.get_total(i)), at(i + 1), "{ctx}");
                }
                for _ in 0..8 {
                    let start = rng.below(model.len() + 2);
                    let end = rng.below(model.len() + 2);
                    let want = if start < end { at(end) - at(start) } else { 0 };
                    assert_eq!(T::new_sum(p.sum_range(start, end)), want, "{ctx}");
                    assert_eq!(T::old_sum(c.sum_range(start..end)), want, "{ctx}");
                }
            }
            let loaded = vb::VerifiedPrefixColumn::<T>::load(&c.save()).unwrap();
            assert_eq!(loaded.save(), p.save());
            let last = model.len();
            assert_eq!(
                T::new_sum(loaded.get_prefix(last)),
                sums_before(&model)[last]
            );
        }
    }

    #[test]
    fn sums_agree() {
        sums::<u64>();
        sums::<i64>();
        sums::<u32>();
        sums::<NonZeroU32>();
        sums::<bool>();
        sums::<Option<u64>>();
        sums::<Option<i64>>();
        sums::<Option<u32>>();
        sums::<Option<NonZeroU32>>();
    }

    /// A prefix column with bijou64 as its codec is searched by sum as the
    /// old API's is.
    #[test]
    fn sums_are_searched() {
        let mut rng = Rng(0x005e_a2c4);
        for _ in 0..20 {
            let model: Vec<u64> = (0..rng.below(80)).map(|_| rng.next() % 5).collect();
            let p = vb::VerifiedPrefixColumn::<u64>::from_values(&model);
            let c = ob::PrefixColumn::<u64>::from_values(model.clone());
            let all: u128 = model.iter().map(|&m| m as u128).sum();
            for t in 0..all + 3 {
                assert_eq!(p.get_index_for_prefix(t), c.get_index_for_prefix(t));
                assert_eq!(p.get_index_for_total(t), c.get_index_for_total(t));
            }
        }
    }

    /// An accumulator column with bijou64 as its codec gives what the same
    /// values give with LEB128, and saves what a plain column of them
    /// saves with bijou64.
    fn accs<T: Accd + VValue<Bijou64>>() {
        let mut rng = Rng(0xacc5_b170);
        for _ in 0..6 {
            let model: Vec<T> = rng.values(60);
            let a = vb::VerifiedAccColumn::<T>::from_values(&lent(&model));
            let l = VerifiedAccColumn::<T>::from_values(&lent(&model));
            let plain = vb::VerifiedColumn::<T>::from_values(&lent(&model));
            assert_eq!(a.save(), plain.save());
            let kept: Vec<T> = a.to_vec().into_iter().map(T::keep).collect();
            assert_eq!(kept, model);
            for i in 0..model.len() + 2 {
                assert!(a.get_prefix(i) == l.get_prefix(i));
                assert!(a.get_total(i) == l.get_total(i));
                let end = i + rng.below(model.len() + 1);
                assert!(a.sum_range(i, end) == l.sum_range(i, end));
            }
            let loaded = vb::VerifiedAccColumn::<T>::load(&a.save()).unwrap();
            assert!(loaded.get_prefix(model.len()) == l.get_prefix(model.len()));
        }
    }

    #[test]
    fn accumulators_agree() {
        accs::<Mark>();
        accs::<Option<Mark>>();
        accs::<Step>();
        accs::<Option<Step>>();
    }

    /// A value type of both APIs' delta columns, whichever the codec.
    trait BDelta: Deltad + VDeltaValue<Bijou64> {}

    impl<T: Deltad + VDeltaValue<Bijou64>> BDelta for T {}

    /// Whether both APIs load `bytes` as a delta column, into columns that
    /// are alike, and make checked decoders of them that read alike; if
    /// not, they refuse them in the same words.
    fn same_delta_load<T: BDelta>(bytes: &[u8]) -> bool {
        let x = vb::VerifiedDeltaDecoder::<T>::try_new(bytes);
        let y = ob::DeltaDecoder::<T>::try_new(bytes);
        match (x, y) {
            (Ok(x), Ok(y)) => assert!(x.take(3000).eq(y.take(3000)), "{bytes:?}"),
            (Err(x), Err(y)) => assert_eq!(x.to_string(), y.to_string(), "{bytes:?}"),
            (x, y) => panic!("a decoder: {} but {} of {bytes:?}", x.is_ok(), y.is_ok()),
        }
        let new = vb::VerifiedDeltaColumn::<T>::load(bytes);
        let old = ob::DeltaColumn::<T>::load(bytes);
        match (new, old) {
            (Ok(new), Ok(old)) => {
                assert_eq!(new.len(), old.len(), "{bytes:?}");
                let len = new.len();
                for i in [0, 1, 2, len / 2, len.saturating_sub(1), len] {
                    assert_eq!(new.get(i), old.get(i), "{bytes:?}");
                }
                true
            }
            (Err(new), Err(old)) => {
                assert_eq!(new.to_string(), old.to_string(), "{bytes:?}");
                false
            }
            (new, old) => panic!("a load: {} but {} of {bytes:?}", new.is_ok(), old.is_ok()),
        }
    }

    fn deltas<T: BDelta>(seed: u64) {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9).wrapping_add(0xde17) | 1);
        let mut last = 0;
        let mut some = |rng: &mut Rng, n: usize| -> Vec<T> {
            (0..n)
                .map(|_| {
                    let (v, l) = T::near(rng, last);
                    last = l;
                    v
                })
                .collect()
        };
        let n = rng.below(120);
        let mut model = some(&mut rng, n);
        let mut new = vb::VerifiedDeltaColumn::<T>::from_values(&model);
        let mut old = ob::DeltaColumn::<T>::from_values(model.clone());
        for step in 0..20 {
            let ctx = format!("seed {seed} step {step}");
            let at = rng.below(model.len() + 1);
            let del = rng.below((model.len() - at).min(6) + 1);
            let n = rng.below(9);
            let values = some(&mut rng, n);
            new.splice(at, del, &values);
            old.splice(at, del, values.clone());
            model.splice(at..at + del, values);
            assert_eq!((new.len(), old.len()), (model.len(), model.len()), "{ctx}");
            assert_eq!(new.to_vec(), model, "{ctx}");
            for i in 0..model.len() + 2 {
                assert_eq!(new.get(i), old.get(i), "{ctx}");
            }
            assert_eq!(new.save(), old.save(), "{ctx}");
        }

        // what the columns save: loaded, written by the encoder at once
        // and value by value, and read by both decoders
        let bytes = new.save();
        assert!(same_delta_load::<T>(&bytes));
        let loaded = vb::VerifiedDeltaColumn::<T>::load(&bytes).unwrap();
        assert_eq!(loaded.to_vec(), model);
        assert_eq!(vb::VerifiedDeltaEncoder::<T>::encode(model.clone()), bytes);
        let mut e = vb::VerifiedDeltaEncoder::<T>::new();
        for &m in &model {
            e.append(m);
        }
        assert_eq!(e.save(), bytes);
        let read: Vec<T> = vb::VerifiedDeltaDecoder::<T>::new(&bytes).collect();
        assert_eq!(read, model);
        let read: Vec<T> = ob::DeltaDecoder::<T>::new(&bytes).collect();
        assert_eq!(read, model);

        // and spoilt
        for _ in 0..12 {
            let mut bad = bytes.clone();
            match rng.below(4) {
                0 => bad.truncate(rng.below(bad.len() + 1)),
                1 => bad = (0..rng.below(10)).map(|_| rng.next() as u8).collect(),
                2 => {
                    let at = rng.below(bad.len() + 1);
                    bad.insert(at, 0xFF - rng.below(8) as u8);
                }
                _ => {
                    if !bad.is_empty() {
                        let at = rng.below(bad.len());
                        bad[at] = rng.next() as u8;
                    }
                }
            }
            same_delta_load::<T>(&bad);
        }
    }

    /// The delta columns of the two APIs, with bijou64 as their codec, are
    /// alike under the same edits, save the same bytes, and take and
    /// refuse the same bytes, for every type of value.
    #[test]
    fn deltas_agree() {
        for seed in 0..6 {
            deltas::<u32>(seed);
            deltas::<u64>(seed);
            deltas::<i32>(seed);
            deltas::<i64>(seed);
            deltas::<usize>(seed);
            deltas::<Option<u32>>(seed);
            deltas::<Option<u64>>(seed);
            deltas::<Option<i32>>(seed);
            deltas::<Option<i64>>(seed);
            deltas::<Option<usize>>(seed);
        }
    }
}

// ── The edit cursor ──────────────────────────────────────────

/// One step of an edit cursor.
#[derive(Clone, Debug)]
enum Stroke<T> {
    Advance(usize),
    /// to a place at or after the cursor's
    Seek(usize),
    Insert(T),
    InsertRun(T, usize),
    InsertRuns(Vec<(usize, T)>),
    Replace(T),
    Delete(usize),
}

/// What the steps of a cursor make of a column, told by lists: the values
/// the column began with, the cursor's place among them, and the values
/// written so far.
struct Told<T> {
    orig: Vec<T>,
    at: usize,
    out: Vec<T>,
    changed: bool,
}

impl<T: Kind> Told<T> {
    fn new(orig: &[T], at: usize) -> Self {
        Told {
            orig: orig.to_vec(),
            at,
            out: orig[..at].to_vec(),
            changed: false,
        }
    }

    fn left(&self) -> usize {
        self.orig.len() - self.at
    }

    fn keep(&mut self, n: usize) {
        let k = n.min(self.left());
        self.out.extend_from_slice(&self.orig[self.at..self.at + k]);
        self.at += k;
    }

    fn step(&mut self, m: &Stroke<T>) {
        match m {
            Stroke::Advance(n) => self.keep(*n),
            Stroke::Seek(to) => self.keep(*to - self.at),
            Stroke::Insert(x) => {
                self.out.push(x.clone());
                self.changed = true;
            }
            Stroke::InsertRun(x, n) => {
                self.out.extend(std::iter::repeat_n(x, *n).cloned());
                self.changed |= *n > 0;
            }
            Stroke::InsertRuns(runs) => {
                let values = run_model(runs);
                self.changed |= !values.is_empty();
                self.out.extend(values);
            }
            Stroke::Replace(x) => {
                if self.left() > 0 {
                    self.changed |= self.orig[self.at] != *x;
                    self.out.push(x.clone());
                    self.at += 1;
                }
            }
            Stroke::Delete(n) => {
                let k = (*n).min(self.left());
                self.changed |= k > 0;
                self.at += k;
            }
        }
    }

    fn peek(&self) -> Option<&T> {
        self.orig.get(self.at)
    }

    /// The column, were the edit finished now.
    fn done(mut self) -> Vec<T> {
        let rest = self.orig.split_off(self.at);
        self.out.extend(rest);
        self.out
    }
}

/// Steps for a cursor at `at` of `model`: most within the values left,
/// some of none, some to the end, and some past it, where a cursor stops.
fn some_moves<T: Kind>(rng: &mut Rng, model: &[T], at: usize) -> Vec<Stroke<T>> {
    let mut at = at;
    let mut moves = vec![];
    for _ in 0..rng.below(9) {
        let left = model.len() - at;
        let count = match rng.below(12) {
            0 => 0,
            1 => left,
            2 => left + 1 + rng.below(3),
            3 => usize::MAX,
            _ => rng.below(left.min(30) + 1),
        };
        let x = match model.get(at) {
            Some(x) => rng.value(x),
            None => T::fresh(rng),
        };
        let m = match rng.below(13) {
            0 | 1 => Stroke::Advance(count),
            2 | 3 => Stroke::Seek(at.saturating_add(count)),
            4 | 5 => Stroke::Insert(x),
            6 => Stroke::InsertRun(x, [0, 1, 2, 9, 70][rng.below(5)]),
            7 => Stroke::InsertRuns(some_runs(rng, 4)),
            8 | 9 => Stroke::Replace(x),
            // the value that is there, which is no change
            10 => Stroke::Replace(model.get(at).cloned().unwrap_or(x)),
            _ => Stroke::Delete(count),
        };
        at = match &m {
            Stroke::Advance(n) | Stroke::Delete(n) => at + (*n).min(left),
            Stroke::Seek(to) => (*to).min(model.len()),
            Stroke::Replace(_) => at + left.min(1),
            _ => at,
        };
        moves.push(m);
    }
    moves
}

/// Where a cursor of a column of `len` values is opened: often at an end.
fn some_place(rng: &mut Rng, len: usize) -> usize {
    match rng.below(5) {
        0 => 0,
        1 => len,
        _ => rng.below(len + 1),
    }
}

/// A step of a cursor of the verified API.
macro_rules! new_move {
    ($e:expr, $m:expr) => {
        match $m {
            Stroke::Advance(n) => {
                $e.advance(*n);
            }
            Stroke::Seek(to) => {
                $e.seek(*to);
            }
            Stroke::Insert(x) => {
                $e.insert(x.lend());
            }
            Stroke::InsertRun(x, n) => {
                $e.insert_run(x.lend(), *n);
            }
            Stroke::InsertRuns(runs) => {
                $e.insert_runs(runs.iter().map(|(n, x)| Run {
                    count: *n,
                    value: x.lend(),
                }));
            }
            Stroke::Replace(x) => {
                $e.replace_with(x.lend());
            }
            Stroke::Delete(n) => {
                $e.delete(*n);
            }
        }
    };
}

/// The same step of a cursor of the old API, whose `replace` takes a
/// function of the value that is there.
macro_rules! old_move {
    ($e:expr, $m:expr) => {
        match $m {
            Stroke::Advance(n) => {
                $e.advance(*n);
            }
            Stroke::Seek(to) => {
                $e.seek(*to);
            }
            Stroke::Insert(x) => {
                $e.insert(x.clone());
            }
            Stroke::InsertRun(x, n) => {
                $e.insert_run(x.clone(), *n);
            }
            Stroke::InsertRuns(runs) => {
                $e.insert_runs(runs.iter().map(|(n, x)| Run {
                    count: *n,
                    value: x.clone(),
                }));
            }
            Stroke::Replace(x) => {
                $e.replace(|_| x.clone());
            }
            Stroke::Delete(n) => {
                $e.delete(*n);
            }
        }
    };
}

fn cursors<T: Kind>() {
    for seed in 1..=8u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0xed17);
        let budget = [2, 3, 4, 8, 64][rng.below(5)];
        let mut model: Vec<T> = rng.values(200);
        let (mut v, mut c) = both_with(&model, budget);
        for round in 0..40 {
            let at = some_place(&mut rng, model.len());
            let moves = some_moves(&mut rng, &model, at);
            let mut told = Told::new(&model, at);
            let (s, t) = (v.iter().suspend(), c.iter().suspend());
            // `edit` is `edit_at(0)`
            let first = at == 0 && round % 2 == 0;
            let mut e = if first { v.edit() } else { v.edit_at(at) };
            let mut o = if first { c.edit() } else { c.edit_at(at) };
            assert_eq!((e.pos(), e.out_pos()), (at, at));
            assert_eq!((o.pos(), o.out_pos()), (at, at));
            for m in &moves {
                new_move!(e, m);
                old_move!(o, m);
                told.step(m);
                let place = (told.at, told.out.len());
                assert_eq!((e.pos(), e.out_pos()), place, "{m:?}");
                assert_eq!((o.pos(), o.out_pos()), place, "{m:?}");
                assert_eq!(e.rebuilds(), o.rebuilds(), "{m:?}");
                if rng.below(3) == 0 {
                    // a look at the value there, which moves nothing
                    assert_eq!(e.peek().map(T::keep).as_ref(), told.peek(), "{m:?}");
                    assert_eq!(o.peek().map(old::<T>).as_ref(), told.peek(), "{m:?}");
                    assert_eq!((e.pos(), e.out_pos()), place, "{m:?}");
                    assert_eq!(e.rebuilds(), o.rebuilds(), "{m:?}");
                }
            }
            e.finish();
            // the old cursor is finished by the call or when it is dropped
            if round % 3 == 0 {
                o.finish();
            }
            drop(o);
            let changed = told.changed;
            model = told.done();
            check(&v, &c, &model);
            // an edit is counted exactly where a step put a value in or
            // took one out: an iterator's state taken before resumes
            // exactly where none did
            assert_eq!(s.try_resume(&v).is_ok(), !changed, "{moves:?}");
            assert_eq!(t.try_resume(&c).is_ok(), !changed, "{moves:?}");
            if round % 10 == 9 {
                check_loads(&v, &c, &model);
            }
        }
        check_loads(&v, &c, &model);
    }
}

/// An edit cursor of the verified API (`VerifiedEdit`) does what
/// `Column`'s does: under the same steps both stand at the same places,
/// see the same values, rebuild as many slabs, and leave the same values
/// in the same slabs, whose bytes are the same. The places and the values
/// are what the steps make of a list. And the column counts an edit
/// exactly where a step changed it.
#[test]
fn a_cursor_edits_as_the_old_one_does() {
    for_each_kind!(cursors);
}

fn sum_cursors<T: Totalled>() {
    for seed in 1..=6u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0xed18);
        let budget = [2, 3, 4, 8, 64][rng.below(5)];
        let mut p = VerifiedPrefixColumn::<T>::with_max_segments(budget);
        let mut v = VerifiedColumn::<T>::with_max_segments(budget);
        let mut c = PrefixColumn::<T>::with_max_segments(budget);
        let mut model: Vec<T> = rng.values(200);
        p.splice(0, 0, &lent(&model));
        v.splice(0, 0, &lent(&model));
        c.splice(0, 0, model.iter().cloned());
        for round in 0..30 {
            let at = some_place(&mut rng, model.len());
            let moves = some_moves(&mut rng, &model, at);
            let mut told = Told::new(&model, at);
            let (mut e, mut f, mut o) = (p.edit_at(at), v.edit_at(at), c.edit_at(at));
            for m in &moves {
                new_move!(e, m);
                new_move!(f, m);
                old_move!(o, m);
                told.step(m);
                let place = (told.at, told.out.len());
                assert_eq!((e.pos(), e.out_pos()), place, "{m:?}");
                assert_eq!((f.pos(), f.out_pos()), place, "{m:?}");
                assert_eq!((o.pos(), o.out_pos()), place, "{m:?}");
                assert_eq!((e.rebuilds(), f.rebuilds()), (o.rebuilds(), o.rebuilds()));
                assert_eq!(e.peek().map(T::keep).as_ref(), told.peek(), "{m:?}");
            }
            e.finish();
            f.finish();
            drop(o);
            model = told.done();
            check_sums(&p, &v, &c, &model, true);
            check_sum_reads(&mut rng, &p, &v, &model);
            check_prefix_reads(&mut rng, &p, &c, &model);
            if round % 10 == 9 {
                check_sum_loads(&p, &c, &model);
            }
        }
        check_sum_loads(&p, &c, &model);
    }
}

/// A cursor edits a prefix column as it edits a plain one, and as
/// `PrefixColumn`'s cursor does: the same values in the same slabs, and
/// an index that answers with the sums of the values as they are after
/// the edit.
#[test]
fn a_cursor_of_a_prefix_column() {
    for_each_sum!(sum_cursors);
}

fn acc_cursors<T: Accd>() {
    for seed in 1..=6u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0xed19);
        let budget = [2, 3, 4, 8, 64][rng.below(5)];
        let mut p = VerifiedAccColumn::<T>::with_max_segments(budget);
        let mut v = VerifiedColumn::<T>::with_max_segments(budget);
        let mut model: Vec<T> = rng.values(200);
        p.splice(0, 0, &lent(&model));
        v.splice(0, 0, &lent(&model));
        for round in 0..30 {
            let at = some_place(&mut rng, model.len());
            let moves = some_moves(&mut rng, &model, at);
            let mut told = Told::new(&model, at);
            let (mut e, mut f) = (p.edit_at(at), v.edit_at(at));
            for m in &moves {
                new_move!(e, m);
                new_move!(f, m);
                told.step(m);
                let place = (told.at, told.out.len());
                assert_eq!((e.pos(), e.out_pos()), place, "{m:?}");
                assert_eq!((f.pos(), f.out_pos()), place, "{m:?}");
                assert_eq!(e.rebuilds(), f.rebuilds(), "{m:?}");
                assert_eq!(e.peek().map(T::keep).as_ref(), told.peek(), "{m:?}");
            }
            e.finish();
            f.finish();
            model = told.done();
            check_acc(&p, &v, &model, true);
            check_acc_reads(&mut rng, &p, &model);
            check_acc_plain_reads(&mut rng, &p, &v, &model);
            T::check_old(&mut rng, &p, &model);
            if round % 10 == 9 {
                check_acc_loads(&mut rng, &p, &model);
            }
        }
        check_acc_loads(&mut rng, &p, &model);
    }
}

/// A cursor edits an accumulator column as it edits a plain one: the
/// same values in the same slabs, and an index that answers with what
/// the values add up to as they are after the edit.
#[test]
fn a_cursor_of_an_accumulator_column() {
    for_each_acc!(acc_cursors);
}

/// The steps of a cursor one by one, on a column short enough to read.
#[test]
fn a_cursor_step_by_step() {
    let mut v = VerifiedColumn::<u64>::from_values(&[1, 2, 3, 4, 5]);
    let mut e = v.edit();
    assert_eq!((e.pos(), e.out_pos(), e.peek()), (0, 0, Some(1)));
    e.advance(1);
    assert_eq!((e.pos(), e.out_pos(), e.peek()), (1, 1, Some(2)));
    e.delete(2);
    assert_eq!((e.pos(), e.out_pos(), e.peek()), (3, 1, Some(4)));
    e.insert(9).insert_run(8, 2);
    assert_eq!((e.pos(), e.out_pos(), e.peek()), (3, 4, Some(4)));
    e.seek(4).replace_with(7);
    assert_eq!((e.pos(), e.out_pos(), e.peek()), (5, 6, None));
    // at the end there is nothing to replace, to delete or to pass over
    e.replace_with(0)
        .delete(3)
        .advance(usize::MAX)
        .seek(usize::MAX);
    assert_eq!((e.pos(), e.out_pos(), e.peek()), (5, 6, None));
    let runs = [Run { count: 2, value: 6 }, Run { count: 0, value: 5 }];
    e.insert_runs(runs);
    assert_eq!((e.pos(), e.out_pos()), (5, 8));
    e.finish();
    assert_eq!(v.to_vec(), [1, 9, 8, 8, 4, 7, 6, 6]);

    // a cursor opened in the middle keeps what is before it
    let mut e = v.edit_at(6);
    assert_eq!((e.pos(), e.out_pos(), e.peek()), (6, 6, Some(6)));
    e.delete(usize::MAX);
    e.finish();
    assert_eq!(v.to_vec(), [1, 9, 8, 8, 4, 7]);

    // values that are lent: a look at one, and another in its place
    let mut b = VerifiedColumn::<Vec<u8>>::from_values(&[&b"ab"[..], b"", b"c"]);
    let mut e = b.edit();
    assert_eq!(e.peek(), Some(&b"ab"[..]));
    e.replace_with(b"xyz").replace_with(b"");
    assert_eq!(e.peek(), Some(&b"c"[..]));
    e.finish();
    assert_eq!(b.to_vec(), [&b"xyz"[..], b"", b"c"]);
}

/// A cursor that is not finished does not give the column its values
/// back: the column reads as an empty one, and an edit of it panics, as
/// after a panic in the middle of an edit. `Column`'s cursor finishes
/// when it is dropped; forgotten instead, it leaves the same.
#[cfg(panic = "unwind")]
#[test]
fn a_cursor_dropped_unfinished() {
    let lost = "column used after a panic during an edit";
    let mut v = VerifiedColumn::<u64>::from_values(&[1, 2, 3]);
    {
        let mut e = v.edit();
        e.seek(1).insert(9);
    }
    assert_eq!((v.len(), v.is_empty()), (0, true));
    assert!(v.to_vec().is_empty());
    assert!(v.iter().next().is_none());
    assert_eq!(v.get(0), None);
    let said = panic_of(std::panic::AssertUnwindSafe(|| v.push(4)));
    assert_eq!(said.as_deref(), Some(lost));
    let said = panic_of(std::panic::AssertUnwindSafe(|| v.edit().finish()));
    assert_eq!(said.as_deref(), Some(lost));

    let mut c = Column::<u64>::from_values(vec![1, 2, 3]);
    {
        let mut o = c.edit();
        o.seek(1).insert(9);
    }
    assert_eq!(old_values(&c), [1, 9, 2, 3]);
    let mut o = c.edit();
    o.seek(1).insert(9);
    std::mem::forget(o);
    assert_eq!((c.len(), c.is_empty()), (0, true));
    let said = panic_of(std::panic::AssertUnwindSafe(|| c.push(4)));
    assert_eq!(said.as_deref(), Some(lost));
}

/// A cursor goes forward only: a seek to a place behind it panics, in
/// both APIs, and leaves the column as any panic in the middle of an
/// edit does. A cursor opened past the column's end panics too, and
/// leaves the column as it was.
#[cfg(panic = "unwind")]
#[test]
fn a_cursor_goes_forward_only() {
    let (mut v, mut c) = both::<u64>(&[1, 2, 3, 4]);
    let new = panic_of(std::panic::AssertUnwindSafe(|| v.edit_at(5).finish()));
    let old = panic_of(std::panic::AssertUnwindSafe(|| c.edit_at(5).finish()));
    assert!(new.is_some());
    assert_eq!(new, old);
    check(&v, &c, &[1, 2, 3, 4]);
    // the end is a place
    v.edit_at(4).finish();
    c.edit_at(4).finish();
    check(&v, &c, &[1, 2, 3, 4]);

    let new = panic_of(std::panic::AssertUnwindSafe(|| {
        let mut e = v.edit();
        e.seek(3).seek(2);
        e.finish();
    }));
    assert_eq!(new.as_deref(), Some("VerifiedEdit::seek is forward-only"));
    let old = panic_of(std::panic::AssertUnwindSafe(|| {
        c.edit().seek(3).seek(2);
    }));
    assert_eq!(old.as_deref(), Some("Edit::seek is forward-only"));
    // a panic in the middle of an edit leaves either column without
    // its values: the old cursor does not finish as a panic unwinds
    assert_eq!((v.len(), v.is_empty()), (0, true));
    assert_eq!((c.len(), c.is_empty()), (0, true));
    let new = panic_of(std::panic::AssertUnwindSafe(|| v.push(9)));
    let old = panic_of(std::panic::AssertUnwindSafe(|| c.push(9)));
    assert_eq!(
        new.as_deref(),
        Some("column used after a panic during an edit")
    );
    assert_eq!(new, old);
}

#[cfg(feature = "bijou64")]
mod bijou_cursors {
    use super::*;
    use hexane::bijou as ob;
    use hexane::verified::bijou as vb;
    use hexane::Bijou64;

    fn cursors<T: Kind + VValue<Bijou64>>() {
        for seed in 1..=4u64 {
            let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0xed1a);
            let budget = [2, 3, 8, 64][rng.below(4)];
            let mut model: Vec<T> = rng.values(200);
            let mut v = vb::VerifiedColumn::<T>::with_max_segments(budget);
            let mut c = ob::Column::<T>::with_max_segments(budget);
            v.splice(0, 0, &lent(&model));
            c.splice(0, 0, model.iter().cloned());
            for _ in 0..25 {
                let at = some_place(&mut rng, model.len());
                let moves = some_moves(&mut rng, &model, at);
                let mut told = Told::new(&model, at);
                let (mut e, mut o) = (v.edit_at(at), c.edit_at(at));
                for m in &moves {
                    new_move!(e, m);
                    old_move!(o, m);
                    told.step(m);
                    let place = (told.at, told.out.len());
                    assert_eq!((e.pos(), e.out_pos()), place, "{m:?}");
                    assert_eq!((o.pos(), o.out_pos()), place, "{m:?}");
                    assert_eq!(e.rebuilds(), o.rebuilds(), "{m:?}");
                    assert_eq!(e.peek().map(T::keep).as_ref(), told.peek(), "{m:?}");
                }
                e.finish();
                drop(o);
                model = told.done();
                let new: Vec<T> = v.to_vec().into_iter().map(T::keep).collect();
                assert_eq!(new, model);
                assert_eq!(v.save(), c.save());
                assert_eq!(v.slab_count(), c.slab_count());
            }
        }
    }

    /// A cursor of a column whose numbers are written with bijou64 does
    /// what the old API's does of such a column.
    #[test]
    fn a_cursor_with_bijou64() {
        cursors::<u64>();
        cursors::<i64>();
        cursors::<Option<i64>>();
        cursors::<Option<u32>>();
        cursors::<bool>();
        cursors::<Vec<u8>>();
        cursors::<String>();
    }
}

// ── The edit cursor of a delta column ───────────────────────────────────────

/// One step of a delta column's edit cursor.
#[derive(Clone, Debug)]
enum DStroke<T> {
    Advance(usize),
    /// to any place: one before the cursor leaves it where it stands
    Seek(usize),
    Insert(T),
    InsertRun(T, usize),
    InsertRuns(Vec<hexane::DeltaRun>),
    Replace(T),
    Delete(usize),
}

/// The values of runs `runs`, as the old API's delta column takes them.
fn delta_runs_as_values<T: Deltad>(runs: &[hexane::DeltaRun]) -> Vec<T> {
    let mut c = DeltaColumn::<T>::new();
    c.splice_runs(0, 0, runs.iter().copied());
    (0..c.len()).map(|i| c.get(i).unwrap()).collect()
}

/// What the steps of a delta cursor make of a column, told by lists: the
/// values the column began with, the cursor's place among them, and the
/// values written so far.
struct DTold<T> {
    orig: Vec<T>,
    at: usize,
    out: Vec<T>,
    changed: bool,
}

impl<T: Deltad> DTold<T> {
    fn new(orig: &[T], at: usize) -> Self {
        DTold {
            orig: orig.to_vec(),
            at,
            out: orig[..at].to_vec(),
            changed: false,
        }
    }

    fn left(&self) -> usize {
        self.orig.len() - self.at
    }

    fn keep(&mut self, n: usize) {
        let k = n.min(self.left());
        self.out.extend_from_slice(&self.orig[self.at..self.at + k]);
        self.at += k;
    }

    fn step(&mut self, m: &DStroke<T>) {
        match m {
            DStroke::Advance(n) => self.keep(*n),
            DStroke::Seek(to) => self.keep(to.saturating_sub(self.at)),
            DStroke::Insert(x) => {
                self.out.push(*x);
                self.changed = true;
            }
            DStroke::InsertRun(x, n) => {
                self.out.extend(std::iter::repeat_n(*x, *n));
                self.changed |= *n > 0;
            }
            DStroke::InsertRuns(runs) => {
                let values = delta_runs_as_values::<T>(runs);
                self.changed |= !values.is_empty();
                self.out.extend(values);
            }
            DStroke::Replace(x) => {
                if self.left() > 0 {
                    self.changed |= self.orig[self.at] != *x;
                    self.out.push(*x);
                    self.at += 1;
                }
            }
            DStroke::Delete(n) => {
                let k = (*n).min(self.left());
                self.changed |= k > 0;
                self.at += k;
            }
        }
    }

    fn peek(&self) -> Option<T> {
        self.orig.get(self.at).copied()
    }

    /// The column, were the edit finished now.
    fn done(mut self) -> Vec<T> {
        let rest = self.orig.split_off(self.at);
        self.out.extend(rest);
        self.out
    }
}

/// Runs for a delta cursor to put in: those of a column of values near
/// each other, or a few made up, which pass the check of `run_fits`.
fn some_delta_runs<T: Deltad>(rng: &mut Rng) -> Vec<hexane::DeltaRun> {
    if rng.below(2) == 0 {
        let mut last = 0;
        let mut values: Vec<T> = vec![];
        for _ in 0..rng.below(30) {
            let (v, l) = T::near(rng, last);
            last = l;
            values.push(v);
        }
        let src = DeltaColumn::<T>::from_values(values);
        let runs = src.iter().runs().collect();
        runs
    } else {
        let mut runs = vec![];
        for _ in 0..rng.below(4) {
            let (_, prefix) = T::near(rng, 0);
            let delta = match rng.below(6) {
                0 => None,
                _ => Some(rng.below(9) as i64 - 4),
            };
            let count = rng.below(6);
            runs.push(hexane::DeltaRun {
                prefix,
                delta,
                count,
            });
        }
        runs.retain(VerifiedDeltaColumn::<T>::run_fits);
        runs
    }
}

/// Steps for a delta cursor at `at` of `model`: most within the values
/// left, some of none, some to the end, some past it, where a cursor
/// stops, and some seeks back, where it stays.
fn some_delta_moves<T: Deltad>(rng: &mut Rng, model: &[T], at: usize) -> Vec<DStroke<T>> {
    let mut at = at;
    let mut moves = vec![];
    for _ in 0..rng.below(9) {
        let left = model.len() - at;
        let count = match rng.below(12) {
            0 => 0,
            1 => left,
            2 => left + 1 + rng.below(3),
            3 => usize::MAX,
            _ => rng.below(left.min(30) + 1),
        };
        let (x, _) = T::near(rng, 0);
        let m = match rng.below(14) {
            0 | 1 => DStroke::Advance(count),
            2 | 3 => DStroke::Seek(at.saturating_add(count)),
            4 => DStroke::Seek(rng.below(at + 1)),
            5 | 6 => DStroke::Insert(x),
            7 => DStroke::InsertRun(x, [0, 1, 2, 9, 70][rng.below(5)]),
            8 => DStroke::InsertRuns(some_delta_runs::<T>(rng)),
            9 | 10 => DStroke::Replace(x),
            // the value that is there, which is no change
            11 => DStroke::Replace(model.get(at).copied().unwrap_or(x)),
            _ => DStroke::Delete(count),
        };
        at = match &m {
            DStroke::Advance(n) | DStroke::Delete(n) => at + (*n).min(left),
            DStroke::Seek(to) => (*to).max(at).min(model.len()),
            DStroke::Replace(_) => at + left.min(1),
            _ => at,
        };
        moves.push(m);
    }
    moves
}

/// A step of a delta cursor of the verified API.
macro_rules! new_delta_move {
    ($e:expr, $m:expr) => {
        match $m {
            DStroke::Advance(n) => {
                $e.advance(*n);
            }
            DStroke::Seek(to) => {
                $e.seek(*to);
            }
            DStroke::Insert(x) => {
                $e.insert(*x);
            }
            DStroke::InsertRun(x, n) => {
                $e.insert_run(*x, *n);
            }
            DStroke::InsertRuns(runs) => {
                $e.insert_runs(runs);
            }
            DStroke::Replace(x) => {
                $e.replace_with(*x);
            }
            DStroke::Delete(n) => {
                $e.delete(*n);
            }
        }
    };
}

/// The same step of a delta cursor of the old API, whose `replace` takes
/// a function of the value that is there.
macro_rules! old_delta_move {
    ($e:expr, $m:expr) => {
        match $m {
            DStroke::Advance(n) => {
                $e.advance(*n);
            }
            DStroke::Seek(to) => {
                $e.seek(*to);
            }
            DStroke::Insert(x) => {
                $e.insert(*x);
            }
            DStroke::InsertRun(x, n) => {
                $e.insert_run(*x, *n);
            }
            DStroke::InsertRuns(runs) => {
                $e.insert_runs(runs.iter().copied());
            }
            DStroke::Replace(x) => {
                $e.replace(|_| *x);
            }
            DStroke::Delete(n) => {
                $e.delete(*n);
            }
        }
    };
}

/// Both delta columns hold `model`, saved alike in as many slabs.
fn delta_cursor_check<T: Deltad>(v: &VerifiedDeltaColumn<T>, c: &DeltaColumn<T>, model: &[T]) {
    assert_eq!(v.to_vec(), model);
    delta_agrees(v, c);
    assert_eq!(v.save(), c.save());
    if !model.is_empty() {
        assert_eq!(v.slab_count(), c.slab_count());
    }
}

fn delta_cursors<T: Deltad>(seed: u64, max_segments: Option<usize>) {
    let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0xde1e);
    let (mut v, mut c) = match max_segments {
        Some(m) => (
            VerifiedDeltaColumn::<T>::with_max_segments(m),
            DeltaColumn::<T>::with_max_segments(m),
        ),
        None => (
            VerifiedDeltaColumn::<T>::default(),
            DeltaColumn::<T>::default(),
        ),
    };
    let mut last = 0;
    let mut model: Vec<T> = vec![];
    for _ in 0..rng.below(200) {
        let (x, l) = T::near(&mut rng, last);
        last = l;
        model.push(x);
    }
    v.splice(0, 0, &model);
    c.splice(0, 0, model.clone());
    for round in 0..30 {
        let at = some_place(&mut rng, model.len());
        let moves = some_delta_moves(&mut rng, &model, at);
        let mut told = DTold::new(&model, at);
        let (s, t) = (v.iter().suspend(), c.iter().suspend());
        // `edit` is `edit_at(0)`
        let first = at == 0 && round % 2 == 0;
        let mut e = if first { v.edit() } else { v.edit_at(at) };
        let mut o = if first { c.edit() } else { c.edit_at(at) };
        assert_eq!((e.pos(), e.out_pos(), o.pos()), (at, at, at));
        for m in &moves {
            new_delta_move!(e, m);
            old_delta_move!(o, m);
            told.step(m);
            let place = (told.at, told.out.len(), told.at);
            assert_eq!((e.pos(), e.out_pos(), o.pos()), place, "{m:?}");
            if rng.below(3) == 0 {
                // a look at the value there, which moves nothing
                assert_eq!(e.peek(), told.peek(), "{m:?}");
                assert_eq!(o.peek(), told.peek(), "{m:?}");
                assert_eq!((e.pos(), e.out_pos(), o.pos()), place, "{m:?}");
            }
        }
        e.finish();
        // the old cursor is finished by the call or when it is dropped
        if round % 3 == 0 {
            o.finish();
        } else {
            drop(o);
        }
        let changed = told.changed;
        model = told.done();
        delta_cursor_check(&v, &c, &model);
        // an edit is counted exactly where a step put a value in or
        // took one out: an iterator's state taken before resumes
        // exactly where none did
        assert_eq!(s.try_resume(&v).is_ok(), !changed, "{moves:?}");
        assert_eq!(t.try_resume(&c).is_ok(), !changed, "{moves:?}");
    }
}

/// An edit cursor of a delta column of the verified API
/// (`VerifiedDeltaEdit`) does what `DeltaColumn`'s does: under the same
/// steps both stand at the same places, see the same values, and leave
/// the same values in the same slabs, whose bytes are the same. The
/// places and the values are what the steps make of a list. And the
/// column counts an edit exactly where a step changed it.
#[test]
fn a_delta_cursor_edits_as_the_old_one_does() {
    for seed in 0..6 {
        let m = [None, Some(2), Some(5)][seed as usize % 3];
        delta_cursors::<u32>(seed, m);
        delta_cursors::<u64>(seed, m);
        delta_cursors::<i32>(seed, m);
        delta_cursors::<i64>(seed, m);
        delta_cursors::<usize>(seed, m);
        delta_cursors::<Option<u32>>(seed, m);
        delta_cursors::<Option<u64>>(seed, m);
        delta_cursors::<Option<i32>>(seed, m);
        delta_cursors::<Option<i64>>(seed, m);
        delta_cursors::<Option<usize>>(seed, m);
    }
}

#[test]
fn a_delta_cursor_step_by_step() {
    let mut v = VerifiedDeltaColumn::<u64>::from_values(&[10, 20, 30, 40, 50]);
    let mut e = v.edit();
    assert_eq!((e.pos(), e.out_pos(), e.peek()), (0, 0, Some(10)));
    e.advance(1);
    assert_eq!((e.pos(), e.out_pos(), e.peek()), (1, 1, Some(20)));
    e.delete(2);
    assert_eq!((e.pos(), e.out_pos(), e.peek()), (3, 1, Some(40)));
    e.insert(25).insert_run(27, 2);
    assert_eq!((e.pos(), e.out_pos(), e.peek()), (3, 4, Some(40)));
    // a seek back stays where the cursor stands
    e.seek(1);
    assert_eq!((e.pos(), e.out_pos(), e.peek()), (3, 4, Some(40)));
    e.seek(4).replace_with(45);
    assert_eq!((e.pos(), e.out_pos(), e.peek()), (5, 6, None));
    // at the end there is nothing to replace, to delete or to pass over
    e.replace_with(0)
        .delete(3)
        .advance(usize::MAX)
        .seek(usize::MAX);
    assert_eq!((e.pos(), e.out_pos(), e.peek()), (5, 6, None));
    // runs say their own values, whatever is before them
    let runs = [
        hexane::DeltaRun {
            prefix: 3,
            delta: Some(2),
            count: 2,
        },
        hexane::DeltaRun {
            prefix: 9,
            delta: Some(1),
            count: 0,
        },
        hexane::DeltaRun {
            prefix: 100,
            delta: Some(0),
            count: 1,
        },
    ];
    e.insert_runs(&runs);
    assert_eq!((e.pos(), e.out_pos()), (5, 9));
    e.finish();
    assert_eq!(v.to_vec(), [10, 25, 27, 27, 40, 45, 5, 7, 100]);

    // a cursor opened in the middle keeps what is before it
    let mut e = v.edit_at(6);
    assert_eq!((e.pos(), e.out_pos(), e.peek()), (6, 6, Some(5)));
    e.delete(usize::MAX);
    e.finish();
    assert_eq!(v.to_vec(), [10, 25, 27, 27, 40, 45]);

    // nulls: looked at, put in and replaced
    let mut n = VerifiedDeltaColumn::<Option<i32>>::from_values(&[Some(-1), None, Some(4)]);
    let mut e = n.edit();
    assert_eq!(e.peek(), Some(Some(-1)));
    e.replace_with(None).insert_run(None, 2);
    assert_eq!(e.peek(), Some(None));
    e.replace_with(Some(7));
    assert_eq!(e.peek(), Some(Some(4)));
    e.finish();
    assert_eq!(n.to_vec(), [None, None, None, Some(7), Some(4)]);
}

/// A delta cursor that is not finished does not give the column its
/// values back: the column is an empty one, which can be used.
/// `DeltaColumn`'s cursor finishes when it is dropped.
#[test]
fn a_delta_cursor_dropped_unfinished() {
    let mut v = VerifiedDeltaColumn::<u64>::from_values(&[1, 2, 3]);
    {
        let mut e = v.edit();
        e.seek(1).insert(9);
    }
    assert_eq!((v.len(), v.is_empty()), (0, true));
    assert!(v.to_vec().is_empty());
    assert_eq!(v.get(0), None);
    v.push(4);
    let mut e = v.edit();
    e.insert(3);
    e.finish();
    assert_eq!(v.to_vec(), [3, 4]);

    let mut c = DeltaColumn::<u64>::from_values(vec![1, 2, 3]);
    {
        let mut o = c.edit();
        o.seek(1).insert(9);
    }
    let old: Vec<u64> = (0..c.len()).map(|i| c.get(i).unwrap()).collect();
    assert_eq!(old, [1, 9, 2, 3]);
}

/// A delta cursor opened past the column's end panics, in both APIs, and
/// leaves the column as it was. One that leaves values that span 2^63 or
/// more panics when it is finished, in both, and leaves the verified
/// column an empty one, which can be used.
#[cfg(panic = "unwind")]
#[test]
fn a_delta_cursor_panics_as_the_old_one_does() {
    let mut v = VerifiedDeltaColumn::<u64>::from_values(&[1, 2, 3, 4]);
    let mut c = DeltaColumn::<u64>::from_values(vec![1, 2, 3, 4]);
    let new = panic_of(std::panic::AssertUnwindSafe(|| v.edit_at(5).finish()));
    let old = panic_of(std::panic::AssertUnwindSafe(|| c.edit_at(5).finish()));
    assert_eq!(new.as_deref(), Some("edit_at out of bounds"));
    assert_eq!(new, old);
    delta_cursor_check(&v, &c, &[1, 2, 3, 4]);
    // the end is a place
    v.edit_at(4).finish();
    c.edit_at(4).finish();
    delta_cursor_check(&v, &c, &[1, 2, 3, 4]);

    let mut v = VerifiedDeltaColumn::<i64>::from_values(&[0]);
    let mut c = DeltaColumn::<i64>::from_values(vec![0]);
    let new = panic_of(std::panic::AssertUnwindSafe(|| {
        let mut e = v.edit();
        e.insert(i64::MIN).seek(1).insert(i64::MAX);
        e.finish();
    }));
    let old = panic_of(std::panic::AssertUnwindSafe(|| {
        c.edit().insert(i64::MIN).seek(1).insert(i64::MAX);
    }));
    assert_eq!(
        new.as_deref(),
        Some("delta values out of domain: they would span 2^63 or more")
    );
    assert_eq!(new, old);
    assert_eq!((v.len(), v.is_empty()), (0, true));
    v.push(5);
    assert_eq!(v.to_vec(), [5]);
}

#[cfg(feature = "bijou64")]
mod bijou_delta_cursors {
    use super::*;
    use hexane::bijou as ob;
    use hexane::verified::bijou as vb;
    use hexane::Bijou64;

    fn cursors<T: Deltad + VDeltaValue<Bijou64>>(seed: u64)
    where
        T::Inner:
            hexane::ColumnValueRef<Encoding<Bijou64> = hexane::RleEncoding<T::Inner, Bijou64>>,
    {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0xde1a);
        let budget = [2, 3, 8, 64][rng.below(4)];
        let mut v = vb::VerifiedDeltaColumn::<T>::with_max_segments(budget);
        let mut c = ob::DeltaColumn::<T>::with_max_segments(budget);
        let mut last = 0;
        let mut model: Vec<T> = vec![];
        for _ in 0..rng.below(200) {
            let (x, l) = T::near(&mut rng, last);
            last = l;
            model.push(x);
        }
        v.splice(0, 0, &model);
        c.splice(0, 0, model.clone());
        for _ in 0..20 {
            let at = some_place(&mut rng, model.len());
            let moves = some_delta_moves(&mut rng, &model, at);
            let mut told = DTold::new(&model, at);
            let (mut e, mut o) = (v.edit_at(at), c.edit_at(at));
            for m in &moves {
                new_delta_move!(e, m);
                old_delta_move!(o, m);
                told.step(m);
                let place = (told.at, told.out.len(), told.at);
                assert_eq!((e.pos(), e.out_pos(), o.pos()), place, "{m:?}");
                assert_eq!(e.peek(), told.peek(), "{m:?}");
            }
            e.finish();
            drop(o);
            model = told.done();
            assert_eq!(v.to_vec(), model);
            assert_eq!(v.save(), c.save());
            if !model.is_empty() {
                assert_eq!(v.slab_count(), c.slab_count());
            }
        }
    }

    /// A cursor of a delta column whose numbers are written with bijou64
    /// does what the old API's does of such a column.
    #[test]
    fn a_delta_cursor_with_bijou64() {
        for seed in 0..3 {
            cursors::<u64>(seed);
            cursors::<i64>(seed);
            cursors::<i32>(seed);
            cursors::<Option<i64>>(seed);
            cursors::<Option<u32>>(seed);
        }
    }
}

// ── Ranges copied from one column into another ─────────────────────────────

use hexane::Splice;

/// Splices for `copy_ranges` of a source of `src_len` values into a
/// column of `dst_len`: in order in both columns, some deleting, some
/// copying nothing, some with a range past the source's end, reversed,
/// or to `usize::MAX`.
fn some_splices(rng: &mut Rng, dst_len: usize, src_len: usize) -> Vec<Splice> {
    let mut out = vec![];
    let (mut at, mut from) = (0usize, 0usize);
    for _ in 0..rng.below(5) {
        let pos = at + rng.below(dst_len - at + 1);
        let delete = if rng.below(3) == 0 {
            0
        } else {
            rng.below((dst_len - pos).min(5) + 1)
        };
        let start = from + rng.below(src_len.saturating_sub(from) + 2);
        let end = match rng.below(6) {
            0 => start,
            1 => usize::MAX,
            2 => start.saturating_sub(1).max(from),
            _ => start + rng.below(20),
        };
        out.push(Splice {
            pos,
            delete,
            range: start..end,
        });
        at = pos + delete;
        from = from.max(end);
        if end == usize::MAX {
            break;
        }
    }
    out
}

/// What `splices` make of `dst`, with the source `src`, as lists.
fn applied_to<T: Clone>(dst: &[T], src: &[T], splices: &[Splice]) -> Vec<T> {
    let mut out = vec![];
    let mut at = 0;
    for sp in splices {
        out.extend_from_slice(&dst[at..sp.pos]);
        let (s, e) = (sp.range.start.min(src.len()), sp.range.end.min(src.len()));
        if s < e {
            out.extend_from_slice(&src[s..e]);
        }
        at = sp.pos + sp.delete;
    }
    out.extend_from_slice(&dst[at..]);
    out
}

/// Whether one of `splices` deletes a value or copies one.
fn splices_change(splices: &[Splice], src_len: usize) -> bool {
    splices
        .iter()
        .any(|sp| sp.delete > 0 || sp.range.start.min(src_len) < sp.range.end.min(src_len))
}

fn copies<T: Kind>()
where
    for<'b> <T as ColumnValueRef>::Get<'b>: hexane::AsColumnRef<T>,
{
    for seed in 1..=6u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0xc097);
        let budget = [2, 3, 8, 64][rng.below(4)];
        let mut model: Vec<T> = rng.values(150);
        let (mut v, mut c) = both_with(&model, budget);
        for round in 0..12 {
            // a source long enough, now and then, and in slabs of the
            // same budget, for the copy to keep its slabs
            let n = [0, 3, 40, 400][rng.below(4)];
            let src: Vec<T> = rng.values(n);
            let sb = if round % 3 == 0 { 64 } else { budget };
            let (vs, cs) = both_with(&src, sb);
            let splices = some_splices(&mut rng, model.len(), src.len());
            let change = splices_change(&splices, src.len());
            let (s, t) = (v.iter().suspend(), c.iter().suspend());
            v.copy_ranges(vs, &splices);
            c.copy_ranges(cs, splices.iter().cloned());
            model = applied_to(&model, &src, &splices);
            check(&v, &c, &model);
            // an edit is counted exactly where a splice deletes a value or
            // copies one
            assert_eq!(s.try_resume(&v).is_ok(), !change, "{splices:?}");
            assert_eq!(t.try_resume(&c).is_ok(), !change, "{splices:?}");
        }
        check_loads(&v, &c, &model);
    }
}

/// `copy_ranges` of the verified API does what `Column`'s does: the same
/// values, in the same slabs, saved as the same bytes, whichever way the
/// copy runs; the values are what the splices make of lists; and an edit
/// is counted exactly where a splice changes something.
#[test]
fn copies_as_the_old_api_does() {
    for_each_kind!(copies);
}

fn sum_copies<T: Totalled>()
where
    for<'b> <T as ColumnValueRef>::Get<'b>: hexane::AsColumnRef<T>,
{
    for seed in 1..=4u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0xc098);
        let budget = [2, 3, 8, 64][rng.below(4)];
        let mut p = VerifiedPrefixColumn::<T>::with_max_segments(budget);
        let mut v = VerifiedColumn::<T>::with_max_segments(budget);
        let mut c = PrefixColumn::<T>::with_max_segments(budget);
        let mut model: Vec<T> = rng.values(150);
        p.splice(0, 0, &lent(&model));
        v.splice(0, 0, &lent(&model));
        c.splice(0, 0, model.iter().cloned());
        for _ in 0..10 {
            let n = [0, 3, 40, 400][rng.below(4)];
            let src: Vec<T> = rng.values(n);
            let mut ps = VerifiedPrefixColumn::<T>::with_max_segments(budget);
            let mut vs = VerifiedColumn::<T>::with_max_segments(budget);
            let mut cs = PrefixColumn::<T>::with_max_segments(budget);
            ps.splice(0, 0, &lent(&src));
            vs.splice(0, 0, &lent(&src));
            cs.splice(0, 0, src.iter().cloned());
            let splices = some_splices(&mut rng, model.len(), src.len());
            p.copy_ranges(ps, &splices);
            v.copy_ranges(vs, &splices);
            c.copy_ranges(cs, splices.iter().cloned());
            model = applied_to(&model, &src, &splices);
            check_sums(&p, &v, &c, &model, true);
            check_sum_reads(&mut rng, &p, &v, &model);
            check_prefix_reads(&mut rng, &p, &c, &model);
        }
        check_sum_loads(&p, &c, &model);
    }
}

/// `copy_ranges` of a prefix column: the values of a plain column's, in
/// the same slabs as `PrefixColumn`'s, and an index that answers with the
/// sums of the values as they are after the copy.
#[test]
fn copies_of_a_prefix_column() {
    for_each_sum!(sum_copies);
}

// a reversed range is one a splice may hold: it copies nothing
#[allow(clippy::reversed_empty_ranges)]
#[test]
fn copies_step_by_step() {
    let mut v = VerifiedColumn::<u64>::from_values(&[1, 2, 3]);
    let src = VerifiedColumn::<u64>::from_values(&[7, 8, 9, 10]);
    let splices = [
        Splice {
            pos: 1,
            delete: 1,
            range: 1..3,
        },
        Splice {
            pos: 3,
            delete: 0,
            range: 3..100,
        },
    ];
    v.copy_ranges(src, &splices);
    assert_eq!(v.to_vec(), [1, 8, 9, 3, 10]);
    // the default splice: all of the source, at the front
    let src = VerifiedColumn::<u64>::from_values(&[5, 6]);
    v.copy_ranges(src, &[Splice::default()]);
    assert_eq!(v.to_vec(), [5, 6, 1, 8, 9, 3, 10]);
    // no splices, and splices that change nothing
    let src = VerifiedColumn::<u64>::from_values(&[4]);
    v.copy_ranges(src, &[]);
    let src = VerifiedColumn::<u64>::from_values(&[4]);
    let none = [Splice {
        pos: 2,
        delete: 0,
        range: 5..2,
    }];
    v.copy_ranges(src, &none);
    assert_eq!(v.to_vec(), [5, 6, 1, 8, 9, 3, 10]);
}

/// Splices out of order or out of bounds panic, in both APIs, in the
/// same words.
#[cfg(panic = "unwind")]
#[test]
fn copies_out_of_order_panic() {
    let bad: [&[Splice]; 3] = [
        &[
            Splice {
                pos: 2,
                delete: 1,
                range: 0..1,
            },
            Splice {
                pos: 1,
                delete: 0,
                range: 1..2,
            },
        ],
        &[Splice {
            pos: 3,
            delete: 2,
            range: 0..1,
        }],
        &[
            Splice {
                pos: 0,
                delete: 0,
                range: 2..4,
            },
            Splice {
                pos: 1,
                delete: 0,
                range: 3..4,
            },
        ],
    ];
    for splices in bad {
        let mut v = VerifiedColumn::<u64>::from_values(&[1, 2, 3, 4]);
        let mut c = Column::<u64>::from_values(vec![1, 2, 3, 4]);
        let vs = VerifiedColumn::<u64>::from_values(&[7, 8, 9, 10]);
        let cs = Column::<u64>::from_values(vec![7, 8, 9, 10]);
        let new = panic_of(std::panic::AssertUnwindSafe(|| v.copy_ranges(vs, splices)));
        let old = panic_of(std::panic::AssertUnwindSafe(|| {
            c.copy_ranges(cs, splices.iter().cloned())
        }));
        assert!(new.is_some(), "{splices:?}");
        assert_eq!(new, old, "{splices:?}");
    }
}

// ── Ranges copied from one delta column into another ────────────────────────

fn delta_copies<T: Deltad>(seed: u64, max_segments: Option<usize>) {
    let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0xdc09);
    let mut last = 0;
    let mut some = |rng: &mut Rng, n: usize| -> Vec<T> {
        (0..n)
            .map(|_| {
                let (v, l) = T::near(rng, last);
                last = l;
                v
            })
            .collect()
    };
    let made = |values: &[T], m: Option<usize>| match m {
        Some(m) => {
            let mut v = VerifiedDeltaColumn::<T>::with_max_segments(m);
            let mut c = DeltaColumn::<T>::with_max_segments(m);
            v.splice(0, 0, values);
            c.splice(0, 0, values.to_vec());
            (v, c)
        }
        None => (
            VerifiedDeltaColumn::<T>::from_values(values),
            DeltaColumn::<T>::from_values(values.to_vec()),
        ),
    };
    let n = rng.below(150);
    let mut model = some(&mut rng, n);
    let (mut v, mut c) = made(&model, max_segments);
    for round in 0..12 {
        let n = [0, 3, 40, 400][rng.below(4)];
        let src = some(&mut rng, n);
        // in slabs of the same budget now and then, for the copy to keep
        // the source's slabs
        let m = if round % 3 == 0 {
            Some(64)
        } else {
            max_segments
        };
        let (vs, cs) = made(&src, m);
        let splices = some_splices(&mut rng, model.len(), src.len());
        let change = splices_change(&splices, src.len());
        let (s, t) = (v.iter().suspend(), c.iter().suspend());
        v.copy_ranges(vs, &splices);
        c.copy_ranges(cs, splices.iter().cloned());
        model = applied_to(&model, &src, &splices);
        delta_cursor_check(&v, &c, &model);
        assert_eq!(s.try_resume(&v).is_ok(), !change, "{splices:?}");
        assert_eq!(t.try_resume(&c).is_ok(), !change, "{splices:?}");
    }
}

/// `copy_ranges` of a verified delta column does what `DeltaColumn`'s
/// does: the same values in the same slabs, saved as the same bytes,
/// whichever way the copy runs; the values are what the splices make of
/// lists; and an edit is counted exactly where a splice changes
/// something.
#[test]
fn delta_copies_as_the_old_api_does() {
    for seed in 0..6 {
        let m = [None, Some(2), Some(5)][seed as usize % 3];
        delta_copies::<u32>(seed, m);
        delta_copies::<u64>(seed, m);
        delta_copies::<i32>(seed, m);
        delta_copies::<i64>(seed, m);
        delta_copies::<usize>(seed, m);
        delta_copies::<Option<u32>>(seed, m);
        delta_copies::<Option<u64>>(seed, m);
        delta_copies::<Option<i32>>(seed, m);
        delta_copies::<Option<i64>>(seed, m);
        delta_copies::<Option<usize>>(seed, m);
    }
}

/// Splices out of order panic in both APIs, in the same words; values
/// that would span 2^63 or more panic in both too, and leave the
/// verified column an empty one, which can be used.
#[cfg(panic = "unwind")]
#[test]
fn delta_copies_panic_as_the_old_ones_do() {
    let bad = [
        Splice {
            pos: 2,
            delete: 0,
            range: 0..1,
        },
        Splice {
            pos: 1,
            delete: 0,
            range: 1..2,
        },
    ];
    let mut v = VerifiedDeltaColumn::<i64>::from_values(&[1, 2, 3]);
    let mut c = DeltaColumn::<i64>::from_values(vec![1, 2, 3]);
    let vs = VerifiedDeltaColumn::<i64>::from_values(&[7, 8]);
    let cs = DeltaColumn::<i64>::from_values(vec![7, 8]);
    let new = panic_of(std::panic::AssertUnwindSafe(|| v.copy_ranges(vs, &bad)));
    let old = panic_of(std::panic::AssertUnwindSafe(|| {
        c.copy_ranges(cs, bad.iter().cloned())
    }));
    assert!(new.is_some());
    assert_eq!(new, old);

    let mut v = VerifiedDeltaColumn::<i64>::from_values(&[i64::MIN]);
    let mut c = DeltaColumn::<i64>::from_values(vec![i64::MIN]);
    let vs = VerifiedDeltaColumn::<i64>::from_values(&[i64::MAX]);
    let cs = DeltaColumn::<i64>::from_values(vec![i64::MAX]);
    let onto = [Splice {
        pos: 1,
        ..Default::default()
    }];
    let new = panic_of(std::panic::AssertUnwindSafe(|| v.copy_ranges(vs, &onto)));
    let old = panic_of(std::panic::AssertUnwindSafe(|| {
        c.copy_ranges(cs, onto.iter().cloned())
    }));
    assert_eq!(
        new.as_deref(),
        Some("delta values out of domain: they would span 2^63 or more")
    );
    assert_eq!(new, old);
    assert_eq!((v.len(), v.is_empty()), (0, true));
    v.push(5);
    assert_eq!(v.to_vec(), [5]);
}

// ── Repositioning an iterator: VShiftable, Unshift, Ranges ──────────────────

use hexane::verified::VShiftable;
use hexane::Shiftable;

/// Ranges for `ranges` of a column of `len` values, from `from` on:
/// ascending and not overlapping, some touching, some empty, some past
/// the end.
fn some_ranges(rng: &mut Rng, from: usize, len: usize) -> Vec<Range<usize>> {
    let mut out = vec![];
    let mut at = from;
    for _ in 0..rng.below(5) {
        let start = at + rng.below(len.saturating_sub(at).min(20) + 3);
        let end = match rng.below(5) {
            0 => start,
            1 => start + 100,
            _ => start + rng.below(10),
        };
        out.push(start..end);
        at = end;
    }
    out
}

/// The values of `ranges` of `model`, one range after another.
fn ranges_of<T: Clone>(model: &[T], ranges: &[Range<usize>]) -> Vec<T> {
    let mut out = vec![];
    for r in ranges {
        let (s, e) = (r.start.min(model.len()), r.end.min(model.len()));
        if s < e {
            out.extend_from_slice(&model[s..e]);
        }
    }
    out
}

fn shifted<T: Kind>() {
    for seed in 1..=6u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0x5417);
        let budget = [2, 3, 8, 64][rng.below(4)];
        let model: Vec<T> = rng.values(120);
        let (v, c) = both_with(&model, budget);
        let len = model.len();
        for _ in 0..20 {
            // ranges, from a place the iterators have moved on to
            let a = rng.below(len + 2);
            let rs = some_ranges(&mut rng, a, len);
            let mut x = v.iter();
            let mut y = c.iter();
            VShiftable::advance_to(&mut x, a);
            Shiftable::advance_to(&mut y, a);
            let new: Vec<T> = x.ranges(&rs).map(T::keep).collect();
            let old_: Vec<T> = y.ranges(rs.clone()).map(old::<T>).collect();
            let want = ranges_of(&model, &rs);
            assert_eq!(new, want, "{rs:?}");
            assert_eq!(old_, want, "{rs:?}");

            // a lookahead, moved to a range at once, then looked at and
            // walked
            let (s, e) = (rng.below(len + 2), rng.below(len + 10));
            let mut u = v.iter().unshift();
            let mut w = c.iter().unshift();
            if rng.below(2) == 0 {
                u.shift(s..e);
                w.shift(s..e);
            }
            for _ in 0..8 {
                match rng.below(3) {
                    0 => assert_eq!(
                        u.peek().map(|g| T::keep(*g)),
                        w.peek().map(|g| old::<T>(*g))
                    ),
                    1 => assert_eq!(u.next().map(T::keep), w.next().map(old::<T>)),
                    _ => {
                        let n = rng.below(5);
                        assert_eq!(u.nth(n).map(T::keep), w.nth(n).map(old::<T>));
                    }
                }
            }

            // the moves of `VShiftable`, beside `Shiftable`'s
            let mut x = v.iter();
            let mut y = c.iter();
            for _ in 0..6 {
                let p = VShiftable::get_pos(&x);
                assert_eq!(p, Shiftable::get_pos(&y));
                assert_eq!(VShiftable::get_max(&x), Shiftable::get_max(&y));
                assert_eq!(VShiftable::get_range(&x), Shiftable::get_range(&y));
                let t = p + rng.below(15);
                match rng.below(6) {
                    0 => {
                        VShiftable::set_max(&mut x, t);
                        Shiftable::set_max(&mut y, t);
                    }
                    1 => {
                        VShiftable::advance_to(&mut x, t);
                        Shiftable::advance_to(&mut y, t);
                    }
                    2 => {
                        VShiftable::advance_by(&mut x, t - p);
                        Shiftable::advance_by(&mut y, t - p);
                    }
                    3 => {
                        let r = t..t + rng.below(20);
                        VShiftable::shift(&mut x, r.clone());
                        Shiftable::shift(&mut y, r);
                    }
                    4 => {
                        let r = t..t + rng.below(20);
                        let a = VShiftable::shift_next(&mut x, r.clone()).map(T::keep);
                        assert_eq!(a, Shiftable::shift_next(&mut y, r).map(old::<T>));
                    }
                    _ => {
                        let a = VShiftable::seek_to(&mut x, t).map(T::keep);
                        assert_eq!(a, Shiftable::seek_to(&mut y, t).map(old::<T>));
                    }
                }
                assert_eq!(x.next().map(T::keep), y.next().map(old::<T>));
            }
        }
    }
}

/// A verified iterator repositions as `Column`'s does: the values of
/// several ranges in turn (`ranges`), a lookahead and its moves
/// (`unshift`), and each move of `VShiftable` beside `Shiftable`'s.
#[test]
fn an_iterator_repositions_as_the_old_one_does() {
    for_each_kind!(shifted);
}

/// Ranges out of order panic in both APIs, in the same words.
#[cfg(panic = "unwind")]
#[test]
fn ranges_out_of_order_panic() {
    let (v, c) = both::<u64>(&[1, 2, 3, 4, 5, 6]);
    let bad = [3..4, 1..2];
    let new = panic_of(std::panic::AssertUnwindSafe(|| {
        v.iter().ranges(&bad).count()
    }));
    let old_ = panic_of(std::panic::AssertUnwindSafe(|| {
        c.iter().ranges(bad.clone()).count()
    }));
    assert_eq!(
        new.as_deref(),
        Some("ranges must be ascending and non-overlapping")
    );
    assert_eq!(new, old_);
}

// ── Repositioning the prefix, accumulator and delta iterators ──────────────

fn prefix_shifted<T: Totalled>() {
    for seed in 1..=4u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0x5418);
        let budget = [2, 3, 8, 64][rng.below(4)];
        let mut p = VerifiedPrefixColumn::<T>::with_max_segments(budget);
        let mut c = PrefixColumn::<T>::with_max_segments(budget);
        let model: Vec<T> = rng.values(120);
        p.splice(0, 0, &lent(&model));
        c.splice(0, 0, model.iter().cloned());
        let len = model.len();
        for _ in 0..15 {
            let a = rng.below(len + 2);
            let rs = some_ranges(&mut rng, a, len);
            let mut x = p.prefix_iter();
            let mut y = c.iter();
            VShiftable::advance_to(&mut x, a);
            Shiftable::advance_to(&mut y, a);
            let new: Vec<Item<T>> = x.ranges(&rs).map(|pv| new_item(Some(pv))).collect();
            let old_: Vec<Item<T>> = y.ranges(rs.clone()).map(|pv| old_item(Some(pv))).collect();
            assert_eq!(new, old_, "{rs:?}");

            let (s, e) = (rng.below(len + 2), rng.below(len + 10));
            let mut u = p.prefix_iter().unshift();
            let mut w = c.iter().unshift();
            if rng.below(2) == 0 {
                u.shift(s..e);
                w.shift(s..e);
            }
            for _ in 0..8 {
                match rng.below(3) {
                    0 => assert_eq!(new_item(u.peek().cloned()), old_item(w.peek().cloned())),
                    1 => assert_eq!(new_item(u.next()), old_item(w.next())),
                    _ => {
                        let n = rng.below(5);
                        assert_eq!(new_item(u.nth(n)), old_item(w.nth(n)));
                    }
                }
            }
        }
    }
}

/// A prefix iterator repositions as `PrefixColumn`'s does: the values
/// with their sums, of several ranges in turn and through a lookahead.
#[test]
fn a_prefix_iterator_repositions_as_the_old_one_does() {
    for_each_sum!(prefix_shifted);
}

fn acc_shifted<T: Accd>() {
    for seed in 1..=4u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0x5419);
        let budget = [2, 3, 8, 64][rng.below(4)];
        let mut p = VerifiedAccColumn::<T>::with_max_segments(budget);
        let model: Vec<T> = rng.values(120);
        p.splice(0, 0, &lent(&model));
        let len = model.len();
        // every value with what it accumulates to, as the iterator walks
        // them all
        let all: Vec<AccItem<T>> = p.prefix_iter().map(|av| acc_item(Some(av))).collect();
        // the items of some ranges, by their places (they are not `Clone`)
        let places: Vec<usize> = (0..len).collect();
        let of = |rs: &[Range<usize>]| -> Vec<&AccItem<T>> {
            ranges_of(&places, rs).iter().map(|&i| &all[i]).collect()
        };
        for _ in 0..15 {
            let a = rng.below(len + 2);
            let rs = some_ranges(&mut rng, a, len);
            let mut x = p.prefix_iter();
            VShiftable::advance_to(&mut x, a);
            let new: Vec<AccItem<T>> = x.ranges(&rs).map(|av| acc_item(Some(av))).collect();
            assert_eq!(new.iter().collect::<Vec<_>>(), of(&rs), "{rs:?}");

            let (s, e) = (rng.below(len + 2), rng.below(len + 10));
            let mut u = p.prefix_iter().unshift();
            u.shift(s..e);
            let got: Vec<AccItem<T>> = u.map(|av| acc_item(Some(av))).collect();
            assert_eq!(
                got.iter().collect::<Vec<_>>(),
                of(std::slice::from_ref(&(s..e)))
            );
        }
    }
}

/// An accumulator iterator repositions as a list of its items would: the
/// values with what they accumulate to, of several ranges in turn and
/// through a lookahead moved to a range.
#[test]
fn an_accumulator_iterator_repositions() {
    for_each_acc!(acc_shifted);
}

fn delta_shifted<T: Deltad>(seed: u64) {
    let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0x541a);
    let mut last = 0;
    let mut model: Vec<T> = vec![];
    for _ in 0..rng.below(150) {
        let (x, l) = T::near(&mut rng, last);
        last = l;
        model.push(x);
    }
    let v = VerifiedDeltaColumn::<T>::from_values(&model);
    let c = DeltaColumn::<T>::from_values(model.clone());
    let len = model.len();
    for _ in 0..15 {
        let a = rng.below(len + 2);
        let rs = some_ranges(&mut rng, a, len);
        let mut x = v.iter();
        let mut y = c.iter();
        VShiftable::advance_to(&mut x, a);
        Shiftable::advance_to(&mut y, a);
        let new: Vec<T> = x.ranges(&rs).collect();
        let old_: Vec<T> = y.ranges(rs.clone()).collect();
        assert_eq!(new, ranges_of(&model, &rs), "{rs:?}");
        assert_eq!(old_, new);

        let (s, e) = (rng.below(len + 2), rng.below(len + 10));
        let mut u = v.iter().unshift();
        let mut w = c.iter().unshift();
        if rng.below(2) == 0 {
            u.shift(s..e);
            w.shift(s..e);
        }
        for _ in 0..8 {
            match rng.below(3) {
                0 => assert_eq!(u.peek().copied(), w.peek().copied()),
                1 => assert_eq!(u.next(), w.next()),
                _ => {
                    let n = rng.below(5);
                    assert_eq!(u.nth(n), w.nth(n));
                }
            }
        }
    }
}

/// A delta iterator repositions as `DeltaColumn`'s does: the values of
/// several ranges in turn, and through a lookahead.
#[test]
fn a_delta_iterator_repositions_as_the_old_one_does() {
    for seed in 0..4 {
        delta_shifted::<u32>(seed);
        delta_shifted::<i64>(seed);
        delta_shifted::<Option<i32>>(seed);
        delta_shifted::<Option<u64>>(seed);
    }
}

// ── Repositioning the runs of a column ─────────────────────────────────────

fn runs_shifted<T: Kind>() {
    for seed in 1..=4u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0x541b);
        let budget = [2, 3, 8, 64][rng.below(4)];
        // values that lie together in runs
        let base: Vec<T> = rng.values(40);
        let mut model: Vec<T> = vec![];
        for x in base {
            for _ in 0..1 + rng.below(4) {
                model.push(x.clone());
            }
        }
        let (v, c) = both_with(&model, budget);
        let len = model.len();
        for _ in 0..20 {
            // the runs of ranges, from a place the runs have moved on to
            let a = rng.below(len + 2);
            let rs = some_ranges(&mut rng, a, len);
            let mut x = v.iter().runs();
            let mut y = c.iter().runs();
            VShiftable::advance_to(&mut x, a);
            Shiftable::advance_to(&mut y, a);
            let new: Vec<(usize, T)> = x.ranges(&rs).map(|r| (r.count, T::keep(r.value))).collect();
            let old_: Vec<(usize, T)> = y
                .ranges(rs.clone())
                .map(|r| (r.count, old::<T>(r.value)))
                .collect();
            assert_eq!(new, old_, "{rs:?}");
            let flat: Vec<T> = new
                .iter()
                .flat_map(|(n, x)| std::iter::repeat_n(x.clone(), *n))
                .collect();
            assert_eq!(flat, ranges_of(&model, &rs), "{rs:?}");

            // a lookahead, looked at and walked
            let mut u = v.iter().runs().unshift();
            let mut w = c.iter().runs().unshift();
            for _ in 0..6 {
                match rng.below(3) {
                    0 => assert_eq!(
                        u.peek().map(|r| (r.count, T::keep(r.value))),
                        w.peek().map(|r| (r.count, old::<T>(r.value)))
                    ),
                    1 => assert_eq!(
                        u.next().map(|r| (r.count, T::keep(r.value))),
                        w.next().map(|r| (r.count, old::<T>(r.value)))
                    ),
                    _ => {
                        let n = rng.below(4);
                        assert_eq!(
                            u.nth(n).map(|r| (r.count, T::keep(r.value))),
                            w.nth(n).map(|r| (r.count, old::<T>(r.value)))
                        );
                    }
                }
            }

            // the moves of `VShiftable`, beside `Shiftable`'s
            let mut x = v.iter().runs();
            let mut y = c.iter().runs();
            for _ in 0..6 {
                let p = VShiftable::get_pos(&x);
                assert_eq!(p, Shiftable::get_pos(&y));
                assert_eq!(VShiftable::get_max(&x), Shiftable::get_max(&y));
                assert_eq!(VShiftable::get_range(&x), Shiftable::get_range(&y));
                let t = p + rng.below(15);
                match rng.below(6) {
                    0 => {
                        VShiftable::set_max(&mut x, t);
                        Shiftable::set_max(&mut y, t);
                    }
                    1 => {
                        VShiftable::advance_to(&mut x, t);
                        Shiftable::advance_to(&mut y, t);
                    }
                    2 => {
                        VShiftable::advance_by(&mut x, t - p);
                        Shiftable::advance_by(&mut y, t - p);
                    }
                    3 => {
                        let r = t..t + rng.below(20);
                        VShiftable::shift(&mut x, r.clone());
                        Shiftable::shift(&mut y, r);
                    }
                    4 => {
                        let r = t..t + rng.below(20);
                        let a = VShiftable::shift_next(&mut x, r.clone())
                            .map(|r| (r.count, T::keep(r.value)));
                        let b =
                            Shiftable::shift_next(&mut y, r).map(|r| (r.count, old::<T>(r.value)));
                        assert_eq!(a, b);
                    }
                    _ => {
                        let a = VShiftable::seek_to(&mut x, t).map(|r| (r.count, T::keep(r.value)));
                        let b = Shiftable::seek_to(&mut y, t).map(|r| (r.count, old::<T>(r.value)));
                        assert_eq!(a, b);
                    }
                }
                assert_eq!(
                    x.next().map(|r| (r.count, T::keep(r.value))),
                    y.next().map(|r| (r.count, old::<T>(r.value)))
                );
            }
        }
    }
}

/// The runs of a verified column reposition as `Column`'s do: the runs
/// of several ranges in turn (`ranges`), a lookahead (`unshift`), and
/// each move of `VShiftable` beside `Shiftable`'s.
#[test]
fn runs_reposition_as_the_old_ones_do() {
    for_each_kind!(runs_shifted);
}

// ── Repositioning the runs of a delta column ───────────────────────────────

use hexane::DeltaRun;

fn delta_runs_shifted<T: Deltad>(seed: u64) {
    let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0x541c);
    // values in runs of equal differences: the same step, taken again
    let mut last = 0;
    let mut model: Vec<T> = vec![];
    for _ in 0..rng.below(40) {
        let state = rng.0;
        for _ in 0..1 + rng.below(5) {
            let (x, l) = T::near(&mut Rng(state), last);
            last = l;
            model.push(x);
        }
        rng.next();
    }
    let len = model.len();
    let v = VerifiedDeltaColumn::<T>::from_values(&model);
    let c = DeltaColumn::<T>::from_values(model);
    for _ in 0..15 {
        // the runs of ranges, from a place the runs have moved on to
        let a = rng.below(len + 2);
        let rs = some_ranges(&mut rng, a, len);
        let mut x = v.iter().runs();
        let mut y = c.iter().runs();
        VShiftable::advance_to(&mut x, a);
        Shiftable::advance_to(&mut y, a);
        let new: Vec<DeltaRun> = x.ranges(&rs).collect();
        let old_: Vec<DeltaRun> = y.ranges(rs.clone()).collect();
        assert_eq!(new, old_, "{rs:?}");

        // a lookahead, looked at and walked
        let mut u = v.iter().runs().unshift();
        let mut w = c.iter().runs().unshift();
        for _ in 0..6 {
            match rng.below(3) {
                0 => assert_eq!(u.peek(), w.peek()),
                1 => assert_eq!(u.next(), w.next()),
                _ => {
                    let n = rng.below(4);
                    assert_eq!(u.nth(n), w.nth(n));
                }
            }
        }

        // the moves of `VShiftable`, beside `Shiftable`'s
        let mut x = v.iter().runs();
        let mut y = c.iter().runs();
        for _ in 0..6 {
            let p = VShiftable::get_pos(&x);
            assert_eq!(p, Shiftable::get_pos(&y));
            assert_eq!(VShiftable::get_max(&x), Shiftable::get_max(&y));
            assert_eq!(VShiftable::get_range(&x), Shiftable::get_range(&y));
            let t = p + rng.below(15);
            match rng.below(6) {
                0 => {
                    VShiftable::set_max(&mut x, t);
                    Shiftable::set_max(&mut y, t);
                }
                1 => {
                    VShiftable::advance_to(&mut x, t);
                    Shiftable::advance_to(&mut y, t);
                }
                2 => {
                    VShiftable::advance_by(&mut x, t - p);
                    Shiftable::advance_by(&mut y, t - p);
                }
                3 => {
                    let r = t..t + rng.below(20);
                    VShiftable::shift(&mut x, r.clone());
                    Shiftable::shift(&mut y, r);
                }
                4 => {
                    let r = t..t + rng.below(20);
                    let a = VShiftable::shift_next(&mut x, r.clone());
                    assert_eq!(a, Shiftable::shift_next(&mut y, r));
                }
                _ => {
                    let a = VShiftable::seek_to(&mut x, t);
                    assert_eq!(a, Shiftable::seek_to(&mut y, t));
                }
            }
            assert_eq!(x.next(), y.next());
        }
    }
}

/// The runs of a verified delta column reposition as `DeltaColumn`'s do:
/// the runs of several ranges in turn (`ranges`), each with the number
/// before it, a lookahead (`unshift`), and each move of `VShiftable`
/// beside `Shiftable`'s.
#[test]
fn delta_runs_reposition_as_the_old_ones_do() {
    for seed in 0..6 {
        delta_runs_shifted::<u32>(seed);
        delta_runs_shifted::<i64>(seed);
        delta_runs_shifted::<Option<i32>>(seed);
        delta_runs_shifted::<Option<u64>>(seed);
    }
}

// ── Loading a column run by run ────────────────────────────────────────────

/// Pulls up to `k` runs from a load, and says what each step gave: the
/// run, as owned values, or the error.
fn pulled<T, F: FnMut() -> Result<Option<(usize, T)>, PackError>>(
    k: usize,
    mut f: F,
) -> Vec<Result<Option<(usize, T)>, String>> {
    let mut out = vec![];
    for _ in 0..k {
        let r = f().map_err(|e| format!("{e:?}"));
        let stop = !matches!(r, Ok(Some(_)));
        out.push(r);
        if stop {
            break;
        }
    }
    out
}

fn loaded_by_runs<T: Kind>() {
    for seed in 1..=6u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0x541d);
        let budget = [2, 3, 8, 64][rng.below(4)];
        let n = rng.below(120);
        let model: Vec<T> = rng.values(n);
        let (v, _c) = both_with(&model, budget);
        let mut bytes = v.save();
        // now and then, bytes that are no column
        if !bytes.is_empty() && rng.below(4) == 0 {
            let i = rng.below(bytes.len());
            bytes[i] ^= 1 << rng.below(8);
        }
        let opts = LoadOpts::new().with_max_segments(budget);
        let k = rng.below(200);
        let mut x = VerifiedColumn::<T>::load_iter(&bytes, opts);
        let mut y = Column::<T>::load_iter(&bytes, opts);
        let a = pulled(k, || {
            x.try_next_run()
                .map(|r| r.map(|r| (r.count, T::keep(r.value))))
        });
        let b = pulled(k, || {
            y.try_next_run()
                .map(|r| r.map(|r| (r.count, old::<T>(r.value))))
        });
        assert_eq!(a, b);
        let xc = x
            .finalize()
            .map(|c| c.iter().map(T::keep).collect::<Vec<T>>())
            .map_err(|e| format!("{e:?}"));
        let yc = y
            .finalize()
            .map(|c| c.iter().map(old::<T>).collect::<Vec<T>>())
            .map_err(|e| format!("{e:?}"));
        assert_eq!(xc, yc);
        if let Ok(vs) = &xc {
            // the runs pulled are the first of the column's values
            let given: Vec<T> = a
                .iter()
                .flat_map(|r| match r {
                    Ok(Some((n, v))) => vec![v.clone(); *n],
                    _ => vec![],
                })
                .collect();
            assert_eq!(&vs[..given.len()], &given[..]);
        }
    }
}

/// A verified column loads run by run as `Column`'s does: the same runs,
/// the same errors, the same column, which begins with the runs pulled.
#[test]
fn a_column_loads_by_runs_as_the_old_one_does() {
    for_each_kind!(loaded_by_runs);
}

fn prefix_loaded_by_runs<T: Totalled>() {
    for seed in 1..=4u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0x541e);
        let budget = [2, 3, 8, 64][rng.below(4)];
        let n = rng.below(120);
        let model: Vec<T> = rng.values(n);
        let mut p = VerifiedPrefixColumn::<T>::with_max_segments(budget);
        p.splice(0, 0, &lent(&model));
        let bytes = p.save();
        let opts = LoadOpts::new().with_max_segments(budget);
        let k = rng.below(60);
        let mut x = VerifiedPrefixColumn::<T>::load_iter(&bytes, opts);
        let mut y = PrefixColumn::<T>::load_iter(&bytes, opts);
        let a = pulled(k, || {
            x.try_next_run()
                .map(|r| r.map(|r| (r.count, T::keep(r.value))))
        });
        let b = pulled(k, || {
            y.try_next_run()
                .map(|r| r.map(|r| (r.count, old::<T>(r.value))))
        });
        assert_eq!(a, b);
        let xc = x.finalize().unwrap();
        let yc = y.finalize().unwrap();
        let xs: Vec<i128> = (0..model.len())
            .map(|i| T::new_sum(xc.get_total(i)))
            .collect();
        let ys: Vec<i128> = (0..model.len())
            .map(|i| T::old_sum(yc.get_total(i)))
            .collect();
        assert_eq!(xs, ys);
    }
}

/// A verified prefix column loads run by run as `PrefixColumn`'s does.
#[test]
fn a_prefix_column_loads_by_runs_as_the_old_one_does() {
    for_each_sum!(prefix_loaded_by_runs);
}

/// No bytes, with a length and a fill value, load as one run of that
/// many of the value, as `Column`'s do.
#[test]
fn a_fill_loads_by_runs_as_the_old_one_does() {
    for n in [0usize, 1, 5, 1000] {
        let opts = LoadOpts::new().with_length(n).with_fill(7u64);
        let mut x = VerifiedColumn::<u64>::load_iter(&[], opts);
        let mut y = Column::<u64>::load_iter(&[], opts);
        let a = pulled(5, || {
            x.try_next_run().map(|r| r.map(|r| (r.count, r.value)))
        });
        let b = pulled(5, || {
            y.try_next_run().map(|r| r.map(|r| (r.count, r.value)))
        });
        assert_eq!(a, b);
        assert_eq!(x.finalize().unwrap().to_vec(), vec![7u64; n]);
        assert_eq!(y.finalize().unwrap().to_vec(), vec![7u64; n]);
    }
}

// ── A client's own encoding ────────────────────────────────────────────────

/// A value type of a client's own with an encoding of its own, as a
/// client compiled without Verus writes it: a tag stored as one byte of
/// the client's choosing, whatever the codec (`Int` as 0x00, `Str` as
/// 0xff). Any other byte is no tag. The tags are declared in the order
/// the client gives them, which is the order a column has them in.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Tag {
    Int,
    Str,
}

impl Tag {
    fn byte(self) -> u8 {
        match self {
            Tag::Int => 0x00,
            Tag::Str => 0xff,
        }
    }

    fn of_byte(data: &[u8]) -> Result<(usize, Tag), PackError> {
        match data.first() {
            Some(0x00) => Ok((1, Tag::Int)),
            Some(0xff) => Ok((1, Tag::Str)),
            _ => Err(refused("not a tag")),
        }
    }
}

impl VPack for Tag {
    fn same(a: &Tag, b: &Tag) -> bool {
        a == b
    }

    fn compare(a: &Tag, b: &Tag) -> Ordering {
        a.cmp(b)
    }

    fn try_unpack<C: VCodec>(data: &[u8]) -> Result<(usize, Tag), PackError> {
        Tag::of_byte(data)
    }

    fn unpack<C: VCodec>(data: &[u8]) -> (usize, Tag) {
        Tag::of_byte(data).unwrap()
    }

    fn value_len<C: VCodec>(_data: &[u8]) -> Option<usize> {
        Some(1)
    }

    fn pack<C: VCodec>(v: &Tag, buf: &mut Vec<u8>) -> usize {
        buf.push(v.byte());
        1
    }
}

// the same type for `Column`, with the same bytes
impl ColumnValue for Tag {
    type Encoding<C: Codec> = RleEncoding<Tag, C>;
}

impl RleValue for Tag {
    fn try_unpack<C: Codec>(data: &[u8]) -> Result<(usize, Tag), PackError> {
        Tag::of_byte(data)
    }

    fn pack<C: Codec>(value: Tag, out: &mut Vec<u8>) -> bool {
        out.push(value.byte());
        true
    }
}

plain_kind!(Tag, |rng| [Tag::Int, Tag::Str][rng.below(2)]);

/// The tags are the bytes the client chose, whatever the codec, in a
/// column of each API; a byte that is no tag's loads in neither, and
/// with the client's error; and the order of a search is the client's.
#[test]
fn a_clients_own_encoding() {
    let (v, c) = both::<Tag>(&[Tag::Int, Tag::Str, Tag::Str]);
    // a literal run of one `Int` (a count of -1), then a run of two
    // `Str`s; with bijou64 the counts are other bytes, the tags not
    assert_eq!(v.save(), [0x7f, 0x00, 0x02, 0xff]);
    assert_eq!(c.save(), v.save());
    let (v, c) = both::<Option<Tag>>(&[Some(Tag::Str), None, None]);
    assert_eq!(v.save(), [0x7f, 0xff, 0x00, 0x02]);
    assert_eq!(c.save(), v.save());
    #[cfg(feature = "bijou64")]
    {
        let w = hexane::verified::bijou::VerifiedColumn::<Tag>::from_values(&[
            Tag::Int,
            Tag::Str,
            Tag::Str,
        ]);
        assert_eq!(w.save(), [0x01, 0x00, 0x04, 0xff]);
    }

    for bad in [&[0x7f, 0x01][..], &[0x02, 0x80], &[0x7e, 0x00, 0xfe]] {
        assert_eq!(refusal::<Tag>(bad), "invalid value: not a tag");
        assert_eq!(refusal::<Option<Tag>>(bad), "invalid value: not a tag");
    }
    assert!(check_same_load::<Tag>(&[0x7e, 0x00, 0xff]));
    assert!(check_same_load::<Option<Tag>>(&[0x02, 0xff, 0x00, 0x03]));

    let mut rng = Rng(0x7a6);
    for _ in 0..50 {
        let mut model = rng.values::<Tag>(40);
        model.sort();
        let (v, c) = both::<Tag>(&model);
        for t in [Tag::Int, Tag::Str] {
            let n = model.len();
            assert_eq!(v.scope_to_value(t, 0, n), c.scope_to_value(t, 0..n));
        }
        assert!(check_same_load::<Tag>(&v.save()));
    }
}

// ── A client's own codec ───────────────────────────────────────────────────

/// A codec of a client's own, as a client compiled without Verus writes
/// it for both APIs: every number as its eight bytes, the least
/// significant first.
#[derive(Debug)]
struct Fixed8;

impl Fixed8 {
    fn eight(data: &[u8]) -> Option<[u8; 8]> {
        data.get(..8)?.try_into().ok()
    }
}

impl VCodec for Fixed8 {
    fn put_u(n: u64, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&n.to_le_bytes());
    }

    fn put_i(n: i64, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&n.to_le_bytes());
    }

    fn read_u(data: &[u8]) -> Result<(usize, u64), PackError> {
        <Fixed8 as VCodec>::read_u_opt(data).ok_or_else(|| refused("fewer than eight bytes"))
    }

    fn read_u_opt(data: &[u8]) -> Option<(usize, u64)> {
        Some((8, u64::from_le_bytes(Fixed8::eight(data)?)))
    }

    fn read_i(data: &[u8]) -> Result<(usize, i64), PackError> {
        <Fixed8 as VCodec>::read_i_opt(data).ok_or_else(|| refused("fewer than eight bytes"))
    }

    fn read_i_opt(data: &[u8]) -> Option<(usize, i64)> {
        Some((8, i64::from_le_bytes(Fixed8::eight(data)?)))
    }

    fn len_u(_data: &[u8]) -> Option<usize> {
        Some(8)
    }

    fn len_i(_data: &[u8]) -> Option<usize> {
        Some(8)
    }

    fn rewrite_header(buf: &mut Vec<u8>, pos: usize, total: usize) -> i64 {
        buf[pos..pos + 8].copy_from_slice(&(-(total as i64)).to_le_bytes());
        0
    }
}

impl Codec for Fixed8 {
    fn encode_unsigned(n: u64) -> hexane::VarBuf {
        let mut b = hexane::VarBuf::new();
        n.to_le_bytes().into_iter().for_each(|x| b.push(x));
        b
    }

    fn encode_signed(n: i64) -> hexane::VarBuf {
        Fixed8::encode_unsigned(n as u64)
    }

    fn read_unsigned(data: &[u8]) -> Option<(usize, u64)> {
        <Fixed8 as VCodec>::read_u_opt(data)
    }

    fn read_signed(data: &[u8]) -> Option<(usize, i64)> {
        <Fixed8 as VCodec>::read_i_opt(data)
    }

    fn try_read_unsigned(data: &[u8]) -> Result<(usize, u64), PackError> {
        <Fixed8 as VCodec>::read_u(data)
    }

    fn try_read_signed(data: &[u8]) -> Result<(usize, i64), PackError> {
        <Fixed8 as VCodec>::read_i(data)
    }
}

/// Columns of each API with the client's codec and the values `vals`
/// save to the same bytes, which load back in both to `vals`; and each
/// API's edits keep them the same.
fn fixed8_agree<T>(vals: &[T])
where
    T: Kind + VValue<Fixed8>,
{
    let mut v = VerifiedColumn::<T, Fixed8>::new();
    v.splice(0, 0, &lent(vals));
    let mut c = Column::<T, Fixed8>::new();
    c.splice(0, 0, vals.iter().cloned());
    let bytes = v.save();
    assert_eq!(c.save(), bytes);
    let back = VerifiedColumn::<T, Fixed8>::load(&bytes).unwrap();
    let back: Vec<T> = back.to_vec().into_iter().map(T::keep).collect();
    assert_eq!(back, vals);
    let old_back = Column::<T, Fixed8>::load(&bytes).unwrap();
    let old_back: Vec<T> = old_back.to_vec().into_iter().map(old).collect();
    assert_eq!(old_back, vals);
    // a literal run grown and cut, by each API: its header rewritten
    if vals.len() >= 2 {
        let at = vals.len() / 2;
        v.splice(at, 1, &lent(&vals[..2]));
        c.splice(at, 1, vals[..2].iter().cloned());
        assert_eq!(v.save(), c.save());
        assert!(VerifiedColumn::<T, Fixed8>::load(&v.save()).is_ok());
    }
}

/// A client's own codec: the numbers of a column are its bytes, in each
/// API, for every kind of value; and bytes it has no number for are
/// refused.
#[test]
fn a_clients_own_codec() {
    // a literal run of two (a header of -2), then the numbers, eight
    // bytes each
    let v = VerifiedColumn::<u64, Fixed8>::from_values(&[5, 6]);
    let mut want = (-2i64).to_le_bytes().to_vec();
    want.extend(5u64.to_le_bytes());
    want.extend(6u64.to_le_bytes());
    assert_eq!(v.save(), want);
    // a header cut short, or a number
    assert!(VerifiedColumn::<u64, Fixed8>::load(&want[..5]).is_err());
    assert!(VerifiedColumn::<u64, Fixed8>::load(&want[..20]).is_err());
    assert!(Column::<u64, Fixed8>::load(&want[..20]).is_err());

    let mut rng = Rng(0xf1c8);
    for _ in 0..20 {
        fixed8_agree::<u64>(&rng.values(40));
        fixed8_agree::<i64>(&rng.values(40));
        fixed8_agree::<Option<u32>>(&rng.values(40));
        fixed8_agree::<bool>(&rng.values(40));
        fixed8_agree::<Action>(&rng.values(40));
        fixed8_agree::<Tag>(&rng.values(40));
        fixed8_agree::<Option<Tag>>(&rng.values(40));
        fixed8_agree::<Vec<u8>>(&rng.values(40));
        fixed8_agree::<Option<Vec<u8>>>(&rng.values(40));
        fixed8_agree::<String>(&rng.values(40));
    }
}

// ── A plain column with a Fenwick index ────────────────────────────────────

/// A column kept with a Fenwick tree of its slabs' counts, in each API,
/// and a plain column: the same values after every edit, saved to the
/// same bytes, read the same at places and loaded back the same.
fn bit_index_agrees<T: Kind>() {
    let mut rng = Rng(0xb17);
    let mut v = VerifiedColumn::<T, Leb128, LenBitIndex>::new();
    let mut c = Column::<T, Leb128, LenWeight, BitIndex<usize>>::new();
    let mut plain = VerifiedColumn::<T>::new();
    let mut model: Vec<T> = vec![];
    for _ in 0..150 {
        let at = rng.below(model.len() + 1);
        let del = rng.below(model.len() - at + 1).min(6);
        let n = rng.below(40);
        let ins = rng.values::<T>(n);
        v.splice(at, del, &lent(&ins));
        c.splice(at, del, ins.iter().cloned());
        plain.splice(at, del, &lent(&ins));
        model.splice(at..at + del, ins);
        let bytes = v.save();
        assert_eq!(c.save(), bytes);
        assert_eq!(plain.save(), bytes);
        assert_eq!(v.len(), model.len());
        assert_eq!(v.slab_count(), plain.slab_count());
        for i in [
            0,
            model.len() / 3,
            model.len() / 2,
            model.len().saturating_sub(1),
        ] {
            assert_eq!(v.get(i).map(T::keep), model.get(i).cloned());
            assert_eq!(c.get(i).map(old), model.get(i).cloned());
        }
    }
    let got: Vec<T> = v.to_vec().into_iter().map(T::keep).collect();
    assert_eq!(got, model);
    let back = VerifiedColumn::<T, Leb128, LenBitIndex>::load(&v.save()).unwrap();
    let back: Vec<T> = back.to_vec().into_iter().map(T::keep).collect();
    assert_eq!(back, model);
}

#[test]
fn a_fenwick_index_keeps_a_plain_column() {
    for_each_kind!(bit_index_agrees);
}

// ── The lengths of LEB128 numbers ──────────────────────────────────────────

/// The verified sizes are the old API's, and the lengths of what the
/// codec writes: at every length's edges, and at random.
#[test]
fn leb_sizes_agree() {
    let mut us: Vec<u64> = vec![0, u64::MAX];
    let mut is: Vec<i64> = vec![0, -1, i64::MIN, i64::MAX];
    for k in 1..64 {
        us.extend([(1u64 << k) - 1, 1u64 << k]);
    }
    for k in 0..63 {
        is.extend([(1i64 << k) - 1, 1i64 << k, -(1i64 << k), -(1i64 << k) - 1]);
    }
    let mut rng = Rng(0x1eb);
    for _ in 0..1000 {
        let n = rng.next() >> rng.below(64);
        us.push(n);
        is.push(n as i64);
    }
    for n in us {
        let mut buf = vec![];
        <Leb128 as hexane::verified::VCodec>::put_u(n, &mut buf);
        assert_eq!(hexane::verified::ulebsize(n), buf.len() as u64, "{n}");
        assert_eq!(hexane::verified::ulebsize(n), hexane::ulebsize(n), "{n}");
    }
    for i in is {
        let mut buf = vec![];
        <Leb128 as hexane::verified::VCodec>::put_i(i, &mut buf);
        assert_eq!(hexane::verified::lebsize(i), buf.len() as u64, "{i}");
        assert_eq!(hexane::verified::lebsize(i), hexane::lebsize(i), "{i}");
    }
}
