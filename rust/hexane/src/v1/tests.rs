use crate::v1::{Column, ColumnValueRef, DeltaColumn, LoadOpts, PrefixColumn};
use proptest::prelude::*;

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Build a column by appending values left-to-right.
fn v1_build(values: &[u64]) -> Column<u64> {
    let mut col = Column::<u64>::new();
    for (i, &v) in values.iter().enumerate() {
        col.insert(i, v);
    }
    col
}

/// Helper trait to bridge comparison between `T::Get<'a>` and `T`.
trait GetEq<T> {
    fn get_eq(&self, other: &T) -> bool;
}

// For types where Get = Self (u64, i64, bool, Option<u64>, Option<i64>).
impl GetEq<u64> for u64 {
    fn get_eq(&self, other: &u64) -> bool {
        self == other
    }
}
impl GetEq<Option<u64>> for Option<u64> {
    fn get_eq(&self, other: &Option<u64>) -> bool {
        self == other
    }
}
impl GetEq<i64> for i64 {
    fn get_eq(&self, other: &i64) -> bool {
        self == other
    }
}
impl GetEq<Option<i64>> for Option<i64> {
    fn get_eq(&self, other: &Option<i64>) -> bool {
        self == other
    }
}
impl GetEq<bool> for bool {
    fn get_eq(&self, other: &bool) -> bool {
        self == other
    }
}
// For ref types.
impl GetEq<String> for &str {
    fn get_eq(&self, other: &String) -> bool {
        *self == other.as_str()
    }
}
impl GetEq<Option<String>> for Option<&str> {
    fn get_eq(&self, other: &Option<String>) -> bool {
        match (self, other) {
            (Some(a), Some(b)) => *a == b.as_str(),
            (None, None) => true,
            _ => false,
        }
    }
}
impl GetEq<Vec<u8>> for &[u8] {
    fn get_eq(&self, other: &Vec<u8>) -> bool {
        *self == other.as_slice()
    }
}
impl GetEq<Option<Vec<u8>>> for Option<&[u8]> {
    fn get_eq(&self, other: &Option<Vec<u8>>) -> bool {
        match (self, other) {
            (Some(a), Some(b)) => *a == b.as_slice(),
            (None, None) => true,
            _ => false,
        }
    }
}

/// Assert column contents match `expected`, and encoding is canonical.
fn assert_col<T>(col: &Column<T>, expected: &[T])
where
    T: ColumnValueRef + std::fmt::Debug,
    for<'a> T::Get<'a>: GetEq<T> + std::fmt::Debug,
{
    assert_eq!(col.len(), expected.len(), "length mismatch");
    for (i, v) in expected.iter().enumerate() {
        match col.get(i) {
            Some(g) => assert!(g.get_eq(v), "mismatch at {i}: got {g:?}, expected {v:?}"),
            None => panic!("mismatch at {i}: got None, expected {v:?}"),
        }
    }
    col.validate_encoding().unwrap();
}

/// Build a nullable column from (value, index) pairs, assert at end.
fn build_and_check(steps: &[(Option<u64>, usize)]) {
    let mut col = Column::<Option<u64>>::new();
    let mut mirror: Vec<Option<u64>> = vec![];
    for &(v, idx) in steps {
        col.insert(idx, v);
        mirror.insert(idx, v);
    }
    assert_col(&col, &mirror);
}

// ── Basic construction ──────────────────────────────────────────────────────

#[test]
fn build_sequential_values() {
    let vals = [1u64, 2, 3, 4, 5];
    assert_col(&v1_build(&vals), &vals);
}

#[test]
fn build_with_repeats() {
    let vals = [1u64, 2, 3, 3, 3, 4, 5];
    assert_col(&v1_build(&vals), &vals);
}

#[test]
fn build_all_same() {
    let vals = [7u64; 10];
    assert_col(&v1_build(&vals), &vals);
}

// ── get ──────────────────────────────────────────────────────────────────────

#[test]
fn get_in_bounds() {
    let col = v1_build(&[10, 20, 30]);
    assert_eq!(col.get(0), Some(10));
    assert_eq!(col.get(1), Some(20));
    assert_eq!(col.get(2), Some(30));
}

#[test]
fn get_out_of_bounds() {
    let col = v1_build(&[10, 20]);
    assert_eq!(col.get(2), None);
    assert_eq!(col.get(100), None);
}

// ── insert ──────────────────────────────────────────────────────────────────

#[test]
fn insert_at_beginning() {
    let mut col = v1_build(&[2, 3]);
    col.insert(0, 1);
    assert_col(&col, &[1, 2, 3]);
}

#[test]
fn insert_at_end() {
    let mut col = v1_build(&[1, 2]);
    col.insert(2, 3);
    assert_col(&col, &[1, 2, 3]);
}

#[test]
fn insert_in_middle() {
    let mut col = v1_build(&[1, 3]);
    col.insert(1, 2);
    assert_col(&col, &[1, 2, 3]);
}

// ── remove ──────────────────────────────────────────────────────────────────

#[test]
fn remove_first() {
    let mut col = v1_build(&[1, 2, 3]);
    col.remove(0);
    assert_col(&col, &[2, 3]);
}

#[test]
fn remove_last() {
    let mut col = v1_build(&[1, 2, 3]);
    col.remove(2);
    assert_col(&col, &[1, 2]);
}

#[test]
fn remove_middle() {
    let mut col = v1_build(&[1, 2, 3]);
    col.remove(1);
    assert_col(&col, &[1, 3]);
}

// ── Nullable columns ────────────────────────────────────────────────────────

#[test]
fn nullable_basic() {
    let mut col = Column::<Option<u64>>::new();
    col.insert(0, Some(1));
    col.insert(1, None);
    col.insert(2, Some(2));
    assert_col(&col, &[Some(1), None, Some(2)]);
}

#[test]
fn nullable_all_null() {
    let mut col = Column::<Option<u64>>::new();
    for i in 0..5 {
        col.insert(i, None);
    }
    assert_col(&col, &[None, None, None, None, None]);
}

// ── Boolean columns ─────────────────────────────────────────────────────────

#[test]
fn bool_basic() {
    let mut col = Column::<bool>::new();
    col.insert(0, true);
    col.insert(1, false);
    col.insert(2, true);
    assert_col(&col, &[true, false, true]);
}

#[test]
fn bool_all_same() {
    let mut col = Column::<bool>::new();
    for i in 0..10 {
        col.insert(i, true);
    }
    assert_col(&col, &[true; 10]);
}

// ── String columns ──────────────────────────────────────────────────────────

#[test]
fn string_basic() {
    let mut col = Column::<String>::new();
    col.insert(0, "hello".to_string());
    col.insert(1, "world".to_string());
    assert_col(&col, &["hello".to_string(), "world".to_string()]);
}

// ── Multi-slab operations ───────────────────────────────────────────────────

#[test]
fn slab_splitting() {
    let mut col = Column::<u64>::with_max_segments(4);
    for i in 0..20 {
        col.insert(i, i as u64);
    }
    assert!(col.slab_count() > 1, "should split into multiple slabs");
    let expected: Vec<u64> = (0..20).collect();
    assert_col(&col, &expected);
}

#[test]
fn slab_merging_after_removes() {
    let mut col = Column::<u64>::with_max_segments(4);
    let mut expected: Vec<u64> = vec![];
    for i in 0..20 {
        col.insert(i, i as u64);
        expected.push(i as u64);
    }
    for _ in 0..15 {
        col.remove(0);
        expected.remove(0);
    }
    assert_col(&col, &expected);
}

// ── save/load ───────────────────────────────────────────────────────────────

#[test]
fn save_empty() {
    let col = Column::<u64>::new();
    assert!(col.save().is_empty());
}

#[test]
fn save_single_slab() {
    let col = v1_build(&[1, 2, 3]);
    let bytes = col.save();
    assert!(!bytes.is_empty());
}

// ── Op-based test infrastructure ────────────────────────────────────────────

enum Op<T> {
    Insert(T),
    Delete,
}

fn apply_ops<T>(ops: &[Op<T>], positions: &[usize], col: &mut Column<T>, mirror: &mut Vec<T>)
where
    T: ColumnValueRef + Clone + std::fmt::Debug,
    for<'a> T::Get<'a>: GetEq<T> + std::fmt::Debug,
{
    for (op, &raw_pos) in ops.iter().zip(positions.iter()) {
        match op {
            Op::Insert(v) => {
                let idx = if mirror.is_empty() {
                    0
                } else {
                    raw_pos % (mirror.len() + 1)
                };
                col.insert(idx, v.clone());
                mirror.insert(idx, v.clone());
            }
            Op::Delete => {
                if mirror.is_empty() {
                    continue;
                }
                let idx = raw_pos % mirror.len();
                col.remove(idx);
                mirror.remove(idx);
            }
        }
    }
}

// ── AsColumnRef (borrowed insert/splice) ────────────────────────────────

#[test]
fn insert_str_into_string_column() {
    let mut col = Column::<String>::new();
    col.insert(0, "hello");
    col.insert(1, "world");
    assert_eq!(col.get(0), Some("hello"));
    assert_eq!(col.get(1), Some("world"));
    // Owned still works.
    col.insert(2, String::from("owned"));
    assert_eq!(col.get(2), Some("owned"));
}

#[test]
fn insert_option_str_into_nullable_string_column() {
    let mut col = Column::<Option<String>>::new();
    col.insert(0, Some("hello"));
    col.insert(1, None::<&str>);
    col.insert(2, "bare_str"); // &str → Some(String)
    assert_eq!(col.get(0), Some(Some("hello")));
    assert_eq!(col.get(1), Some(None));
    assert_eq!(col.get(2), Some(Some("bare_str")));
    // Owned still works.
    col.insert(3, Some(String::from("owned")));
    assert_eq!(col.get(3), Some(Some("owned")));
}

#[test]
fn insert_slice_into_bytes_column() {
    let mut col = Column::<Vec<u8>>::new();
    col.insert(0, b"hello".as_slice());
    col.insert(1, vec![1, 2, 3]);
    assert_eq!(col.get(0), Some(b"hello".as_slice()));
    assert_eq!(col.get(1), Some([1, 2, 3].as_slice()));
}

#[test]
fn insert_option_slice_into_nullable_bytes_column() {
    let mut col = Column::<Option<Vec<u8>>>::new();
    col.insert(0, Some(b"hello".as_slice()));
    col.insert(1, None::<&[u8]>);
    col.insert(2, b"bare".as_slice()); // &[u8] → Some(Vec<u8>)
    assert_eq!(col.get(0), Some(Some(b"hello".as_slice())));
    assert_eq!(col.get(1), Some(None));
    assert_eq!(col.get(2), Some(Some(b"bare".as_slice())));
}

#[test]
fn splice_str_items() {
    let mut col = Column::<String>::from_values(vec![
        "a".into(),
        "b".into(),
        "c".into(),
        "d".into(),
        "e".into(),
    ]);
    col.splice(1, 2, ["x", "y", "z"]);
    assert_eq!(col.len(), 6);
    assert_eq!(col.get(0), Some("a"));
    assert_eq!(col.get(1), Some("x"));
    assert_eq!(col.get(2), Some("y"));
    assert_eq!(col.get(3), Some("z"));
    assert_eq!(col.get(4), Some("d"));
    assert_eq!(col.get(5), Some("e"));
}

#[test]
fn splice_option_str_items() {
    let mut col = Column::<Option<String>>::new();
    for i in 0..5 {
        col.insert(i, Some("x"));
    }
    col.splice(1, 2, [Some("new"), None]);
    assert_eq!(col.get(0), Some(Some("x")));
    assert_eq!(col.get(1), Some(Some("new")));
    assert_eq!(col.get(2), Some(None));
    assert_eq!(col.get(3), Some(Some("x")));
}

#[test]
fn insert_string_ref_via_deref() {
    let mut col = Column::<String>::new();
    let owned = String::from("hello");
    col.insert(0, &*owned); // &String derefs to &str
    assert_eq!(col.get(0), Some("hello"));
}

// ── FromIterator ────────────────────────────────────────────────────────────

#[test]
fn column_from_iterator() {
    let col: Column<u64> = vec![10, 20, 30, 40, 50].into_iter().collect();
    assert_eq!(col.len(), 5);
    for (i, &v) in [10, 20, 30, 40, 50].iter().enumerate() {
        assert_eq!(col.get(i), Some(v));
    }
}

#[test]
fn column_bool_from_iterator() {
    let col: Column<bool> = vec![true, false, true, true].into_iter().collect();
    assert_eq!(col.len(), 4);
    assert_eq!(col.get(0), Some(true));
    assert_eq!(col.get(1), Some(false));
}

#[test]
fn column_nullable_from_iterator() {
    let col: Column<Option<u64>> = vec![Some(1), None, Some(3)].into_iter().collect();
    assert_eq!(col.len(), 3);
    assert_eq!(col.get(0), Some(Some(1)));
    assert_eq!(col.get(1), Some(None));
    assert_eq!(col.get(2), Some(Some(3)));
}

#[test]
fn column_string_from_iterator() {
    let col: Column<String> = vec!["hello".to_string(), "world".to_string()]
        .into_iter()
        .collect();
    assert_eq!(col.len(), 2);
    assert_eq!(col.get(0), Some("hello"));
    assert_eq!(col.get(1), Some("world"));
}

#[test]
fn prefix_column_from_iterator() {
    let col: PrefixColumn<u64> = vec![3, 1, 4, 1, 5].into_iter().collect();
    assert_eq!(col.len(), 5);
    assert_eq!(col.get_prefix(3), 8); // 3+1+4
}

#[test]
fn delta_column_from_iterator() {
    let col: DeltaColumn<u64> = vec![10, 20, 30, 40].into_iter().collect();
    assert_eq!(col.len(), 4);
    assert_eq!(col.get(0), Some(10));
    assert_eq!(col.get(3), Some(40));
}

#[test]
fn column_from_empty_iterator() {
    let col: Column<u64> = std::iter::empty().collect();
    assert_eq!(col.len(), 0);
    assert!(col.is_empty());
}

// ── pos() and shift_next() ───────────────────────────────────────────────────

#[test]
fn iter_pos_tracks_position() {
    let col = Column::<u64>::from_values(vec![10, 20, 30, 40, 50]);
    let mut iter = col.iter();
    assert_eq!(iter.pos(), 0);
    iter.next();
    assert_eq!(iter.pos(), 1);
    iter.next();
    assert_eq!(iter.pos(), 2);
    iter.nth(1); // skip 30, land on 40
    assert_eq!(iter.pos(), 4);
    iter.next(); // 50
    assert_eq!(iter.pos(), 5);
    assert_eq!(iter.next(), None);
    assert_eq!(iter.pos(), 5);
}

#[test]
fn iter_range_pos_starts_at_range_start() {
    let col = Column::<u64>::from_values(vec![10, 20, 30, 40, 50]);
    let mut iter = col.iter_range(2..4);
    assert_eq!(iter.pos(), 2);
    assert_eq!(iter.next(), Some(30));
    assert_eq!(iter.pos(), 3);
    assert_eq!(iter.next(), Some(40));
    assert_eq!(iter.pos(), 4);
    assert_eq!(iter.next(), None);
    assert_eq!(iter.pos(), 4);
}

#[test]
fn shift_next_matches_v0() {
    // Reproduce exact v0 test case from columndata.rs::shift_next
    let col = Column::<u64>::from_values(vec![
        0, 0, 0, 1, 1, 1, 6, 6, 6, 7, 7, 7, 8, 8, 8, 9, 9, 9, 10, 10, 10,
    ]);
    let mut iter = col.iter_range(1..4);
    assert_eq!(iter.next(), Some(0));
    assert_eq!(iter.next(), Some(0));
    assert_eq!(iter.next(), Some(1));
    assert_eq!(iter.next(), None);

    let next = iter.shift_next(5..7);
    assert_eq!(next, Some(1));
    assert_eq!(iter.next(), Some(6));
    assert_eq!(iter.next(), None);

    let next = iter.shift_next(8..10);
    assert_eq!(next, Some(6));
    assert_eq!(iter.next(), Some(7));
    assert_eq!(iter.next(), None);
}

#[test]
fn shift_next_bool_column() {
    let col = Column::<bool>::from_values(vec![
        true, false, true, true, false, false, true, false, true, true,
    ]);
    let mut iter = col.iter_range(0..3);
    assert_eq!(iter.next(), Some(true));
    assert_eq!(iter.pos(), 1);

    let next = iter.shift_next(4..7);
    assert_eq!(next, Some(false));
    assert_eq!(iter.pos(), 5);
    assert_eq!(iter.next(), Some(false));
    assert_eq!(iter.next(), Some(true));
    assert_eq!(iter.next(), None);
}

#[test]
fn shift_next_nullable_column() {
    let col = Column::<Option<u64>>::from_values(vec![
        Some(1),
        None,
        Some(3),
        None,
        Some(5),
        None,
        Some(7),
    ]);
    let mut iter = col.iter_range(0..2);
    assert_eq!(iter.next(), Some(Some(1)));
    assert_eq!(iter.next(), Some(None));
    assert_eq!(iter.next(), None);

    let next = iter.shift_next(3..5);
    assert_eq!(next, Some(None));
    assert_eq!(iter.next(), Some(Some(5)));
    assert_eq!(iter.next(), None);
}

#[test]
fn shift_next_at_current_pos() {
    let col = Column::<u64>::from_values(vec![10, 20, 30, 40, 50]);
    let mut iter = col.iter_range(0..2);
    assert_eq!(iter.next(), Some(10));
    assert_eq!(iter.next(), Some(20));
    assert_eq!(iter.next(), None);
    // shift_next starting exactly at current pos
    let next = iter.shift_next(2..5);
    assert_eq!(next, Some(30));
    assert_eq!(iter.pos(), 3);
}

#[test]
fn shift_next_cross_validation_with_v0() {
    use crate::{ColumnData, UIntCursor};

    let data: Vec<u64> = (0..50).map(|i| i / 3).collect();
    let v0: ColumnData<UIntCursor> = data.iter().collect();
    let v1 = Column::<u64>::from_values(data.clone());

    // Create iterators from same range
    let mut v0_iter = v0.iter_range(0..10);
    let mut v1_iter = v1.iter_range(0..10);

    // Consume some
    for _ in 0..5 {
        let a = v0_iter.next().map(|o| o.map(|c| *c));
        let b = v1_iter.next();
        assert_eq!(a, b.map(Some));
    }
    assert_eq!(v0_iter.pos(), v1_iter.pos());

    // shift_next to a later range
    let a = v0_iter.shift_next(15..20).map(|o| o.map(|c| *c));
    let b = v1_iter.shift_next(15..20);
    assert_eq!(a, b.map(Some));
    assert_eq!(v0_iter.pos(), v1_iter.pos());

    // Continue iterating
    loop {
        let a = v0_iter.next().map(|o| o.map(|c| *c));
        let b = v1_iter.next();
        assert_eq!(a, b.map(Some));
        if b.is_none() {
            break;
        }
    }
    assert_eq!(v0_iter.pos(), v1_iter.pos());
}

#[test]
fn prefix_iter_pos_and_shift_next() {
    let col: PrefixColumn<u64> = PrefixColumn::from_values(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let mut iter = col.iter_range(0..4);
    assert_eq!(iter.pos(), 0);

    let (prefix, val) = iter.next().unwrap();
    assert_eq!(val, 1);
    assert_eq!(prefix, 1);
    assert_eq!(iter.pos(), 1);

    // shift_next to 5..8
    let result = iter.shift_next(5..8);
    let (prefix, val) = result.unwrap();
    assert_eq!(val, 6);
    assert_eq!(prefix, 1 + 2 + 3 + 4 + 5 + 6); // prefix through item 5
    assert_eq!(iter.pos(), 6);

    let (prefix, val) = iter.next().unwrap();
    assert_eq!(val, 7);
    assert_eq!(prefix, 1 + 2 + 3 + 4 + 5 + 6 + 7);

    let (prefix, val) = iter.next().unwrap();
    assert_eq!(val, 8);
    assert_eq!(prefix, 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8);

    assert!(iter.next().is_none());
}

// ── fill / save_to_unless ────────────────────────────────────────────────────

#[test]
fn fill_creates_all_null_column() {
    let col = Column::<Option<u64>>::fill(100, None);
    assert_eq!(col.len(), 100);
    for i in 0..100 {
        assert_eq!(col.get(i), Some(None));
    }
}

#[test]
fn fill_zero_length() {
    let col = Column::<Option<u64>>::fill(0, None);
    assert_eq!(col.len(), 0);
    assert!(col.is_empty());
}

#[test]
fn save_to_unless_skips_all_null() {
    let col = Column::<Option<u64>>::from_values(vec![None, None, None, None, None]);
    let mut out = vec![];
    let range = col.save_to_unless(&mut out, None);
    assert!(range.is_empty());
}

#[test]
fn save_to_unless_writes_non_null() {
    let col = Column::<Option<u64>>::from_values(vec![Some(1), Some(2), None, Some(3)]);
    let mut out = vec![];
    let range = col.save_to_unless(&mut out, None);
    assert!(!range.is_empty());
}

#[test]
fn save_to_unless_writes_mixed_null() {
    let col = Column::<Option<u64>>::from_values(vec![None, None, Some(0), None]);
    let mut out = vec![];
    let range = col.save_to_unless(&mut out, None);
    assert!(!range.is_empty());
}

#[test]
fn fill_nullable_types() {
    let col = Column::<Option<i64>>::fill(50, None);
    assert_eq!(col.get(0), Some(None));

    let col = Column::<Option<String>>::fill(10, Option::<&str>::None);
    assert_eq!(col.get(0), Some(None));

    let col = Column::<Option<Vec<u8>>>::fill(10, Option::<&[u8]>::None);
    assert_eq!(col.get(0), Some(None));
}

#[test]
fn fill_null_roundtrips_through_save_load() {
    let col = Column::<Option<u64>>::fill(1000, None);
    let bytes = col.save();
    let loaded = Column::<Option<u64>>::load(&bytes).unwrap();
    assert_eq!(loaded.len(), 1000);
}

#[test]
fn save_to_unless_after_insert() {
    let mut col = Column::<Option<u64>>::fill(10, None);
    let mut out = vec![];
    assert!(col.save_to_unless(&mut out, None).is_empty());
    col.insert(5, Some(42));
    assert!(!col.save_to_unless(&mut out, None).is_empty());
}

#[test]
fn fill_creates_all_false_column() {
    let col = Column::<bool>::fill(100, false);
    assert_eq!(col.len(), 100);
    for i in 0..100 {
        assert_eq!(col.get(i), Some(false));
    }
}

#[test]
fn fill_bool_zero_length() {
    let col = Column::<bool>::fill(0, false);
    assert_eq!(col.len(), 0);
    assert!(col.is_empty());
}

#[test]
fn save_to_unless_skips_all_false() {
    let col = Column::<bool>::from_values(vec![false, false, false, false]);
    let mut out = vec![];
    assert!(col.save_to_unless(&mut out, false).is_empty());
}

#[test]
fn save_to_unless_writes_mixed_bool() {
    let col = Column::<bool>::from_values(vec![false, false, true, false]);
    let mut out = vec![];
    assert!(!col.save_to_unless(&mut out, false).is_empty());
}

#[test]
fn save_to_unless_skips_all_true() {
    let col = Column::<bool>::from_values(vec![true, true, true]);
    let mut out = vec![];
    assert!(col.save_to_unless(&mut out, true).is_empty());
    assert!(!col.save_to_unless(&mut out, false).is_empty());
}

#[test]
fn fill_bool_roundtrips_through_save_load() {
    let col = Column::<bool>::fill(1000, false);
    let bytes = col.save();
    let loaded = Column::<bool>::load(&bytes).unwrap();
    assert_eq!(loaded.len(), 1000);
}

#[test]
fn save_to_unless_bool_after_insert() {
    let mut col = Column::<bool>::fill(10, false);
    let mut out = vec![];
    assert!(col.save_to_unless(&mut out, false).is_empty());
    col.insert(5, true);
    assert!(!col.save_to_unless(&mut out, false).is_empty());
}

// ── Proptest ──────────────────────────────────────────────────────────────────

fn nullable_uint_value() -> impl Strategy<Value = Option<u64>> {
    prop_oneof![
        3 => Just(None),
        7 => (0..4u64).prop_map(Some),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// Random splice operations on a nullable u64 column.
    #[test]
    fn fuzz_splice_ops(
        splice_ops in prop::collection::vec(
            (0..1000usize, 0..10usize,
             prop::collection::vec(nullable_uint_value(), 0..5)),
            1..20,
        ),
    ) {
        let mut col = Column::<Option<u64>>::new();
        let mut mirror: Vec<Option<u64>> = vec![];

        for (pos_raw, del_raw, values) in splice_ops {
            let idx = if mirror.is_empty() { 0 } else { pos_raw % (mirror.len() + 1) };
            let del = std::cmp::min(del_raw, mirror.len() - std::cmp::min(idx, mirror.len()));
            col.splice(idx, del, values.iter().cloned());
            let new_vals: Vec<_> = values.into_iter().collect();
            mirror.splice(idx..idx + del, new_vals);
        }
        assert_col(&col, &mirror);
    }

    /// Sequential inserts then deletes — the hot path for splice-like usage.
    #[test]
    fn fuzz_sequential_splice_uint(
        values in prop::collection::vec(nullable_uint_value(), 1..80),
        start_pos in 0..50usize,
    ) {
        let mut col = Column::<Option<u64>>::new();
        let mut mirror: Vec<Option<u64>> = vec![];

        for (i, v) in values.iter().enumerate() {
            let idx = std::cmp::min(start_pos + i, mirror.len());
            col.insert(idx, *v);
            mirror.insert(idx, *v);
        }
        assert_col(&col, &mirror);

        let del_pos = std::cmp::min(start_pos, mirror.len().saturating_sub(1));
        let del_count = std::cmp::min(values.len() / 2, mirror.len() - del_pos);
        for _ in 0..del_count {
            if mirror.is_empty() { break; }
            let idx = std::cmp::min(del_pos, mirror.len() - 1);
            col.remove(idx);
            mirror.remove(idx);
        }
        assert_col(&col, &mirror);
    }
}

// ── Regression: nullable u64 edge cases ─────────────────────────────────────

/// Inserting a duplicate at position 0 of a literal → should form repeat.
#[test]
fn insert_duplicate_into_literal() {
    build_and_check(&[(None, 0), (Some(1), 0), (Some(1), 0)]);
}

/// Deleting a null between two value runs merges the literals.
#[test]
fn delete_null_merges_adjacent_literals() {
    let mut col = Column::<Option<u64>>::new();
    let mut mirror = vec![];
    for (v, idx) in [(Some(2), 0), (None, 1), (Some(1), 2), (Some(3), 3)] {
        col.insert(idx, v);
        mirror.insert(idx, v);
    }
    assert_col(&col, &mirror);

    col.remove(1);
    mirror.remove(1);
    assert_col(&col, &mirror); // [Some(2), Some(1), Some(3)]
}

/// Deleting from repeat-2 → lit-1 should merge with neighbor literals.
#[test]
fn delete_repeat2_merges_into_literal() {
    let mut col = Column::<Option<u64>>::new();
    let mut mirror = vec![];
    for (v, idx) in [(Some(2), 0), (Some(5), 1), (Some(1), 2), (Some(3), 3)] {
        col.insert(idx, v);
        mirror.insert(idx, v);
    }
    // Insert dup to create repeat-2 in middle
    col.insert(2, Some(5));
    mirror.insert(2, Some(5));
    assert_col(&col, &mirror);

    // Delete one → repeat-2 becomes lit-1, merges with neighbors
    col.remove(2);
    mirror.remove(2);
    assert_col(&col, &mirror);
}

/// Inserting a value into a null run should merge resulting lit-1 with neighbors.
#[test]
fn insert_into_null_run_merges_literals() {
    let mut col = Column::<Option<u64>>::new();
    let mut mirror = vec![];
    for (v, idx) in [(Some(2), 0), (None, 1), (None, 2), (Some(3), 3)] {
        col.insert(idx, v);
        mirror.insert(idx, v);
    }
    assert_col(&col, &mirror);

    // Insert Some(5) into the null run → [Some(2), Some(5), None, None, Some(3)]
    col.insert(1, Some(5));
    mirror.insert(1, Some(5));
    assert_col(&col, &mirror);
}

/// Insert into repeat then null split.
#[test]
fn null_split_no_merge() {
    build_and_check(&[
        (Some(1), 0),
        (Some(1), 0),
        (Some(2), 0),
        (None, 2),
        (None, 0),
    ]);
}

/// Various ways adjacent null runs can be created.
#[test]
fn adjacent_null_various() {
    // Case 1: [Some(3), Some(2), None, None]
    build_and_check(&[(Some(3), 0), (Some(2), 1), (None, 2), (None, 3)]);

    // Case 2: [None, Some(1), None]
    build_and_check(&[(None, 0), (Some(1), 0), (None, 0)]);

    // Case 3: Delete value between two null runs → [None, None]
    let mut col = Column::<Option<u64>>::new();
    let mut mirror = vec![];
    for (v, idx) in [(None, 0), (Some(1), 1), (None, 2)] {
        col.insert(idx, v);
        mirror.insert(idx, v);
    }
    col.remove(1);
    mirror.remove(1);
    assert_col(&col, &mirror);
}

/// Adjacent repeats of same value after a sequence of inserts/deletes.
#[test]
fn adjacent_repeats() {
    let ops: Vec<Op<Option<u64>>> = vec![
        Op::Insert(None),
        Op::Insert(None),
        Op::Insert(Some(1)),
        Op::Insert(Some(1)),
        Op::Insert(None),
        Op::Insert(None),
        Op::Delete,
        Op::Insert(None),
        Op::Insert(None),
        Op::Insert(Some(1)),
        Op::Insert(None),
        Op::Insert(None),
        Op::Insert(None),
        Op::Insert(None),
        Op::Insert(None),
        Op::Delete,
    ];
    let positions = [
        0, 0, 669, 144, 931, 129, 577, 498, 171, 337, 369, 204, 270, 276, 72, 265,
    ];
    let mut col = Column::<Option<u64>>::new();
    let mut mirror = vec![];
    apply_ops(&ops, &positions, &mut col, &mut mirror);
    assert_col(&col, &mirror);
}

/// Literal with 3 repeating values at end should normalize to repeat.
#[test]
fn lit_with_triple_dup() {
    build_and_check(&[
        (Some(2), 0),
        (Some(1), 1),
        (Some(1), 2),
        (Some(1), 3),
        (None, 4),
        (Some(2), 5),
    ]);
}

/// [lit-1: v] adjacent to [repeat-2: v] should be absorbed into [repeat-3: v].
#[test]
fn lit1_adjacent_repeat_same_value() {
    let mut col = Column::<Option<u64>>::new();
    let mut mirror = vec![];
    // Build [None, None, Some(2), None, None, None, None, None, None]
    for &(v, idx) in &[(None, 0), (None, 0), (None, 0)] {
        col.insert(idx, v);
        mirror.insert(idx, v);
    }
    // Delete@0 → [None, None]
    col.remove(0);
    mirror.remove(0);
    // More inserts
    for &(v, idx) in &[
        (None, 0),
        (None, 0),
        (None, 0),
        (None, 0),
        (Some(2), 0),
        (None, 0),
        (None, 0),
    ] {
        col.insert(idx, v);
        mirror.insert(idx, v);
    }
    assert_col(&col, &mirror);
}

/// Insert value at position 0 of null run, adjacent to another null.
#[test]
fn insert_value_into_null_adjacent_null() {
    build_and_check(&[(None, 0), (None, 0), (Some(2), 0), (Some(2), 1)]);
}

/// Null runs should coalesce when adjacent.
#[test]
fn null_coalesce() {
    build_and_check(&[(None, 0), (Some(1), 0), (None, 2)]);
}

/// Insert value in the middle of a null run splits it correctly.
#[test]
fn null_split() {
    let mut col = Column::<Option<u64>>::new();
    let mut mirror = vec![];
    for i in 0..3 {
        col.insert(i, None);
        mirror.insert(i, None);
    }
    assert_col(&col, &mirror);

    col.insert(1, Some(1));
    mirror.insert(1, Some(1));
    assert_col(&col, &mirror); // [None, Some(1), None, None]
}

/// Insert at position 0 with nulls following.
#[test]
fn adjacent_null_from_value_insert() {
    let mut col = Column::<Option<u64>>::new();
    let mut mirror = vec![];
    for (v, idx) in [(None, 0), (None, 0), (Some(1), 0), (None, 3)] {
        col.insert(idx, v);
        mirror.insert(idx, v);
    }
    assert_col(&col, &mirror); // [Some(1), None, None, None]
}

/// Adjacent literals not merged after null-1 deletion.
#[test]
fn adjacent_lits_after_null_delete() {
    let mut col = Column::<Option<u64>>::new();
    let mut mirror = vec![];
    let ops: &[(&str, Option<u64>, usize)] = &[
        ("ins", Some(3), 0),
        ("ins", Some(3), 0),
        ("ins", Some(2), 1),
        ("del", None, 2),
        ("ins", Some(2), 1),
        ("ins", Some(2), 2),
        ("ins", None, 2),
        ("ins", Some(1), 1),
        ("del", None, 3),
    ];
    for &(kind, v, idx) in ops {
        if kind == "del" {
            col.remove(idx);
            mirror.remove(idx);
        } else {
            col.insert(idx, v);
            mirror.insert(idx, v);
        }
    }
    assert_col(&col, &mirror); // [Some(3), Some(1), Some(2), Some(2), Some(2)]
}

// ── Splice ──────────────────────────────────────────────────────────────────

/// splice(0, 0, [1,2,3]) on empty column inserts values.
#[test]
fn splice_insert_only() {
    let mut col = Column::<u64>::new();
    col.splice(0, 0, [1, 2, 3]);
    assert_col(&col, &[1, 2, 3]);
}

/// splice(1, 2, []) removes 2 items.
#[test]
fn splice_delete_only() {
    let mut col = v1_build(&[10, 20, 30, 40]);
    col.splice(1, 2, std::iter::empty::<u64>());
    assert_col(&col, &[10, 40]);
}

/// splice(1, 2, [10, 20, 30]) replaces 2 items with 3.
#[test]
fn splice_replace() {
    let mut col = v1_build(&[1, 2, 3, 4, 5]);
    col.splice(1, 2, [10, 20, 30]);
    assert_col(&col, &[1, 10, 20, 30, 4, 5]);
}

/// splice(len, 0, [4, 5]) appends to end.
#[test]
fn splice_at_end() {
    let mut col = v1_build(&[1, 2, 3]);
    let len = col.len();
    col.splice(len, 0, [4, 5]);
    assert_col(&col, &[1, 2, 3, 4, 5]);
}

/// splice with nullable values.
#[test]
fn splice_nullable() {
    let mut col = Column::<Option<u64>>::new();
    col.splice(0, 0, [Some(1), None, Some(2)]);
    assert_col(&col, &[Some(1), None, Some(2)]);
    col.splice(1, 1, [Some(10), Some(20)]);
    assert_col(&col, &[Some(1), Some(10), Some(20), Some(2)]);
}

/// [lit-1: 1][repeat-2: 1] should be [repeat-3: 1].
#[test]
fn lit1_before_repeat_same_value() {
    let mut col = Column::<Option<u64>>::new();
    let mut mirror = vec![];
    let ops: &[(&str, Option<u64>, usize)] = &[
        ("ins", Some(1), 0),
        ("ins", Some(1), 1),
        ("ins", None, 0),
        ("ins", Some(3), 2),
        ("ins", Some(1), 2),
        ("ins", Some(2), 4),
        ("del", None, 4),
        ("ins", Some(1), 0),
        ("del", None, 1),
    ];
    for &(kind, v, idx) in ops {
        if kind == "del" {
            col.remove(idx);
            mirror.remove(idx);
        } else {
            col.insert(idx, v);
            mirror.insert(idx, v);
        }
    }
    assert_col(&col, &mirror); // [Some(1), Some(1), Some(1), Some(3), Some(1)]
}

/// Regression: split_at_item on repeat(2) could produce adjacent literal
/// runs when the resulting lit-1 was next to an existing literal.
#[test]
fn split_repeat2_adjacent_literal_regression() {
    let values: Vec<Option<u64>> = vec![
        None,
        None,
        Some(2),
        None,
        Some(2),
        None,
        Some(2),
        Some(1),
        Some(1),
        Some(1),
        Some(1),
        Some(3),
        Some(2),
        Some(2),
        Some(1),
        Some(2),
        Some(3),
        Some(2),
        Some(2),
        Some(2),
        Some(2),
        Some(1),
        None,
        None,
        Some(3),
        None,
    ];
    let start_pos = 0usize;

    let mut col = Column::<Option<u64>>::new();
    let mut mirror: Vec<Option<u64>> = vec![];

    for (i, v) in values.iter().enumerate() {
        let idx = std::cmp::min(start_pos + i, mirror.len());
        col.insert(idx, *v);
        mirror.insert(idx, *v);
    }
    assert_col(&col, &mirror);

    let del_pos = std::cmp::min(start_pos, mirror.len().saturating_sub(1));
    let del_count = std::cmp::min(values.len() / 2, mirror.len() - del_pos);
    for _ in 0..del_count {
        if mirror.is_empty() {
            break;
        }
        let idx = std::cmp::min(del_pos, mirror.len() - 1);
        col.remove(idx);
        mirror.remove(idx);
    }
    assert_col(&col, &mirror);
}

// ── Load tests ──────────────────────────────────────────────────────────

#[test]
fn load_bool_alternating_slab_boundary() {
    // Regression: alternating booleans exceeding max_segments caused
    // incorrect values after a slab cut in bool_encode_all_slabs.
    let values: Vec<bool> = vec![
        false, true, false, true, false, true, false, true, false, true, false, true, false, true,
        false, true, false, false,
    ];
    let original = Column::<bool>::from_values(values.clone());
    let bytes = original.save();
    let loaded = Column::<bool>::load(&bytes).unwrap();
    assert_eq!(loaded.len(), values.len());
    for (i, &expected) in values.iter().enumerate() {
        assert_eq!(loaded.get(i), Some(expected), "mismatch at index {i}");
        assert_eq!(
            original.get(i),
            Some(expected),
            "original mismatch at index {i}"
        );
    }
}

#[test]
fn load_empty() {
    let col = Column::<u64>::load(&[]).unwrap();
    assert_eq!(col.len(), 0);
    assert!(col.is_empty());
}

#[test]
fn load_roundtrip_u64() {
    let original = Column::<u64>::from_values(vec![10, 20, 20, 30, 40, 40, 40]);
    let bytes = original.save();
    let loaded = Column::<u64>::load(&bytes).unwrap();
    assert_eq!(loaded.len(), original.len());
    for i in 0..original.len() {
        assert_eq!(loaded.get(i), original.get(i));
    }
}

#[test]
fn load_roundtrip_i64() {
    let original = Column::<i64>::from_values(vec![-5, 0, 100, -1, -1]);
    let bytes = original.save();
    let loaded = Column::<i64>::load(&bytes).unwrap();
    assert_eq!(loaded.len(), original.len());
    for i in 0..original.len() {
        assert_eq!(loaded.get(i), original.get(i));
    }
}

#[test]
fn load_roundtrip_nullable() {
    let original = Column::<Option<u64>>::from_values(vec![Some(1), None, None, Some(3), Some(3)]);
    let bytes = original.save();
    let loaded = Column::<Option<u64>>::load(&bytes).unwrap();
    assert_eq!(loaded.len(), original.len());
    for i in 0..original.len() {
        assert_eq!(loaded.get(i), original.get(i));
    }
}

#[test]
fn load_roundtrip_string() {
    let original =
        Column::<String>::from_values(vec!["hello".into(), "world".into(), "foo".into()]);
    let bytes = original.save();
    let loaded = Column::<String>::load(&bytes).unwrap();
    assert_eq!(loaded.len(), original.len());
    for i in 0..original.len() {
        assert_eq!(loaded.get(i), original.get(i));
    }
}

#[test]
fn load_roundtrip_nullable_string() {
    let original = Column::<Option<String>>::from_values(vec![
        Some("hello".into()),
        None,
        Some("world".into()),
    ]);
    let bytes = original.save();
    let loaded = Column::<Option<String>>::load(&bytes).unwrap();
    assert_eq!(loaded.len(), original.len());
    for i in 0..original.len() {
        assert_eq!(loaded.get(i), original.get(i));
    }
}

#[test]
fn load_roundtrip_bytes() {
    let original = Column::<Vec<u8>>::from_values(vec![vec![1, 2, 3], vec![4, 5], vec![6]]);
    let bytes = original.save();
    let loaded = Column::<Vec<u8>>::load(&bytes).unwrap();
    assert_eq!(loaded.len(), original.len());
    for i in 0..original.len() {
        assert_eq!(loaded.get(i), original.get(i));
    }
}

#[test]
fn load_roundtrip_bool() {
    let original = Column::<bool>::from_values(vec![true, true, false, false, true, false]);
    let bytes = original.save();
    let loaded = Column::<bool>::load(&bytes).unwrap();
    assert_eq!(loaded.len(), original.len());
    for i in 0..original.len() {
        assert_eq!(loaded.get(i), original.get(i));
    }
}

#[test]
fn load_roundtrip_bool_all_true() {
    let original = Column::<bool>::from_values(vec![true; 100]);
    let bytes = original.save();
    let loaded = Column::<bool>::load(&bytes).unwrap();
    assert_eq!(loaded.len(), 100);
    for i in 0..100 {
        assert_eq!(loaded.get(i), Some(true));
    }
}

#[test]
fn load_roundtrip_bool_all_false() {
    let original = Column::<bool>::from_values(vec![false; 100]);
    let bytes = original.save();
    let loaded = Column::<bool>::load(&bytes).unwrap();
    assert_eq!(loaded.len(), 100);
    for i in 0..100 {
        assert_eq!(loaded.get(i), Some(false));
    }
}

#[test]
fn load_invalid_utf8_in_string_is_error() {
    // Build a valid bytes column with non-UTF8 data, then try to load as String.
    let col = Column::<Vec<u8>>::from_values(vec![vec![0xFF, 0xFE]]);
    let bytes = col.save();
    let result = Column::<String>::load(&bytes);
    assert!(
        result.is_err(),
        "loading non-UTF8 bytes as Column<String> should fail"
    );
    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert!(
        matches!(err, crate::PackError::InvalidUtf8),
        "expected InvalidUtf8, got: {err:?}"
    );
}

#[test]
fn load_invalid_utf8_in_nullable_string_is_error() {
    let col = Column::<Vec<u8>>::from_values(vec![vec![0xFF, 0xFE]]);
    let bytes = col.save();
    let result = Column::<Option<String>>::load(&bytes);
    assert!(
        result.is_err(),
        "loading non-UTF8 bytes as Column<Option<String>> should fail"
    );
    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert!(
        matches!(err, crate::PackError::InvalidUtf8),
        "expected InvalidUtf8, got: {err:?}"
    );
}

#[test]
fn load_null_in_non_nullable_u64_is_error() {
    let nullable = Column::<Option<u64>>::from_values(vec![Some(1), None, Some(3)]);
    let bytes = nullable.save();
    let result = Column::<u64>::load(&bytes);
    assert!(
        result.is_err(),
        "loading nulls into Column<u64> should fail"
    );
}

#[test]
fn load_null_in_non_nullable_i64_is_error() {
    let nullable = Column::<Option<i64>>::from_values(vec![Some(1), None, Some(3)]);
    let bytes = nullable.save();
    let result = Column::<i64>::load(&bytes);
    assert!(
        result.is_err(),
        "loading nulls into Column<i64> should fail"
    );
}

#[test]
fn load_null_in_non_nullable_string_is_error() {
    let nullable = Column::<Option<String>>::from_values(vec![Some("a".into()), None]);
    let bytes = nullable.save();
    let result = Column::<String>::load(&bytes);
    assert!(
        result.is_err(),
        "loading nulls into Column<String> should fail"
    );
}

#[test]
fn load_null_in_non_nullable_bytes_is_error() {
    let nullable = Column::<Option<Vec<u8>>>::from_values(vec![Some(vec![1]), None]);
    let bytes = nullable.save();
    let result = Column::<Vec<u8>>::load(&bytes);
    assert!(
        result.is_err(),
        "loading nulls into Column<Vec<u8>> should fail"
    );
}

#[test]
fn load_null_in_nullable_is_ok() {
    let nullable = Column::<Option<u64>>::from_values(vec![Some(1), None, Some(3)]);
    let bytes = nullable.save();
    let loaded = Column::<Option<u64>>::load(&bytes).unwrap();
    assert_eq!(loaded.len(), 3);
    assert_eq!(loaded.get(0), Some(Some(1)));
    assert_eq!(loaded.get(1), Some(None));
    assert_eq!(loaded.get(2), Some(Some(3)));
}

#[test]
fn load_truncated_data_is_error() {
    let col = Column::<u64>::from_values(vec![1, 2, 3, 4, 5]);
    let bytes = col.save();
    // Truncate to 1 byte — guaranteed to be mid-value.
    let result = Column::<u64>::load(&bytes[..1]);
    // Should either error or load fewer items — not panic.
    if let Ok(loaded) = &result {
        assert!(loaded.len() < col.len());
    }
}

#[test]
fn load_garbage_data_is_error() {
    let garbage = vec![
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    ];
    let result = Column::<u64>::load(&garbage);
    assert!(result.is_err());
}

#[test]
fn load_roundtrip_after_mutations() {
    let mut col = Column::<u64>::from_values(vec![10, 20, 30, 40, 50]);
    col.insert(2, 25);
    col.remove(4);
    col.splice(0, 1, [99, 100]);
    let bytes = col.save();
    let loaded = Column::<u64>::load(&bytes).unwrap();
    assert_eq!(loaded.len(), col.len());
    for i in 0..col.len() {
        assert_eq!(loaded.get(i), col.get(i));
    }
}

#[test]
fn load_roundtrip_large() {
    let values: Vec<u64> = (0..10_000).collect();
    let original = Column::<u64>::from_values(values);
    let bytes = original.save();
    let loaded = Column::<u64>::load(&bytes).unwrap();
    assert_eq!(loaded.len(), original.len());
    for i in (0..10_000).step_by(100) {
        assert_eq!(loaded.get(i), original.get(i));
    }
}

proptest! {
    #[test]
    fn load_roundtrip_proptest_u64(values in prop::collection::vec(0u64..1000, 0..500)) {
        let original = Column::<u64>::from_values(values.clone());
        let bytes = original.save();
        let loaded = Column::<u64>::load(&bytes).unwrap();
        prop_assert_eq!(loaded.len(), values.len());
        for (i, v) in values.iter().enumerate() {
            prop_assert_eq!(loaded.get(i), Some(*v));
        }
    }

    #[test]
    fn load_roundtrip_proptest_bool(values in prop::collection::vec(proptest::bool::ANY, 0..500)) {
        let original = Column::<bool>::from_values(values.clone());
        let bytes = original.save();
        let loaded = Column::<bool>::load(&bytes).unwrap();
        prop_assert_eq!(loaded.len(), values.len());
        for (i, v) in values.iter().enumerate() {
            prop_assert_eq!(loaded.get(i), Some(*v));
        }
    }

    #[test]
    fn load_roundtrip_proptest_nullable(
        values in prop::collection::vec(
            prop::option::of(0u64..1000),
            0..500,
        )
    ) {
        let original = Column::<Option<u64>>::from_values(values.clone());
        let bytes = original.save();
        let loaded = Column::<Option<u64>>::load(&bytes).unwrap();
        prop_assert_eq!(loaded.len(), values.len());
        for (i, v) in values.iter().enumerate() {
            prop_assert_eq!(loaded.get(i), Some(*v));
        }
    }
}

// ── Iterator tests ──────────────────────────────────────────────────────────

#[test]
fn iter_empty() {
    let col = Column::<u64>::new();
    assert_eq!(col.iter().count(), 0);
    assert_eq!(col.iter_range(0..0).count(), 0);
    assert_eq!(col.iter_range(0..10).count(), 0);
}

#[test]
fn iter_u64() {
    let vals: Vec<u64> = vec![10, 20, 20, 30, 40, 40, 40, 50];
    let col = Column::<u64>::from_values(vals.clone());
    let got: Vec<u64> = col.iter().collect();
    assert_eq!(got, vals);
}

#[test]
fn iter_bool() {
    let vals = vec![true, true, false, true, false, false, true];
    let col = Column::<bool>::from_values(vals.clone());
    let got: Vec<bool> = col.iter().collect();
    assert_eq!(got, vals);
}

#[test]
fn iter_string() {
    let vals = vec![
        "hello".to_string(),
        "world".to_string(),
        "hello".to_string(),
    ];
    let col = Column::<String>::from_values(vals.clone());
    let got: Vec<&str> = col.iter().collect();
    assert_eq!(got, vec!["hello", "world", "hello"]);
}

#[test]
fn iter_nullable() {
    let vals: Vec<Option<u64>> = vec![Some(1), None, None, Some(2), Some(2), None];
    let col = Column::<Option<u64>>::from_values(vals.clone());
    let got: Vec<Option<u64>> = col.iter().collect();
    assert_eq!(got, vals);
}

#[test]
fn iter_range_basic() {
    let vals: Vec<u64> = (0..100).collect();
    let col = Column::<u64>::from_values(vals.clone());
    let got: Vec<u64> = col.iter_range(10..20).collect();
    assert_eq!(got, (10..20).collect::<Vec<u64>>());
}

#[test]
fn iter_range_full() {
    let vals: Vec<u64> = (0..50).collect();
    let col = Column::<u64>::from_values(vals.clone());
    let got: Vec<u64> = col.iter_range(0..50).collect();
    assert_eq!(got, vals);
}

#[test]
fn iter_range_clamped() {
    let vals: Vec<u64> = (0..10).collect();
    let col = Column::<u64>::from_values(vals.clone());
    let got: Vec<u64> = col.iter_range(5..100).collect();
    assert_eq!(got, (5..10).collect::<Vec<u64>>());
}

#[test]
fn iter_range_empty() {
    let vals: Vec<u64> = (0..10).collect();
    let col = Column::<u64>::from_values(vals);
    assert_eq!(col.iter_range(5..5).count(), 0);
    #[allow(clippy::reversed_empty_ranges)]
    {
        assert_eq!(col.iter_range(7..3).count(), 0);
    }
}

#[test]
fn iter_range_bool() {
    let vals: Vec<bool> = (0..100).map(|i| i % 3 == 0).collect();
    let col = Column::<bool>::from_values(vals.clone());
    let got: Vec<bool> = col.iter_range(20..40).collect();
    assert_eq!(got, vals[20..40]);
}

#[test]
fn iter_exact_size() {
    let col = Column::<u64>::from_values((0..50).collect());
    let iter = col.iter();
    assert_eq!(iter.len(), 50);
    let iter = col.iter_range(10..30);
    assert_eq!(iter.len(), 20);
}

#[test]
fn iter_multi_slab() {
    // Use small max_segments to force multiple slabs
    let vals: Vec<u64> = (0..200).map(|i| i * 7 % 100).collect();
    let col = Column::<u64>::from_values_with_max_segments(vals.clone(), 4);
    assert!(col.slab_count() > 1, "should have multiple slabs");
    let got: Vec<u64> = col.iter().collect();
    assert_eq!(got, vals);
}

#[test]
fn iter_range_multi_slab() {
    let vals: Vec<u64> = (0..200).map(|i| i * 7 % 100).collect();
    let col = Column::<u64>::from_values_with_max_segments(vals.clone(), 4);
    assert!(col.slab_count() > 1);
    let got: Vec<u64> = col.iter_range(50..150).collect();
    assert_eq!(got, vals[50..150]);
}

// ── nth() tests ─────────────────────────────────────────────────────────────

#[test]
fn nth_within_repeat_run() {
    // 10k zeros = one repeat run — nth should skip in O(1)
    let vals: Vec<u64> = vec![42; 10_000];
    let col = Column::<u64>::from_values(vals);
    assert_eq!(col.iter().nth(5_000), Some(42));
    assert_eq!(col.iter().nth(9_999), Some(42));
    assert_eq!(col.iter().nth(10_000), None);
}

#[test]
fn nth_across_runs() {
    // Alternating runs: 100 zeros, 100 ones, 100 twos, ...
    let vals: Vec<u64> = (0..10).flat_map(|v| vec![v; 100]).collect();
    let col = Column::<u64>::from_values(vals.clone());
    assert_eq!(col.iter().next(), Some(0));
    // nth(99) = last zero
    assert_eq!(col.iter().nth(99), Some(0));
    // nth(100) = first one
    assert_eq!(col.iter().nth(100), Some(1));
    // nth(550) = middle of run 5
    assert_eq!(col.iter().nth(550), Some(5));
    // nth(999) = last item
    assert_eq!(col.iter().nth(999), Some(9));
}

#[test]
fn nth_across_slabs() {
    // Force many slabs with small max_segments, use alternating runs
    let vals: Vec<u64> = (0..200).flat_map(|v| vec![v; 50]).collect();
    let col = Column::<u64>::from_values_with_max_segments(vals.clone(), 4);
    assert!(col.slab_count() > 5, "should have many slabs");
    for &n in &[0, 100, 1000, 5000, 9999] {
        assert_eq!(col.iter().nth(n), Some(vals[n]), "mismatch at nth({n})");
    }
}

#[test]
fn nth_from_iter_range() {
    let vals: Vec<u64> = (0..100).flat_map(|v| vec![v; 100]).collect();
    let col = Column::<u64>::from_values(vals.clone());
    // iter_range(2000..).nth(500) = vals[2500]
    assert_eq!(col.iter_range(2000..col.len()).nth(500), Some(vals[2500]));
    // iter_range(2000..).nth(7999) = vals[9999]
    assert_eq!(col.iter_range(2000..col.len()).nth(7999), Some(vals[9999]));
    // iter_range(2000..).nth(8000) = None (out of range)
    assert_eq!(col.iter_range(2000..col.len()).nth(8000), None);
}

#[test]
fn nth_bool() {
    let vals: Vec<bool> = (0..1000).map(|i| i % 100 < 50).collect();
    let col = Column::<bool>::from_values(vals.clone());
    assert_eq!(col.iter().next(), Some(vals[0]));
    assert_eq!(col.iter().nth(49), Some(vals[49]));
    assert_eq!(col.iter().nth(50), Some(vals[50]));
    assert_eq!(col.iter().nth(500), Some(vals[500]));
    assert_eq!(col.iter().nth(999), Some(vals[999]));
}

#[test]
fn nth_nullable() {
    let vals: Vec<Option<u64>> = (0..100)
        .map(|i| if i % 3 == 0 { None } else { Some(i) })
        .collect();
    let col = Column::<Option<u64>>::from_values(vals.clone());
    for &n in &[0, 1, 2, 3, 50, 99] {
        assert_eq!(col.iter().nth(n), Some(vals[n]), "mismatch at nth({n})");
    }
}

#[test]
fn nth_sequential_calls() {
    // nth() should compose: iter.nth(5) then iter.nth(3) = items[5], items[9]
    let vals: Vec<u64> = (0..100).collect();
    let col = Column::<u64>::from_values(vals);
    let mut iter = col.iter();
    assert_eq!(iter.nth(5), Some(5)); // consumes 0..=5, next is 6
    assert_eq!(iter.nth(3), Some(9)); // skips 6,7,8, returns 9
    assert_eq!(iter.next(), Some(10)); // returns 10
    assert_eq!(iter.len(), 89); // 100 - 11 consumed
}

// ── PrefixIter ↔ v0 cross-validation ────────────────────────────────────────

/// Build both v0 and v1 columns from the same u64 data.
fn build_both(values: &[u64]) -> (crate::ColumnData<crate::UIntCursor>, PrefixColumn<u64>) {
    let v0: crate::ColumnData<crate::UIntCursor> = values.iter().copied().collect();
    let v1 = PrefixColumn::<u64>::from_values(values.to_vec());
    (v0, v1)
}

#[test]
fn prefix_iter_range_matches_inner_iter_range() {
    let values: Vec<u64> = (1..=20).collect();
    let col = PrefixColumn::<u64>::from_values(values);

    for start in 0..20 {
        for end in start..=20 {
            let inner_vals: Vec<u64> = col.values().iter_range(start..end).collect();
            let prefix_vals: Vec<u64> = col.iter_range(start..end).map(|(_, v)| v).collect();
            assert_eq!(
                inner_vals, prefix_vals,
                "iter_range({start}..{end}) mismatch"
            );
        }
    }
}

#[test]
fn prefix_iter_values_match_v0() {
    let values: Vec<u64> = vec![1, 0, 3, 3, 0, 5, 2, 2, 2, 7];
    let (v0, v1) = build_both(&values);

    // v0 iter values
    let v0_vals: Vec<u64> = v0
        .iter()
        .map(|v| v.unwrap_or_default().into_owned())
        .collect();
    // v1 prefix iter values (drop prefix)
    let v1_vals: Vec<u64> = v1.iter().map(|(_, v)| v).collect();
    assert_eq!(v0_vals, v1_vals);
}

#[test]
fn prefix_iter_acc_matches_v0_with_acc() {
    let values: Vec<u64> = vec![1, 0, 3, 3, 0, 5, 2, 2, 2, 7];
    let (v0, v1) = build_both(&values);

    // v0 ColGroupIter yields acc BEFORE the item
    let v0_items: Vec<_> = v0.iter().with_acc().collect();
    // v1 PrefixIter yields total (inclusive sum THROUGH the item)
    let v1_items: Vec<_> = v1.iter().collect();

    assert_eq!(v0_items.len(), v1_items.len());
    for (i, (v0i, &(v1_total, v1_val))) in v0_items.iter().zip(v1_items.iter()).enumerate() {
        let v0_val = v0i.item.as_ref().map(|c| *c.as_ref()).unwrap_or(0);
        assert_eq!(v0_val, v1_val, "value mismatch at {i}");
        // v0 acc_before + val == v1 prefix_through
        let v0_acc_through = v0i.acc.as_u64() as u128 + v0_val as u128;
        assert_eq!(v0_acc_through, v1_total, "total mismatch at {i}");
    }
}

#[test]
fn prefix_iter_range_acc_matches_v0() {
    let values: Vec<u64> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let (v0, v1) = build_both(&values);

    for start in 0..10 {
        for end in start..=10 {
            let v0_items: Vec<_> = v0.iter_range(start..end).with_acc().collect();
            let v1_items: Vec<_> = v1.iter_range(start..end).collect();
            assert_eq!(
                v0_items.len(),
                v1_items.len(),
                "len mismatch for {start}..{end}"
            );

            for (i, (v0i, &(v1_total, v1_val))) in v0_items.iter().zip(v1_items.iter()).enumerate()
            {
                let v0_val = v0i.item.as_ref().map(|c| *c.as_ref()).unwrap_or(0);
                assert_eq!(
                    v0_val, v1_val,
                    "value mismatch at {i} for range {start}..{end}"
                );
                let v0_acc_through = v0i.acc.as_u64() as u128 + v0_val as u128;
                assert_eq!(
                    v0_acc_through, v1_total,
                    "total mismatch at {i} for range {start}..{end}"
                );
            }
        }
    }
}

#[test]
fn prefix_iter_nth_matches_v0() {
    let values: Vec<u64> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let (v0, v1) = build_both(&values);

    // v0 with_acc collects all items; we use nth on v1 to verify
    let v0_all: Vec<_> = v0.iter().with_acc().collect();

    for (skip, v0_item) in v0_all.iter().take(10).enumerate() {
        let v1_result = v1.iter().nth(skip);
        let (v1_total, v1_val) = v1_result.unwrap();
        let v0_val = v0_item.item.as_ref().map(|c| *c.as_ref()).unwrap_or(0);
        assert_eq!(v0_val, v1_val, "nth({skip}) value mismatch");
        let v0_acc_through = v0_item.acc.as_u64() as u128 + v0_val as u128;
        assert_eq!(v0_acc_through, v1_total, "nth({skip}) prefix mismatch");
    }
}

/// Test next_run across a slab boundary where the next slab starts with a
/// different value.  The cross-slab merge in Iter::next_run peeks the first
/// run of the next slab.  If it doesn't match, the decoder must be reset so
/// subsequent next()/next_run() calls read the correct data.
#[test]
fn next_run_cross_slab_mismatch_resets_decoder() {
    // Build a multi-slab column: slab 1 ends with 5s, slab 2 starts with 7s.
    // Use small max_segments to force multiple slabs.
    let mut vals: Vec<u64> = vec![5; 10];
    vals.extend(vec![7; 10]);
    vals.extend(vec![5; 10]);
    let col = Column::<u64>::from_values_with_max_segments(vals.clone(), 2);
    assert!(col.slab_count() > 1, "need multiple slabs for this test");

    // Iterate using next_run and collect values, then compare with next().
    let run_vals: Vec<u64> = {
        let mut iter = col.iter();
        let mut result = Vec::new();
        while let Some(run) = iter.next_run() {
            for _ in 0..run.count {
                result.push(run.value);
            }
        }
        result
    };
    assert_eq!(
        run_vals, vals,
        "next_run should produce same values as direct iteration"
    );

    // Also verify by mixing next() and next_run() calls.
    let mut iter = col.iter();
    // Consume first few with next()
    for (i, val) in vals[..8].iter().enumerate() {
        assert_eq!(iter.next(), Some(*val), "next() at {}", i);
    }
    // Now next_run() should get the rest of the 5s run (2 items),
    // NOT merge into the 7s from the next slab.
    let run = iter.next_run().unwrap();
    assert_eq!(run.value, 5);
    assert_eq!(
        run.count, 2,
        "should get remaining 2 fives, not merge across slab"
    );

    // The next call should yield 7s from the second slab.
    let next_val = iter.next().unwrap();
    assert_eq!(next_val, 7, "after consuming 5s run, next item should be 7");

    // Verify remaining iteration is correct.
    // We consumed 8 via next(), 2 via next_run(), 1 via next(). Items 0..11 gone.
    let remaining: Vec<u64> = iter.collect();
    assert_eq!(
        remaining,
        vals[11..],
        "remaining values after mixed iteration"
    );
}

// ── PrefixIter::nth branch coverage ──────────────────────────────────────────

/// Helper: build a multi-slab PrefixColumn<u64> and collect all (prefix, value)
/// via next() as the ground truth.
fn prefix_ground_truth(vals: &[u64], max_seg: usize) -> (PrefixColumn<u64>, Vec<(u128, u64)>) {
    let mut col = PrefixColumn::<u64>::with_max_segments(max_seg);
    for &v in vals {
        col.push(v);
    }
    let all: Vec<_> = col.iter().collect();
    assert_eq!(all.len(), vals.len());
    (col, all)
}

#[allow(clippy::iter_nth_zero)]
/// nth: branch 1 — n >= items_left → returns None, exhausts iterator.
#[test]
fn prefix_nth_past_end() {
    let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3]);
    let mut iter = col.iter();
    assert!(iter.nth(3).is_none(), "nth(3) on 3-item column");
    assert_eq!(iter.items_left(), 0);
    assert!(iter.next().is_none(), "exhausted after nth past end");

    // Also: nth(0) on empty range
    let mut iter = col.iter_range(5..5);
    assert!(iter.nth(0).is_none());
}

#[allow(clippy::iter_nth_zero)]
/// nth: branch 2 — n < slab_remaining → fast path within current slab.
#[test]
fn prefix_nth_fast_path_within_slab() {
    let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3, 4, 5]);
    let all: Vec<_> = col.iter().collect();

    // nth(0) == next()
    let mut iter = col.iter();
    assert_eq!(iter.nth(0).unwrap(), all[0]);
    assert_eq!(iter.next().unwrap(), all[1]);

    // nth(2) skips 2 items
    let mut iter = col.iter();
    assert_eq!(iter.nth(2).unwrap(), all[2]);
    assert_eq!(iter.next().unwrap(), all[3]);

    // nth to last item
    let mut iter = col.iter();
    assert_eq!(iter.nth(4).unwrap(), all[4]);
    assert!(iter.next().is_none());
}

/// nth: branch 2 — fast path after partial consumption (next then nth).
#[test]
fn prefix_nth_fast_path_after_next() {
    let col = PrefixColumn::<u64>::from_values(vec![10, 20, 30, 40, 50]);
    let all: Vec<_> = col.iter().collect();

    let mut iter = col.iter();
    assert_eq!(iter.next().unwrap(), all[0]);
    assert_eq!(iter.nth(1).unwrap(), all[2]); // skip 1, land on [2]
    assert_eq!(iter.next().unwrap(), all[3]);
}

/// nth: branch 3 — n >= slab_remaining → BIT traversal across slabs.
#[test]
fn prefix_nth_bit_traversal() {
    let vals: Vec<u64> = (1..=30).collect();
    let (col, all) = prefix_ground_truth(&vals, 2);
    assert!(col.values().slab_count() > 1, "need multi-slab");

    // nth that crosses slab boundary
    let mut iter = col.iter();
    let mid = all.len() / 2;
    assert_eq!(iter.nth(mid).unwrap(), all[mid]);
    assert_eq!(iter.next().unwrap(), all[mid + 1]);

    // nth to last item via BIT
    let mut iter = col.iter();
    let last = all.len() - 1;
    assert_eq!(iter.nth(last).unwrap(), all[last]);
    assert!(iter.next().is_none());
}

/// nth: branch 3a — BIT traversal finds si >= slabs.len() → None.
#[test]
fn prefix_nth_bit_out_of_bounds() {
    let vals: Vec<u64> = (1..=20).collect();
    let (col, _all) = prefix_ground_truth(&vals, 2);

    // Restrict range then try nth past it
    let mut iter = col.iter_range(0..10);
    assert!(iter.nth(10).is_none());
    assert_eq!(iter.items_left(), 0);
}

/// nth: verify prefix values match next()-based ground truth for every position.
#[test]
fn prefix_nth_exhaustive_vs_next() {
    let vals: Vec<u64> = vec![3, 0, 1, 0, 2, 0, 1, 5, 0, 0, 4, 0, 1, 2, 3];
    let (col, all) = prefix_ground_truth(&vals, 3);

    for (i, expected) in all.iter().enumerate() {
        let result = col.iter().nth(i);
        assert_eq!(result.unwrap(), *expected, "nth({}) mismatch", i);
    }
    assert!(col.iter().nth(vals.len()).is_none());
}

// ── PrefixIter::shift_next branch coverage ───────────────────────────────────

/// shift_next: branch 1 — empty range → None.
#[test]
fn prefix_shift_next_empty_range() {
    let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3]);
    let mut iter = col.iter();
    assert!(iter.shift_next(2..2).is_none());
    // iterator now spent
    assert!(iter.next().is_none());
}

/// shift_next: branch 2 — normal case, from start.
#[test]
fn prefix_shift_next_from_start() {
    let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3, 4, 5]);
    let all: Vec<_> = col.iter().collect();

    let mut iter = col.iter();
    let first = iter.shift_next(0..5).unwrap();
    assert_eq!(first, all[0]);
    assert_eq!(iter.items_left(), 4);
}

/// shift_next: branch 2 — skip forward then shift_next.
#[test]
fn prefix_shift_next_skip_forward() {
    let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3, 4, 5]);
    let all: Vec<_> = col.iter().collect();

    let mut iter = col.iter();
    let _ = iter.next(); // pos=1
    let shifted = iter.shift_next(3..5).unwrap();
    assert_eq!(shifted, all[3], "shift_next(3..5) after consuming 1");
    assert_eq!(iter.items_left(), 1);
    assert_eq!(iter.next().unwrap(), all[4]);
    assert!(iter.next().is_none());
}

/// shift_next: consecutive shift_next calls.
#[test]
fn prefix_shift_next_consecutive() {
    let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3, 4, 5, 6, 7, 8]);
    let all: Vec<_> = col.iter().collect();

    let mut iter = col.iter();
    assert_eq!(iter.shift_next(0..8).unwrap(), all[0]);
    assert_eq!(iter.shift_next(3..8).unwrap(), all[3]);
    assert_eq!(iter.shift_next(6..8).unwrap(), all[6]);
    assert_eq!(iter.next().unwrap(), all[7]);
    assert!(iter.next().is_none());
}

/// shift_next: branch 2a — range is valid but column is empty at that range.
#[test]
fn prefix_shift_next_beyond_data() {
    let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3]);
    let mut iter = col.iter();
    // shift_next to a range that starts at the end of the column
    let result = iter.shift_next(3..5);
    assert!(result.is_none(), "no data at position 3");
}

#[allow(clippy::iter_nth_zero)]
/// shift_next then nth — the original automerge bug scenario.
#[test]
fn prefix_shift_next_then_nth() {
    let col = PrefixColumn::<bool>::from_values(vec![true, false, true, false, true, false]);
    let all: Vec<_> = col.iter().collect();

    // shift_next then nth(0)
    let mut iter = col.iter();
    assert_eq!(iter.shift_next(0..6).unwrap(), all[0]);
    assert_eq!(iter.nth(0).unwrap(), all[1]);

    // shift_next to middle, then nth(0)
    let mut iter = col.iter();
    let _ = iter.next();
    assert_eq!(iter.shift_next(2..6).unwrap(), all[2]);
    assert_eq!(iter.nth(0).unwrap(), all[3]);

    // shift_next then nth(1) — skip one
    let mut iter = col.iter();
    assert_eq!(iter.shift_next(0..6).unwrap(), all[0]);
    assert_eq!(iter.nth(1).unwrap(), all[2]);
}

/// shift_next on multi-slab column — exercises BIT-based prefix recomputation.
#[test]
fn prefix_shift_next_multi_slab() {
    let vals: Vec<u64> = (1..=30).collect();
    let (col, all) = prefix_ground_truth(&vals, 2);
    assert!(col.values().slab_count() > 1);

    let mut iter = col.iter();
    // Jump to middle of a later slab
    let mid = 15;
    let shifted = iter.shift_next(mid..30).unwrap();
    assert_eq!(shifted, all[mid], "shift_next to mid of multi-slab");

    // Verify remaining iteration
    let remaining: Vec<_> = iter.collect();
    let expected: Vec<_> = all[mid + 1..].to_vec();
    assert_eq!(remaining, expected);
}

proptest! {
    #[test]
    fn iter_proptest_u64(values in prop::collection::vec(0..1000u64, 0..500)) {
        let col = Column::<u64>::from_values(values.clone());
        let got: Vec<u64> = col.iter().collect();
        prop_assert_eq!(&got, &values);
    }

    #[test]
    fn iter_range_proptest_u64(
        values in prop::collection::vec(0..1000u64, 10..500),
        start in 0..10usize,
        len in 0..10usize,
    ) {
        let n = values.len();
        let s = start.min(n);
        let e = (s + len).min(n);
        let col = Column::<u64>::from_values(values.clone());
        let got: Vec<u64> = col.iter_range(s..e).collect();
        prop_assert_eq!(&got, &values[s..e]);
    }

    #[test]
    fn nth_proptest(
        values in prop::collection::vec(0..100u64, 1..500),
        skip in 0..500usize,
    ) {
        let col = Column::<u64>::from_values(values.clone());
        let expected = values.get(skip).copied();
        prop_assert_eq!(col.iter().nth(skip), expected);
    }

    /// Cross-validate v0 and v1 prefix/acc iteration on random data.
    #[test]
    fn prefix_iter_proptest(values in prop::collection::vec(0..100u64, 1..200)) {
        let v0: crate::ColumnData<crate::UIntCursor> = values.iter().copied().collect();
        let v1 = PrefixColumn::<u64>::from_values(values.clone());

        // 1. iter_range values match inner
        let inner_vals: Vec<u64> = v1.values().iter().collect();
        let prefix_vals: Vec<u64> = v1.iter().map(|(_, v)| v).collect();
        prop_assert_eq!(&inner_vals, &prefix_vals);
        prop_assert_eq!(&inner_vals, &values);

        // 2. Prefix/acc consistency
        let v0_items: Vec<_> = v0.iter().with_acc().collect();
        let v1_items: Vec<_> = v1.iter().collect();
        prop_assert_eq!(v0_items.len(), v1_items.len());
        for (i, (v0i, &(v1_total, v1_val))) in v0_items.iter().zip(v1_items.iter()).enumerate() {
            let v0_val = v0i.item.as_ref().map(|c| *c.as_ref()).unwrap_or(0);
            prop_assert_eq!(v0_val, v1_val, "value mismatch at {}", i);
            let v0_acc_through = v0i.acc.as_u64() as u128 + v0_val as u128;
            prop_assert_eq!(v0_acc_through, v1_total, "total mismatch at {}", i);
        }

        // 3. iter_range consistency for random subranges
        let n = values.len();
        for s in [0, n / 4, n / 2, 3 * n / 4] {
            let e = (s + n / 4).min(n);
            let v0_sub: Vec<_> = v0.iter_range(s..e).with_acc().collect();
            let v1_sub: Vec<_> = v1.iter_range(s..e).collect();
            prop_assert_eq!(v0_sub.len(), v1_sub.len(), "range len mismatch for {}..{}", s, e);
            for (j, (v0j, &(v1p, v1v))) in v0_sub.iter().zip(v1_sub.iter()).enumerate() {
                let v0v = v0j.item.as_ref().map(|c| *c.as_ref()).unwrap_or(0);
                prop_assert_eq!(v0v, v1v, "range value at {}", j);
                let v0at = v0j.acc.as_u64() as u128 + v0v as u128;
                prop_assert_eq!(v0at, v1p, "range prefix at {}", j);
            }
        }

        // 4. nth consistency
        for skip in [0, 1, n / 2, n.saturating_sub(1)] {
            if skip < n {
                let v1_nth = v1.iter().nth(skip);
                let (v1p, v1v) = v1_nth.unwrap();
                let v0i = &v0_items[skip];
                let v0v = v0i.item.as_ref().map(|c| *c.as_ref()).unwrap_or(0);
                prop_assert_eq!(v0v, v1v, "nth({}) value", skip);
                let v0at = v0i.acc.as_u64() as u128 + v0v as u128;
                prop_assert_eq!(v0at, v1p, "nth({}) prefix", skip);
            }
        }
    }

}

// ── LoadOpts tests ─────────────────────────────────────────────────────────

#[test]
fn load_with_empty_data_and_fill_gives_filled_bool() {
    let col =
        Column::<bool>::load_with(&[], LoadOpts::new().with_length(5).with_fill(false)).unwrap();
    assert_eq!(col.len(), 5);
    assert!(col.iter().all(|v| !v));
}

#[test]
fn load_with_empty_data_and_fill_gives_filled_nullable() {
    let col = Column::<Option<u64>>::load_with(&[], LoadOpts::new().with_length(3).with_fill(None))
        .unwrap();
    assert_eq!(col.len(), 3);
    assert!(col.iter().all(|v| v.is_none()));
}

#[test]
fn load_with_empty_data_length_without_fill_errors() {
    let err = Column::<bool>::load_with(&[], LoadOpts::new().with_length(5).into());
    assert!(matches!(err, Err(crate::PackError::InvalidLength(0, 5))));
}

#[test]
fn load_with_empty_data_and_zero_length() {
    let col = Column::<bool>::load_with(&[], LoadOpts::new().with_length(0).into()).unwrap();
    assert_eq!(col.len(), 0);
}

#[test]
fn load_with_length_mismatch_errors() {
    let col = Column::<Option<u64>>::from_values(vec![Some(1), Some(2)]);
    let data = col.save();
    let err = Column::<Option<u64>>::load_with(&data, LoadOpts::new().with_length(5).into());
    assert!(matches!(err, Err(crate::PackError::InvalidLength(2, 5))));
}

#[test]
fn load_with_length_match_succeeds() {
    let col = Column::<Option<u64>>::from_values(vec![Some(1), None, Some(3)]);
    let data = col.save();
    let loaded =
        Column::<Option<u64>>::load_with(&data, LoadOpts::new().with_length(3).into()).unwrap();
    assert_eq!(loaded.len(), 3);
    assert_eq!(loaded.get(0), Some(Some(1)));
    assert_eq!(loaded.get(1), Some(None));
    assert_eq!(loaded.get(2), Some(Some(3)));
}

#[test]
fn load_with_validation_rejects_bad_values() {
    let col = Column::<Option<u64>>::from_values(vec![Some(1), Some(999), None]);
    let data = col.save();
    fn reject_large(v: Option<u64>) -> Option<String> {
        match v {
            Some(n) if n > 100 => Some(format!("too large: {}", n)),
            _ => None,
        }
    }
    let err =
        Column::<Option<u64>>::load_with(&data, LoadOpts::new().with_validation(reject_large));
    assert!(matches!(err, Err(crate::PackError::InvalidValue(_))));
}

#[test]
fn load_with_no_validation_accepts_all() {
    let col = Column::<Option<u64>>::from_values(vec![Some(1), Some(2), None]);
    let data = col.save();
    let loaded = Column::<Option<u64>>::load(&data).unwrap();
    assert_eq!(loaded.len(), 3);
}

#[test]
fn load_with_length() {
    let col = Column::<Option<u64>>::from_values(vec![Some(1), Some(2), Some(3)]);
    let data = col.save();
    let loaded =
        Column::<Option<u64>>::load_with(&data, LoadOpts::new().with_length(3).into()).unwrap();
    assert_eq!(loaded.len(), 3);
}

#[test]
fn load_with_opts_is_copy() {
    let opts = LoadOpts::new().with_length(5).with_fill::<bool>(false);
    let opts2 = opts; // copy
    let _ = opts; // still usable
    let _ = opts2;
}

#[test]
fn prefix_column_load_with_empty_and_fill() {
    let col = PrefixColumn::<bool>::load_with(&[], LoadOpts::new().with_length(4).with_fill(false))
        .unwrap();
    assert_eq!(col.len(), 4);
    assert!(col.values().iter().all(|v| !v));
}

#[test]
fn prefix_column_load_with_roundtrip() {
    let col = PrefixColumn::<bool>::from_values(vec![true, false, true]);
    let data = col.save();
    let loaded =
        PrefixColumn::<bool>::load_with(&data, LoadOpts::new().with_length(3).into()).unwrap();
    assert_eq!(loaded.save(), data);
}

#[test]
fn prefix_column_load_with_length_mismatch() {
    let col = PrefixColumn::<bool>::from_values(vec![true, false]);
    let data = col.save();
    let err = PrefixColumn::<bool>::load_with(&data, LoadOpts::new().with_length(10).into());
    assert!(err.is_err());
}

#[test]
fn delta_column_load_with_roundtrip() {
    let col = DeltaColumn::<Option<u64>>::from_values(vec![Some(10), None, Some(30)]);
    let data = col.save();
    let loaded = DeltaColumn::<Option<u64>>::load_with(&data, LoadOpts::new().into()).unwrap();
    assert_eq!(loaded.len(), 3);
    assert_eq!(loaded.get(0), Some(Some(10)));
    assert_eq!(loaded.get(1), Some(None));
    assert_eq!(loaded.get(2), Some(Some(30)));
}

#[test]
fn delta_column_load_with_nullable_empty_fill() {
    let col =
        DeltaColumn::<Option<u64>>::load_with(&[], LoadOpts::new().with_length(5).with_fill(None))
            .unwrap();
    assert_eq!(col.len(), 5);
    for i in 0..5 {
        assert_eq!(col.get(i), Some(None));
    }
}

// ── Cross-slab splice tests ────────────────────────────────────────────────

/// Build a multi-slab column with small max_segments.
fn multi_slab_col(vals: &[u64], max_seg: usize) -> Column<u64> {
    Column::<u64>::from_values_with_max_segments(vals.to_vec(), max_seg)
}

#[test]
fn cross_slab_delete_middle() {
    // 10 literal values with max_segments=4 → multiple slabs.
    let vals: Vec<u64> = (0..10).collect();
    let mut col = multi_slab_col(&vals, 4);
    assert!(col.slab_count() > 1, "need multiple slabs");
    // Delete across a slab boundary.
    col.splice(3, 4, std::iter::empty::<u64>());
    let expected: Vec<u64> = vec![0, 1, 2, 7, 8, 9];
    assert_col(&col, &expected);
}

#[test]
fn cross_slab_delete_to_end() {
    let vals: Vec<u64> = (0..10).collect();
    let mut col = multi_slab_col(&vals, 4);
    col.splice(6, 4, std::iter::empty::<u64>());
    let expected: Vec<u64> = (0..6).collect();
    assert_col(&col, &expected);
}

#[test]
fn cross_slab_delete_from_start() {
    let vals: Vec<u64> = (0..10).collect();
    let mut col = multi_slab_col(&vals, 4);
    col.splice(0, 5, std::iter::empty::<u64>());
    let expected: Vec<u64> = (5..10).collect();
    assert_col(&col, &expected);
}

#[test]
fn cross_slab_replace() {
    let vals: Vec<u64> = (0..10).collect();
    let mut col = multi_slab_col(&vals, 4);
    col.splice(3, 4, [99, 88]);
    let expected: Vec<u64> = vec![0, 1, 2, 99, 88, 7, 8, 9];
    assert_col(&col, &expected);
}

#[test]
fn cross_slab_delete_all() {
    let vals: Vec<u64> = (0..10).collect();
    let mut col = multi_slab_col(&vals, 4);
    col.splice(0, 10, std::iter::empty::<u64>());
    assert_eq!(col.len(), 0);
}

#[test]
fn cross_slab_fuzz_regression() {
    // Replay seed 799 from cross_slab_fuzz
    use rand::{RngExt, SeedableRng};
    let mut r = rand::rngs::SmallRng::seed_from_u64(799);

    let n = r.random_range(0u32..20) as usize + 5;
    let vals: Vec<u64> = (0..n).map(|_| r.random_range(0u64..5)).collect();
    let max_seg = r.random_range(0u32..6) as usize + 3;
    let mut col = multi_slab_col(&vals, max_seg);
    let mut mirror = vals.clone();

    for op in 0..10 {
        let len = col.len();
        if len == 0 {
            break;
        }
        let idx = r.random_range(0..len);
        let max_del = (len - idx).min(5);
        let del = r.random_range(0..max_del + 1);
        let ins_count = r.random_range(0u32..4) as usize;
        let new_vals: Vec<u64> = (0..ins_count).map(|_| r.random_range(0u64..5)).collect();

        eprintln!(
            "op={op} len={len} splice({idx}, {del}, {new_vals:?}) slabs={} info={:?}",
            col.slab_count(),
            ()
        );
        col.splice(idx, del, new_vals.iter().copied());
        mirror.splice(idx..idx + del, new_vals);
        assert_eq!(col.to_vec(), mirror, "mismatch at op {op}");
    }
}

#[test]
fn cross_slab_fuzz() {
    use rand::{RngExt, SeedableRng};
    for seed in 0..1000u64 {
        let mut r = rand::rngs::SmallRng::seed_from_u64(seed);
        let round = seed;
        let n = r.random_range(0u32..20) as usize + 5;
        let vals: Vec<u64> = (0..n).map(|_| r.random_range(0u64..5)).collect();
        let max_seg = r.random_range(0u32..6) as usize + 3;
        let mut col = multi_slab_col(&vals, max_seg);
        let mut mirror = vals.clone();

        // Perform 10 random operations.
        for op in 0..10 {
            let len = col.len();
            if len == 0 {
                break;
            }
            let idx = r.random_range(0..len);
            let max_del = (len - idx).min(5);
            let del = r.random_range(0..max_del + 1);
            let ins_count = r.random_range(0u32..4) as usize;
            let new_vals: Vec<u64> = (0..ins_count).map(|_| r.random_range(0u64..5)).collect();

            col.splice(idx, del, new_vals.iter().copied());
            mirror.splice(idx..idx + del, new_vals.clone());
            if col.len() != mirror.len() || col.to_vec() != mirror {
                panic!(
                    "cross_slab_fuzz FAILED round={round} op={op}\n\
                     initial vals={vals:?} max_seg={max_seg}\n\
                     splice(idx={idx}, del={del}, ins={new_vals:?})\n\
                     col={:?} (slabs={} info={:?})\n\
                     mirror={mirror:?}",
                    col.to_vec(),
                    col.slab_count(),
                    (),
                );
            }
        }
    }
}

// ── Aggressive fuzz tests ──────────────────────────────────────────────────
//
// Run splice at every position with insert/delete/replace of 1/10/100 items,
// validating the full column invariants after each operation.
// Marked #[ignore] — run with `cargo test -- --ignored fuzz_`.

use super::encoding::ColumnEncoding;

/// Validate every invariant on a Column<T>:
/// - total_len matches sum of slab lens
/// - no empty slabs (when column has items)
/// - no slab exceeds max_segments
/// - each slab's encoding is valid (len, segments match wire data)
/// - BIT is correct (matches rebuild from scratch)
fn validate_column<T: super::ColumnValueRef>(col: &Column<T>)
where
    for<'a> T::Get<'a>: std::fmt::Debug,
{
    let max_segments = col.max_segments;

    let sum_len: usize = col.slabs.iter().map(|s| s.len).sum();
    assert_eq!(
        col.total_len, sum_len,
        "total_len mismatch: stored={} sum={sum_len}",
        col.total_len,
    );

    for (i, slab) in col.slabs.iter().enumerate() {
        assert!(
            slab.segments <= max_segments,
            "slab {i}: segments={} exceeds max_segments={max_segments}",
            slab.segments
        );
        let info = T::Encoding::validate_encoding(&slab.data)
            .unwrap_or_else(|e| panic!("slab {i} encoding invalid: {e}"));
        assert_eq!(slab.len, info.len, "slab {i}: len mismatch");
        assert_eq!(slab.segments, info.segments, "slab {i}: segments mismatch");
    }

    // BIT correctness check removed — Column no longer uses a Fenwick BIT
    // (replaced by B-tree index).  Equivalent invariant now covered by the
    // index's own invariants checked during splice.
}

fn validate_rle_column<T>(col: &Column<T>)
where
    T: super::RleValue + super::ColumnValueRef<Encoding = super::rle::RleEncoding<T>>,
    for<'a> T::Get<'a>: std::fmt::Debug,
{
    validate_column(col);
    for (i, slab) in col.slabs.iter().enumerate() {
        let expected = super::rle::compute_rle_tail::<T>(&slab.data);
        assert_eq!(
            slab.tail.lit_tail, expected.lit_tail,
            "slab {i}: tail.lit_tail mismatch"
        );
        assert_eq!(
            slab.tail.bytes, expected.bytes,
            "slab {i}: tail.bytes mismatch"
        );
    }
}

fn validate_bool_column(col: &Column<bool>) {
    validate_column(col);
}

#[test]
#[ignore]
fn fuzz_bool_splice_exhaustive() {
    use rand::{rng, RngExt};
    let mut r = rng();

    let n = 200;
    let initial: Vec<bool> = (0..n).map(|_| r.random()).collect();

    let ops: &[(usize, usize)] = &[
        (0, 1),
        (0, 10),
        (0, 100),
        (1, 0),
        (10, 0),
        (100, 0),
        (1, 1),
        (10, 10),
        (100, 100),
    ];

    for &(del_count, ins_count) in ops {
        let col = Column::<bool>::from_values_with_max_segments(initial.clone(), 8);
        validate_bool_column(&col);

        let max_pos = if del_count == 0 {
            col.len() + 1
        } else {
            col.len().saturating_sub(del_count) + 1
        };

        for pos in 0..max_pos {
            let mut c = col.clone();
            let new_vals: Vec<bool> = (0..ins_count).map(|_| r.random()).collect();
            let del = del_count.min(c.len() - pos);
            c.splice(pos, del, new_vals);
            validate_bool_column(&c);
        }
    }
}

#[test]
#[ignore]
fn fuzz_option_u64_splice_exhaustive() {
    use rand::{rng, RngExt};
    let mut r = rng();

    let choices: [Option<u64>; 5] = [None, Some(1), Some(2), Some(3), Some(4)];
    let n = 200;
    let initial: Vec<Option<u64>> = (0..n)
        .map(|_| choices[r.random_range(0..choices.len())])
        .collect();

    let ops: &[(usize, usize)] = &[
        (0, 1),
        (0, 10),
        (0, 100),
        (1, 0),
        (10, 0),
        (100, 0),
        (1, 1),
        (10, 10),
        (100, 100),
    ];

    for &(del_count, ins_count) in ops {
        let col = Column::<Option<u64>>::from_values_with_max_segments(initial.clone(), 8);
        validate_rle_column(&col);

        let max_pos = if del_count == 0 {
            col.len() + 1
        } else {
            col.len().saturating_sub(del_count) + 1
        };

        for pos in 0..max_pos {
            let mut c = col.clone();
            let new_vals: Vec<Option<u64>> = (0..ins_count)
                .map(|_| choices[r.random_range(0..choices.len())])
                .collect();
            let del = del_count.min(c.len() - pos);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                c.splice(pos, del, new_vals.clone());
                validate_rle_column(&c);
            }));
            if result.is_err() {
                panic!(
                    "FAILED: del_count={del_count} ins_count={ins_count} pos={pos} del={del}\n  new_vals={new_vals:?}\n  col_len={} slabs={} info={:?}",
                    col.len(), col.slab_count(), ()
                );
            }
        }
    }
}

#[test]
fn save_to_multi_column_concatenation() {
    use rand::{rng, RngExt};
    let mut r = rng();

    // Build columns of different types with random data.
    let n = 1000;

    let u64_col = Column::<u64>::from_values_with_max_segments(
        (0..n).map(|_| r.random_range(0..100u64)).collect(),
        16,
    );
    let opt_col = Column::<Option<u64>>::from_values_with_max_segments(
        (0..n)
            .map(|_| {
                if r.random_range(0..5u32) == 0 {
                    None
                } else {
                    Some(r.random_range(0..5u64))
                }
            })
            .collect(),
        16,
    );
    let bool_col =
        Column::<bool>::from_values_with_max_segments((0..n).map(|_| r.random()).collect(), 16);
    let str_col = Column::<String>::from_values_with_max_segments(
        (0..n)
            .map(|i| ["alpha", "beta", "gamma", "delta"][i % 4].to_string())
            .collect(),
        16,
    );

    // save_to into a shared buffer, collecting ranges.
    let mut buf = Vec::new();
    let r_u64 = u64_col.save_to(&mut buf);
    let r_opt = opt_col.save_to(&mut buf);
    let r_bool = bool_col.save_to(&mut buf);
    let r_str = str_col.save_to(&mut buf);

    // Individual saves.
    let s_u64 = u64_col.save();
    let s_opt = opt_col.save();
    let s_bool = bool_col.save();
    let s_str = str_col.save();

    // Each range matches the individual save.
    assert_eq!(&buf[r_u64.clone()], &s_u64[..], "u64 range mismatch");
    assert_eq!(&buf[r_opt.clone()], &s_opt[..], "opt range mismatch");
    assert_eq!(&buf[r_bool.clone()], &s_bool[..], "bool range mismatch");
    assert_eq!(&buf[r_str.clone()], &s_str[..], "str range mismatch");

    // Concatenation matches.
    let mut expected = Vec::new();
    expected.extend_from_slice(&s_u64);
    expected.extend_from_slice(&s_opt);
    expected.extend_from_slice(&s_bool);
    expected.extend_from_slice(&s_str);
    assert_eq!(buf, expected, "concatenated buffer mismatch");

    // Ranges are contiguous and non-overlapping.
    assert_eq!(r_u64.start, 0);
    assert_eq!(r_u64.end, r_opt.start);
    assert_eq!(r_opt.end, r_bool.start);
    assert_eq!(r_bool.end, r_str.start);
    assert_eq!(r_str.end, buf.len());

    // Roundtrip: load each range back and verify values.
    let loaded_u64 = Column::<u64>::load(&buf[r_u64]).unwrap();
    assert_eq!(loaded_u64.to_vec(), u64_col.to_vec(), "u64 roundtrip");

    let loaded_opt = Column::<Option<u64>>::load(&buf[r_opt]).unwrap();
    assert_eq!(loaded_opt.to_vec(), opt_col.to_vec(), "opt roundtrip");

    let loaded_bool = Column::<bool>::load(&buf[r_bool]).unwrap();
    assert_eq!(loaded_bool.to_vec(), bool_col.to_vec(), "bool roundtrip");

    let loaded_str = Column::<String>::load(&buf[r_str]).unwrap();
    assert_eq!(loaded_str.to_vec(), str_col.to_vec(), "str roundtrip");
}

// ── Edge case: splice at literal/repeat boundaries ──────────────────────────

#[test]
fn splice_at_literal_repeat_boundary() {
    // [1, 2, 3, 3, 3] — literal [1,2] then repeat [3,3,3]
    // Splice at every position with insert, delete, and replace.
    let initial = vec![1u64, 2, 3, 3, 3];
    let col = Column::<u64>::from_values_with_max_segments(initial.clone(), 8);
    validate_rle_column(&col);

    for pos in 0..=initial.len() {
        // Insert at boundary
        let mut c = col.clone();
        c.insert(pos, 99u64);
        validate_rle_column(&c);
        assert_eq!(c.len(), initial.len() + 1);

        // Insert the same value as what's at the boundary
        if pos < initial.len() {
            let mut c = col.clone();
            c.insert(pos, initial[pos]);
            validate_rle_column(&c);
        }
    }

    for pos in 0..initial.len() {
        // Delete at boundary
        let mut c = col.clone();
        c.remove(pos);
        validate_rle_column(&c);
        assert_eq!(c.len(), initial.len() - 1);

        // Replace with same value
        let mut c = col.clone();
        c.splice(pos, 1, [initial[pos]]);
        validate_rle_column(&c);
        assert_eq!(c.to_vec(), initial);

        // Replace with different value
        let mut c = col.clone();
        c.splice(pos, 1, [99u64]);
        validate_rle_column(&c);
    }
}

#[test]
fn splice_at_repeat_then_literal_boundary() {
    // [5, 5, 5, 1, 2, 3] — repeat [5,5,5] then literal [1,2,3]
    let initial = vec![5u64, 5, 5, 1, 2, 3];
    let col = Column::<u64>::from_values_with_max_segments(initial.clone(), 8);
    validate_rle_column(&col);

    for pos in 0..=initial.len() {
        let mut c = col.clone();
        c.insert(pos, 99u64);
        validate_rle_column(&c);

        // Insert the repeat value at the boundary (index 3)
        let mut c = col.clone();
        c.insert(pos, 5u64);
        validate_rle_column(&c);
    }

    for pos in 0..initial.len() {
        let mut c = col.clone();
        c.remove(pos);
        validate_rle_column(&c);
    }
}

#[test]
fn splice_insert_same_value_at_literal_repeat_junction() {
    // Specifically insert the repeat value right at the junction.
    // [1, 2, 3, 3, 3] — insert 3 at index 2 (last literal, same as repeat value)
    let initial = vec![1u64, 2, 3, 3, 3];
    let mut col = Column::<u64>::from_values_with_max_segments(initial, 8);
    col.insert(2, 3u64);
    validate_rle_column(&col);
    assert_eq!(col.to_vec(), vec![1u64, 2, 3, 3, 3, 3]);

    // Insert 2 at index 3 — splits the repeat, value matches last literal
    let initial = vec![1u64, 2, 3, 3, 3];
    let mut col = Column::<u64>::from_values_with_max_segments(initial, 8);
    col.insert(3, 2u64);
    validate_rle_column(&col);
    assert_eq!(col.to_vec(), vec![1u64, 2, 3, 2, 3, 3]);
}

// ── Edge case: try_merge_range with multiple small slabs ────────────────────

#[test]
fn try_merge_range_multiple_merges() {
    // Create a column with very small max_segments to get many slabs,
    // then do a splice that produces overflow, triggering merge.
    let initial: Vec<u64> = (0..50).collect();
    let mut col = Column::<u64>::from_values_with_max_segments(initial.clone(), 4);
    validate_rle_column(&col);
    let slab_count_before = col.slab_count();
    assert!(slab_count_before > 5, "need many slabs for this test");

    // Big insert in the middle — will overflow and trigger merges
    let new_vals: Vec<u64> = (100..120).collect();
    col.splice(25, 0, new_vals.clone());
    validate_rle_column(&col);

    let mut expected = initial.clone();
    expected.splice(25..25, new_vals);
    assert_eq!(col.to_vec(), expected);
}

#[test]
fn try_merge_range_delete_across_slabs() {
    // Delete across multiple slab boundaries, triggering cross-slab delete + merge.
    let initial: Vec<u64> = (0..100).collect();
    let mut col = Column::<u64>::from_values_with_max_segments(initial.clone(), 4);
    validate_rle_column(&col);

    // Delete a large range spanning multiple slabs
    col.splice(10, 30, std::iter::empty::<u64>());
    validate_rle_column(&col);

    let mut expected = initial;
    expected.splice(10..40, []);
    assert_eq!(col.to_vec(), expected);
}

#[test]
fn try_merge_range_replace_across_slabs() {
    // Replace across slab boundaries: delete many, insert many.
    let initial: Vec<u64> = (0..80).map(|i| i % 7).collect();
    let mut col = Column::<u64>::from_values_with_max_segments(initial.clone(), 4);
    validate_rle_column(&col);

    let new_vals: Vec<u64> = (200..215).collect();
    col.splice(20, 25, new_vals.clone());
    validate_rle_column(&col);

    let mut expected = initial;
    expected.splice(20..45, new_vals);
    assert_eq!(col.to_vec(), expected);
}

// ── Edge case: overflow at postfix boundary ─────────────────────────────────

#[test]
fn splice_overflow_at_postfix_boundary() {
    // Insert enough values that overflow happens right when the postfix
    // needs to be attached. Use small max_segments.
    let initial = vec![1u64, 2, 3, 4, 5, 6, 7, 8];
    let col = Column::<u64>::from_values_with_max_segments(initial.clone(), 4);
    validate_rle_column(&col);

    // Insert in the middle of a slab — the postfix needs to go on the overflow slab.
    let new_vals: Vec<u64> = (10..20).collect();
    for pos in 0..=initial.len() {
        let mut c = col.clone();
        c.splice(pos, 0, new_vals.clone());
        validate_rle_column(&c);

        let mut expected = initial.clone();
        expected.splice(pos..pos, new_vals.clone());
        assert_eq!(c.to_vec(), expected);
    }
}

#[test]
fn splice_overflow_with_postfix_in_literal() {
    // Postfix falls inside a literal run during overflow.
    let initial = vec![1u64, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let mut col = Column::<u64>::from_values_with_max_segments(initial.clone(), 4);
    validate_rle_column(&col);

    // Delete from middle of a literal and insert many
    let new_vals: Vec<u64> = (50..60).collect();
    col.splice(3, 2, new_vals.clone());
    validate_rle_column(&col);

    let mut expected = initial;
    expected.splice(3..5, new_vals);
    assert_eq!(col.to_vec(), expected);
}

// ── Edge case: LonePlusLit postfix ──────────────────────────────────────────

#[test]
fn splice_lone_plus_lit_postfix() {
    // [7, 7, 7, 1, 2, 3] — delete index 1, leaving [7, 7, 1, 2, 3].
    // The postfix after deleting inside the repeat: count=1 of 7, followed by lit [1, 2, 3].
    // This should trigger LonePlusLit if the repeat remainder is 1 and next is literal.
    let initial = vec![7u64, 7, 7, 1, 2, 3];
    let mut col = Column::<u64>::from_values_with_max_segments(initial.clone(), 16);
    validate_rle_column(&col);

    col.splice(1, 1, std::iter::empty::<u64>());
    validate_rle_column(&col);
    assert_eq!(col.to_vec(), vec![7u64, 7, 1, 2, 3]);

    // Delete 2 from middle of repeat, leaving lone 7 + lit
    let initial = vec![7u64, 7, 7, 1, 2, 3];
    let mut col = Column::<u64>::from_values_with_max_segments(initial, 16);
    col.splice(0, 2, std::iter::empty::<u64>());
    validate_rle_column(&col);
    assert_eq!(col.to_vec(), vec![7u64, 1, 2, 3]);
}

#[test]
fn splice_lone_plus_lit_with_insert() {
    // Delete into repeat leaving 1, next is literal, AND insert values.
    let initial = vec![7u64, 7, 7, 1, 2, 3];
    let mut col = Column::<u64>::from_values_with_max_segments(initial, 16);
    col.splice(1, 1, [99u64]);
    validate_rle_column(&col);
    assert_eq!(col.to_vec(), vec![7u64, 99, 7, 1, 2, 3]);
}

#[test]
fn splice_lone_plus_lit_insert_same_as_lone() {
    // Lone 7 + lit [1,2,3] — insert 7, should extend the repeat
    let initial = vec![7u64, 7, 7, 1, 2, 3];
    let mut col = Column::<u64>::from_values_with_max_segments(initial, 16);
    col.splice(0, 2, std::iter::empty::<u64>()); // → [7, 1, 2, 3]
    validate_rle_column(&col);
    col.insert(0, 7u64); // → [7, 7, 1, 2, 3]
    validate_rle_column(&col);
    assert_eq!(col.to_vec(), vec![7u64, 7, 1, 2, 3]);
}

// ── Edge case: string column splice at literal/repeat boundary ──────────────

#[test]
fn string_splice_at_literal_repeat_boundary() {
    // Strings have variable-length values, testing byte offset calculations.
    let initial: Vec<String> = vec![
        "alpha".into(),
        "beta".into(),
        "gamma".into(),
        "gamma".into(),
        "gamma".into(),
        "delta".into(),
    ];
    let col = Column::<String>::from_values_with_max_segments(initial.clone(), 16);
    validate_rle_column(&col);

    // Insert at literal/repeat boundary
    for pos in 0..=initial.len() {
        let mut c = col.clone();
        c.insert(pos, "new".to_string());
        validate_rle_column(&c);
    }

    // Delete at every position
    for pos in 0..initial.len() {
        let mut c = col.clone();
        c.remove(pos);
        validate_rle_column(&c);
    }

    // Replace at the boundary with the repeat value
    let mut c = col.clone();
    c.splice(2, 1, ["gamma".to_string()]); // replace last lit with repeat value
    validate_rle_column(&c);
    assert_eq!(
        c.to_vec(),
        vec!["alpha", "beta", "gamma", "gamma", "gamma", "delta"]
    );

    // Replace repeat value with unique — breaks the run
    let mut c = col.clone();
    c.splice(3, 1, ["epsilon".to_string()]);
    validate_rle_column(&c);
    assert_eq!(
        c.to_vec(),
        vec!["alpha", "beta", "gamma", "epsilon", "gamma", "delta"]
    );
}

#[test]
fn string_splice_variable_length_values() {
    // Mix of short and long strings to stress byte offset tracking.
    let initial: Vec<String> = vec![
        "a".into(),
        "bb".into(),
        "ccc".into(),
        "ccc".into(),
        "dddddddddd".into(),
        "ee".into(),
        "ee".into(),
        "ee".into(),
    ];
    let col = Column::<String>::from_values_with_max_segments(initial.clone(), 8);
    validate_rle_column(&col);

    // Insert a long string in the middle of the literal run
    let mut c = col.clone();
    c.insert(2, "xxxxxxxxxxxxxxxxxx".to_string());
    validate_rle_column(&c);

    // Delete across the literal/repeat boundary
    let mut c = col.clone();
    c.splice(2, 3, std::iter::empty::<String>());
    validate_rle_column(&c);
    assert_eq!(c.to_vec(), vec!["a", "bb", "ee", "ee", "ee"]);
}

// ── Edge case: merge slab ending in literal with same-value start ───────────

#[test]
fn merge_slab_literal_to_same_value_start() {
    use super::encoding::{ColumnEncoding, EncoderApi};

    let a_vals: &[u64] = &[1, 2, 3, 4, 5];
    let b_vals: &[u64] = &[5, 5, 5, 6, 7];
    let mut a = super::Encoder::<u64>::encode_slab(a_vals.iter().copied());
    let b = super::Encoder::<u64>::encode_slab(b_vals.iter().copied());
    validate_rle_column_slab::<u64>(&a);
    validate_rle_column_slab::<u64>(&b);

    super::rle::RleEncoding::<u64>::merge_slabs(&mut a, b);
    validate_rle_column_slab::<u64>(&a);

    let merged: Vec<u64> = super::rle::RleDecoder::<u64>::new(&a.data).collect();
    assert_eq!(merged, vec![1, 2, 3, 4, 5, 5, 5, 5, 6, 7]);
}

#[test]
fn merge_slab_repeat_to_literal_same_value() {
    use super::encoding::{ColumnEncoding, EncoderApi};

    let a_vals: &[u64] = &[1, 2, 3, 3, 3];
    let b_vals: &[u64] = &[3, 4, 5];
    let mut a = super::Encoder::<u64>::encode_slab(a_vals.iter().copied());
    let b = super::Encoder::<u64>::encode_slab(b_vals.iter().copied());

    super::rle::RleEncoding::<u64>::merge_slabs(&mut a, b);
    validate_rle_column_slab::<u64>(&a);

    let merged: Vec<u64> = super::rle::RleDecoder::<u64>::new(&a.data).collect();
    assert_eq!(merged, vec![1, 2, 3, 3, 3, 3, 4, 5]);
}

#[test]
fn merge_slab_literal_to_literal_same_value() {
    use super::encoding::{ColumnEncoding, EncoderApi};

    let a_vals: &[u64] = &[1, 2, 5];
    let b_vals: &[u64] = &[5, 3, 4];
    let mut a = super::Encoder::<u64>::encode_slab(a_vals.iter().copied());
    let b = super::Encoder::<u64>::encode_slab(b_vals.iter().copied());

    super::rle::RleEncoding::<u64>::merge_slabs(&mut a, b);
    validate_rle_column_slab::<u64>(&a);

    let merged: Vec<u64> = super::rle::RleDecoder::<u64>::new(&a.data).collect();
    assert_eq!(merged, vec![1, 2, 5, 5, 3, 4]);
}

#[test]
fn merge_slab_string_literal_boundary() {
    use super::encoding::{ColumnEncoding, EncoderApi};

    let mut a = super::Encoder::<String>::encode_slab(["x", "y", "hello"]);
    let b = super::Encoder::<String>::encode_slab(["hello", "hello", "world"]);

    super::rle::RleEncoding::<String>::merge_slabs(&mut a, b);
    validate_rle_column_slab::<String>(&a);

    let merged: Vec<&str> = super::rle::RleDecoder::<String>::new(&a.data).collect();
    assert_eq!(merged, vec!["x", "y", "hello", "hello", "hello", "world"]);
}

/// Validate a single RLE slab (encoding + tail).
fn validate_rle_column_slab<T>(slab: &super::column::Slab<super::rle::RleTail>)
where
    T: super::RleValue + super::ColumnValueRef<Encoding = super::rle::RleEncoding<T>>,
    for<'a> T::Get<'a>: std::fmt::Debug,
{
    let info = super::rle::rle_validate_encoding::<T>(&slab.data)
        .unwrap_or_else(|e| panic!("slab encoding invalid: {e}"));
    assert_eq!(slab.len, info.len, "len mismatch");
    assert_eq!(slab.segments, info.segments, "segments mismatch");
    let expected_tail = super::rle::compute_rle_tail::<T>(&slab.data);
    assert_eq!(slab.tail.bytes, expected_tail.bytes, "tail.bytes mismatch");
    assert_eq!(
        slab.tail.lit_tail, expected_tail.lit_tail,
        "tail.lit_tail mismatch"
    );
}

// ── scope_to_value tests ────────────────────────────────────────────────────

#[test]
fn scope_to_value_basic() {
    // Same data as v0 test: [2,2,2, 3,3,3,3, 4,4,4,4,4,4,4,4, 5,5,5,5, 6,6,6, 8, 9,9]
    let data: Vec<Option<u64>> = vec![
        2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 6, 8, 9, 9,
    ]
    .into_iter()
    .map(Some)
    .collect();
    let col = Column::<Option<u64>>::from_values(data);

    assert_eq!(col.scope_to_value(Some(4u64), ..), 7..15);
    assert_eq!(col.scope_to_value(Some(4u64), ..11), 7..11);
    assert_eq!(col.scope_to_value(Some(4u64), ..8), 7..8);
    assert_eq!(col.scope_to_value(Some(4u64), 0..1), 1..1);
    assert_eq!(col.scope_to_value(Some(4u64), 8..9), 8..9);
    assert_eq!(col.scope_to_value(Some(4u64), 9..), 9..15);
    assert_eq!(col.scope_to_value(Some(4u64), 14..16), 14..15);

    assert_eq!(col.scope_to_value(Some(2u64), ..), 0..3);
    assert_eq!(col.scope_to_value(Some(7u64), ..), 22..22);
    assert_eq!(col.scope_to_value(Some(8u64), ..), 22..23);
    assert_eq!(col.scope_to_value(Some(9u64), ..), 23..25);
}

#[test]
fn scope_to_value_strings() {
    let data: Vec<Option<String>> = ["aaa", "aaa", "bbb", "bbb", "bbb", "ccc"]
        .into_iter()
        .map(|s| Some(s.to_string()))
        .collect();
    let col = Column::<Option<String>>::from_values(data);

    assert_eq!(col.scope_to_value(Some("aaa"), ..), 0..2);
    assert_eq!(col.scope_to_value(Some("bbb"), ..), 2..5);
    assert_eq!(col.scope_to_value(Some("ccc"), ..), 5..6);
    assert_eq!(col.scope_to_value(Some("ddd"), ..), 6..6);
    assert_eq!(col.scope_to_value(Some("aab"), ..), 2..2);
}

#[test]
fn scope_to_value_not_found() {
    let data: Vec<u64> = vec![1, 2, 3, 4, 5];
    let col = Column::<u64>::from_values(data);

    // Value less than all
    assert_eq!(col.scope_to_value(0u64, ..), 0..0);
    // Value greater than all
    assert_eq!(col.scope_to_value(6u64, ..), 5..5);
    // Value in a gap
    assert_eq!(col.scope_to_value(3u64, 0..2), 2..2);
}

#[test]
fn scope_to_value_empty() {
    let col = Column::<u64>::from_values(vec![]);
    assert_eq!(col.scope_to_value(1u64, ..), 0..0);
}

#[test]
fn scope_to_value_single_element() {
    let col = Column::<u64>::from_values(vec![5]);
    assert_eq!(col.scope_to_value(5u64, ..), 0..1);
    assert_eq!(col.scope_to_value(4u64, ..), 0..0);
    assert_eq!(col.scope_to_value(6u64, ..), 1..1);
}

#[test]
fn scope_to_value_range_truncation() {
    // Run extends past the range end — must truncate
    let data: Vec<u64> = vec![1, 1, 1, 2, 2, 2, 2, 2, 3, 3];
    let col = Column::<u64>::from_values(data);

    assert_eq!(col.scope_to_value(2u64, 4..6), 4..6);
    assert_eq!(col.scope_to_value(2u64, 3..5), 3..5);
    assert_eq!(col.scope_to_value(2u64, 3..), 3..8);
    assert_eq!(col.scope_to_value(1u64, ..2), 0..2);
}

#[test]
fn scope_to_value_multi_slab() {
    // Force multiple slabs with small max_segments
    let data: Vec<u64> = (0..100)
        .flat_map(|i| std::iter::repeat(i).take(3))
        .collect();
    let col = Column::<u64>::from_values_with_max_segments(data, 4);
    assert!(col.slab_count() > 1);

    assert_eq!(col.scope_to_value(0u64, ..), 0..3);
    assert_eq!(col.scope_to_value(50u64, ..), 150..153);
    assert_eq!(col.scope_to_value(99u64, ..), 297..300);
    // Not present
    assert_eq!(col.scope_to_value(100u64, ..), 300..300);
    // Subrange
    assert_eq!(col.scope_to_value(50u64, 151..152), 151..152);
    // 50 is past all values in 140..145 (which are ~46-48), insertion point at end
    assert_eq!(col.scope_to_value(50u64, 140..145), 145..145);
}

#[test]
fn scope_to_value_fuzz_vs_linear() {
    use rand::{rng, RngExt};
    let mut r = rng();

    const N: u32 = 1000;
    const STEP: u32 = 3;

    // Build sorted data: each value i appears STEP times
    let data: Vec<u64> = (0..N)
        .flat_map(|i| std::iter::repeat(i as u64 * 2 + 1).take(STEP as usize))
        .collect();
    let col = Column::<u64>::from_values_with_max_segments(data.clone(), 8);
    assert!(col.slab_count() > 1);

    for _ in 0..1000 {
        let roll = r.random_range(0..N);
        let target_present = roll * 2 + 1;
        let target_absent = roll * 2;

        let mut a = r.random_range(0..(N * STEP)) as usize;
        let mut b = r.random_range(0..(N * STEP)) as usize;
        if a > b {
            std::mem::swap(&mut a, &mut b);
        }

        // Compute expected result by linear scan
        let expected_present = {
            let first = data[a..b].iter().position(|&v| v == target_present as u64);
            let last = data[a..b].iter().rposition(|&v| v == target_present as u64);
            match (first, last) {
                (Some(f), Some(l)) => (a + f)..(a + l + 1),
                _ => {
                    // insertion point
                    let ip = data[a..b]
                        .iter()
                        .position(|&v| v >= target_present as u64)
                        .map(|p| a + p)
                        .unwrap_or(b);
                    ip..ip
                }
            }
        };

        let expected_absent = {
            let ip = data[a..b]
                .iter()
                .position(|&v| v >= target_absent as u64)
                .map(|p| a + p)
                .unwrap_or(b);
            ip..ip
        };

        let result_present = col.scope_to_value(target_present as u64, a..b);
        let result_absent = col.scope_to_value(target_absent as u64, a..b);

        assert_eq!(
            result_present, expected_present,
            "target={} range={}..{}",
            target_present, a, b
        );
        assert_eq!(
            result_absent, expected_absent,
            "target={} range={}..{}",
            target_absent, a, b
        );
    }
}

// ── Column::remap tests ─────────────────────────────────────────────────────

#[test]
fn remap_u64_basic() {
    let mut col = Column::<u64>::from_values(vec![1, 2, 2, 3, 4, 4, 4, 5]);
    col.remap(|v| v * 10);
    assert_eq!(col.to_vec(), vec![10, 20, 20, 30, 40, 40, 40, 50]);
}

#[test]
fn remap_runs_preserved() {
    // Long runs should still be runs after remap.
    let vals: Vec<u64> = std::iter::repeat(7).take(100).collect();
    let mut col = Column::<u64>::from_values(vals);
    col.remap(|v| v + 1);
    assert_eq!(col.len(), 100);
    assert!(col.iter().all(|v| v == 8));
    // Run-encoded so the column should be small.
    let bytes = col.save();
    assert!(
        bytes.len() < 10,
        "expected compact run encoding, got {} bytes",
        bytes.len()
    );
}

#[test]
fn remap_multi_slab() {
    let vals: Vec<u64> = (0..200u64).collect();
    let mut col = Column::<u64>::from_values_with_max_segments(vals, 4);
    assert!(col.slab_count() > 1);
    col.remap(|v| v + 1000);
    let expected: Vec<u64> = (0..200u64).map(|v| v + 1000).collect();
    assert_eq!(col.to_vec(), expected);
}

#[test]
fn remap_empty() {
    let mut col = Column::<u64>::from_values(vec![]);
    col.remap(|v| v * 2);
    assert_eq!(col.len(), 0);
}

// ── seek_to_value tests ───────────────────────────────────────────────────

#[test]
fn seek_to_value_basic() {
    let data: Vec<u64> = vec![
        0, 0, 0, 1, 1, 1, 6, 6, 6, 7, 7, 7, 8, 8, 8, 9, 9, 9, 10, 10, 10,
    ];
    let col = Column::<u64>::from_values(data);

    let mut iter = col.iter();
    assert_eq!(iter.seek_to_value(0u64, ..), 0..3);
    assert_eq!(iter.seek_to_value(6u64, ..), 6..9);
    assert_eq!(iter.seek_to_value(8u64, ..), 12..15);

    let mut iter = col.iter();
    assert_eq!(iter.seek_to_value(0u64, ..), 0..3);
    assert_eq!(iter.seek_to_value(1u64, ..), 3..6);
    assert_eq!(iter.seek_to_value(6u64, ..), 6..9);
}

#[test]
fn seek_to_value_sequential() {
    let data: Vec<u64> = vec![
        0, 0, 0, 1, 1, 1, 6, 6, 6, 7, 7, 7, 8, 8, 8, 9, 9, 9, 10, 10, 10,
    ];
    let col = Column::<u64>::from_values(data);

    let mut iter = col.iter();
    assert_eq!(iter.seek_to_value(0u64, ..), 0..3);
    assert_eq!(iter.seek_to_value(1u64, ..), 3..6);
    assert_eq!(iter.seek_to_value(6u64, ..), 6..9);
    assert_eq!(iter.seek_to_value(7u64, ..), 9..12);
    assert_eq!(iter.seek_to_value(8u64, ..), 12..15);
    assert_eq!(iter.seek_to_value(9u64, ..), 15..18);
    assert_eq!(iter.seek_to_value(10u64, ..), 18..21);
}

#[test]
fn seek_to_value_missing() {
    let data: Vec<u64> = vec![0, 0, 0, 2, 2, 2, 4, 4, 4];
    let col = Column::<u64>::from_values(data);

    let mut iter = col.iter();
    assert_eq!(iter.seek_to_value(1u64, ..), 3..3);
    assert_eq!(iter.seek_to_value(3u64, ..), 6..6);
    assert_eq!(iter.seek_to_value(5u64, ..), 9..9);
}

#[test]
fn seek_to_value_with_range() {
    let data: Vec<u64> = vec![0, 0, 1, 1, 2, 2, 3, 3];
    let col = Column::<u64>::from_values(data);

    let mut iter = col.iter();
    assert_eq!(iter.seek_to_value(1u64, 2..6), 2..4);
    assert_eq!(iter.seek_to_value(2u64, 4..8), 4..6);
}

#[test]
fn seek_to_value_nullable() {
    let data: Vec<Option<u32>> = vec![
        None,
        None,
        None,
        Some(1),
        Some(1),
        Some(2),
        Some(2),
        Some(3),
    ];
    let col = Column::<Option<u32>>::from_values(data);

    let mut iter = col.iter();
    assert_eq!(iter.seek_to_value(None, ..), 0..3);
    assert_eq!(iter.seek_to_value(Some(1u32), ..), 3..5);
    assert_eq!(iter.seek_to_value(Some(2u32), ..), 5..7);
    assert_eq!(iter.seek_to_value(Some(3u32), ..), 7..8);
}

#[test]
fn seek_to_value_after_reads() {
    let data: Vec<u64> = vec![0, 0, 0, 1, 1, 1, 2, 2, 2];
    let col = Column::<u64>::from_values(data);

    let mut iter = col.iter();
    assert_eq!(iter.next(), Some(0));
    assert_eq!(iter.next(), Some(0));
    assert_eq!(iter.next(), Some(0));
    assert_eq!(iter.seek_to_value(1u64, ..), 3..6);
    assert_eq!(iter.next(), Some(1));
    assert_eq!(iter.next(), Some(1));
    assert_eq!(iter.next(), Some(1));
    assert_eq!(iter.seek_to_value(2u64, ..), 6..9);
}

#[test]
fn scope_to_value_multi_slab_span() {
    // Build a column with sorted groups where the middle group spans 3+ slabs
    // by inserting at random positions (creates many literal RLE segments)
    let mut col = Column::<Option<u32>>::with_max_segments(4);
    // group 0: None
    col.insert(0, None);
    // group 1: Some(1) — insert one at a time at random-ish positions to create many segments
    let mut rng: u64 = 42;
    let group1_start = 1;
    for i in 0..200usize {
        rng ^= rng.wrapping_shl(13);
        rng ^= rng.wrapping_shr(7);
        rng ^= rng.wrapping_shl(17);
        let pos = group1_start + (rng as usize % (i + 1));
        col.insert(pos, Some(1u32));
    }
    let group1_end = 201;
    // group 2: Some(2)
    for i in 0..10 {
        col.insert(group1_end + i, Some(2u32));
    }

    assert_eq!(col.len(), 211);
    if col.slab_count() < 3 {
        eprintln!(
            "WARNING: only {} slabs, test may not trigger multi-slab span bug",
            col.slab_count()
        );
    }
    let result = col.scope_to_value(Some(1u32), ..);
    assert_eq!(
        result,
        1..201,
        "scope_to_value for Some(1) returned {:?}",
        result
    );
    let result2 = col.scope_to_value(None, ..);
    assert_eq!(result2, 0..1);
    let result3 = col.scope_to_value(Some(2u32), ..);
    assert_eq!(result3, 201..211);
}

#[test]
fn scope_to_value_fuzz_multi_slab() {
    for max_seg in [4, 8, 16, 32] {
        for prefix_len in [0, 1, 2, 5] {
            for run_len in [10, 50, 100, 200] {
                for suffix_len in [0, 1, 5, 10] {
                    let mut data: Vec<u64> = vec![0; prefix_len];
                    let expected_start = data.len();
                    data.resize(expected_start + run_len, 1);
                    let expected_end = data.len();
                    data.resize(expected_end + suffix_len, 2);
                    let col = Column::<u64>::from_values_with_max_segments(data, max_seg);
                    let result = col.scope_to_value(1u64, ..);
                    assert_eq!(
                        result,
                        expected_start..expected_end,
                        "max_seg={max_seg} prefix={prefix_len} run={run_len} suffix={suffix_len} slabs={} result={:?}",
                        col.slab_count(), result,
                    );
                }
            }
        }
    }
}
/// Brute-force fuzz: build a sorted column via random inserts, then verify
/// scope_to_value and seek_to_value for every distinct value against a
/// naive linear scan.
#[test]
fn fuzz_scope_and_seek_to_value() {
    let mut rng: u64 = 777;
    let next = |rng: &mut u64| -> u64 {
        *rng ^= rng.wrapping_shl(13);
        *rng ^= rng.wrapping_shr(7);
        *rng ^= rng.wrapping_shl(17);
        *rng
    };

    for trial in 0..500 {
        let max_seg = match trial % 4 {
            0 => 4,
            1 => 8,
            2 => 16,
            _ => 64,
        };
        let mut col = Column::<u64>::with_max_segments(max_seg);
        let n = 50 + (next(&mut rng) as usize % 200);
        let num_distinct = 2 + (next(&mut rng) as usize % 8);

        // Build sorted column via inserts at random positions
        // Values are in range 0..num_distinct, inserted in sorted order
        let mut vals: Vec<u64> = Vec::new();
        for _ in 0..n {
            let v = next(&mut rng) % num_distinct as u64;
            vals.push(v);
        }
        vals.sort();

        // Insert one-by-one (randomizing insertion order within equal-value
        // groups to create varied slab layouts)
        let mut indices: Vec<usize> = (0..n).collect();
        for i in (1..indices.len()).rev() {
            let j = next(&mut rng) as usize % (i + 1);
            indices.swap(i, j);
        }
        let mut sorted_positions: Vec<usize> = vec![0; n];
        let mut current = Vec::new();
        for &orig_idx in &indices {
            let val = vals[orig_idx];
            // Find the correct sorted position among already-inserted values
            let pos = current.partition_point(|&v| v <= val);
            current.insert(pos, val);
            col.insert(pos, val);
            sorted_positions[orig_idx] = pos;
        }

        // Verify column matches expected sorted values
        let col_vals: Vec<u64> = col.iter().collect();
        assert_eq!(
            col_vals, vals,
            "trial={trial}: column values don't match sorted input"
        );

        // Test scope_to_value for every value 0..num_distinct+1
        for target in 0..=(num_distinct as u64) {
            let expected_start = vals.partition_point(|&v| v < target);
            let expected_end = vals.partition_point(|&v| v <= target);
            let expected = expected_start..expected_end;

            let scope_result = col.scope_to_value(target, ..);
            assert_eq!(
                scope_result, expected,
                "trial={trial} target={target} scope_to_value: got {scope_result:?} expected {expected:?} slabs={} segs={:?}",
                col.slab_count(), col.slab_segments(),
            );

            // Test scope_to_value with sub-ranges
            if n > 10 {
                let mid = n / 2;
                let sub_start = vals[..mid].partition_point(|&v| v < target);
                let sub_end = vals[..mid].partition_point(|&v| v <= target);
                let sub_result = col.scope_to_value(target, ..mid);
                assert_eq!(
                    sub_result, sub_start..sub_end,
                    "trial={trial} target={target} scope_to_value(..{mid}): got {sub_result:?} expected {:?}",
                    sub_start..sub_end,
                );
            }
        }

        // Test seek_to_value: sequential seeks (like ObjIdIter does)
        let mut iter = col.iter();
        for target in 0..=(num_distinct as u64) {
            let expected_start = vals.partition_point(|&v| v < target);
            let expected_end = vals.partition_point(|&v| v <= target);
            let expected = expected_start..expected_end;

            let seek_result = iter.seek_to_value(target, ..);
            assert_eq!(
                seek_result, expected,
                "trial={trial} target={target} seek_to_value: got {seek_result:?} expected {expected:?}",
            );
        }

        // Test seek_to_value with skips (seek non-adjacent values)
        let mut iter2 = col.iter();
        for target in (0..=(num_distinct as u64)).step_by(2) {
            let expected_start = vals.partition_point(|&v| v < target);
            let expected_end = vals.partition_point(|&v| v <= target);
            let seek_result = iter2.seek_to_value(target, ..);
            assert_eq!(
                seek_result,
                expected_start..expected_end,
                "trial={trial} target={target} seek_to_value(skip): got {seek_result:?}",
            );
        }

        // Test seek_to_value with sub-range (fresh iter each time since
        // seek_to_value advances the iterator)
        if n > 20 {
            let lo = n / 4;
            let hi = 3 * n / 4;
            let sub = &vals[lo..hi];
            for target in 0..=(num_distinct as u64) {
                let s = sub.partition_point(|&v| v < target) + lo;
                let e = sub.partition_point(|&v| v <= target) + lo;
                let mut iter3 = col.iter();
                let seek_result = iter3.seek_to_value(target, lo..hi);
                assert_eq!(
                    seek_result,
                    s..e,
                    "trial={trial} target={target} seek_to_value({lo}..{hi}): got {seek_result:?}",
                );
            }
        }
    }
}

/// Fuzz nullable columns — scope_to_value with Option<u32>
#[test]
fn fuzz_scope_to_value_nullable() {
    let mut rng: u64 = 42424242;
    let next = |rng: &mut u64| -> u64 {
        *rng ^= rng.wrapping_shl(13);
        *rng ^= rng.wrapping_shr(7);
        *rng ^= rng.wrapping_shl(17);
        *rng
    };

    for trial in 0..200 {
        let max_seg = match trial % 3 {
            0 => 4,
            1 => 8,
            _ => 64,
        };
        let mut col = Column::<Option<u32>>::with_max_segments(max_seg);
        let n = 30 + (next(&mut rng) as usize % 150);
        let num_distinct = 2 + (next(&mut rng) as usize % 5);

        let mut vals: Vec<Option<u32>> = Vec::new();
        for _ in 0..n {
            let r = next(&mut rng);
            if r % 5 == 0 {
                vals.push(None);
            } else {
                vals.push(Some((r % num_distinct as u64) as u32));
            }
        }
        vals.sort();

        // Insert shuffled
        let mut indices: Vec<usize> = (0..n).collect();
        for i in (1..indices.len()).rev() {
            let j = next(&mut rng) as usize % (i + 1);
            indices.swap(i, j);
        }
        let mut current: Vec<Option<u32>> = Vec::new();
        for &orig_idx in &indices {
            let val = vals[orig_idx];
            let pos = current.partition_point(|v| v <= &val);
            current.insert(pos, val);
            col.insert(pos, val);
        }

        let col_vals: Vec<Option<u32>> = col.iter().collect();
        assert_eq!(col_vals, vals, "trial={trial}: nullable column mismatch");

        // Test scope_to_value
        let mut targets: Vec<Option<u32>> = vec![None];
        for v in 0..=(num_distinct as u32) {
            targets.push(Some(v));
        }
        for target in &targets {
            let expected_start = vals.partition_point(|v| v < target);
            let expected_end = vals.partition_point(|v| v <= target);
            let result = col.scope_to_value(*target, ..);
            assert_eq!(
                result,
                expected_start..expected_end,
                "trial={trial} target={target:?} scope_to_value: got {result:?}",
            );
        }

        // Sequential seek_to_value
        let mut iter = col.iter();
        for target in &targets {
            let expected_start = vals.partition_point(|v| v < target);
            let expected_end = vals.partition_point(|v| v <= target);
            let result = iter.seek_to_value(*target, ..);
            assert_eq!(
                result,
                expected_start..expected_end,
                "trial={trial} target={target:?} seek_to_value: got {result:?}",
            );
        }
    }
}

fn check_slab_merge_invariant<T: ColumnValueRef>(col: &Column<T>, context: &str) {
    let segs = col.slab_segments();
    let max = crate::v1::column::DEFAULT_MAX_SEG;
    let min = max / 4;
    for i in 0..segs.len() {
        if segs[i] < min {
            if i > 0 && segs[i] + segs[i - 1] <= max {
                panic!(
                    "{context}: slab[{i}] has {} segments, left neighbor slab[{}] has {} — \
                     mergeable ({} + {} = {} <= {max}) but wasn't merged. all_segs={segs:?}",
                    segs[i],
                    i - 1,
                    segs[i - 1],
                    segs[i],
                    segs[i - 1],
                    segs[i] + segs[i - 1],
                );
            }
            if i + 1 < segs.len() && segs[i] + segs[i + 1] <= max {
                panic!(
                    "{context}: slab[{i}] has {} segments, right neighbor slab[{}] has {} — \
                     mergeable ({} + {} = {} <= {max}) but wasn't merged. all_segs={segs:?}",
                    segs[i],
                    i + 1,
                    segs[i + 1],
                    segs[i],
                    segs[i + 1],
                    segs[i] + segs[i + 1],
                );
            }
        }
    }
}

#[test]
fn fuzz_slab_merge_invariant() {
    let mut rng: u64 = 98765;
    let next = |rng: &mut u64| -> u64 {
        *rng ^= rng.wrapping_shl(13);
        *rng ^= rng.wrapping_shr(7);
        *rng ^= rng.wrapping_shl(17);
        *rng
    };

    for trial in 0..200 {
        let mut col = Column::<u64>::new();
        for i in 0..500 {
            let len = col.len();
            let r = next(&mut rng);
            if len == 0 || r % 3 != 0 {
                let pos = if len == 0 {
                    0
                } else {
                    (next(&mut rng) as usize) % (len + 1)
                };
                let val = next(&mut rng) % 20;
                col.insert(pos, val);
            } else {
                let pos = (next(&mut rng) as usize) % len;
                col.remove(pos);
            }
            check_slab_merge_invariant(&col, &format!("trial={trial} op={i}"));
        }
    }
}

// ── Comprehensive fuzz tests ──────────────────────────────────────────────

fn xorshift(state: &mut u64) -> u64 {
    *state ^= state.wrapping_shl(13);
    *state ^= state.wrapping_shr(7);
    *state ^= state.wrapping_shl(17);
    *state
}

#[test]
fn fuzz_delta_column() {
    let mut rng: u64 = 11111;

    for trial in 0..200 {
        let mut col = DeltaColumn::<u32>::new();
        let mut mirror: Vec<u32> = Vec::new();

        for i in 0..300 {
            let len = col.len();
            let r = xorshift(&mut rng);
            if len == 0 || r % 4 != 0 {
                let pos = if len == 0 {
                    0
                } else {
                    (xorshift(&mut rng) as usize) % (len + 1)
                };
                let val = (xorshift(&mut rng) % 1000) as u32;
                col.insert(pos, val);
                mirror.insert(pos, val);
            } else if r % 4 == 1 && len > 0 {
                let pos = (xorshift(&mut rng) as usize) % len;
                col.remove(pos);
                mirror.remove(pos);
            } else {
                let pos = (xorshift(&mut rng) as usize) % (len + 1);
                let del = if pos < len {
                    (xorshift(&mut rng) as usize) % (len - pos).min(5)
                } else {
                    0
                };
                let ins_count = (xorshift(&mut rng) as usize) % 4;
                let vals: Vec<u32> = (0..ins_count)
                    .map(|_| (xorshift(&mut rng) % 1000) as u32)
                    .collect();
                col.splice(pos, del, vals.iter().copied());
                mirror.splice(pos..pos + del, vals);
            }

            assert_eq!(
                col.len(),
                mirror.len(),
                "trial={trial} op={i}: len mismatch"
            );
            let col_vals = col.to_vec();
            assert_eq!(col_vals, mirror, "trial={trial} op={i}: values mismatch");

            if len > 0 {
                let idx = (xorshift(&mut rng) as usize) % col.len();
                assert_eq!(
                    col.get(idx),
                    Some(mirror[idx]),
                    "trial={trial} op={i}: get({idx})"
                );
            }
        }

        if !mirror.is_empty() {
            assert_eq!(col.first(), Some(mirror[0]));
            assert_eq!(col.last(), Some(*mirror.last().unwrap()));
        }

        // iter_range
        if col.len() > 10 {
            let lo = (xorshift(&mut rng) as usize) % col.len();
            let hi = lo + (xorshift(&mut rng) as usize) % (col.len() - lo);
            let range_vals: Vec<u32> = col.iter_range(lo..hi).collect();
            assert_eq!(range_vals, mirror[lo..hi], "trial={trial}: iter_range");
        }

        // DeltaIter nth
        if col.len() > 5 {
            let mut iter = col.iter();
            let skip = (xorshift(&mut rng) as usize) % col.len();
            let val = iter.nth(skip);
            assert_eq!(val, Some(mirror[skip]), "trial={trial}: nth({skip})");
        }

        // save/load roundtrip
        let saved = col.save();
        let loaded = DeltaColumn::<u32>::load(&saved).unwrap();
        assert_eq!(saved, loaded.save(), "trial={trial}: save/load roundtrip");
    }
}

#[test]
fn fuzz_delta_column_nullable() {
    let mut rng: u64 = 22222;

    for trial in 0..200 {
        let mut col = DeltaColumn::<Option<u32>>::new();
        let mut mirror: Vec<Option<u32>> = Vec::new();

        for i in 0..200 {
            let len = col.len();
            let pos = if len == 0 {
                0
            } else {
                (xorshift(&mut rng) as usize) % (len + 1)
            };
            let val = if xorshift(&mut rng) % 5 == 0 {
                None
            } else {
                Some((xorshift(&mut rng) % 500) as u32)
            };
            col.insert(pos, val);
            mirror.insert(pos, val);
            assert_eq!(col.to_vec(), mirror, "trial={trial} op={i}");
        }

        let saved = col.save();
        let loaded = DeltaColumn::<Option<u32>>::load(&saved).unwrap();
        assert_eq!(saved, loaded.save(), "trial={trial}: save/load roundtrip");
    }
}

#[test]
fn fuzz_prefix_column() {
    let mut rng: u64 = 33333;

    for trial in 0..200 {
        let mut col = PrefixColumn::<u32>::new();
        let mut mirror: Vec<u32> = Vec::new();

        for i in 0..300 {
            let len = col.len();
            let r = xorshift(&mut rng);
            if len == 0 || r % 4 != 0 {
                let pos = if len == 0 {
                    0
                } else {
                    (xorshift(&mut rng) as usize) % (len + 1)
                };
                let val = (xorshift(&mut rng) % 20) as u32;
                col.insert(pos, val);
                mirror.insert(pos, val);
            } else if r % 4 == 1 && len > 0 {
                let pos = (xorshift(&mut rng) as usize) % len;
                col.remove(pos);
                mirror.remove(pos);
            } else {
                let pos = (xorshift(&mut rng) as usize) % (len + 1);
                let del = if pos < len {
                    (xorshift(&mut rng) as usize) % (len - pos).min(5)
                } else {
                    0
                };
                let ins_count = (xorshift(&mut rng) as usize) % 4;
                let vals: Vec<u32> = (0..ins_count)
                    .map(|_| (xorshift(&mut rng) % 20) as u32)
                    .collect();
                col.splice(pos, del, vals.iter().copied());
                mirror.splice(pos..pos + del, vals);
            }

            assert_eq!(col.len(), mirror.len(), "trial={trial} op={i}: len");

            // verify prefix sums
            let mut prefix = 0u64;
            for (j, &v) in mirror.iter().enumerate() {
                assert_eq!(
                    col.get_prefix(j),
                    prefix,
                    "trial={trial} op={i}: get_prefix({j})"
                );
                prefix += v as u64;
                assert_eq!(
                    col.get_total(j),
                    prefix,
                    "trial={trial} op={i}: get_total({j})"
                );
            }
            assert_eq!(
                col.get_prefix(mirror.len()),
                prefix,
                "trial={trial} op={i}: get_prefix(len)"
            );

            // prefix_delta
            if mirror.len() > 2 {
                let lo = (xorshift(&mut rng) as usize) % mirror.len();
                let hi = lo + (xorshift(&mut rng) as usize) % (mirror.len() - lo);
                let expected: u64 = mirror[lo..hi].iter().map(|&v| v as u64).sum();
                assert_eq!(
                    col.prefix_delta(lo..hi),
                    expected,
                    "trial={trial} op={i}: prefix_delta({lo}..{hi})"
                );
            }
        }

        // get_index_for_prefix / get_index_for_total
        let total: u64 = mirror.iter().map(|&v| v as u64).sum();
        if total > 0 {
            let target = (xorshift(&mut rng) % total) + 1;
            let idx = col.get_index_for_total(target);
            let actual_total = col.get_total(idx);
            assert!(
                actual_total >= target,
                "trial={trial}: get_index_for_total({target}) -> {idx}, total={actual_total}"
            );
            if idx > 0 {
                assert!(col.get_total(idx - 1) < target, "trial={trial}: off by one");
            }
        }

        // PrefixIter advance_prefix
        if !mirror.is_empty() {
            let target_prefix = xorshift(&mut rng) % (total.max(1));
            let mut iter = col.iter();
            if let Some(seek) = iter.advance_prefix(target_prefix) {
                assert!(
                    seek.total >= target_prefix,
                    "trial={trial}: advance_prefix undershoot"
                );
            }
        }

        // PrefixIter advance_to
        if col.len() > 5 {
            let target_pos = (xorshift(&mut rng) as usize) % col.len();
            let mut iter = col.iter();
            if let Some(seek) = iter.delta_nth(target_pos) {
                assert_eq!(seek.pos, target_pos, "trial={trial}: advance_to pos");
                assert_eq!(
                    seek.total,
                    col.get_total(target_pos),
                    "trial={trial}: advance_to prefix"
                );
            }
        }

        // seek / get_delta
        if col.len() > 5 && total > 0 {
            let start = (xorshift(&mut rng) as usize) % col.len();
            let n = xorshift(&mut rng) % total.min(100);
            let seek_result = col.seek(start, n);
            let delta_result = if start < col.len() - 1 {
                let pos =
                    start + 1 + (xorshift(&mut rng) as usize) % (col.len() - start - 1).max(1);
                col.get_delta(start, pos)
                //col.prefix_delta(start.. pos)
            } else {
                None
            };
            // Just verify they don't panic
            let _ = seek_result;
            let _ = delta_result;
        }

        // save/load roundtrip
        let saved = col.save();
        let loaded = PrefixColumn::<u32>::load(&saved).unwrap();
        assert_eq!(saved, loaded.save(), "trial={trial}: save/load roundtrip");
    }
}

#[test]
fn fuzz_prefix_column_bool() {
    let mut rng: u64 = 44444;

    for trial in 0..200 {
        let mut col = PrefixColumn::<bool>::new();
        let mut mirror: Vec<bool> = Vec::new();

        for i in 0..300 {
            let len = col.len();
            let r = xorshift(&mut rng);
            if len == 0 || r % 3 != 0 {
                let pos = if len == 0 {
                    0
                } else {
                    (xorshift(&mut rng) as usize) % (len + 1)
                };
                let val = xorshift(&mut rng) % 2 == 0;
                col.insert(pos, val);
                mirror.insert(pos, val);
            } else {
                let pos = (xorshift(&mut rng) as usize) % len;
                col.remove(pos);
                mirror.remove(pos);
            }

            let true_count: usize = mirror.iter().filter(|&&v| v).count();
            assert_eq!(
                col.get_prefix(mirror.len()),
                true_count,
                "trial={trial} op={i}: prefix mismatch"
            );
        }

        let saved = col.save();
        let loaded = PrefixColumn::<bool>::load(&saved).unwrap();
        assert_eq!(saved, loaded.save(), "trial={trial}: save/load roundtrip");
    }
}

#[test]
fn fuzz_indexed_delta_column() {
    let mut rng: u64 = 55555;

    for trial in 0..200 {
        let mut col = DeltaColumn::<u32>::new();
        let mut mirror: Vec<u32> = Vec::new();

        for _ in 0..200 {
            let len = col.len();
            let pos = if len == 0 {
                0
            } else {
                (xorshift(&mut rng) as usize) % (len + 1)
            };
            let val = (xorshift(&mut rng) % 500) as u32;
            col.insert(pos, val);
            mirror.insert(pos, val);
        }

        assert_eq!(col.to_vec(), mirror, "trial={trial}: values");

        // find_by_value
        for target in [0u32, 1, 50, 100, 250, 499] {
            let expected: Vec<usize> = mirror
                .iter()
                .enumerate()
                .filter(|(_, &v)| v == target)
                .map(|(i, _)| i)
                .collect();
            let found: Vec<usize> = col.find_by_value(target).collect();
            assert_eq!(found, expected, "trial={trial}: find_by_value({target})");
        }

        // find_by_range
        let lo = (xorshift(&mut rng) % 200) as u32;
        let hi = lo + (xorshift(&mut rng) % 100) as u32;
        let expected: Vec<usize> = mirror
            .iter()
            .enumerate()
            .filter(|(_, &v)| v >= lo && v < hi)
            .map(|(i, _)| i)
            .collect();
        let found: Vec<usize> = col.find_by_range(lo..hi).collect();
        assert_eq!(found, expected, "trial={trial}: find_by_range({lo}..{hi})");

        // find_first
        if let Some(&first_val) = mirror.first() {
            let expected_pos = mirror.iter().position(|&v| v == first_val);
            let found_pos = col.find_first(first_val);
            assert_eq!(
                found_pos, expected_pos,
                "trial={trial}: find_first({first_val})"
            );
        }

        // save/load roundtrip
        let saved = col.save();
        let loaded = DeltaColumn::<u32>::load(&saved).unwrap();
        assert_eq!(saved, loaded.save(), "trial={trial}: save/load roundtrip");
    }
}

#[test]
fn fuzz_column_remap() {
    let mut rng: u64 = 66666;

    for trial in 0..100 {
        let mut col = Column::<u64>::new();
        let mut mirror: Vec<u64> = Vec::new();

        for _ in 0..100 {
            let len = col.len();
            let pos = if len == 0 {
                0
            } else {
                (xorshift(&mut rng) as usize) % (len + 1)
            };
            let val = xorshift(&mut rng) % 50;
            col.insert(pos, val);
            mirror.insert(pos, val);
        }

        let f = |v: u64| -> u64 { v.wrapping_mul(3).wrapping_add(7) % 100 };
        col.remap(f);
        let remapped_mirror: Vec<u64> = mirror.iter().map(|&v| f(v)).collect();
        assert_eq!(col.to_vec(), remapped_mirror, "trial={trial}: remap values");

        let saved = col.save();
        let loaded = Column::<u64>::load(&saved).unwrap();
        assert_eq!(
            saved,
            loaded.save(),
            "trial={trial}: save/load roundtrip after remap"
        );
    }
}

#[test]
fn fuzz_column_save_load_after_mutations() {
    let mut rng: u64 = 77777;

    for trial in 0..200 {
        let mut col = Column::<Option<u64>>::new();

        for _ in 0..200 {
            let len = col.len();
            let r = xorshift(&mut rng);
            if len == 0 || r % 3 != 0 {
                let pos = if len == 0 {
                    0
                } else {
                    (xorshift(&mut rng) as usize) % (len + 1)
                };
                let val = if xorshift(&mut rng) % 5 == 0 {
                    None
                } else {
                    Some(xorshift(&mut rng) % 100)
                };
                col.insert(pos, val);
            } else {
                let pos = (xorshift(&mut rng) as usize) % len;
                col.remove(pos);
            }
        }

        let vals_before = col.to_vec();
        let saved = col.save();
        let loaded = Column::<Option<u64>>::load(&saved).unwrap();
        assert_eq!(
            loaded.to_vec(),
            vals_before,
            "trial={trial}: values after reload"
        );
        assert_eq!(saved, loaded.save(), "trial={trial}: save/load roundtrip");
    }
}
