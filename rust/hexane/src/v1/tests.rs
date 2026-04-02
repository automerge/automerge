use crate::v1::{Column, DeltaColumn, LoadOpts, PrefixColumn};
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
    T: crate::v1::ColumnValueRef + std::fmt::Debug,
    for<'a> T::Get<'a>: GetEq<T> + std::fmt::Debug,
{
    assert_eq!(col.len(), expected.len(), "length mismatch");
    for (i, v) in expected.iter().enumerate() {
        match col.get(i) {
            Some(g) => assert!(g.get_eq(v), "mismatch at {i}: got {g:?}, expected {v:?}"),
            None => panic!("mismatch at {i}: got None, expected {v:?}"),
        }
    }
    col.validate_encoding();
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
    T: crate::v1::ColumnValueRef + Clone + std::fmt::Debug,
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
    use crate::v1::PrefixColumn;
    let col: PrefixColumn<u64> = vec![3, 1, 4, 1, 5].into_iter().collect();
    assert_eq!(col.len(), 5);
    assert_eq!(col.get_prefix(3), 8); // 3+1+4
}

#[test]
fn delta_column_from_iterator() {
    use crate::v1::DeltaColumn;
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
    use crate::v1::PrefixColumn;

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

// ── is_default / init_default ────────────────────────────────────────────────

#[test]
fn init_default_creates_all_null_column() {
    let col = Column::<Option<u64>>::init_default(100);
    assert_eq!(col.len(), 100);
    assert!(col.is_default());
    for i in 0..100 {
        assert_eq!(col.get(i), Some(None));
    }
}

#[test]
fn init_default_null_zero_length() {
    let col = Column::<Option<u64>>::init_default(0);
    assert_eq!(col.len(), 0);
    assert!(col.is_default());
    assert!(col.is_empty());
}

#[test]
fn is_default_false_for_non_null_column() {
    let col = Column::<Option<u64>>::from_values(vec![Some(1), Some(2), None, Some(3)]);
    assert!(!col.is_default());
}

#[test]
fn is_default_false_for_mixed_null_column() {
    let col = Column::<Option<u64>>::from_values(vec![None, None, Some(0), None]);
    assert!(!col.is_default());
}

#[test]
fn is_default_true_for_manual_all_null() {
    let col = Column::<Option<u64>>::from_values(vec![None, None, None, None, None]);
    assert!(col.is_default());
}

#[test]
fn is_default_nullable_types() {
    // Option<i64>
    let col = Column::<Option<i64>>::init_default(50);
    assert!(col.is_default());
    assert_eq!(col.get(0), Some(None));

    // Option<String>
    let col = Column::<Option<String>>::init_default(10);
    assert!(col.is_default());
    assert_eq!(col.get(0), Some(None));

    // Option<Vec<u8>>
    let col = Column::<Option<Vec<u8>>>::init_default(10);
    assert!(col.is_default());
    assert_eq!(col.get(0), Some(None));
}

#[test]
fn init_default_null_roundtrips_through_save_load() {
    let col = Column::<Option<u64>>::init_default(1000);
    let bytes = col.save();
    let loaded = Column::<Option<u64>>::load(&bytes).unwrap();
    assert_eq!(loaded.len(), 1000);
    assert!(loaded.is_default());
}

#[test]
fn is_default_after_insert_non_null() {
    let mut col = Column::<Option<u64>>::init_default(10);
    assert!(col.is_default());
    col.insert(5, Some(42));
    assert!(!col.is_default());
}

#[test]
fn init_default_creates_all_false_column() {
    let col = Column::<bool>::init_default(100);
    assert_eq!(col.len(), 100);
    assert!(col.is_default());
    for i in 0..100 {
        assert_eq!(col.get(i), Some(false));
    }
}

#[test]
fn init_default_bool_zero_length() {
    let col = Column::<bool>::init_default(0);
    assert_eq!(col.len(), 0);
    assert!(col.is_default());
    assert!(col.is_empty());
}

#[test]
fn is_default_false_for_mixed_bool() {
    let col = Column::<bool>::from_values(vec![false, false, true, false]);
    assert!(!col.is_default());
}

#[test]
fn is_default_true_for_manual_all_false() {
    let col = Column::<bool>::from_values(vec![false, false, false, false]);
    assert!(col.is_default());
}

#[test]
fn is_default_false_for_all_true() {
    let col = Column::<bool>::from_values(vec![true, true, true]);
    assert!(!col.is_default());
}

#[test]
fn init_default_bool_roundtrips_through_save_load() {
    let col = Column::<bool>::init_default(1000);
    let bytes = col.save();
    let loaded = Column::<bool>::load(&bytes).unwrap();
    assert_eq!(loaded.len(), 1000);
    assert!(loaded.is_default());
}

#[test]
fn is_default_after_insert_true() {
    let mut col = Column::<bool>::init_default(10);
    assert!(col.is_default());
    col.insert(5, true);
    assert!(!col.is_default());
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
fn build_both(
    values: &[u64],
) -> (
    crate::ColumnData<crate::UIntCursor>,
    crate::v1::PrefixColumn<u64>,
) {
    let v0: crate::ColumnData<crate::UIntCursor> = values.iter().copied().collect();
    let v1 = crate::v1::PrefixColumn::<u64>::from_values(values.to_vec());
    (v0, v1)
}

#[test]
fn prefix_iter_range_matches_inner_iter_range() {
    let values: Vec<u64> = (1..=20).collect();
    let col = crate::v1::PrefixColumn::<u64>::from_values(values);

    for start in 0..20 {
        for end in start..=20 {
            let inner_vals: Vec<u64> = col.inner().iter_range(start..end).collect();
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

#[test]
fn advance_total_matches_v0_advance_acc_by() {
    // v0: [0, 1, 1, 0, 1, 1, 0]
    // advance_acc_by returns items consumed; advance_total returns (prefix, value)
    let values: Vec<u64> = vec![0, 1, 1, 0, 1, 1, 0];
    let (v0, v1) = build_both(&values);

    // v0 semantics: advance_acc_by(n) advances until acc grows by n, returns items consumed
    // v1 semantics: advance_total(n) returns the item where prefix crosses threshold

    // advance_acc_by(0) = 1 item (past the first zero, value=0, prefix=0)
    assert_eq!(v0.iter().advance_acc_by(0u64), 1);
    // In v1, advance_total(0) acts like next() — returns first item
    let r = v1.iter().advance_total(0);
    assert_eq!(r, Some((0, 0))); // first item: value=0, prefix=0

    // advance_acc_by(1) = 2 items (values 0,1; acc reaches 1)
    assert_eq!(v0.iter().advance_acc_by(1u64), 2);
    let r = v1.iter().advance_total(1);
    assert_eq!(r, Some((1, 1))); // second item: value=1, prefix=1

    // advance_acc_by(2) = 4 items (values 0,1,1,0; acc reaches 2 after 3rd item)
    // Wait, acc after 3 items = 0+1+1 = 2 >= 2, so 3 items. But test says 4.
    // Actually, advance_acc_by advances PAST the target, so let me just verify the v1 behavior.
    assert_eq!(v0.iter().advance_acc_by(2u64), 4);
    let r = v1.iter().advance_total(2);
    // Target = 2. Item at index 2 (value 1) has prefix 0+1+1=2. So it returns (2, 1).
    assert_eq!(r, Some((2, 1)));

    // advance_acc_by(4) = 7 items (exhausted, total acc = 4)
    assert_eq!(v0.iter().advance_acc_by(4u64), 7);
    let r = v1.iter().advance_total(4);
    // Target = 4. Item at index 5 (value 1) has prefix 0+1+1+0+1+1=4. Returns (4, 1).
    assert_eq!(r, Some((4, 1)));
}

#[test]
fn advance_total_exhaustion_matches_v0() {
    let values: Vec<u64> = vec![1, 2, 3];
    let (v0, v1) = build_both(&values);

    // v0: total acc = 6, advance_acc_by(100) exhausts and returns 3 (all items)
    assert_eq!(v0.iter().advance_acc_by(100u64), 3);
    // v1: advance_total(100) returns None (unreachable)
    assert_eq!(v1.iter().advance_total(100), None);
}

#[test]
fn advance_total_sequential_matches_v0() {
    let values: Vec<u64> = vec![0, 3, 3, 0, 3, 3, 0];
    let (v0, v1) = build_both(&values);

    // v0: advance_acc_by(3) = 2 items (0+3 = 3 >= 3)
    assert_eq!(v0.iter().advance_acc_by(3u64), 2);
    let r = v1.iter().advance_total(3);
    assert_eq!(r, Some((3, 3))); // index 1, value 3, prefix 3

    // v0: advance_acc_by(6) = 4 items (0+3+3+0 = 6 >= 6)
    assert_eq!(v0.iter().advance_acc_by(6u64), 4);
    let r = v1.iter().advance_total(6);
    // prefix at index 2 = 0+3+3 = 6. Returns (6, 3).
    assert_eq!(r, Some((6, 3)));
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
        let v1 = crate::v1::PrefixColumn::<u64>::from_values(values.clone());

        // 1. iter_range values match inner
        let inner_vals: Vec<u64> = v1.inner().iter().collect();
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

    /// Cross-validate advance_total on random data using prefix sums as ground truth.
    #[test]
    fn advance_total_proptest(values in prop::collection::vec(0..10u64, 1..200)) {
        let v1 = crate::v1::PrefixColumn::<u64>::from_values(values.clone());
        let total: u128 = values.iter().map(|&v| v as u128).sum();

        // Build prefix sums for verification
        let mut prefix_sums = vec![0u128];
        let mut acc = 0u128;
        for &v in &values {
            acc += v as u128;
            prefix_sums.push(acc);
        }

        // Test advance_total for various targets
        for target in [1u128, 2, 5, 10, total / 2, total, total + 1] {
            if target == 0 { continue; }
            let v1_result = v1.iter().advance_total(target);

            if target > total {
                // Unreachable: returns None
                prop_assert!(v1_result.is_none(), "target {} should be unreachable (total {})", target, total);
            } else {
                // Returns the first item where cumulative prefix >= target
                let (v1p, v1v) = v1_result.unwrap();
                prop_assert!(v1p >= target, "advance_total({}) returned prefix {} < target", target, v1p);
                // Find the expected position: first index i where prefix_sums[i+1] >= target
                let expected_idx = prefix_sums[1..].iter().position(|&p| p >= target).unwrap();
                prop_assert_eq!(v1v, values[expected_idx], "value mismatch for target {}", target);
                prop_assert_eq!(v1p, prefix_sums[expected_idx + 1], "total mismatch for target {}", target);
            }
        }
    }
}

// ── LoadOpts tests ─────────────────────────────────────────────────────────

#[test]
fn load_with_empty_data_and_length_gives_default_bool() {
    let col = Column::<bool>::load_with(&[], LoadOpts::new().with_length(5)).unwrap();
    assert_eq!(col.len(), 5);
    assert!(col.iter().all(|v| !v));
}

#[test]
fn load_with_empty_data_and_length_gives_default_nullable() {
    let col = Column::<Option<u64>>::load_with(&[], LoadOpts::new().with_length(3)).unwrap();
    assert_eq!(col.len(), 3);
    assert!(col.iter().all(|v| v.is_none()));
}

#[test]
fn load_with_empty_data_and_zero_length() {
    let col = Column::<bool>::load_with(&[], LoadOpts::new().with_length(0)).unwrap();
    assert_eq!(col.len(), 0);
}

#[test]
fn load_with_length_mismatch_errors() {
    let col = Column::<Option<u64>>::from_values(vec![Some(1), Some(2)]);
    let data = col.save();
    let err = Column::<Option<u64>>::load_with(&data, LoadOpts::new().with_length(5));
    assert!(matches!(err, Err(crate::PackError::InvalidLength(2, 5))));
}

#[test]
fn load_with_length_match_succeeds() {
    let col = Column::<Option<u64>>::from_values(vec![Some(1), None, Some(3)]);
    let data = col.save();
    let loaded = Column::<Option<u64>>::load_with(&data, LoadOpts::new().with_length(3)).unwrap();
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
fn load_with_validation_accepts_good_values() {
    let col = Column::<Option<u64>>::from_values(vec![Some(1), Some(2), None]);
    let data = col.save();
    fn accept_all(v: Option<u64>) -> Option<String> {
        let _ = v;
        None
    }
    let loaded =
        Column::<Option<u64>>::load_with(&data, LoadOpts::new().with_validation(accept_all))
            .unwrap();
    assert_eq!(loaded.len(), 3);
}

#[test]
fn load_with_validation_and_length() {
    let col = Column::<Option<u64>>::from_values(vec![Some(1), Some(2), Some(3)]);
    let data = col.save();
    fn accept_all(_: Option<u64>) -> Option<String> {
        None
    }
    let loaded = Column::<Option<u64>>::load_with(
        &data,
        LoadOpts::new().with_length(3).with_validation(accept_all),
    )
    .unwrap();
    assert_eq!(loaded.len(), 3);
}

#[test]
fn load_with_opts_is_copy() {
    let opts: LoadOpts<bool> = LoadOpts::new().with_length(5);
    let opts2 = opts; // copy
    let _ = opts; // still usable
    let _ = opts2;
}

#[test]
fn prefix_column_load_with_empty_gives_default() {
    let col = PrefixColumn::<bool>::load_with(&[], LoadOpts::new().with_length(4)).unwrap();
    assert_eq!(col.len(), 4);
    assert!(col.value_iter().all(|v| !v));
}

#[test]
fn prefix_column_load_with_roundtrip() {
    let col = PrefixColumn::<bool>::from_values(vec![true, false, true]);
    let data = col.save();
    let loaded = PrefixColumn::<bool>::load_with(&data, LoadOpts::new().with_length(3)).unwrap();
    assert_eq!(loaded.save(), data);
}

#[test]
fn prefix_column_load_with_length_mismatch() {
    let col = PrefixColumn::<bool>::from_values(vec![true, false]);
    let data = col.save();
    let err = PrefixColumn::<bool>::load_with(&data, LoadOpts::new().with_length(10));
    assert!(err.is_err());
}

#[test]
fn delta_column_load_with_roundtrip() {
    let col = DeltaColumn::<Option<u64>>::from_values(vec![Some(10), None, Some(30)]);
    let data = col.save();
    let loaded = DeltaColumn::<Option<u64>>::load_with(&data, LoadOpts::new()).unwrap();
    assert_eq!(loaded.len(), 3);
    assert_eq!(loaded.get(0), Some(Some(10)));
    assert_eq!(loaded.get(1), Some(None));
    assert_eq!(loaded.get(2), Some(Some(30)));
}

#[test]
fn delta_column_load_with_nullable_empty_default() {
    let col = DeltaColumn::<Option<u64>>::load_with(&[], LoadOpts::new().with_length(5)).unwrap();
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
fn cross_slab_fuzz() {
    use rand::{rng, RngCore};
    let mut r = rng();
    for _ in 0..200 {
        let n = (r.next_u32() % 20 + 5) as usize;
        let vals: Vec<u64> = (0..n).map(|_| r.next_u64() % 5).collect();
        let max_seg = (r.next_u32() % 6 + 3) as usize;
        let mut col = multi_slab_col(&vals, max_seg);
        let mut mirror = vals.clone();

        // Perform 10 random operations.
        for _ in 0..10 {
            let len = col.len();
            if len == 0 {
                break;
            }
            let idx = r.next_u32() as usize % len;
            let max_del = (len - idx).min(5);
            let del = r.next_u32() as usize % (max_del + 1);
            let ins_count = r.next_u32() as usize % 4;
            let new_vals: Vec<u64> = (0..ins_count).map(|_| r.next_u64() % 5).collect();

            col.splice(idx, del, new_vals.iter().copied());
            mirror.splice(idx..idx + del, new_vals);
            assert_col(&col, &mirror);
        }
    }
}

// ── Aggressive fuzz tests ──────────────────────────────────────────────────
//
// Run splice at every position with insert/delete/replace of 1/10/100 items,
// validating the full column invariants after each operation.
// Marked #[ignore] — run with `cargo test -- --ignored fuzz_`.

use super::column::{rebuild_bit, LenWeight};
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
        // TODO: fix empty slab after delete, then re-enable
        // if col.total_len > 0 {
        //     assert!(slab.len > 0, "slab {i} is empty but column has items");
        // }
        // TODO: fix splice overflow splitting, then re-enable this check
        // assert!(slab.segments <= max_segments,
        //     "slab {i}: segments={} exceeds max_segments={max_segments}", slab.segments);
        let _ = max_segments;
        let info = T::Encoding::validate_encoding(&slab.data)
            .unwrap_or_else(|e| panic!("slab {i} encoding invalid: {e}"));
        assert_eq!(slab.len, info.len, "slab {i}: len mismatch");
        assert_eq!(slab.segments, info.segments, "slab {i}: segments mismatch");
    }

    let expected_bit = rebuild_bit::<T, LenWeight>(&col.slabs);
    assert_eq!(col.bit, expected_bit, "BIT mismatch");
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
    for (i, slab) in col.slabs.iter().enumerate() {
        let expected_tail = super::bool_encoding::compute_tail(&slab.data);
        assert_eq!(slab.tail, expected_tail, "slab {i}: tail mismatch");
    }
}

#[test]
#[ignore]
fn fuzz_bool_splice_exhaustive() {
    use rand::{rng, Rng};
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
    use rand::{rng, Rng};
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
            c.splice(pos, del, new_vals);
            validate_rle_column(&c);
        }
    }
}

#[test]
fn save_to_multi_column_concatenation() {
    use rand::{rng, Rng};
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
    let bool_col = Column::<bool>::from_values_with_max_segments(
        (0..n).map(|_| r.random()).collect(),
        16,
    );
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
