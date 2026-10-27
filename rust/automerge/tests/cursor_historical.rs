//! Cursor positions at historical heads.
//!
//! Reading a cursor position at `heads` must give the same answer as reading
//! it on a document whose current state is the state at `heads` (a fork at
//! `heads`). In particular a cursor whose element is deleted at `heads` and
//! has no visible element after it there resolves to the length of the
//! sequence at `heads` (`MoveCursor::After`) or to the previous visible index
//! (`MoveCursor::Before`), rather than to `InvalidCursor`.

use automerge::{
    transaction::Transactable, AutoCommit, AutomergeError, ChangeHash, Cursor, MoveCursor, ObjId,
    ObjType, Prop, ReadDoc, ScalarValue, TextEncoding, ROOT,
};
use proptest::prelude::*;

const ENCODINGS: [TextEncoding; 4] = [
    TextEncoding::UnicodeCodePoint,
    TextEncoding::Utf8CodeUnit,
    TextEncoding::Utf16CodeUnit,
    TextEncoding::GraphemeCluster,
];

/// Resolve `cursor` at `heads` on `doc` and on a fork of `doc` at `heads`,
/// assert both agree and return the result.
fn historical_and_forked(
    doc: &mut AutoCommit,
    obj: &ObjId,
    cursor: &Cursor,
    heads: &[ChangeHash],
) -> Result<usize, AutomergeError> {
    let historical = doc.get_cursor_position(obj, cursor, Some(heads));
    let fork = doc.fork_at(heads).unwrap();
    let forked = fork.get_cursor_position(obj, cursor, None);
    match (&historical, &forked) {
        (Ok(h), Ok(f)) => assert_eq!(h, f, "historical and forked positions differ"),
        (Err(AutomergeError::InvalidCursor(_)), Err(AutomergeError::InvalidCursor(_))) => {}
        _ => panic!(
            "historical and forked positions differ: {:?} vs {:?}",
            historical, forked
        ),
    }
    historical
}

fn assert_position(
    doc: &mut AutoCommit,
    obj: &ObjId,
    cursor: &Cursor,
    heads: &[ChangeHash],
    expected: usize,
) {
    let pos = historical_and_forked(doc, obj, cursor, heads).unwrap();
    assert_eq!(pos, expected);
}

#[test]
fn reproduction_trailing_tombstone_text() {
    let mut doc = AutoCommit::new();
    let t = doc.put_object(ROOT, "t", ObjType::Text).unwrap();
    doc.splice_text(&t, 0, 0, "xab").unwrap();
    let cursor = doc.get_cursor(&t, 2, None).unwrap();
    doc.splice_text(&t, 2, 1, "").unwrap();
    let heads = doc.get_heads();
    assert_eq!(
        doc.get_cursor_position(&t, &cursor, Some(&heads)).unwrap(),
        2
    );
    doc.splice_text(&t, 0, 1, "").unwrap();
    assert_eq!(
        doc.get_cursor_position(&t, &cursor, Some(&heads)).unwrap(),
        2
    );
    assert_eq!(doc.get_cursor_position(&t, &cursor, None).unwrap(), 1);
}

/// Text in every encoding, with multi-unit characters so that the index
/// widths differ between encodings.
#[test]
fn trailing_tombstones_text_all_encodings() {
    for encoding in ENCODINGS {
        let mut doc = AutoCommit::new_with_encoding(encoding);
        let t = doc.put_object(ROOT, "t", ObjType::Text).unwrap();
        doc.splice_text(&t, 0, 0, "x😀").unwrap();
        let prefix_len = doc.length(&t);
        doc.splice_text(&t, prefix_len, 0, "a👩‍👩‍👧‍👦bc").unwrap();
        let len = doc.length(&t);
        // index of "a"
        let a = prefix_len;
        // indexes of "b" and "c", the last two characters
        let c = len - 1;
        let b = len - 2;

        let after_b = doc.get_cursor(&t, b, None).unwrap();
        let after_c = doc.get_cursor(&t, c, None).unwrap();
        let before_b = doc
            .get_cursor_moving(&t, b, None, MoveCursor::Before)
            .unwrap();
        let before_c = doc
            .get_cursor_moving(&t, c, None, MoveCursor::Before)
            .unwrap();
        // everything before "b"
        let len_at_heads = b;
        // `Before` walks back along the insertion keys: c -> b -> the last
        // code point of the family emoji
        let before_expected = match encoding {
            TextEncoding::GraphemeCluster => None,
            _ => Some(b - text_width(encoding, "👦")),
        };

        // delete "bc": both are trailing tombstones
        doc.splice_text(&t, b, 2, "").unwrap();
        let heads = doc.get_heads();
        assert_eq!(doc.length_at(&t, &heads), len_at_heads);

        // later changes: delete at the front, append text after the tombstones
        doc.splice_text(&t, 0, 1, "").unwrap();
        let now = doc.length(&t);
        doc.splice_text(&t, now, 0, "yz").unwrap();

        for cursor in [&after_b, &after_c] {
            assert_position(&mut doc, &t, cursor, &heads, len_at_heads);
        }
        for cursor in [&before_b, &before_c] {
            let pos = historical_and_forked(&mut doc, &t, cursor, &heads).unwrap();
            assert!(
                pos >= a && pos < b,
                "{:?}: {} not in {}..{}",
                encoding,
                pos,
                a,
                b
            );
            if let Some(expected) = before_expected {
                assert_eq!(pos, expected, "{:?}", encoding);
            }
        }
    }
}

#[test]
fn trailing_tombstone_text_move_before() {
    let mut doc = AutoCommit::new();
    let t = doc.put_object(ROOT, "t", ObjType::Text).unwrap();
    doc.splice_text(&t, 0, 0, "xab").unwrap();
    let cursor = doc
        .get_cursor_moving(&t, 2, None, MoveCursor::Before)
        .unwrap();
    doc.splice_text(&t, 2, 1, "").unwrap();
    let heads = doc.get_heads();
    doc.splice_text(&t, 0, 1, "").unwrap();
    assert_position(&mut doc, &t, &cursor, &heads, 1);
    assert_eq!(doc.get_cursor_position(&t, &cursor, None).unwrap(), 0);
}

#[test]
fn whole_sequence_deleted_at_heads() {
    let mut doc = AutoCommit::new();
    let t = doc.put_object(ROOT, "t", ObjType::Text).unwrap();
    doc.splice_text(&t, 0, 0, "ab").unwrap();
    let cursors = [
        doc.get_cursor(&t, 0, None).unwrap(),
        doc.get_cursor(&t, 1, None).unwrap(),
        doc.get_cursor_moving(&t, 0, None, MoveCursor::Before)
            .unwrap(),
        doc.get_cursor_moving(&t, 1, None, MoveCursor::Before)
            .unwrap(),
    ];
    doc.splice_text(&t, 0, 2, "").unwrap();
    let heads = doc.get_heads();
    doc.splice_text(&t, 0, 0, "new").unwrap();
    for cursor in &cursors {
        assert_position(&mut doc, &t, cursor, &heads, 0);
    }
}

/// Elements that are visible now but were inserted after `heads` must not
/// count towards the position at `heads`, whether they are ordered before or
/// after the tombstone.
#[test]
fn trailing_tombstone_followed_by_later_inserts() {
    let mut doc = AutoCommit::new();
    let t = doc.put_object(ROOT, "t", ObjType::Text).unwrap();
    doc.splice_text(&t, 0, 0, "xab").unwrap();
    let after = doc.get_cursor(&t, 2, None).unwrap();
    let before = doc
        .get_cursor_moving(&t, 2, None, MoveCursor::Before)
        .unwrap();
    // a concurrent peer inserts after "b", so its text is ordered after the
    // tombstone
    let mut peer = doc.fork();
    peer.splice_text(&t, 3, 0, "pq").unwrap();

    doc.splice_text(&t, 2, 1, "").unwrap();
    let heads = doc.get_heads();
    // inserted after "a", so ordered before the tombstone of "b"
    doc.splice_text(&t, 2, 0, "yz").unwrap();
    doc.merge(&mut peer).unwrap();
    assert_eq!(doc.text(&t).unwrap(), "xayzpq");

    assert_position(&mut doc, &t, &after, &heads, 2);
    assert_position(&mut doc, &t, &before, &heads, 1);
    assert_eq!(doc.get_cursor_position(&t, &after, None).unwrap(), 4);
    assert_eq!(doc.get_cursor_position(&t, &before, None).unwrap(), 1);
}

/// A cursor for an element that does not exist yet at `heads` is invalid at
/// `heads`, whether or not visible elements follow it now.
#[test]
fn element_not_yet_existing_at_heads() {
    let mut doc = AutoCommit::new();
    let t = doc.put_object(ROOT, "t", ObjType::Text).unwrap();
    doc.splice_text(&t, 0, 0, "ab").unwrap();
    let heads = doc.get_heads();
    // at the end
    doc.splice_text(&t, 2, 0, "c").unwrap();
    // in the middle, with "b" visible after it at heads
    doc.splice_text(&t, 1, 0, "m").unwrap();
    assert_eq!(doc.text(&t).unwrap(), "ambc");
    for index in [1, 3] {
        for move_cursor in [MoveCursor::After, MoveCursor::Before] {
            let cursor = doc.get_cursor_moving(&t, index, None, move_cursor).unwrap();
            let res = historical_and_forked(&mut doc, &t, &cursor, &heads);
            assert!(
                matches!(res, Err(AutomergeError::InvalidCursor(_))),
                "{:?}",
                res
            );
        }
    }
}

#[test]
fn trailing_tombstones_list() {
    let mut doc = AutoCommit::new();
    let l = doc.put_object(ROOT, "l", ObjType::List).unwrap();
    for (i, v) in [1, 2, 3, 4].into_iter().enumerate() {
        doc.insert(&l, i, v).unwrap();
    }
    let after: Vec<_> = (1..4)
        .map(|i| doc.get_cursor(&l, i, None).unwrap())
        .collect();
    let before: Vec<_> = (1..4)
        .map(|i| {
            doc.get_cursor_moving(&l, i, None, MoveCursor::Before)
                .unwrap()
        })
        .collect();
    // delete the last three elements: [1]
    for _ in 0..3 {
        doc.delete(&l, 1).unwrap();
    }
    let heads = doc.get_heads();
    assert_eq!(doc.length_at(&l, &heads), 1);
    // later changes: delete at the front, append after the tombstones
    doc.delete(&l, 0).unwrap();
    doc.insert(&l, 0, 5).unwrap();
    doc.insert(&l, 1, 6).unwrap();

    for cursor in &after {
        assert_position(&mut doc, &l, cursor, &heads, 1);
    }
    for cursor in &before {
        assert_position(&mut doc, &l, cursor, &heads, 0);
    }
}

#[test]
fn trailing_tombstone_list_conflicting_put() {
    // the element is deleted at heads but was overwritten before; neither
    // value is visible at heads
    let mut doc = AutoCommit::new();
    let l = doc.put_object(ROOT, "l", ObjType::List).unwrap();
    doc.insert(&l, 0, "a").unwrap();
    doc.insert(&l, 1, "b").unwrap();
    let cursor = doc.get_cursor(&l, 1, None).unwrap();
    doc.put(&l, 1, "c").unwrap();
    doc.delete(&l, 1).unwrap();
    let heads = doc.get_heads();
    doc.insert(&l, 1, "d").unwrap();
    assert_position(&mut doc, &l, &cursor, &heads, 1);
}

/// `parents_at` locates a list element by the same lookup as cursors do.
#[test]
fn parents_at_of_object_in_trailing_tombstone() {
    let mut doc = AutoCommit::new();
    let l = doc.put_object(ROOT, "l", ObjType::List).unwrap();
    doc.insert(&l, 0, 1).unwrap();
    let m = doc.insert_object(&l, 1, ObjType::Map).unwrap();
    doc.delete(&l, 1).unwrap();
    let heads = doc.get_heads();
    doc.insert(&l, 1, 2).unwrap();

    let path = doc.parents_at(&m, &heads).unwrap().path();
    let fork = doc.fork_at(&heads).unwrap();
    let forked = fork.parents(&m).unwrap().path();
    assert_eq!(path, forked);
    assert_eq!(
        path,
        vec![(ROOT, Prop::Map("l".into())), (l.clone(), Prop::Seq(1))]
    );
    assert_eq!(doc.parents_at(&m, &heads).unwrap().visible_path(), None);
}

#[derive(Debug, Clone)]
enum Step {
    Insert { pos: usize, text: String },
    Delete { pos: usize, len: usize },
    Cursor { pos: usize, before: bool },
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        (any::<usize>(), "[ab😀é👩‍👩‍👧‍👦]{1,3}").prop_map(|(pos, text)| Step::Insert { pos, text }),
        (any::<usize>(), 1..4usize).prop_map(|(pos, len)| Step::Delete { pos, len }),
        (any::<usize>(), any::<bool>()).prop_map(|(pos, before)| Step::Cursor { pos, before }),
    ]
}

fn encoding() -> impl Strategy<Value = TextEncoding> {
    prop::sample::select(ENCODINGS.to_vec())
}

fn move_cursor(before: bool) -> MoveCursor {
    if before {
        MoveCursor::Before
    } else {
        MoveCursor::After
    }
}

/// Apply `steps` to a text and a list object (the list gets one element per
/// inserted string), committing after each step, then check that every
/// cursor resolves at every intermediate heads exactly as on a fork at
/// those heads.
fn check_steps(encoding: TextEncoding, steps: &[Step]) -> Result<(), TestCaseError> {
    let mut doc = AutoCommit::new_with_encoding(encoding);
    let t = doc.put_object(ROOT, "t", ObjType::Text).unwrap();
    let l = doc.put_object(ROOT, "l", ObjType::List).unwrap();
    let mut cursors: Vec<(ObjId, Cursor)> = Vec::new();
    let mut all_heads = vec![doc.get_heads()];
    for step in steps {
        match step {
            Step::Insert { pos, text } => {
                // splice at a code point boundary; with grapheme encoding
                // a code point boundary may not be a grapheme boundary, so
                // append instead
                let index = if encoding == TextEncoding::GraphemeCluster {
                    doc.length(&t)
                } else {
                    let s = doc.text(&t).unwrap();
                    let n = s.chars().count();
                    let prefix: String = s.chars().take(pos % (n + 1)).collect();
                    text_width(encoding, &prefix)
                };
                doc.splice_text(&t, index, 0, text).unwrap();
                let llen = doc.length(&l);
                doc.insert(&l, pos % (llen + 1), ScalarValue::Str(text.into()))
                    .unwrap();
            }
            Step::Delete { pos, len } => {
                let s = doc.text(&t).unwrap();
                let tlen = doc.length(&t);
                if encoding == TextEncoding::GraphemeCluster {
                    if tlen > 0 {
                        let start = pos % tlen;
                        let del = (*len).min(tlen - start);
                        doc.splice_text(&t, start, del as isize, "").unwrap();
                    }
                } else {
                    // delete whole code points
                    let chars: Vec<char> = s.chars().collect();
                    if !chars.is_empty() {
                        let start = pos % chars.len();
                        let end = (start + len).min(chars.len());
                        let prefix: String = chars[..start].iter().collect();
                        let deleted: String = chars[start..end].iter().collect();
                        doc.splice_text(
                            &t,
                            text_width(encoding, &prefix),
                            text_width(encoding, &deleted) as isize,
                            "",
                        )
                        .unwrap();
                    }
                }
                let llen = doc.length(&l);
                if llen > 0 {
                    let start = pos % llen;
                    let del = (*len).min(llen - start);
                    for _ in 0..del {
                        doc.delete(&l, start).unwrap();
                    }
                }
            }
            Step::Cursor { pos, before } => {
                for obj in [&t, &l] {
                    let len = doc.length(obj);
                    if len == 0 {
                        continue;
                    }
                    let c = doc
                        .get_cursor_moving(obj, pos % len, None, move_cursor(*before))
                        .unwrap();
                    cursors.push((obj.clone(), c));
                }
            }
        }
        doc.commit();
        all_heads.push(doc.get_heads());
    }
    for heads in &all_heads {
        let fork = doc.fork_at(heads).unwrap();
        for (obj, cursor) in &cursors {
            let historical = doc.get_cursor_position(obj, cursor, Some(heads));
            let forked = fork.get_cursor_position(obj, cursor, None);
            match (&historical, &forked) {
                (Ok(h), Ok(f)) => prop_assert_eq!(h, f),
                (Err(AutomergeError::InvalidCursor(_)), Err(AutomergeError::InvalidCursor(_))) => {}
                _ => prop_assert!(false, "historical {:?} vs forked {:?}", historical, forked),
            }
            if let Ok(p) = historical {
                prop_assert!(p <= doc.length_at(obj, heads));
            }
        }
    }
    Ok(())
}

fn text_width(encoding: TextEncoding, s: &str) -> usize {
    match encoding {
        TextEncoding::UnicodeCodePoint => s.chars().count(),
        TextEncoding::Utf8CodeUnit => s.len(),
        TextEncoding::Utf16CodeUnit => s.encode_utf16().count(),
        TextEncoding::GraphemeCluster => unreachable!("not computed for graphemes"),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn historical_cursors_match_forks(
        encoding in encoding(),
        steps in prop::collection::vec(step(), 1..24),
    ) {
        check_steps(encoding, &steps)?;
    }
}
