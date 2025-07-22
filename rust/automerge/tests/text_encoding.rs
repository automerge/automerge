use std::borrow::Cow;

// Test that looking up by index works across diferent ways of calculating
// the index
//
// Places in the codebase where we use indexes for manipulating text:
//
// # Reading values
//
// - ReadDoc::length and ReadDoc::length_at
// - ReadDoc::marks and ReadDoc::marks_at (via the Mark::start and Mark::end)
//   fields
// - ReadDoc::get_cursor
// - ReadDoc::get and ReadDoc::get_at
// - ReadDoc::get_all and ReadDoc::get_all_at
//
// # Writing values
//
// - Transactable::put
// - Transactable::insert
// - Transactable::delete
// - Transactable::splice_text
// - Transactable::mark
// - Transactable::unmark
// - Transactable::split_block
//
// # Patches
//
// - PatchAction::PutSeq
// - PatchAction::Insert
// - PatchAction::SpliceText
// - PatchAction::Conflict
// - PatchAction::DeleteSeq
// - PatchAction::Mark
//
// The task here is to ensure that all these methods work correctly when
// different ways of calculating the index are used. There are four different
// ways of calculating indexes in a text object:
//
// - The unicode code point index within a stream of characters
// - The UTF-8 code unit offset, i.e. the byte offset into a UTF-8 encoding of
//   the text
// - The UTF-16 code unit offset
// - The grapheme cluster index
use automerge::{
    marks::{ExpandMark, Mark},
    transaction::Transactable,
    AutoCommit, ObjId, ObjType, ReadDoc, ScalarValue, Value, ROOT,
};

#[derive(Debug, PartialEq, Clone, Copy)]
enum Encoding {
    UnicodeCodePoint,
    Utf8CodeUnit,
    Utf16CodeUnit,
    GraphemeCluster,
}

impl From<Encoding> for automerge::TextEncoding {
    fn from(value: Encoding) -> Self {
        match value {
            Encoding::UnicodeCodePoint => automerge::TextEncoding::UnicodeCodePoint,
            Encoding::Utf8CodeUnit => automerge::TextEncoding::Utf8CodeUnit,
            Encoding::Utf16CodeUnit => automerge::TextEncoding::Utf16CodeUnit,
            Encoding::GraphemeCluster => automerge::TextEncoding::GraphemeCluster,
        }
    }
}

impl std::fmt::Display for Encoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnicodeCodePoint => write!(f, "UnicodeCodePoint"),
            Self::Utf8CodeUnit => write!(f, "Utf8CodeUnit"),
            Self::Utf16CodeUnit => write!(f, "Utf16CodeUnit"),
            Self::GraphemeCluster => write!(f, "GraphemeCluster"),
        }
    }
}

enum Expected<T> {
    Always(T),
    ByEncoding {
        code_point: T,
        utf8: T,
        utf16: T,
        grapheme: T,
    },
}

impl<T: PartialEq + std::fmt::Debug> Expected<T> {
    fn assert(&self, actual: &T, encoding: Encoding) {
        match self {
            Self::Always(t) => assert_eq!(actual, t, "failed for {}", encoding),
            Self::ByEncoding {
                code_point,
                utf8,
                utf16,
                grapheme,
            } => match encoding {
                Encoding::UnicodeCodePoint => {
                    assert_eq!(actual, code_point, "failed for {}", encoding)
                }
                Encoding::Utf8CodeUnit => assert_eq!(actual, utf8, "failed for {}", encoding),
                Encoding::Utf16CodeUnit => assert_eq!(actual, utf16, "failed for {}", encoding),
                Encoding::GraphemeCluster => {
                    assert_eq!(actual, grapheme, "failed for {}", encoding)
                }
            },
        }
    }
}

struct Scenario<F, T> {
    text: &'static str,
    action: F,
    expected: Expected<T>,
}

impl<F: Fn(&mut AutoCommit, &automerge::ObjId, Encoding) -> T, T: PartialEq + std::fmt::Debug>
    Scenario<F, T>
{
    fn run(&self) {
        for encoding in [
            Encoding::UnicodeCodePoint,
            Encoding::Utf8CodeUnit,
            Encoding::Utf16CodeUnit,
            Encoding::GraphemeCluster,
        ] {
            self.run_with_encoding(encoding);
        }
    }

    fn run_with_encoding(&self, encoding: Encoding) {
        let mut doc = AutoCommit::new_with_encoding(encoding.into());
        let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
        doc.splice_text(&text, 0, 0, self.text).unwrap();
        let result = (self.action)(&mut doc, &text, encoding);
        self.expected.assert(&result, encoding);
    }
}

impl<
        F: Fn(&mut AutoCommit, &automerge::ObjId, Encoding) -> Result<T, String>,
        T: PartialEq + std::fmt::Debug,
    > Scenario<F, T>
{
    fn run_fallible(&self) {
        for encoding in [
            Encoding::UnicodeCodePoint,
            Encoding::Utf8CodeUnit,
            Encoding::Utf16CodeUnit,
            Encoding::GraphemeCluster,
        ] {
            self.run_fallible_with_encoding(encoding);
        }
    }

    fn run_fallible_with_encoding(&self, encoding: Encoding) {
        let mut doc = AutoCommit::new_with_encoding(encoding.into());
        let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
        doc.splice_text(&text, 0, 0, self.text).unwrap();
        let result = (self.action)(&mut doc, &text, encoding);
        match result {
            Ok(result) => self.expected.assert(&result, encoding),
            Err(e) => panic!("failed for {}: {}", encoding, e),
        }
    }
}

// All of the following tests use the 👩‍👩‍👧‍👦 emoji which is the following sequence of code points:
//
// - U+1F469 WOM
// - U+200D ZWJ
// - U+1F467 GIRL
// - U+1f466 BOY
//
// This is a useful test case because it is:
//
// * A single grapheme cluster
// * 7 code points
// * 11 utf-16 code units
// * 25 utf-8 code units

#[test]
fn length() {
    Scenario {
        text: "hello👩‍👩‍👧‍👦",
        action: |doc: &mut AutoCommit, text: &ObjId, _encoding: Encoding| doc.length(text),
        expected: Expected::ByEncoding {
            code_point: 12,
            utf8: 30,
            utf16: 16,
            grapheme: 6,
        },
    }
    .run();
}

#[test]
fn splice_text() {
    Scenario {
        text: "hello 👩‍👩‍👧‍👦 world",
        action: |doc: &mut AutoCommit, text: &ObjId, encoding: Encoding| {
            let insert_index = match encoding {
                Encoding::UnicodeCodePoint => 14,
                Encoding::Utf8CodeUnit => 32,
                Encoding::Utf16CodeUnit => 18,
                Encoding::GraphemeCluster => 8,
            };
            doc.splice_text(text, insert_index, 0, "beautiful ")
                .unwrap();
            doc.text(text).unwrap()
        },
        expected: Expected::Always("hello 👩‍👩‍👧‍👦 beautiful world".to_string()),
    }
    .run();
}

#[test]
fn mark() {
    Scenario {
        text: "he👩‍👩‍👧‍👦llo",
        action: |doc: &mut AutoCommit, text: &ObjId, encoding: Encoding| {
            let end_index = match encoding {
                Encoding::UnicodeCodePoint => 11,
                Encoding::Utf8CodeUnit => 27,
                Encoding::Utf16CodeUnit => 13,
                Encoding::GraphemeCluster => 4,
            };
            let mark = Mark::new("bold".to_string(), true, 1, end_index);
            doc.mark(text, mark, ExpandMark::Both).unwrap();
            doc.marks(text)
                .unwrap()
                .into_iter()
                .map(|m| (m.start, m.end))
                .collect::<Vec<_>>()
        },
        expected: Expected::ByEncoding {
            code_point: vec![(1, 11)],
            utf8: vec![(1, 27)],
            utf16: vec![(1, 13)],
            grapheme: vec![(1, 4)],
        },
    }
    .run()
}

#[test]
fn unmark() {
    Scenario {
        text: "he👩‍👩‍👧‍👦llo",
        action: |doc: &mut AutoCommit, text: &ObjId, encoding: Encoding| {
            let end_index = match encoding {
                Encoding::UnicodeCodePoint => 11,
                Encoding::Utf8CodeUnit => 27,
                Encoding::Utf16CodeUnit => 13,
                Encoding::GraphemeCluster => 4,
            };
            let mark = Mark::new("bold".to_string(), true, 1, end_index);
            doc.mark(text, mark, ExpandMark::Both).unwrap();
            doc.unmark(text, "bold", 1, end_index, ExpandMark::Both)
                .unwrap();
            doc.marks(text)
                .unwrap()
                .into_iter()
                .map(|m| (m.start, m.end))
                .collect::<Vec<_>>()
        },
        expected: Expected::Always(Vec::new()),
    }
    .run()
}

#[test]
fn cursors() {
    // Get a cursor for the first 'l' in 'he👩‍👩‍👧‍👦llo', then insert a '👩‍👩‍👧‍👦' before
    // the 'l' and lookup the index of the cursor afterwards.
    Scenario {
        text: "he👩‍👩‍👧‍👦llo",
        action: |doc: &mut AutoCommit, text: &ObjId, encoding: Encoding| {
            let cursor_index = match encoding {
                Encoding::UnicodeCodePoint => 9,
                Encoding::Utf8CodeUnit => 27,
                Encoding::Utf16CodeUnit => 13,
                Encoding::GraphemeCluster => 3,
            };
            let cursor = doc.get_cursor(text, cursor_index, None).unwrap();
            doc.splice_text(text, 2, 0, "👩‍👩‍👧‍👦").unwrap();
            doc.get_cursor_position(text, &cursor, None).unwrap()
        },
        expected: Expected::ByEncoding {
            code_point: 16,
            utf8: 52,
            utf16: 24,
            grapheme: 4,
        },
    }
    .run()
}

#[test]
fn get() {
    Scenario {
        text: "he👩‍👩‍👧‍👦lo",
        action: |doc: &mut AutoCommit, text: &ObjId, encoding: Encoding| {
            let index = match encoding {
                Encoding::UnicodeCodePoint => 9,
                Encoding::Utf8CodeUnit => 27,
                Encoding::Utf16CodeUnit => 13,
                Encoding::GraphemeCluster => 3,
            };
            match doc.get(text, index).unwrap() {
                Some((Value::Scalar(s), _)) => match s.as_ref() {
                    ScalarValue::Str(s) => Some(s.to_string()),
                    _ => None,
                },
                _ => None,
            }
        },
        expected: Expected::Always(Some("l".to_string())),
    }
    .run()
}

#[test]
fn put() {
    Scenario {
        text: "he👩‍👩‍👧‍👦llo",
        action: |doc: &mut AutoCommit, text: &ObjId, encoding: Encoding| {
            let index = match encoding {
                Encoding::UnicodeCodePoint => 9,
                Encoding::Utf8CodeUnit => 27,
                Encoding::Utf16CodeUnit => 13,
                Encoding::GraphemeCluster => 3,
            };
            doc.put(text, index, "L").unwrap();
            doc.text(text).unwrap()
        },
        expected: Expected::Always("he👩‍👩‍👧‍👦Llo".to_string()),
    }
    .run()
}

#[test]
fn insert() {
    Scenario {
        text: "he👩‍👩‍👧‍👦llo",
        action: |doc: &mut AutoCommit, text: &ObjId, encoding: Encoding| {
            let index = match encoding {
                Encoding::UnicodeCodePoint => 9,
                Encoding::Utf8CodeUnit => 27,
                Encoding::Utf16CodeUnit => 13,
                Encoding::GraphemeCluster => 3,
            };
            doc.insert(text, index, "L").unwrap();
            doc.text(text).unwrap()
        },
        expected: Expected::Always("he👩‍👩‍👧‍👦Lllo".to_string()),
    }
    .run()
}

#[test]
fn delete() {
    Scenario {
        text: "he👩‍👩‍👧‍👦llo",
        action: |doc: &mut AutoCommit, text: &ObjId, encoding: Encoding| {
            let index = match encoding {
                Encoding::UnicodeCodePoint => 9,
                Encoding::Utf8CodeUnit => 27,
                Encoding::Utf16CodeUnit => 13,
                Encoding::GraphemeCluster => 3,
            };
            doc.delete(text, index).unwrap();
            doc.text(text).unwrap()
        },
        expected: Expected::Always("he👩‍👩‍👧‍👦lo".to_string()),
    }
    .run()
}

#[test]
fn split_block() {
    Scenario {
        text: "he👩‍👩‍👧‍👦llo",
        action: |doc: &mut AutoCommit, text: &ObjId, encoding: Encoding| {
            let index = match encoding {
                Encoding::UnicodeCodePoint => 9,
                Encoding::Utf8CodeUnit => 27,
                Encoding::Utf16CodeUnit => 13,
                Encoding::GraphemeCluster => 3,
            };
            doc.split_block(text, index).unwrap();
            doc.spans(text)
                .unwrap()
                .filter_map(|s| match s {
                    automerge::iter::Span::Text {
                        text: val,
                        marks: _,
                    } => Some(val),
                    automerge::iter::Span::Block(_) => None,
                })
                .collect::<Vec<_>>()
        },
        expected: Expected::Always(vec!["he👩‍👩‍👧‍👦".to_string(), "llo".to_string()]),
    }
    .run()
}

#[test]
fn patch_put_seq() {
    Scenario {
        text: "he👩‍👩‍👧‍👦llo",
        action: |doc: &mut AutoCommit, text: &ObjId, encoding: Encoding| {
            let index = match encoding {
                Encoding::UnicodeCodePoint => 9,
                Encoding::Utf8CodeUnit => 27,
                Encoding::Utf16CodeUnit => 13,
                Encoding::GraphemeCluster => 3,
            };
            doc.update_diff_cursor();
            println!(" ENCODING = {:?}", encoding);
            doc.put(text, index, "L").unwrap();
            let indexes = doc
                .diff_incremental()
                .into_iter()
                .map(|p| match p {
                    automerge::Patch {
                        action: automerge::PatchAction::PutSeq { index, value, .. },
                        ..
                    } => {
                        if value.0 == Value::Scalar(Cow::Owned(ScalarValue::Str("L".into()))) {
                            Ok(index)
                        } else {
                            Err(format!("unexpected value {}", value.0).to_string())
                        }
                    }
                    other => Err(format!("unexpected patch action {:?}", other).to_string()),
                })
                .collect::<Result<Vec<_>, _>>()?;
            if indexes.len() != 1 {
                return Err(format!("expected 1 patch, got {}", indexes.len()));
            }
            Ok(indexes[0])
        },
        expected: Expected::ByEncoding {
            code_point: 9,
            utf8: 27,
            utf16: 13,
            grapheme: 3,
        },
    }
    .run_fallible()
}

#[test]
fn patch_insert() {
    Scenario {
        text: "he👩‍👩‍👧‍👦llo",
        action: |doc: &mut AutoCommit, text: &ObjId, encoding: Encoding| {
            let index = match encoding {
                Encoding::UnicodeCodePoint => 9,
                Encoding::Utf8CodeUnit => 27,
                Encoding::Utf16CodeUnit => 13,
                Encoding::GraphemeCluster => 3,
            };
            doc.update_diff_cursor();
            doc.insert(text, index, "L").unwrap();
            let indexes = doc
                .diff_incremental()
                .into_iter()
                .map(|p| match p {
                    automerge::Patch {
                        action: automerge::PatchAction::SpliceText { index, value, .. },
                        ..
                    } => {
                        if value.make_string() != "L" {
                            Err(format!("unexpected value {}", value.make_string()).to_string())
                        } else {
                            Ok(index)
                        }
                    }
                    other => Err(format!("unexpected patch action {:?}", other).to_string()),
                })
                .collect::<Result<Vec<_>, _>>()?;
            if indexes.len() != 1 {
                return Err(format!("expected 1 patch, got {}", indexes.len()));
            }
            Ok(indexes[0])
        },
        expected: Expected::ByEncoding {
            code_point: 9,
            utf8: 27,
            utf16: 13,
            grapheme: 3,
        },
    }
    .run_fallible()
}

#[test]
fn patch_splice_text() {
    Scenario {
        text: "he👩‍👩‍👧‍👦llo",
        action: |doc: &mut AutoCommit, text: &ObjId, encoding: Encoding| {
            let index = match encoding {
                Encoding::UnicodeCodePoint => 9,
                Encoding::Utf8CodeUnit => 27,
                Encoding::Utf16CodeUnit => 13,
                Encoding::GraphemeCluster => 3,
            };
            doc.update_diff_cursor();
            doc.splice_text(text, index, 0, "L").unwrap();
            let indexes = doc
                .diff_incremental()
                .into_iter()
                .map(|p| match p {
                    automerge::Patch {
                        action: automerge::PatchAction::SpliceText { index, value, .. },
                        ..
                    } => {
                        if value.make_string() != "L" {
                            Err(format!("unexpected value {}", value.make_string()).to_string())
                        } else {
                            Ok(index)
                        }
                    }
                    other => Err(format!("unexpected patch action {:?}", other).to_string()),
                })
                .collect::<Result<Vec<_>, _>>()?;
            if indexes.len() != 1 {
                return Err(format!("expected 1 patch, got {}", indexes.len()));
            }
            Ok(indexes[0])
        },
        expected: Expected::ByEncoding {
            code_point: 9,
            utf8: 27,
            utf16: 13,
            grapheme: 3,
        },
    }
    .run_fallible()
}

#[test]
fn patch_delete() {
    Scenario {
        text: "he👩‍👩‍👧‍👦llo",
        action: |doc: &mut AutoCommit, text: &ObjId, encoding: Encoding| {
            let index = match encoding {
                Encoding::UnicodeCodePoint => 9,
                Encoding::Utf8CodeUnit => 27,
                Encoding::Utf16CodeUnit => 13,
                Encoding::GraphemeCluster => 3,
            };
            doc.update_diff_cursor();
            doc.delete(text, index).unwrap();
            let indexes = doc
                .diff_incremental()
                .into_iter()
                .map(|p| match p {
                    automerge::Patch {
                        action: automerge::PatchAction::DeleteSeq { index, length, .. },
                        ..
                    } => {
                        if length != 1 {
                            Err(format!("unexpected length {}", length).to_string())
                        } else {
                            Ok(index)
                        }
                    }
                    other => Err(format!("unexpected patch action {:?}", other).to_string()),
                })
                .collect::<Result<Vec<_>, _>>()?;
            if indexes.len() != 1 {
                return Err(format!("expected 1 patch, got {}", indexes.len()));
            }
            Ok(indexes[0])
        },
        expected: Expected::ByEncoding {
            code_point: 9,
            utf8: 27,
            utf16: 13,
            grapheme: 3,
        },
    }
    .run_fallible()
}

#[test]
fn patch_mark() {
    Scenario {
        text: "he👩‍👩‍👧‍👦llo",
        action: |doc: &mut AutoCommit, text: &ObjId, encoding: Encoding| {
            let end_index = match encoding {
                Encoding::UnicodeCodePoint => 9,
                Encoding::Utf8CodeUnit => 27,
                Encoding::Utf16CodeUnit => 13,
                Encoding::GraphemeCluster => 3,
            };
            let mark = Mark::new("bold".to_string(), true, 1, end_index);
            doc.diff_incremental();
            doc.mark(text, mark, ExpandMark::Both).unwrap();
            let indexes = doc
                .diff_incremental()
                .into_iter()
                .filter(|p| p.obj == *text)
                .map(|p| match p {
                    automerge::Patch {
                        action: automerge::PatchAction::Mark { mut marks },
                        ..
                    } => {
                        if marks.len() != 1 {
                            return Err(format!("expected 1 mark, got {}", marks.len()));
                        }
                        let mark = marks.pop().unwrap();
                        if mark.name() != "bold" {
                            return Err(format!("unexpected mark name {}", mark.name()));
                        }
                        Ok((mark.start, mark.end))
                    }
                    other => Err(format!("unexpected patch action {:?}", other).to_string()),
                })
                .collect::<Result<Vec<_>, _>>()?;
            if indexes.len() != 1 {
                return Err(format!("expected 1 patch, got {}", indexes.len()));
            }
            Ok(indexes[0])
        },
        expected: Expected::ByEncoding {
            code_point: (1, 9),
            utf8: (1, 27),
            utf16: (1, 13),
            grapheme: (1, 3),
        },
    }
    .run_fallible()
}
