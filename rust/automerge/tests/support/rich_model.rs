//! A rich-text model for patch replay.
//!
//! `hydrate::Value` cannot hold marks or block markers, so replaying
//! `Mark`/`SplitBlock` patches into a hydrated value is vacuous. This model
//! represents a text object as a sequence of spans — text runs with their
//! active marks, plus block markers — mirroring the shape of
//! [`automerge::ReadDoc::spans`], and updates itself from
//! `SpliceText` / `DeleteSeq` / `Mark` / `Insert` (block) patches.

use std::collections::BTreeMap;

use automerge::{
    hydrate, Automerge, ObjId, ObjType, Patch, PatchAction, ReadDoc, ScalarValue, Value,
};

/// One span of a text object: a text run with its active marks, or a block
/// marker.
#[derive(Debug, Clone, PartialEq)]
pub enum Span {
    Text {
        text: String,
        marks: BTreeMap<String, ScalarValue>,
    },
    Block(hydrate::Value),
}

/// A text object normalised to spans, comparable against
/// [`RichText::from_doc`] after patch replay via [`RichText::apply`].
#[derive(Debug, Clone, PartialEq)]
pub struct RichText(pub Vec<Span>);

/// Internal per-position representation: one character (with its marks) or
/// one block marker. Patch indices address these cells directly.
#[derive(Debug, Clone, PartialEq)]
enum Cell {
    Char {
        ch: char,
        marks: BTreeMap<String, ScalarValue>,
    },
    Block(hydrate::Value),
}

impl RichText {
    /// Read the current spans of `text` out of `doc`.
    pub fn from_doc(doc: &Automerge, text: &ObjId) -> Self {
        let spans = doc
            .spans(text)
            .expect("text object has spans")
            .map(|span| match span {
                automerge::iter::Span::Text { text, marks } => Span::Text {
                    text,
                    marks: marks
                        .as_deref()
                        .map(|m| m.iter().map(|(k, v)| (k.to_string(), v.clone())).collect())
                        .unwrap_or_default(),
                },
                automerge::iter::Span::Block(map) => Span::Block(hydrate::Value::Map(map)),
            })
            .collect();
        RichText(spans)
    }

    /// Update the model from one patch addressed at the text object.
    pub fn apply(&mut self, patch: &Patch) {
        let mut cells = self.cells();
        match &patch.action {
            PatchAction::SpliceText {
                index,
                value,
                marks,
            } => {
                let marks: BTreeMap<String, ScalarValue> = marks
                    .as_ref()
                    .map(|m| m.iter().map(|(k, v)| (k.to_string(), v.clone())).collect())
                    .unwrap_or_default();
                for (offset, ch) in value.make_string().chars().enumerate() {
                    cells.insert(
                        index + offset,
                        Cell::Char {
                            ch,
                            marks: marks.clone(),
                        },
                    );
                }
            }
            PatchAction::DeleteSeq { index, length } => {
                cells.drain(*index..index + length);
            }
            PatchAction::Mark { marks } => {
                for mark in marks {
                    for cell in &mut cells[mark.start..mark.end] {
                        if let Cell::Char { marks, .. } = cell {
                            if mark.value == ScalarValue::Null {
                                marks.remove(mark.name.as_str());
                            } else {
                                marks.insert(mark.name.to_string(), mark.value.clone());
                            }
                        }
                    }
                }
            }
            PatchAction::Insert { index, values } => {
                for (offset, (value, _, _)) in values.iter().enumerate() {
                    assert_eq!(
                        value,
                        &Value::Object(ObjType::Map),
                        "only block-marker inserts reach a text object"
                    );
                    cells.insert(
                        index + offset,
                        Cell::Block(hydrate::Value::Map(hydrate::Map::default())),
                    );
                }
            }
            other => panic!("unexpected patch action on a text object: {other:?}"),
        }
        *self = Self::from_cells(cells);
    }

    fn cells(&self) -> Vec<Cell> {
        self.0
            .iter()
            .flat_map(|span| match span {
                Span::Text { text, marks } => text
                    .chars()
                    .map(|ch| Cell::Char {
                        ch,
                        marks: marks.clone(),
                    })
                    .collect::<Vec<_>>(),
                Span::Block(value) => vec![Cell::Block(value.clone())],
            })
            .collect()
    }

    fn from_cells(cells: Vec<Cell>) -> Self {
        let mut spans: Vec<Span> = Vec::new();
        for cell in cells {
            match cell {
                Cell::Char { ch, marks } => match spans.last_mut() {
                    Some(Span::Text {
                        text,
                        marks: last_marks,
                    }) if *last_marks == marks => text.push(ch),
                    _ => spans.push(Span::Text {
                        text: ch.to_string(),
                        marks,
                    }),
                },
                Cell::Block(value) => spans.push(Span::Block(value)),
            }
        }
        RichText(spans)
    }
}
