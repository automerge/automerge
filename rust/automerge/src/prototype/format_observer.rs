//! Test-only rich-text patch consumer. It never recomputes formatting from the after document.
use crate::clock::Clock;
use crate::{
    hydrate, marks::MarkSet, Automerge, ObjId, ObjType, Patch, PatchAction, ScalarValue, Span,
    TextEncoding, Value,
};
use std::collections::BTreeMap;

type Marks = BTreeMap<String, ScalarValue>;
#[derive(Clone, Debug, PartialEq)]
pub(super) enum Atom {
    Char(char, Marks),
    Block(hydrate::Value),
}
#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct RichText(pub(super) Vec<Atom>);
fn marks(set: Option<&MarkSet>) -> Marks {
    set.into_iter()
        .flat_map(|s| s.iter())
        .filter(|(_, v)| !v.is_null())
        .map(|(n, v)| (n.to_owned(), v.clone()))
        .collect()
}
impl RichText {
    pub(super) fn from_spans(spans: impl Iterator<Item = Span>) -> Self {
        let mut atoms = Vec::new();
        for span in spans {
            match span {
                Span::Text { text, marks: set } => {
                    let m = marks(set.as_deref());
                    atoms.extend(text.chars().map(|c| Atom::Char(c, m.clone())));
                }
                Span::Block(map) => atoms.push(Atom::Block(map.into())),
            }
        }
        Self(atoms)
    }
    pub(super) fn read(doc: &Automerge, obj: &ObjId, clock: Option<Clock>) -> Self {
        if doc
            .parents_for(obj, clock.clone())
            .unwrap()
            .visible_path()
            .is_none()
        {
            return Self::default();
        }
        Self::from_spans(doc.spans_for(obj, clock).unwrap())
    }
    pub(super) fn plain(&self) -> String {
        self.0
            .iter()
            .map(|a| match a {
                Atom::Char(c, _) => *c,
                Atom::Block(_) => '\u{fffc}',
            })
            .collect()
    }
    pub(super) fn marked(&self, name: &str) -> String {
        self.0
            .iter()
            .filter_map(|a| match a {
                Atom::Char(c, m) if m.contains_key(name) => Some(*c),
                _ => None,
            })
            .collect()
    }
    fn index(&self, offset: usize, encoding: TextEncoding) -> usize {
        let mut n = 0;
        for (i, a) in self.0.iter().enumerate() {
            if n == offset {
                return i;
            }
            n += match a {
                Atom::Char(c, _) => width(*c, encoding),
                Atom::Block(_) => width('\u{fffc}', encoding),
            };
        }
        assert_eq!(n, offset, "patch splits a code point or exceeds text");
        self.0.len()
    }
    /// Replay attachment deletion/replacement along the captured before path.
    /// The path is from the before-view, not inferred from after-view formatting.
    pub(super) fn apply_at_path(
        &mut self,
        obj: &ObjId,
        mut path: Vec<(ObjId, crate::Prop)>,
        patches: &[Patch],
        encoding: TextEncoding,
    ) {
        for p in patches {
            for (parent, prop) in &mut path {
                if parent != &p.obj {
                    continue;
                }
                match (&p.action, prop) {
                    (PatchAction::DeleteMap { key }, crate::Prop::Map(name)) if key == name => {
                        self.0.clear()
                    }
                    (PatchAction::PutMap { key, .. }, crate::Prop::Map(name)) if key == name => {
                        self.0.clear()
                    }
                    (PatchAction::PutSeq { index, .. }, crate::Prop::Seq(at)) if index == at => {
                        self.0.clear()
                    }
                    (PatchAction::DeleteSeq { index, length }, crate::Prop::Seq(at)) => {
                        if *at >= *index && *at < index + length {
                            self.0.clear();
                        } else if *at >= index + length {
                            *at -= length;
                        }
                    }
                    (PatchAction::Insert { index, values }, crate::Prop::Seq(at))
                        if index <= at =>
                    {
                        *at += values.len()
                    }
                    _ => {}
                }
            }
            self.apply(obj, std::slice::from_ref(p), encoding);
        }
    }
    pub(super) fn apply(&mut self, obj: &ObjId, patches: &[Patch], encoding: TextEncoding) {
        for p in patches {
            if &p.obj != obj {
                // Preserve block payloads rather than silently discarding Span::Block.
                if let Some(position) = p.path.iter().position(|(id, _)| id == obj) {
                    let index = p.path[position].1.as_index().unwrap();
                    let index = self.index(index, encoding);
                    let Atom::Block(value) = &mut self.0[index] else {
                        panic!("block patch targets text");
                    };
                    let mut nested = p.clone();
                    nested.path = p.path[position + 1..].to_vec();
                    value.apply_patches(encoding, [nested]).unwrap();
                }
                continue;
            }
            match &p.action {
                PatchAction::SpliceText {
                    index,
                    value,
                    marks: set,
                } => {
                    let at = self.index(*index, encoding);
                    let m = marks(set.as_ref());
                    self.0.splice(
                        at..at,
                        value
                            .make_string()
                            .chars()
                            .map(|c| Atom::Char(c, m.clone())),
                    );
                }
                PatchAction::DeleteSeq { index, length } => {
                    let end = self.index(index + length, encoding);
                    let start = self.index(*index, encoding);
                    self.0.drain(start..end);
                }
                PatchAction::Mark { marks } => {
                    for mark in marks {
                        let start = self.index(mark.start, encoding);
                        let end = self.index(mark.end, encoding);
                        for atom in &mut self.0[start..end] {
                            if let Atom::Char(_, m) = atom {
                                if mark.value.is_null() {
                                    m.remove(mark.name.as_str());
                                } else {
                                    m.insert(mark.name.to_string(), mark.value.clone());
                                }
                            }
                        }
                    }
                }
                PatchAction::Insert { index, values } => {
                    let at = self.index(*index, encoding);
                    let atoms = values.iter().map(|(v, _, _)| {
                        assert!(matches!(v, Value::Object(ObjType::Map)));
                        Atom::Block(hydrate::Value::map())
                    });
                    self.0.splice(at..at, atoms);
                }
                _ => panic!("unsupported text patch: {p:?}"),
            }
        }
    }
}
fn width(c: char, encoding: TextEncoding) -> usize {
    match encoding {
        TextEncoding::UnicodeCodePoint => 1,
        TextEncoding::Utf8CodeUnit => c.len_utf8(),
        TextEncoding::Utf16CodeUnit => c.len_utf16(),
        TextEncoding::GraphemeCluster => panic!("observer does not implement grapheme indexing"),
    }
}
