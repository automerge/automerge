//! Deliberately small text-only patch consumer. Hydrate cannot replay Mark patches.
use automerge::{
    iter::Span, marks::MarkSet, Automerge, Change, ObjId, Patch, PatchAction, PatchLog, Prop,
    ReadDoc, ScalarValue, TextEncoding,
};
use std::collections::BTreeMap;

pub(super) type Format = BTreeMap<String, ScalarValue>;
pub(super) type FormattedText = Vec<(u32, Format)>;

pub(super) fn units(text: &str, encoding: TextEncoding) -> Vec<u32> {
    match encoding {
        TextEncoding::UnicodeCodePoint => text.chars().map(u32::from).collect(),
        TextEncoding::Utf8CodeUnit => text.bytes().map(u32::from).collect(),
        TextEncoding::Utf16CodeUnit => text.encode_utf16().map(u32::from).collect(),
        TextEncoding::GraphemeCluster => panic!("not part of this observer's scope"),
    }
}

fn format(marks: Option<&MarkSet>) -> Format {
    marks
        .into_iter()
        .flat_map(MarkSet::iter)
        .filter(|(_, value)| !value.is_null())
        .map(|(name, value)| (name.to_owned(), value.clone()))
        .collect()
}

fn formatted(text: &str, marks: Format, encoding: TextEncoding) -> FormattedText {
    units(text, encoding)
        .into_iter()
        .map(|unit| (unit, marks.clone()))
        .collect()
}

pub(super) fn observe(doc: &Automerge, text: &ObjId) -> FormattedText {
    let result: FormattedText = doc
        .spans(text)
        .unwrap()
        .flat_map(|span| match span {
            Span::Text { text, marks } => {
                formatted(&text, format(marks.as_deref()), doc.text_encoding())
            }
            Span::Block(_) => panic!("block objects are outside this observer's scope"),
        })
        .collect();
    assert_eq!(
        result.iter().map(|(unit, _)| *unit).collect::<Vec<_>>(),
        units(&doc.text(text).unwrap(), doc.text_encoding()),
        "spans must agree with text"
    );
    result
}

fn apply(view: &mut FormattedText, text: &ObjId, encoding: TextEncoding, patches: &[Patch]) {
    for patch in patches {
        assert_eq!(&patch.obj, text, "unexpected non-text patch: {patch:?}");
        match &patch.action {
            PatchAction::SpliceText {
                index,
                value,
                marks,
            } => {
                assert!(*index <= view.len());
                // SpliceText carries the complete inserted format; it does not
                // inherit unspecified marks from the removed or adjacent text.
                view.splice(
                    *index..*index,
                    formatted(&value.make_string(), format(marks.as_ref()), encoding),
                );
            }
            PatchAction::DeleteSeq { index, length } => {
                view.drain(*index..*index + *length);
            }
            PatchAction::Mark { marks } => {
                for mark in marks {
                    for (_, format) in &mut view[mark.start..mark.end] {
                        if mark.value.is_null() {
                            format.remove(mark.name());
                        } else {
                            format.insert(mark.name().to_owned(), mark.value.clone());
                        }
                    }
                }
            }
            PatchAction::Conflict {
                prop: Prop::Seq(index),
            } => {
                assert!(*index < view.len()); // no change to text or formatting
            }
            action => panic!("unsupported text patch: {action:?}"),
        }
    }
}

pub(super) fn replay(mut doc: Automerge, text: &ObjId, changes: Vec<Change>) -> Automerge {
    let mut view = observe(&doc, text);
    let mut log = PatchLog::active();
    doc.apply_changes_log_patches(changes, &mut log).unwrap();
    let patches = doc.make_patches(&mut log);
    apply(&mut view, text, doc.text_encoding(), &patches);
    assert_eq!(
        view,
        observe(&doc, text),
        "encoding={:?}, patches={patches:?}",
        doc.text_encoding()
    );
    doc
}
