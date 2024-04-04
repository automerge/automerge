use std::borrow::Cow;
use std::sync::Arc;

use itertools::Itertools;

use crate::{
    marks::{MarkSet, MarkStateMachine},
    patches::{PatchLog, TextRepresentation},
    types::{Key, ListEncoding, ObjId, Op, OpId},
    Automerge, ObjType, OpType, Value,
};

#[derive(Debug, Default)]
struct TextSpan {
    text: String,
    start: usize,
    marks: Option<Arc<MarkSet>>,
}

#[derive(Debug, Default)]
struct TextState<'a> {
    len: usize,
    spans: Vec<TextSpan>,
    marks: MarkStateMachine<'a>,
}

impl<'a> TextState<'a> {
    fn push_str(&mut self, text: &str, len: usize) {
        if let Some(last_span) = self.spans.last_mut() {
            last_span.text.push_str(text);
        } else {
            self.spans.push(TextSpan {
                text: text.to_owned(),
                start: 0,
                marks: None,
            });
        }
        self.len += len;
    }

    fn push_mark(&mut self) {
        let marks = self.marks.current();
        if let Some(last) = self.spans.last_mut() {
            if last.marks.as_ref() == marks {
                return;
            }
            if last.text.is_empty() {
                last.marks = marks.cloned();
                return;
            }
        }
        self.spans.push(TextSpan {
            text: "".to_owned(),
            start: self.len,
            marks: marks.cloned(),
        })
    }
}

struct Put<'a> {
    value: Value<'a>,
    key: Key,
    id: OpId,
}

/// Traverse the "current" state of the document, logging patches to `patch_log`.
///
/// The "current" state of the document is the set of visible operations. This function will
/// traverse that set of operations and add corresponding patches to `patch_log` as it encounters
/// values.
///
/// Due to only notifying of visible operations the [`PatchLog`] will only be called with `put`,
/// `insert`, and `splice`, operations.
pub(crate) fn log_current_state_patches(doc: &Automerge, patch_log: &mut PatchLog) {
    // The OpSet already exposes operations in the order they appear in the document.
    // `OpSet::iter_objs` iterates over the objects in causal order, this means that parent objects
    // will always appear before their children. Furthermore, the operations within each object are
    // ordered by key (which means by their position in a sequence for sequences).
    //
    // Effectively then we iterate over each object, then we group the operations in the object by
    // key and for each key find the visible operations for that key. Then we notify the patch log
    // for each of those visible operations.
    for (obj, ops) in doc.ops().iter_objs() {
        if obj.typ == ObjType::Text && matches!(patch_log.text_rep(), TextRepresentation::String) {
            log_text_patches(doc, patch_log, &obj.id, ops)
        } else if obj.typ.is_sequence() {
            log_list_patches(doc, patch_log, &obj.id, ops);
        } else {
            log_map_patches(doc, patch_log, &obj.id, ops);
        }
    }
}

fn log_text_patches<'a, I: Iterator<Item = Op<'a>>>(
    doc: &'a Automerge,
    patch_log: &mut PatchLog,
    obj: &ObjId,
    ops: I,
) {
    let ops_by_key = ops.group_by(|o| o.elemid_or_key());
    let encoding = ListEncoding::Text;
    let state = TextState::default();
    let state = ops_by_key
        .into_iter()
        .fold(state, |mut state, (_key, key_ops)| {
            if let Some(o) = key_ops.filter(|o| o.visible_or_mark(None)).last() {
                match o.action() {
                    OpType::Make(_) | OpType::Put(_) => {
                        state.push_str(o.as_str(), o.width(encoding))
                    }
                    OpType::MarkBegin(_, data) => {
                        if state.marks.mark_begin(*o.id(), data, &doc.ops.osd) {
                            state.push_mark();
                        }
                    }
                    OpType::MarkEnd(_) => {
                        if state.marks.mark_end(*o.id(), &doc.ops.osd) {
                            state.push_mark();
                        }
                    }
                    OpType::Increment(_) | OpType::Delete => {}
                }
            }
            state
        });
    for span in state.spans {
        if !span.text.is_empty() {
            patch_log.splice(*obj, span.start, span.text.as_str(), span.marks);
        }
    }
}

fn log_list_patches<'a, I: Iterator<Item = Op<'a>>>(
    doc: &'a Automerge,
    patch_log: &mut PatchLog,
    obj: &ObjId,
    ops: I,
) {
    let mut marks = MarkStateMachine::default();
    let ops_by_key = ops.group_by(|o| o.elemid_or_key());
    let mut len = 0;
    //let mut finished = Vec::new();
    ops_by_key
        .into_iter()
        .filter_map(|(_key, key_ops)| {
            key_ops
                .filter(|o| o.visible_or_mark(None))
                .filter_map(|o| match o.action() {
                    OpType::Make(obj_type) => {
                        Some((Value::Object(*obj_type), *o.id(), marks.current().cloned()))
                    }
                    OpType::Put(value) => Some((
                        Value::Scalar(Cow::Borrowed(value)),
                        *o.id(),
                        marks.current().cloned(),
                    )),
                    OpType::MarkBegin(_, data) => {
                        marks.mark_begin(*o.id(), data, &doc.ops.osd);
                        None
                    }
                    OpType::MarkEnd(_) => {
                        marks.mark_end(*o.id(), &doc.ops.osd);
                        None
                    }
                    _ => None,
                })
                .enumerate()
                .last()
                .map(|value| {
                    let pos = len;
                    len += 1; // increment - side effect
                    (pos, value)
                })
        })
        .for_each(|(index, (val_enum, (value, opid, marks)))| {
            let conflict = val_enum > 0;
            patch_log.insert(*obj, index, value.clone().into(), opid, conflict, marks);
        });
}

fn log_map_key_patches<'a, I: Iterator<Item = Op<'a>>>(
    (key, key_ops): (Key, I),
) -> Option<(usize, Put<'a>)> {
    key_ops
        .filter(|o| o.visible())
        .filter_map(|o| match o.action() {
            OpType::Make(obj_type) => {
                let value = Value::Object(*obj_type);
                Some(Put {
                    value,
                    key,
                    id: *o.id(),
                })
            }
            OpType::Put(value) => {
                let value = Value::Scalar(Cow::Borrowed(value));
                Some(Put {
                    value,
                    key,
                    id: *o.id(),
                })
            }
            _ => None,
        })
        .enumerate()
        .last()
}

fn log_map_patches<'a, I: Iterator<Item = Op<'a>>>(
    doc: &'a Automerge,
    patch_log: &mut PatchLog,
    obj: &ObjId,
    ops: I,
) {
    let ops_by_key = ops.group_by(|o| *o.key());
    ops_by_key
        .into_iter()
        .filter_map(log_map_key_patches)
        .for_each(|(i, put)| {
            if let Some(prop_index) = put.key.prop_index() {
                if let Some(key) = doc.ops().osd.props.safe_get(prop_index) {
                    let conflict = i > 0;
                    patch_log.put_map(*obj, key, put.value.into(), put.id, conflict, false);
                }
            }
        });
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, fs};

    use crate::{
        patches::{PatchLog, TextRepresentation},
        transaction::Transactable,
        Automerge, ObjType, Patch, PatchAction, Prop, Value,
    };

    // Patches often carry a "tagged value", which is a value and the OpID of the op which
    // created that value. For a lot of values (i.e. any scalar value) we don't care about the
    // opid. This type implements `PartialEq` for the `Untagged` variant by ignoring the tag, which
    // allows us to express tests which don't care about the tag.
    #[derive(Clone, Debug)]
    enum PatchValue {
        Tagged(crate::Value<'static>, crate::ObjId),
        Untagged(crate::Value<'static>),
    }

    impl<'a> From<(Value<'a>, crate::ObjId, bool)> for PatchValue {
        fn from(value: (Value<'a>, crate::ObjId, bool)) -> Self {
            Self::Tagged(value.0.into_owned(), value.1)
        }
    }

    impl<'a> From<(Value<'a>, crate::ObjId)> for PatchValue {
        fn from(value: (Value<'a>, crate::ObjId)) -> Self {
            Self::Tagged(value.0.into_owned(), value.1)
        }
    }

    impl PartialEq<PatchValue> for PatchValue {
        fn eq(&self, other: &PatchValue) -> bool {
            match (self, other) {
                (Self::Tagged(v1, o1), Self::Tagged(v2, o2)) => equal_vals(v1, v2) && o1 == o2,
                (Self::Untagged(v1), Self::Untagged(v2)) => equal_vals(v1, v2),
                (Self::Tagged(v1, _), Self::Untagged(v2)) => equal_vals(v1, v2),
                (Self::Untagged(v1), Self::Tagged(v2, _)) => equal_vals(v1, v2),
            }
        }
    }

    /// Consider counters equal if they have the same current value
    fn equal_vals(v1: &Value<'_>, v2: &Value<'_>) -> bool {
        match (v1, v2) {
            (Value::Scalar(v1), Value::Scalar(v2)) => match (v1.as_ref(), v2.as_ref()) {
                (crate::ScalarValue::Counter(c1), crate::ScalarValue::Counter(c2)) => {
                    c1.current == c2.current
                }
                _ => v1 == v2,
            },
            _ => v1 == v2,
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    enum ObservedPatch {
        Put {
            obj: crate::ObjId,
            prop: Prop,
            value: PatchValue,
            conflict: bool,
        },
        Insert {
            obj: crate::ObjId,
            index: usize,
            value: PatchValue,
        },
        SpliceText {
            obj: crate::ObjId,
            index: usize,
            chars: String,
        },
    }

    // A Vec<ObservedPatch> is pretty hard to look at in a test failure. This wrapper prints the
    // calls out in a nice table so it's easier to see what's different
    #[derive(Clone, PartialEq)]
    struct Patches(Vec<ObservedPatch>);

    impl From<Vec<Patch>> for Patches {
        fn from(patches: Vec<Patch>) -> Self {
            let oc = patches.into_iter().fold(Vec::new(), |mut acc, patch| {
                match patch {
                    Patch {
                        obj,
                        action: PatchAction::SpliceText { index, value, .. },
                        ..
                    } => acc.push(ObservedPatch::SpliceText {
                        obj,
                        index,
                        chars: value.make_string(),
                    }),
                    Patch {
                        obj,
                        action:
                            PatchAction::PutMap {
                                key,
                                value,
                                conflict,
                            },
                        ..
                    } => acc.push(ObservedPatch::Put {
                        obj,
                        prop: key.into(),
                        value: value.into(),
                        conflict,
                    }),
                    Patch {
                        obj,
                        action:
                            PatchAction::PutSeq {
                                index,
                                value,
                                conflict,
                            },
                        ..
                    } => acc.push(ObservedPatch::Put {
                        obj,
                        prop: index.into(),
                        value: value.into(),
                        conflict,
                    }),
                    Patch {
                        obj,
                        action: PatchAction::Insert { index, values, .. },
                        ..
                    } => {
                        for (i, v) in values.iter().enumerate() {
                            acc.push(ObservedPatch::Insert {
                                obj: obj.clone(),
                                index: index + i,
                                value: v.clone().into(),
                            })
                        }
                    }
                    _ => panic!("Current state should only log put, splice, and insert ops"),
                };
                acc
            });
            Patches(oc)
        }
    }

    impl std::fmt::Debug for Patches {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let mut table = prettytable::Table::new();
            table.set_format(*prettytable::format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
            table.set_titles(prettytable::row![
                "Op", "Object", "Property", "Value", "Conflict"
            ]);
            for call in &self.0 {
                match call {
                    ObservedPatch::Put {
                        obj,
                        prop,
                        value,
                        conflict,
                    } => {
                        table.add_row(prettytable::row![
                            "Put",
                            format!("{}", obj),
                            prop,
                            match value {
                                PatchValue::Tagged(v, o) => format!("{} ({})", v, o),
                                PatchValue::Untagged(v) => format!("{}", v),
                            },
                            conflict
                        ]);
                    }
                    ObservedPatch::Insert { obj, index, value } => {
                        table.add_row(prettytable::row![
                            "Insert",
                            format!("{}", obj),
                            index,
                            match value {
                                PatchValue::Tagged(v, o) => format!("{} ({})", v, o),
                                PatchValue::Untagged(v) => format!("{}", v),
                            },
                            ""
                        ]);
                    }
                    ObservedPatch::SpliceText { obj, index, chars } => {
                        table.add_row(prettytable::row![
                            "SpliceText",
                            format!("{}", obj),
                            index,
                            chars,
                            ""
                        ]);
                    }
                }
            }
            let mut out = Vec::new();
            table.print(&mut out).unwrap();
            write!(f, "\n{}\n", String::from_utf8(out).unwrap())
        }
    }

    #[test]
    fn basic_test() {
        let mut doc = crate::AutoCommit::new();
        doc.put(crate::ROOT, "key", "value").unwrap();
        let map = doc.put_object(crate::ROOT, "map", ObjType::Map).unwrap();
        doc.put(&map, "nested_key", "value").unwrap();
        let list = doc.put_object(crate::ROOT, "list", ObjType::List).unwrap();
        doc.insert(&list, 0, "value").unwrap();
        let text = doc.put_object(crate::ROOT, "text", ObjType::Text).unwrap();
        doc.insert(&text, 0, "a").unwrap();

        let p = doc.document().current_state(TextRepresentation::default());

        assert_eq!(
            Patches::from(p),
            Patches(vec![
                ObservedPatch::Put {
                    obj: crate::ROOT,
                    prop: "key".into(),
                    value: PatchValue::Untagged("value".into()),
                    conflict: false,
                },
                ObservedPatch::Put {
                    obj: crate::ROOT,
                    prop: "list".into(),
                    value: PatchValue::Tagged(Value::Object(ObjType::List), list.clone()),
                    conflict: false,
                },
                ObservedPatch::Put {
                    obj: crate::ROOT,
                    prop: "map".into(),
                    value: PatchValue::Tagged(Value::Object(ObjType::Map), map.clone()),
                    conflict: false,
                },
                ObservedPatch::Put {
                    obj: crate::ROOT,
                    prop: "text".into(),
                    value: PatchValue::Tagged(Value::Object(ObjType::Text), text.clone()),
                    conflict: false,
                },
                ObservedPatch::Put {
                    obj: map.clone(),
                    prop: "nested_key".into(),
                    value: PatchValue::Untagged("value".into()),
                    conflict: false,
                },
                ObservedPatch::Insert {
                    obj: list,
                    index: 0,
                    value: PatchValue::Untagged("value".into()),
                },
                ObservedPatch::Insert {
                    obj: text,
                    index: 0,
                    value: PatchValue::Untagged("a".into()),
                },
            ])
        );
    }

    #[test]
    fn test_deleted_ops_omitted() {
        let mut doc = crate::AutoCommit::new();
        doc.put(crate::ROOT, "key", "value").unwrap();
        doc.delete(crate::ROOT, "key").unwrap();
        let map = doc.put_object(crate::ROOT, "map", ObjType::Map).unwrap();
        doc.put(&map, "nested_key", "value").unwrap();
        doc.delete(&map, "nested_key").unwrap();
        let list = doc.put_object(crate::ROOT, "list", ObjType::List).unwrap();
        doc.insert(&list, 0, "value").unwrap();
        doc.delete(&list, 0).unwrap();
        let text = doc.put_object(crate::ROOT, "text", ObjType::Text).unwrap();
        doc.insert(&text, 0, "a").unwrap();
        doc.delete(&text, 0).unwrap();

        doc.put_object(crate::ROOT, "deleted_map", ObjType::Map)
            .unwrap();
        doc.delete(crate::ROOT, "deleted_map").unwrap();
        doc.put_object(crate::ROOT, "deleted_list", ObjType::List)
            .unwrap();
        doc.delete(crate::ROOT, "deleted_list").unwrap();
        doc.put_object(crate::ROOT, "deleted_text", ObjType::Text)
            .unwrap();
        doc.delete(crate::ROOT, "deleted_text").unwrap();

        let p = doc.document().current_state(TextRepresentation::default());

        assert_eq!(
            Patches::from(p),
            Patches(vec![
                ObservedPatch::Put {
                    obj: crate::ROOT,
                    prop: "list".into(),
                    value: PatchValue::Tagged(Value::Object(ObjType::List), list.clone()),
                    conflict: false,
                },
                ObservedPatch::Put {
                    obj: crate::ROOT,
                    prop: "map".into(),
                    value: PatchValue::Tagged(Value::Object(ObjType::Map), map.clone()),
                    conflict: false,
                },
                ObservedPatch::Put {
                    obj: crate::ROOT,
                    prop: "text".into(),
                    value: PatchValue::Tagged(Value::Object(ObjType::Text), text.clone()),
                    conflict: false,
                },
            ])
        );
    }

    #[test]
    fn test_text_spliced() {
        let mut doc = crate::AutoCommit::new();
        let text = doc.put_object(crate::ROOT, "text", ObjType::Text).unwrap();
        doc.insert(&text, 0, "a").unwrap();
        doc.splice_text(&text, 1, 0, "bcdef").unwrap();
        doc.splice_text(&text, 2, 2, "g").unwrap();

        doc.set_text_rep(TextRepresentation::String);
        let p = doc.document().current_state(TextRepresentation::String);

        assert_eq!(
            Patches::from(p),
            Patches(vec![
                ObservedPatch::Put {
                    obj: crate::ROOT,
                    prop: "text".into(),
                    value: PatchValue::Tagged(Value::Object(ObjType::Text), text.clone()),
                    conflict: false,
                },
                ObservedPatch::SpliceText {
                    obj: text,
                    index: 0,
                    chars: "abgef".to_string()
                }
            ])
        );
    }

    #[test]
    fn test_counters() {
        let actor1 = crate::ActorId::from("aa".as_bytes());
        let actor2 = crate::ActorId::from("bb".as_bytes());
        let mut doc = crate::AutoCommit::new().with_actor(actor2);

        let mut doc2 = doc.fork().with_actor(actor1);
        doc2.put(crate::ROOT, "key", "someval").unwrap();

        doc.put(crate::ROOT, "key", crate::ScalarValue::Counter(1.into()))
            .unwrap();
        doc.increment(crate::ROOT, "key", 2).unwrap();
        doc.increment(crate::ROOT, "key", 3).unwrap();

        doc.merge(&mut doc2).unwrap();

        doc.set_text_rep(TextRepresentation::String);
        let p = doc.document().current_state(TextRepresentation::String);

        assert_eq!(
            Patches::from(p),
            Patches(vec![ObservedPatch::Put {
                obj: crate::ROOT,
                prop: "key".into(),
                value: PatchValue::Untagged(Value::Scalar(Cow::Owned(
                    crate::ScalarValue::Counter(6.into())
                ))),
                conflict: true,
            },])
        );
    }

    #[test]
    fn test_multiple_list_insertions() {
        let mut doc = crate::AutoCommit::new();

        let list = doc.put_object(crate::ROOT, "list", ObjType::List).unwrap();
        doc.insert(&list, 0, 1).unwrap();
        doc.insert(&list, 1, 2).unwrap();

        doc.set_text_rep(TextRepresentation::String);
        let p = doc.document().current_state(TextRepresentation::String);

        assert_eq!(
            Patches::from(p),
            Patches(vec![
                ObservedPatch::Put {
                    obj: crate::ROOT,
                    prop: "list".into(),
                    value: PatchValue::Tagged(Value::Object(ObjType::List), list.clone()),
                    conflict: false,
                },
                ObservedPatch::Insert {
                    obj: list.clone(),
                    index: 0,
                    value: PatchValue::Untagged(1.into()),
                },
                ObservedPatch::Insert {
                    obj: list,
                    index: 1,
                    value: PatchValue::Untagged(2.into()),
                },
            ])
        );
    }

    #[test]
    fn test_concurrent_insertions_at_same_index() {
        let mut doc = crate::AutoCommit::new().with_actor(crate::ActorId::from("aa".as_bytes()));

        let list = doc.put_object(crate::ROOT, "list", ObjType::List).unwrap();

        let mut doc2 = doc.fork().with_actor(crate::ActorId::from("bb".as_bytes()));

        doc.insert(&list, 0, 1).unwrap();
        doc2.insert(&list, 0, 2).unwrap();
        doc.merge(&mut doc2).unwrap();

        doc.set_text_rep(TextRepresentation::String);
        let p = doc.document().current_state(TextRepresentation::String);

        assert_eq!(
            Patches::from(p),
            Patches(vec![
                ObservedPatch::Put {
                    obj: crate::ROOT,
                    prop: "list".into(),
                    value: PatchValue::Tagged(Value::Object(ObjType::List), list.clone()),
                    conflict: false,
                },
                ObservedPatch::Insert {
                    obj: list.clone(),
                    index: 0,
                    value: PatchValue::Untagged(2.into()),
                },
                ObservedPatch::Insert {
                    obj: list,
                    index: 1,
                    value: PatchValue::Untagged(1.into()),
                },
            ])
        );
    }

    #[test]
    fn test_insert_objects() {
        let mut doc = crate::AutoCommit::new().with_actor(crate::ActorId::from("aa".as_bytes()));

        let list = doc.put_object(crate::ROOT, "list", ObjType::List).unwrap();

        let map = doc.insert_object(&list, 0, ObjType::Map).unwrap();
        doc.put(&map, "key", "value").unwrap();

        doc.set_text_rep(TextRepresentation::String);
        let patches = doc.document().current_state(TextRepresentation::String);

        assert_eq!(
            Patches::from(patches),
            Patches(vec![
                ObservedPatch::Put {
                    obj: crate::ROOT,
                    prop: "list".into(),
                    value: PatchValue::Tagged(Value::Object(ObjType::List), list.clone()),
                    conflict: false,
                },
                ObservedPatch::Insert {
                    obj: list.clone(),
                    index: 0,
                    value: PatchValue::Tagged(Value::Object(ObjType::Map), map.clone()),
                },
                ObservedPatch::Put {
                    obj: map,
                    prop: "key".into(),
                    value: PatchValue::Untagged("value".into()),
                    conflict: false
                },
            ])
        );
    }

    #[test]
    fn test_insert_and_update() {
        let mut doc = crate::AutoCommit::new();

        let list = doc.put_object(crate::ROOT, "list", ObjType::List).unwrap();

        doc.insert(&list, 0, "one").unwrap();
        doc.insert(&list, 1, "two").unwrap();
        doc.put(&list, 0, "three").unwrap();
        doc.put(&list, 1, "four").unwrap();

        doc.set_text_rep(TextRepresentation::String);

        let patches = doc.document().current_state(TextRepresentation::String);

        assert_eq!(
            Patches::from(patches),
            Patches(vec![
                ObservedPatch::Put {
                    obj: crate::ROOT,
                    prop: "list".into(),
                    value: PatchValue::Tagged(Value::Object(ObjType::List), list.clone()),
                    conflict: false,
                },
                ObservedPatch::Insert {
                    obj: list.clone(),
                    index: 0,
                    value: PatchValue::Untagged("three".into()),
                },
                ObservedPatch::Insert {
                    obj: list.clone(),
                    index: 1,
                    value: PatchValue::Untagged("four".into()),
                },
            ])
        );
    }

    #[test]
    fn test_load_changes() {
        fn fixture(name: &str) -> Vec<u8> {
            fs::read("./tests/fixtures/".to_owned() + name).unwrap()
        }

        let mut patch_log = PatchLog::active(TextRepresentation::String);
        let _doc = Automerge::load_with_options(
            &fixture("counter_value_is_ok.automerge"),
            crate::LoadOptions::new()
                .on_partial_load(crate::OnPartialLoad::Error)
                .verification_mode(crate::VerificationMode::Check)
                .patch_log(&mut patch_log),
        )
        .unwrap();
        let p = _doc.make_patches(&mut patch_log);

        assert_eq!(
            Patches::from(p),
            Patches(vec![ObservedPatch::Put {
                obj: crate::ROOT,
                prop: "a".into(),
                value: PatchValue::Untagged(crate::ScalarValue::Counter(2000.into()).into()),
                conflict: false,
            }])
        );
    }
}
