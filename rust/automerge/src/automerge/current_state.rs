use std::borrow::Cow;

use itertools::Itertools;

use crate::{
    history::History,
    marks::{Mark, MarkStateMachine},
    types::{Key, ListEncoding, ObjId, Op, OpId},
    Automerge, ObjType, OpType, Value,
};

#[derive(Debug, Default)]
struct TextState<'a> {
    text: String,
    len: usize,
    marks: MarkStateMachine<'a>,
    finished: Vec<Mark<'a>>,
}

struct Put<'a> {
    value: Value<'a>,
    key: Key,
    id: OpId,
}

/// Traverse the "current" state of the document, notifying `observer`
///
/// The "current" state of the document is the set of visible operations. This function will
/// traverse that set of operations and call the corresponding methods on the `observer` as it
/// encounters values. The `observer` methods will be called in the order in which they appear in
/// the document. That is to say that the observer will be notified of parent objects before the
/// objects they contain and elements of a sequence will be notified in the order they occur.
///
/// Due to only notifying of visible operations the observer will only be called with `put`,
/// `insert`, and `splice`, operations.

pub(crate) fn observe_current_state(doc: &Automerge, history: &mut History) {
    // The OpSet already exposes operations in the order they appear in the document.
    // `OpSet::iter_objs` iterates over the objects in causal order, this means that parent objects
    // will always appear before their children. Furthermore, the operations within each object are
    // ordered by key (which means by their position in a sequence for sequences).
    //
    // Effectively then we iterate over each object, then we group the operations in the object by
    // key and for each key find the visible operations for that key. Then we notify the observer
    // for each of those visible operations.
    for (obj, typ, ops) in doc.ops().iter_objs() {
        if typ == ObjType::Text && !doc.text_as_seq() {
            observe_text(doc, history, obj, ops)
        } else if typ.is_sequence() {
            observe_list(doc, history, obj, ops);
        } else {
            observe_map(doc, history, obj, ops);
        }
    }
}

fn observe_text<'a, I: Iterator<Item = &'a Op>>(
    doc: &'a Automerge,
    //observer: &mut O,
    history: &mut History,
    obj: &ObjId,
    ops: I,
) {
    //let exid = doc.id_to_exid(obj.0);
    let ops_by_key = ops.group_by(|o| o.elemid_or_key());
    let encoding = ListEncoding::Text;
    let state = TextState::default();
    let state = ops_by_key
        .into_iter()
        .fold(state, |mut state, (_key, key_ops)| {
            if let Some(o) = key_ops.filter(|o| o.visible_or_mark(None)).last() {
                match &o.action {
                    OpType::Make(_) | OpType::Put(_) => {
                        state.text.push_str(o.to_str());
                        state.len += o.width(encoding);
                    }
                    OpType::MarkBegin(_, data) => {
                        if let Some(mark) = state.marks.mark_begin(o.id, state.len, data, doc) {
                            state.finished.push(mark);
                        }
                    }
                    OpType::MarkEnd(_) => {
                        if let Some(mark) = state.marks.mark_end(o.id, state.len, doc) {
                            state.finished.push(mark);
                        }
                    }
                    OpType::Increment(_) | OpType::Delete => {}
                }
            }
            state
        });
    history.splice(*obj, 0, state.text.as_str());
    history.mark(*obj, &state.finished);
    //observer.splice_text(doc, exid.clone(), 0, state.text.as_str());
    //observer.mark(doc, exid, state.finished.into_iter());
}

fn observe_list<'a, I: Iterator<Item = &'a Op>>(
    doc: &'a Automerge,
    //observer: &mut O,
    history: &mut History,
    obj: &ObjId,
    ops: I,
) {
    //let exid = doc.id_to_exid(obj.0);
    let mut marks = MarkStateMachine::default();
    let ops_by_key = ops.group_by(|o| o.elemid_or_key());
    let mut len = 0;
    let mut finished = Vec::new();
    ops_by_key
        .into_iter()
        .filter_map(|(_key, key_ops)| {
            key_ops
                .filter(|o| o.visible_or_mark(None))
                .filter_map(|o| match &o.action {
                    OpType::Make(obj_type) => Some((Value::Object(*obj_type), o.id)),
                    OpType::Put(value) => Some((Value::Scalar(Cow::Borrowed(value)), o.id)),
                    OpType::MarkBegin(_, data) => {
                        if let Some(mark) = marks.mark_begin(o.id, len, data, doc) {
                            // side effect
                            finished.push(mark)
                        }
                        None
                    }
                    OpType::MarkEnd(_) => {
                        if let Some(mark) = marks.mark_end(o.id, len, doc) {
                            // side effect
                            finished.push(mark)
                        }
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
        .for_each(|(index, (val_enum, (value, opid)))| {
            let conflict = val_enum > 0;
            history.insert(*obj, index, value.clone().into(), opid, conflict);
            //let tagged_value = (value, doc.id_to_exid(opid));
            //observer.insert(doc, exid.clone(), index, tagged_value, conflict);
        });
    history.mark(*obj, &finished);
    ////observer.mark(doc, exid, finished.into_iter());
}

fn observe_map_key<'a, I: Iterator<Item = &'a Op>>(
    (key, key_ops): (Key, I),
) -> Option<(usize, Put<'a>)> {
    key_ops
        .filter(|o| o.visible())
        .filter_map(|o| match &o.action {
            OpType::Make(obj_type) => {
                let value = Value::Object(*obj_type);
                Some(Put {
                    value,
                    key,
                    id: o.id,
                })
            }
            OpType::Put(value) => {
                let value = Value::Scalar(Cow::Borrowed(value));
                Some(Put {
                    value,
                    key,
                    id: o.id,
                })
            }
            _ => None,
        })
        .enumerate()
        .last()
}

fn observe_map<'a, I: Iterator<Item = &'a Op>>(
    doc: &'a Automerge,
    //observer: &mut O,
    history: &mut History,
    obj: &ObjId,
    ops: I,
) {
    //let exid = doc.id_to_exid(obj.0);
    let ops_by_key = ops.group_by(|o| o.key);
    ops_by_key
        .into_iter()
        .filter_map(observe_map_key)
        //.filter_map(|(i, put)| {
        .for_each(|(i, put)| {
            //let tagged_value = (put.value.clone(), doc.id_to_exid(put.id));
            if let Some(prop_index) = put.key.prop_index() {
                if let Some(key) = doc.ops().m.props.safe_get(prop_index) {
                    let conflict = i > 0;
                    history.put_map(*obj, key, put.value.into(), put.id, conflict, false);
                }
            }
            //let prop = Prop::Map(key.to_string());
            //Some((tagged_value, prop, conflict))
        });
    //.for_each(|(tagged_value, prop, conflict)| {
    //observer.put(doc, exid.clone(), prop, tagged_value, conflict);
    //});
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, fs};

    use crate::{
        marks::Mark, op_observer::TextRepresentation, transaction::Transactable, Automerge,
        ObjType, OpObserver, Prop, ReadDoc, Value,
    };
    //use crate::{transaction::Transactable, Automerge, ObjType, OpObserver, Prop, ReadDoc, Value};

    // Observer ops often carry a "tagged value", which is a value and the OpID of the op which
    // created that value. For a lot of values (i.e. any scalar value) we don't care about the
    // opid. This type implements `PartialEq` for the `Untagged` variant by ignoring the tag, which
    // allows us to express tests which don't care about the tag.
    #[derive(Clone, Debug)]
    enum ObservedValue {
        Tagged(crate::Value<'static>, crate::ObjId),
        Untagged(crate::Value<'static>),
    }

    impl<'a> From<(Value<'a>, crate::ObjId)> for ObservedValue {
        fn from(value: (Value<'a>, crate::ObjId)) -> Self {
            Self::Tagged(value.0.into_owned(), value.1)
        }
    }

    impl PartialEq<ObservedValue> for ObservedValue {
        fn eq(&self, other: &ObservedValue) -> bool {
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
    enum ObserverCall {
        Put {
            obj: crate::ObjId,
            prop: Prop,
            value: ObservedValue,
            conflict: bool,
        },
        Insert {
            obj: crate::ObjId,
            index: usize,
            value: ObservedValue,
        },
        SpliceText {
            obj: crate::ObjId,
            index: usize,
            chars: String,
        },
    }

    // A Vec<ObserverCall> is pretty hard to look at in a test failure. This wrapper prints the
    // calls out in a nice table so it's easier to see what's different
    #[derive(Clone, PartialEq)]
    struct Calls(Vec<ObserverCall>);

    impl std::fmt::Debug for Calls {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let mut table = prettytable::Table::new();
            table.set_format(*prettytable::format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
            table.set_titles(prettytable::row![
                "Op", "Object", "Property", "Value", "Conflict"
            ]);
            for call in &self.0 {
                match call {
                    ObserverCall::Put {
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
                                ObservedValue::Tagged(v, o) => format!("{} ({})", v, o),
                                ObservedValue::Untagged(v) => format!("{}", v),
                            },
                            conflict
                        ]);
                    }
                    ObserverCall::Insert { obj, index, value } => {
                        table.add_row(prettytable::row![
                            "Insert",
                            format!("{}", obj),
                            index,
                            match value {
                                ObservedValue::Tagged(v, o) => format!("{} ({})", v, o),
                                ObservedValue::Untagged(v) => format!("{}", v),
                            },
                            ""
                        ]);
                    }
                    ObserverCall::SpliceText { obj, index, chars } => {
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

    struct ObserverStub {
        ops: Vec<ObserverCall>,
    }

    impl ObserverStub {
        fn new() -> Self {
            Self { ops: Vec::new() }
        }

        /*
                fn new_text_v2() -> Self {
                    Self {
                        ops: Vec::new(),
                    }
                }
        */
    }

    impl OpObserver for ObserverStub {
        fn insert<R: ReadDoc>(
            &mut self,
            _doc: &R,
            objid: crate::ObjId,
            index: usize,
            tagged_value: (crate::Value<'_>, crate::ObjId),
            _conflict: bool,
        ) {
            self.ops.push(ObserverCall::Insert {
                obj: objid,
                index,
                value: tagged_value.into(),
            });
        }

        fn splice_text<R: ReadDoc>(
            &mut self,
            _doc: &R,
            objid: crate::ObjId,
            index: usize,
            value: &str,
        ) {
            self.ops.push(ObserverCall::SpliceText {
                obj: objid,
                index,
                chars: value.to_string(),
            });
        }

        fn put<R: ReadDoc>(
            &mut self,
            _doc: &R,
            objid: crate::ObjId,
            prop: crate::Prop,
            tagged_value: (crate::Value<'_>, crate::ObjId),
            conflict: bool,
        ) {
            self.ops.push(ObserverCall::Put {
                obj: objid,
                prop,
                value: tagged_value.into(),
                conflict,
            });
        }

        fn increment<R: ReadDoc>(
            &mut self,
            _doc: &R,
            _objid: crate::ObjId,
            _prop: crate::Prop,
            _tagged_value: (i64, crate::ObjId),
        ) {
            panic!("increment not expected");
        }

        fn delete_map<R: ReadDoc>(&mut self, _doc: &R, _objid: crate::ObjId, _key: &str) {
            panic!("delete not expected");
        }

        fn delete_seq<R: ReadDoc>(
            &mut self,
            _doc: &R,
            _objid: crate::ObjId,
            _index: usize,
            _num: usize,
        ) {
            panic!("delete not expected");
        }

        fn mark<'a, R: ReadDoc, M: Iterator<Item = Mark<'a>>>(
            &mut self,
            _doc: &R,
            _objid: crate::ObjId,
            _mark: M,
        ) {
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

        let mut obs = ObserverStub::new();
        doc.document().observe_current_state(&mut obs);

        assert_eq!(
            Calls(obs.ops),
            Calls(vec![
                ObserverCall::Put {
                    obj: crate::ROOT,
                    prop: "key".into(),
                    value: ObservedValue::Untagged("value".into()),
                    conflict: false,
                },
                ObserverCall::Put {
                    obj: crate::ROOT,
                    prop: "list".into(),
                    value: ObservedValue::Tagged(Value::Object(ObjType::List), list.clone()),
                    conflict: false,
                },
                ObserverCall::Put {
                    obj: crate::ROOT,
                    prop: "map".into(),
                    value: ObservedValue::Tagged(Value::Object(ObjType::Map), map.clone()),
                    conflict: false,
                },
                ObserverCall::Put {
                    obj: crate::ROOT,
                    prop: "text".into(),
                    value: ObservedValue::Tagged(Value::Object(ObjType::Text), text.clone()),
                    conflict: false,
                },
                ObserverCall::Put {
                    obj: map.clone(),
                    prop: "nested_key".into(),
                    value: ObservedValue::Untagged("value".into()),
                    conflict: false,
                },
                ObserverCall::Insert {
                    obj: list,
                    index: 0,
                    value: ObservedValue::Untagged("value".into()),
                },
                ObserverCall::Insert {
                    obj: text,
                    index: 0,
                    value: ObservedValue::Untagged("a".into()),
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

        let mut obs = ObserverStub::new();
        doc.document().observe_current_state(&mut obs);

        assert_eq!(
            Calls(obs.ops),
            Calls(vec![
                ObserverCall::Put {
                    obj: crate::ROOT,
                    prop: "list".into(),
                    value: ObservedValue::Tagged(Value::Object(ObjType::List), list.clone()),
                    conflict: false,
                },
                ObserverCall::Put {
                    obj: crate::ROOT,
                    prop: "map".into(),
                    value: ObservedValue::Tagged(Value::Object(ObjType::Map), map.clone()),
                    conflict: false,
                },
                ObserverCall::Put {
                    obj: crate::ROOT,
                    prop: "text".into(),
                    value: ObservedValue::Tagged(Value::Object(ObjType::Text), text.clone()),
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

        let mut obs = ObserverStub::new();
        doc.set_text_rep(TextRepresentation::String);
        doc.document().observe_current_state(&mut obs);

        assert_eq!(
            Calls(obs.ops),
            Calls(vec![
                ObserverCall::Put {
                    obj: crate::ROOT,
                    prop: "text".into(),
                    value: ObservedValue::Tagged(Value::Object(ObjType::Text), text.clone()),
                    conflict: false,
                },
                ObserverCall::SpliceText {
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

        let mut obs = ObserverStub::new();
        doc.set_text_rep(TextRepresentation::String);
        doc.document().observe_current_state(&mut obs);

        assert_eq!(
            Calls(obs.ops),
            Calls(vec![ObserverCall::Put {
                obj: crate::ROOT,
                prop: "key".into(),
                value: ObservedValue::Untagged(Value::Scalar(Cow::Owned(
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

        let mut obs = ObserverStub::new();
        doc.set_text_rep(TextRepresentation::String);
        doc.document().observe_current_state(&mut obs);

        assert_eq!(
            Calls(obs.ops),
            Calls(vec![
                ObserverCall::Put {
                    obj: crate::ROOT,
                    prop: "list".into(),
                    value: ObservedValue::Tagged(Value::Object(ObjType::List), list.clone()),
                    conflict: false,
                },
                ObserverCall::Insert {
                    obj: list.clone(),
                    index: 0,
                    value: ObservedValue::Untagged(1.into()),
                },
                ObserverCall::Insert {
                    obj: list,
                    index: 1,
                    value: ObservedValue::Untagged(2.into()),
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

        let mut obs = ObserverStub::new();
        doc.set_text_rep(TextRepresentation::String);
        doc.document().observe_current_state(&mut obs);

        assert_eq!(
            Calls(obs.ops),
            Calls(vec![
                ObserverCall::Put {
                    obj: crate::ROOT,
                    prop: "list".into(),
                    value: ObservedValue::Tagged(Value::Object(ObjType::List), list.clone()),
                    conflict: false,
                },
                ObserverCall::Insert {
                    obj: list.clone(),
                    index: 0,
                    value: ObservedValue::Untagged(2.into()),
                },
                ObserverCall::Insert {
                    obj: list,
                    index: 1,
                    value: ObservedValue::Untagged(1.into()),
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

        let mut obs = ObserverStub::new();
        doc.set_text_rep(TextRepresentation::String);
        doc.document().observe_current_state(&mut obs);

        assert_eq!(
            Calls(obs.ops),
            Calls(vec![
                ObserverCall::Put {
                    obj: crate::ROOT,
                    prop: "list".into(),
                    value: ObservedValue::Tagged(Value::Object(ObjType::List), list.clone()),
                    conflict: false,
                },
                ObserverCall::Insert {
                    obj: list.clone(),
                    index: 0,
                    value: ObservedValue::Tagged(Value::Object(ObjType::Map), map.clone()),
                },
                ObserverCall::Put {
                    obj: map,
                    prop: "key".into(),
                    value: ObservedValue::Untagged("value".into()),
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

        let mut obs = ObserverStub::new();
        doc.document().observe_current_state(&mut obs);

        assert_eq!(
            Calls(obs.ops),
            Calls(vec![
                ObserverCall::Put {
                    obj: crate::ROOT,
                    prop: "list".into(),
                    value: ObservedValue::Tagged(Value::Object(ObjType::List), list.clone()),
                    conflict: false,
                },
                ObserverCall::Insert {
                    obj: list.clone(),
                    index: 0,
                    value: ObservedValue::Untagged("three".into()),
                },
                ObserverCall::Insert {
                    obj: list.clone(),
                    index: 1,
                    value: ObservedValue::Untagged("four".into()),
                },
            ])
        );
    }

    #[test]
    fn test_load_changes() {
        fn fixture(name: &str) -> Vec<u8> {
            fs::read("./tests/fixtures/".to_owned() + name).unwrap()
        }

        let mut obs = ObserverStub::new();
        let _doc = Automerge::load_with(
            &fixture("counter_value_is_ok.automerge"),
            crate::OnPartialLoad::Error,
            crate::storage::VerificationMode::Check,
            Some(&mut obs),
            TextRepresentation::default(),
        );

        assert_eq!(
            Calls(obs.ops),
            Calls(vec![ObserverCall::Put {
                obj: crate::ROOT,
                prop: "a".into(),
                value: ObservedValue::Untagged(crate::ScalarValue::Counter(2000.into()).into()),
                conflict: false,
            },])
        );
    }
}
