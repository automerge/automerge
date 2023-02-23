use std::{borrow::Cow, collections::HashSet, iter::Peekable};

use itertools::Itertools;

use crate::{
    types::{ElemId, Key, ListEncoding, MarkData, MarkStateMachine, ObjId, Op, OpId},
    ObjType, OpObserver, OpType, ScalarValue, Value,
};

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
pub(super) fn observe_current_state<O: OpObserver>(doc: &crate::Automerge, observer: &mut O) {
    // The OpSet already exposes operations in the order they appear in the document.
    // `OpSet::iter_objs` iterates over the objects in causal order, this means that parent objects
    // will always appear before their children. Furthermore, the operations within each object are
    // ordered by key (which means by their position in a sequence for sequences).
    //
    // Effectively then we iterate over each object, then we group the operations in the object by
    // key and for each key find the visible operations for that key. Then we notify the observer
    // for each of those visible operations.
    let mut visible_objs = HashSet::new();
    visible_objs.insert(ObjId::root());
    for (obj, typ, ops) in doc.ops().iter_objs() {
        if !visible_objs.contains(obj) {
            continue;
        }

        // group by key
        // op.insert id=(1@aaa) HEAD value=1
        // op.put id=2@aaa (1@aaa) value=3
        // op.insert id=(1@bbb) 1@aaa value=2

        let ops_by_key = ops.group_by(|o| o.key);
        let actions = ops_by_key
            .into_iter()
            .flat_map(|(key, key_ops)| key_actions(key, key_ops));
        let mut mark_state_machine = MarkStateMachine::new();
        if typ == ObjType::Text && !observer.text_as_seq() {
            track_new_objs_and_notify(
                &mut visible_objs,
                &mut mark_state_machine,
                doc,
                obj,
                typ,
                observer,
                text_actions(actions),
            )
        } else if typ == ObjType::List {
            track_new_objs_and_notify(
                &mut visible_objs,
                &mut mark_state_machine,
                doc,
                obj,
                typ,
                observer,
                list_actions(actions),
            )
        } else {
            track_new_objs_and_notify(
                &mut visible_objs,
                &mut mark_state_machine,
                doc,
                obj,
                typ,
                observer,
                actions,
            )
        }
        if !mark_state_machine.spans.is_empty() {
            let encoding = match typ {
                ObjType::Text => ListEncoding::Text(doc.text_encoding()),
                _ => ListEncoding::List,
            };
            let marks = mark_state_machine
                .spans
                .into_iter()
                .map(|span| span.into_mark(obj, doc, encoding));
            observer.mark(doc, doc.id_to_exid(obj.0), marks);
        }
    }
}

fn track_new_objs_and_notify<N: Action, I: Iterator<Item = N>, O: OpObserver>(
    visible_objs: &mut HashSet<ObjId>,
    mark_state_machine: &mut MarkStateMachine,
    doc: &crate::Automerge,
    obj: &ObjId,
    typ: ObjType,
    observer: &mut O,
    actions: I,
) {
    let exid = doc.id_to_exid(obj.0);
    for action in actions {
        if let Some(obj) = action.made_object() {
            visible_objs.insert(obj);
        }
        action.notify_observer(doc, mark_state_machine, &exid, obj, typ, observer);
    }
}

trait Action {
    /// Notify an observer of whatever this action does
    fn notify_observer<O: OpObserver>(
        self,
        doc: &crate::Automerge,
        mark_state_machine: &mut MarkStateMachine,
        exid: &crate::ObjId,
        obj: &ObjId,
        typ: ObjType,
        observer: &mut O,
    );

    /// If this action created an object, return the ID of that object
    fn made_object(&self) -> Option<ObjId>;
}

fn key_actions<'a, 'b, I: Iterator<Item = &'a Op>>(
    key: Key,
    key_ops: I,
) -> impl Iterator<Item = SimpleAction<'a>> {
    #[derive(Clone)]
    enum CurrentOp<'a> {
        Put {
            value: Value<'a>,
            id: OpId,
            conflicted: bool,
        },
        Insert(Value<'a>, OpId),
        MarkBegin(OpId, &'a MarkData),
        MarkEnd(OpId),
    }
    key_ops
        .filter(|o| o.visible_or_mark())
        .filter_map(|o| match &o.action {
            OpType::Make(obj_type) => {
                let value = Value::Object(*obj_type);
                if o.insert {
                    Some(CurrentOp::Insert(value, o.id))
                } else {
                    Some(CurrentOp::Put {
                        value,
                        id: o.id,
                        conflicted: false,
                    })
                }
            }
            OpType::Put(value) => {
                let value = Value::Scalar(Cow::Borrowed(value));
                if o.insert {
                    Some(CurrentOp::Insert(value, o.id))
                } else {
                    Some(CurrentOp::Put {
                        value,
                        id: o.id,
                        conflicted: false,
                    })
                }
            }
            OpType::MarkBegin(m) => Some(CurrentOp::MarkBegin(o.id, m)),
            OpType::MarkEnd(_) => Some(CurrentOp::MarkEnd(o.id)),
            _ => None,
        })
        .coalesce(|previous, current| match (previous, current) {
            (CurrentOp::Put { .. }, CurrentOp::Put { value, id, .. }) => Ok(CurrentOp::Put {
                value,
                id,
                conflicted: true,
            }),
            (previous, current) => Err((previous, current)),
        })
        .map(move |op| match op {
            CurrentOp::Put {
                value,
                id,
                conflicted,
            } => SimpleAction::Put {
                prop: key,
                tagged_value: (value, id),
                conflict: conflicted,
            },
            CurrentOp::Insert(val, id) => SimpleAction::Insert {
                elem_id: ElemId(id),
                tagged_value: (val, id),
            },
            CurrentOp::MarkBegin(id, data) => SimpleAction::MarkBegin { id, data },
            CurrentOp::MarkEnd(id) => SimpleAction::MarkEnd { id },
        })
}

/// Either a "put" or "insert" action. i.e. not splicing for text values
enum SimpleAction<'a> {
    Put {
        prop: Key,
        tagged_value: (Value<'a>, OpId),
        conflict: bool,
    },
    Insert {
        elem_id: ElemId,
        tagged_value: (Value<'a>, OpId),
    },
    MarkBegin {
        id: OpId,
        data: &'a MarkData,
    },
    MarkEnd {
        id: OpId,
    },
}

impl<'a> Action for SimpleAction<'a> {
    fn notify_observer<O: OpObserver>(
        self,
        doc: &crate::Automerge,
        mark_state_machine: &mut MarkStateMachine,
        exid: &crate::ObjId,
        obj: &ObjId,
        typ: ObjType,
        observer: &mut O,
    ) {
        let encoding = match typ {
            ObjType::Text => ListEncoding::Text(doc.text_encoding()),
            _ => ListEncoding::List,
        };
        match self {
            Self::Put {
                prop,
                tagged_value,
                conflict,
            } => {
                let tagged_value = (tagged_value.0, doc.id_to_exid(tagged_value.1));
                let prop = doc.ops().export_key(*obj, prop, encoding).unwrap();
                observer.put(doc, exid.clone(), prop, tagged_value, conflict);
            }
            Self::Insert {
                elem_id,
                tagged_value: (value, opid),
            } => {
                let index = doc
                    .ops()
                    .search(obj, crate::query::ElemIdPos::new(elem_id, encoding))
                    .index()
                    .unwrap();
                let tagged_value = (value, doc.id_to_exid(opid));
                observer.insert(doc, doc.id_to_exid(obj.0), index, tagged_value);
            }
            Self::MarkBegin { id, data } => {
                mark_state_machine.mark_begin(id, data, doc);
            }
            Self::MarkEnd { id } => {
                mark_state_machine.mark_end(id, doc);
            }
        }
    }

    fn made_object(&self) -> Option<ObjId> {
        match self {
            Self::Put {
                tagged_value: (Value::Object(_), id),
                ..
            } => Some((*id).into()),
            Self::Insert {
                tagged_value: (Value::Object(_), id),
                ..
            } => Some((*id).into()),
            _ => None,
        }
    }
}

/// An `Action` which splices for text values
enum TextAction<'a> {
    Action(SimpleAction<'a>),
    Splice { start: ElemId, chars: String },
}

impl<'a> Action for TextAction<'a> {
    fn notify_observer<O: OpObserver>(
        self,
        doc: &crate::Automerge,
        mark_state_machine: &mut MarkStateMachine,
        exid: &crate::ObjId,
        obj: &ObjId,
        typ: ObjType,
        observer: &mut O,
    ) {
        match self {
            Self::Action(action) => {
                action.notify_observer(doc, mark_state_machine, exid, obj, typ, observer)
            }
            Self::Splice { start, chars } => {
                let index = doc
                    .ops()
                    .search(
                        obj,
                        crate::query::ElemIdPos::new(
                            start,
                            ListEncoding::Text(doc.text_encoding()),
                        ),
                    )
                    .index()
                    .unwrap();
                observer.splice_text(doc, doc.id_to_exid(obj.0), index, chars.as_str());
            }
        }
    }

    fn made_object(&self) -> Option<ObjId> {
        match self {
            Self::Action(action) => action.made_object(),
            _ => None,
        }
    }
}

fn list_actions<'a, I: Iterator<Item = SimpleAction<'a>>>(
    actions: I,
) -> impl Iterator<Item = SimpleAction<'a>> {
    actions.map(|a| match a {
        SimpleAction::Put {
            prop: Key::Seq(elem_id),
            tagged_value,
            ..
        } => SimpleAction::Insert {
            elem_id,
            tagged_value,
        },
        a => a,
    })
}

/// Condense consecutive `SimpleAction::Insert` actions into one `TextAction::Splice`
fn text_actions<'a, I>(actions: I) -> impl Iterator<Item = TextAction<'a>>
where
    I: Iterator<Item = SimpleAction<'a>>,
{
    TextActions {
        ops: actions.peekable(),
    }
}

struct TextActions<'a, I: Iterator<Item = SimpleAction<'a>>> {
    ops: Peekable<I>,
}

impl<'a, I: Iterator<Item = SimpleAction<'a>>> Iterator for TextActions<'a, I> {
    type Item = TextAction<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(SimpleAction::Insert { .. }) = self.ops.peek() {
            let (start, value) = match self.ops.next() {
                Some(SimpleAction::Insert {
                    tagged_value: (value, opid),
                    ..
                }) => (opid, value),
                _ => unreachable!(),
            };
            let mut chars = match value {
                Value::Scalar(Cow::Borrowed(ScalarValue::Str(s))) => s.to_string(),
                _ => "\u{fffc}".to_string(),
            };
            while let Some(SimpleAction::Insert { .. }) = self.ops.peek() {
                if let Some(SimpleAction::Insert {
                    tagged_value: (value, _),
                    ..
                }) = self.ops.next()
                {
                    match value {
                        Value::Scalar(Cow::Borrowed(ScalarValue::Str(s))) => chars.push_str(s),
                        _ => chars.push('\u{fffc}'),
                    }
                }
            }
            Some(TextAction::Splice {
                start: ElemId(start),
                chars,
            })
        } else {
            self.ops.next().map(TextAction::Action)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use crate::{
        marks::Mark, transaction::Transactable, ObjType, OpObserver, Prop, ReadDoc, Value,
    };

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
        text_as_seq: bool,
    }

    impl ObserverStub {
        fn new() -> Self {
            Self {
                ops: Vec::new(),
                text_as_seq: true,
            }
        }

        fn new_text_v2() -> Self {
            Self {
                ops: Vec::new(),
                text_as_seq: false,
            }
        }
    }

    impl OpObserver for ObserverStub {
        fn insert<R: ReadDoc>(
            &mut self,
            _doc: &R,
            objid: crate::ObjId,
            index: usize,
            tagged_value: (crate::Value<'_>, crate::ObjId),
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

        fn expose<R: ReadDoc>(
            &mut self,
            _doc: &R,
            _objid: crate::ObjId,
            _prop: crate::Prop,
            _tagged_value: (crate::Value<'_>, crate::ObjId),
            _conflict: bool,
        ) {
            panic!("expose not expected");
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

        fn text_as_seq(&self) -> bool {
            self.text_as_seq
        }

        fn mark<R: ReadDoc, M: Iterator<Item = Mark>>(
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
        super::observe_current_state(doc.document(), &mut obs);

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
        super::observe_current_state(doc.document(), &mut obs);

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

        let mut obs = ObserverStub::new_text_v2();
        super::observe_current_state(doc.document(), &mut obs);

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

        let mut obs = ObserverStub::new_text_v2();
        super::observe_current_state(doc.document(), &mut obs);

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

        let mut obs = ObserverStub::new_text_v2();
        super::observe_current_state(doc.document(), &mut obs);

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

        let mut obs = ObserverStub::new_text_v2();
        super::observe_current_state(doc.document(), &mut obs);

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

        let mut obs = ObserverStub::new_text_v2();
        super::observe_current_state(doc.document(), &mut obs);

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

        let mut obs = ObserverStub::new_text_v2();
        super::observe_current_state(doc.document(), &mut obs);

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
}
