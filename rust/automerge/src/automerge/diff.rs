use itertools::Itertools;
use std::borrow::Cow;
use std::ops::Deref;

use crate::{
    marks::MarkStateMachine,
    types::{Clock, ListEncoding, ObjId, Op, ScalarValue},
    Automerge, ChangeHash, ObjType, OpObserver, OpType,
};

#[derive(Clone, Debug)]
struct Winner<'a> {
    op: Cow<'a, Op>,
    cross_visible: bool,
    conflict: bool,
}

impl<'a> Deref for Winner<'a> {
    type Target = Op;

    fn deref(&self) -> &Self::Target {
        self.op.as_ref()
    }
}

struct OpState {}

impl OpState {
    fn process<'a, T: Iterator<Item = &'a Op>>(
        ops: T,
        before: &Clock,
        after: &Clock,
    ) -> Option<Patch<'a>> {
        let mut before_op = None;
        let mut after_op = None;

        for op in ops {
            let predates_before = op.predates(before);
            let predates_after = op.predates(after);

            if predates_before && !op.was_deleted_before(before) {
                Self::push_top(&mut before_op, op, predates_after, before);
            }

            if predates_after && !op.was_deleted_before(after) {
                Self::push_top(&mut after_op, op, predates_before, after);
            }
        }
        Self::resolve(before_op, after_op)
    }

    fn push_top<'a>(top: &mut Option<Winner<'a>>, op: &'a Op, cross_visible: bool, clock: &Clock) {
        match &op.action {
            OpType::Put(ScalarValue::Counter(c)) => {
                let mut op = op.clone();
                let value = c.value_at(clock);
                op.action = OpType::Put(ScalarValue::Counter(value.into()));
                top.replace(Winner {
                    op: Cow::Owned(op),
                    cross_visible,
                    conflict: top.is_some(),
                });
            }
            OpType::Increment(_) => {} // can ignore - info captured inside Counter
            _ => {
                top.replace(Winner {
                    op: Cow::Borrowed(op),
                    cross_visible,
                    conflict: top.is_some(),
                });
            }
        }
    }

    fn resolve<'a>(before: Option<Winner<'a>>, after: Option<Winner<'a>>) -> Option<Patch<'a>> {
        match (before, after) {
            (None, Some(b)) => Some(Patch::new(b)),
            (Some(a), None) => Some(Patch::delete(a)),
            (Some(a), Some(b)) if a.op.id == b.op.id => {
                let conflict = !a.conflict && b.conflict;
                Some(Patch::same(a, b, conflict))
            }
            (Some(a), Some(b)) if a.op.id != b.op.id => Some(Patch::update(a, b)),
            _ => None,
        }
    }
}

impl Op {
    fn was_deleted_before(&self, clock: &Clock) -> bool {
        self.succ_iter().any(|i| clock.covers(i))
    }

    fn predates(&self, clock: &Clock) -> bool {
        clock.covers(&self.id)
    }
}

#[derive(Debug, Clone)]
struct Patch<'a> {
    op: Winner<'a>,
    state: PatchState<'a>,
}

impl<'a> Patch<'a> {
    fn same(old: Winner<'a>, new: Winner<'a>, conflict: bool) -> Self {
        if new.op.action != old.op.action {
            let delta = new.op.action.to_i64() - old.op.action.to_i64();
            Patch {
                op: new,
                state: PatchState::Increment(delta, conflict),
            }
        } else {
            Patch {
                op: new,
                state: PatchState::Old(conflict),
            }
        }
    }

    fn conflict(&self) -> bool {
        self.op.conflict
    }

    fn op(&self) -> &Op {
        self.op.op.as_ref()
    }

    fn delete(op: Winner<'a>) -> Self {
        Patch {
            op,
            state: PatchState::Delete,
        }
    }

    fn update(old: Winner<'a>, new: Winner<'a>) -> Self {
        let cross_vis = new.cross_visible;
        Patch {
            op: new,
            state: PatchState::Update(old, cross_vis),
        }
    }

    fn new(op: Winner<'a>) -> Self {
        Patch {
            op,
            state: PatchState::New,
        }
    }
}

#[derive(Debug, Clone)]
enum PatchState<'a> {
    New,
    Old(bool),
    Update(Winner<'a>, bool),
    Delete,
    Increment(i64, bool),
    //Mark,
}

pub(crate) fn observe_diff<O: OpObserver>(
    doc: &Automerge,
    before_heads: &[ChangeHash],
    after_heads: &[ChangeHash],
    observer: &mut O,
) {
    let before = doc.clock_at(before_heads);
    let after = doc.clock_at(after_heads);
    // FIXME - this fork is expensive
    // we really need a Doc::At object to make this cheap and easy
    // this is critical to keep paths accurate when rendering patches
    let doc_at_after = doc.fork_at(after_heads).unwrap();
    for (obj, typ, ops) in doc.ops().iter_objs() {
        let ops_by_key = ops.group_by(|o| o.elemid_or_key());
        let diffs = ops_by_key
            .into_iter()
            .filter_map(|(_key, key_ops)| OpState::process(key_ops, &before, &after));

        if typ == ObjType::Text && !observer.text_as_seq() {
            observe_text_diff(&doc_at_after, observer, obj, diffs)
        } else if typ.is_sequence() {
            observe_list_diff(&doc_at_after, observer, obj, diffs);
        } else {
            observe_map_diff(&doc_at_after, observer, obj, diffs);
        }
    }
}

fn observe_list_diff<'a, I: Iterator<Item = Patch<'a>>, O: OpObserver>(
    doc: &'a Automerge,
    observer: &mut O,
    obj: &ObjId,
    patches: I,
) {
    //let mut marks = MarkStateMachine::default();
    let exid = doc.id_to_exid(obj.0);
    patches.fold(0, |index, patch| match patch.state {
        /*
        PatchState::Mark => {
                        if let Some(mark) = marks.mark_or_unmark(patch.op(), index, &doc) {
                            if mark.is_null() {
                                observer.unmark(doc, exid.clone(), mark.name(), mark.start, mark.end);
                            } else {
                                observer.mark(doc, exid.clone(), Some(mark).into_iter());
                            }
                        }
            index
        }
            */
        PatchState::New => {
            observer.insert(
                doc,
                exid.clone(),
                index,
                doc.tagged_value(patch.op()),
                patch.conflict(),
            );
            index + 1
        }
        PatchState::Update(_, expose) => {
            if expose {
                observer.expose(
                    doc,
                    exid.clone(),
                    index.into(),
                    doc.tagged_value(patch.op()),
                    patch.conflict(),
                );
            } else {
                observer.put(
                    doc,
                    exid.clone(),
                    index.into(),
                    doc.tagged_value(patch.op()),
                    patch.conflict(),
                );
            }
            index + 1
        }
        PatchState::Old(flag) => {
            if flag {
                observer.flag_conflict(doc, exid.clone(), index.into());
            }
            index + 1
        }
        PatchState::Delete => {
            observer.delete_seq(doc, exid.clone(), index, 1);
            index
        }
        PatchState::Increment(n, flag) => {
            if flag {
                observer.flag_conflict(doc, exid.clone(), index.into());
            }
            observer.increment(
                doc,
                exid.clone(),
                index.into(),
                (n, doc.id_to_exid(patch.op().id)),
            );
            index + 1
        }
    });
}

fn observe_text_diff<'a, I: Iterator<Item = Patch<'a>>, O: OpObserver>(
    doc: &'a Automerge,
    observer: &mut O,
    obj: &ObjId,
    patches: I,
) {
    let mut _marks = MarkStateMachine::default();
    let exid = doc.id_to_exid(obj.0);
    let encoding = ListEncoding::Text(doc.text_encoding());
    patches.fold(0, |index, patch| match &patch.state {
        /*
        PatchState::Mark => {
                            if let Some(mark) = marks.mark_or_unmark(patch.op(), index, &doc) {
                                if mark.is_null() {
                                    observer.unmark(doc, exid.clone(), mark.name(), mark.start, mark.end);
                                } else {
                                    observer.mark(doc, exid.clone(), Some(mark).into_iter());
                                }
                            }
            index
        }
            */
        PatchState::New => {
            observer.splice_text(doc, exid.clone(), index, patch.op().to_str());
            index + patch.op().width(encoding)
        }
        PatchState::Update(before, _) => {
            observer.delete_seq(doc, exid.clone(), index, before.width(encoding));
            observer.splice_text(doc, exid.clone(), index, patch.op().to_str());
            index + patch.op().width(encoding)
        }
        PatchState::Increment(_, _) => index,
        PatchState::Old(_) => index + patch.op().width(encoding),
        PatchState::Delete => {
            observer.delete_seq(doc, exid.clone(), index, patch.op().width(encoding));
            index
        }
    });
}

fn observe_map_diff<'a, I: Iterator<Item = Patch<'a>>, O: OpObserver>(
    doc: &Automerge,
    observer: &mut O,
    obj: &ObjId,
    diffs: I,
) {
    let exid = doc.id_to_exid(obj.0);
    diffs
        .filter_map(|patch| Some((get_prop(doc, patch.op())?, patch)))
        .for_each(|(prop, patch)| match patch.state {
            PatchState::New => observer.put(
                doc,
                exid.clone(),
                prop.into(),
                doc.tagged_value(patch.op()),
                patch.conflict(),
            ),
            PatchState::Update(_, expose) => {
                if expose {
                    observer.expose(
                        doc,
                        exid.clone(),
                        prop.into(),
                        doc.tagged_value(patch.op()),
                        patch.conflict(),
                    )
                } else {
                    observer.put(
                        doc,
                        exid.clone(),
                        prop.into(),
                        doc.tagged_value(patch.op()),
                        patch.conflict(),
                    )
                }
            }
            PatchState::Old(flag) => {
                if flag {
                    observer.flag_conflict(doc, exid.clone(), prop.into());
                }
            }
            PatchState::Increment(n, flag) => {
                if flag {
                    observer.flag_conflict(doc, exid.clone(), prop.into());
                }
                observer.increment(
                    doc,
                    exid.clone(),
                    prop.into(),
                    (n, doc.id_to_exid(patch.op.id)),
                )
            }
            PatchState::Delete => observer.delete_map(doc, exid.clone(), prop),
            //PatchState::Mark => {}
        });
}

fn get_prop<'a>(doc: &'a Automerge, op: &Op) -> Option<&'a str> {
    Some(doc.ops().m.props.safe_get(op.key.prop_index()?)?)
}

#[cfg(test)]
mod tests {

    use crate::{
        op_observer::HasPatches, transaction::Observed, transaction::Transactable,
        AutoCommitWithObs, ObjType, Patch, PatchAction, Prop, ScalarValue, Value, VecOpObserver,
        ROOT,
    };
    use itertools::Itertools;

    #[derive(Debug, Clone, PartialEq)]
    struct ObservedPatch {
        action: String,
        path: String,
        value: Value<'static>,
        conflict: bool,
    }

    fn ex_path_and<I: Iterator<Item = Prop>, V: Into<Prop>>(props: I, val: V) -> String {
        format!("/{}", props.chain(Some(val.into())).join("/"))
    }

    // counter + increment in diff
    // old counter + increment in diff
    // old counter + increment in diff plus delete
    // old counter + increment in diff plus overwrite

    impl From<&Patch<char>> for ObservedPatch {
        fn from(patch: &Patch<char>) -> Self {
            let path = patch.path.iter().map(|(_, prop)| prop).cloned();
            match patch.action.clone() {
                PatchAction::PutMap {
                    key,
                    value,
                    conflict,
                    ..
                } => ObservedPatch {
                    action: "put_map".into(),
                    path: ex_path_and(path, key),
                    value: value.0,
                    conflict,
                },
                PatchAction::PutSeq {
                    index,
                    value,
                    conflict,
                    ..
                } => ObservedPatch {
                    action: "put_seq".into(),
                    path: ex_path_and(path, index),
                    value: value.0,
                    conflict,
                },
                PatchAction::DeleteMap { key } => ObservedPatch {
                    action: "del_map".into(),
                    path: ex_path_and(path, key),
                    value: "".into(),
                    conflict: false,
                },
                PatchAction::DeleteSeq { index, .. } => ObservedPatch {
                    action: "del_seq".into(),
                    path: ex_path_and(path, index),
                    value: "".into(),
                    conflict: false,
                },
                PatchAction::Increment { prop, value } => ObservedPatch {
                    action: "inc".into(),
                    path: ex_path_and(path, prop),
                    value: value.into(),
                    conflict: false,
                },
                PatchAction::Insert {
                    index,
                    values,
                    conflict,
                } => ObservedPatch {
                    action: "insert".into(),
                    path: ex_path_and(path, index),
                    value: values.iter().map(|v| format!("{}", &v.0)).join(",").into(),
                    conflict,
                },
                PatchAction::SpliceText { index, value } => ObservedPatch {
                    action: "splice".into(),
                    path: ex_path_and(path, index),
                    value: value.into_iter().collect::<String>().into(),
                    conflict: false,
                },
                PatchAction::Mark { .. } => {
                    todo!()
                }
                PatchAction::Unmark { .. } => {
                    todo!()
                }
            }
        }
    }

    fn exp(patches: Vec<Patch<char>>) -> Vec<ObservedPatch> {
        patches.iter().map(|p| p.into()).collect()
    }

    #[test]
    fn basic_diff_map_put1() {
        let mut doc = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        doc.put(ROOT, "key", "value1").unwrap();
        let heads1 = doc.get_heads();
        doc.put(ROOT, "key", "value2a").unwrap();
        doc.put(ROOT, "key", "value2b").unwrap();
        doc.put(ROOT, "key", "value2c").unwrap();
        let heads2 = doc.get_heads();
        doc.put(ROOT, "key", "value3").unwrap();
        let patches = doc.diff(&heads1, &heads2).unwrap().take_patches();

        assert_eq!(
            exp(patches),
            vec![ObservedPatch {
                path: "/key".into(),
                action: "put_map".into(),
                value: "value2c".into(),
                conflict: false,
            }]
        );
    }

    #[test]
    fn basic_diff_map_put_conflict() {
        let mut doc1 = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        doc1.put(ROOT, "key", "value1").unwrap();
        let heads1 = doc1.get_heads();

        let mut doc2 = doc1.fork();

        doc2.put(ROOT, "key", "v2_value2a").unwrap();
        doc2.put(ROOT, "key", "v2_value2b").unwrap();
        doc2.put(ROOT, "key", "v2_value2c").unwrap();

        doc1.put(ROOT, "key", "v1_value2a").unwrap();

        doc1.merge(&mut doc2).unwrap();

        let heads2 = doc1.get_heads();
        doc1.put(ROOT, "key", "value3").unwrap();
        let patches = doc1.diff(&heads1, &heads2).unwrap().take_patches();

        assert_eq!(
            exp(patches),
            vec![ObservedPatch {
                path: "/key".into(),
                action: "put_map".into(),
                value: "v2_value2c".into(),
                conflict: true,
            }]
        );
    }

    #[test]
    fn basic_diff_map_put_conflict_with_del() {
        let mut doc1 = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        doc1.put(ROOT, "key1", "value1").unwrap();
        doc1.put(ROOT, "key2", "value2").unwrap();
        let heads1 = doc1.get_heads();

        let mut doc2 = doc1.fork();

        doc2.put(ROOT, "key1", "doc2_value2").unwrap();
        doc2.delete(ROOT, "key2").unwrap();

        doc1.delete(ROOT, "key1").unwrap();
        doc1.put(ROOT, "key2", "doc1_value2").unwrap();

        doc1.merge(&mut doc2).unwrap();

        let heads2 = doc1.get_heads();
        doc1.put(ROOT, "key", "value3").unwrap();
        let patches = doc1.diff(&heads1, &heads2).unwrap().take_patches();

        assert_eq!(
            exp(patches),
            vec![
                ObservedPatch {
                    path: "/key1".into(),
                    action: "put_map".into(),
                    value: "doc2_value2".into(),
                    conflict: false,
                },
                ObservedPatch {
                    path: "/key2".into(),
                    action: "put_map".into(),
                    value: "doc1_value2".into(),
                    conflict: false,
                },
            ]
        );
    }

    #[test]
    fn basic_diff_map_put_conflict_old_value() {
        let mut doc1 = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        doc1.put(ROOT, "key", "value1").unwrap();

        let mut doc2 = doc1.fork();

        doc1.put(ROOT, "key", "v1_value2a").unwrap();

        let heads1 = doc1.get_heads();

        doc2.put(ROOT, "key", "v2_value2a").unwrap();
        doc2.put(ROOT, "key", "v2_value2b").unwrap();
        doc2.put(ROOT, "key", "v2_value2c").unwrap();

        doc1.merge(&mut doc2).unwrap();

        let heads2 = doc1.get_heads();
        doc1.put(ROOT, "key", "value3").unwrap();
        let patches = doc1.diff(&heads1, &heads2).unwrap().take_patches();

        assert_eq!(
            exp(patches),
            vec![ObservedPatch {
                path: "/key".into(),
                action: "put_map".into(),
                value: "v2_value2c".into(),
                conflict: true,
            }]
        );
    }

    #[test]
    fn basic_diff_map_put_conflict_old_value_and_del() {
        let mut doc1 = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        doc1.put(ROOT, "key", "value1").unwrap();

        let mut doc2 = doc1.fork();

        doc1.put(ROOT, "key", "v1_value2a").unwrap();

        let heads1 = doc1.get_heads();

        doc2.put(ROOT, "key", "v2_value2a").unwrap();
        doc2.put(ROOT, "key", "v2_value2b").unwrap();
        doc2.put(ROOT, "key", "v2_value2c").unwrap();
        doc2.delete(ROOT, "key").unwrap();

        doc1.merge(&mut doc2).unwrap();

        let heads2 = doc1.get_heads();
        doc1.put(ROOT, "key", "value3").unwrap();
        let patches = doc1.diff(&heads1, &heads2).unwrap().take_patches();

        assert_eq!(exp(patches), vec![],);
        /*
                    vec![ObservedPatch {
                        path: "/key".into(),
                        action: "del_map".into(),
                        value: "".into(),
                        conflict: false,
                    }]
        */
    }

    #[test]
    fn basic_diff_map_del1() {
        let mut doc = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        doc.put(ROOT, "key", "value1").unwrap();
        let heads1 = doc.get_heads();
        doc.delete(ROOT, "key").unwrap();
        let heads2 = doc.get_heads();
        doc.put(ROOT, "key", "value3").unwrap();
        let patches = doc.diff(&heads1, &heads2).unwrap().take_patches();

        assert_eq!(
            exp(patches),
            vec![ObservedPatch {
                path: "/key".into(),
                action: "del_map".into(),
                value: "".into(),
                conflict: false,
            }]
        );
    }

    #[test]
    fn basic_diff_map_del2() {
        let mut doc = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        doc.put(ROOT, "key", "value1").unwrap();
        let heads1 = doc.get_heads();
        doc.put(ROOT, "key", "value2a").unwrap();
        doc.put(ROOT, "key", "value2b").unwrap();
        doc.delete(ROOT, "key").unwrap();
        let heads2 = doc.get_heads();
        doc.put(ROOT, "key", "value3").unwrap();
        let patches = doc.diff(&heads1, &heads2).unwrap().take_patches();

        assert_eq!(
            exp(patches),
            vec![ObservedPatch {
                path: "/key".into(),
                action: "del_map".into(),
                value: "".into(),
                conflict: false,
            }]
        );
    }

    #[test]
    fn basic_diff_map_del3() {
        let mut doc = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        doc.put(ROOT, "key", "value1").unwrap();
        let heads1 = doc.get_heads();
        doc.put(ROOT, "key", "value2a").unwrap();
        doc.put(ROOT, "key", "value2b").unwrap();
        doc.delete(ROOT, "key").unwrap();
        doc.put(ROOT, "key", "value2c").unwrap();
        let heads2 = doc.get_heads();
        doc.put(ROOT, "key", "value3").unwrap();
        let patches = doc.diff(&heads1, &heads2).unwrap().take_patches();

        assert_eq!(
            exp(patches),
            vec![ObservedPatch {
                path: "/key".into(),
                action: "put_map".into(),
                value: "value2c".into(),
                conflict: false,
            }]
        );
    }

    #[test]
    fn basic_diff_map_counter1() {
        let mut doc = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        doc.put(ROOT, "key", ScalarValue::counter(10)).unwrap();
        let heads1 = doc.get_heads();
        doc.increment(ROOT, "key", 3).unwrap();
        doc.increment(ROOT, "key", 4).unwrap();
        doc.increment(ROOT, "key", 5).unwrap();
        let heads2 = doc.get_heads();
        doc.put(ROOT, "key", "overwrite").unwrap();
        let patches = doc.diff(&heads1, &heads2).unwrap().take_patches();

        assert_eq!(
            exp(patches),
            vec![ObservedPatch {
                path: "/key".into(),
                action: "inc".into(),
                value: 12.into(),
                conflict: false,
            }]
        );
    }

    #[test]
    fn basic_diff_map_counter2() {
        let mut doc = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        let heads1 = doc.get_heads();
        doc.put(ROOT, "key", ScalarValue::counter(10)).unwrap();
        doc.increment(ROOT, "key", 3).unwrap();
        doc.increment(ROOT, "key", 4).unwrap();
        let heads2 = doc.get_heads();
        doc.increment(ROOT, "key", 5).unwrap();
        doc.put(ROOT, "key", "overwrite").unwrap();
        let patches = doc.diff(&heads1, &heads2).unwrap().take_patches();

        assert_eq!(
            exp(patches),
            vec![ObservedPatch {
                path: "/key".into(),
                action: "put_map".into(),
                value: ScalarValue::counter(17).into(),
                conflict: false,
            }]
        );
    }

    #[test]
    fn basic_diff_list_insert1() {
        let mut doc = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
        doc.insert(&list, 0, 10).unwrap();
        doc.insert(&list, 1, 20).unwrap();
        doc.insert(&list, 2, 30).unwrap();
        doc.insert(&list, 3, 40).unwrap();
        let heads1 = doc.get_heads();
        doc.insert(&list, 1, 25).unwrap();
        doc.insert(&list, 3, 35).unwrap();
        doc.delete(&list, 0).unwrap();
        let heads2 = doc.get_heads();
        let patches = doc.diff(&heads1, &heads2).unwrap().take_patches();
        assert_eq!(
            exp(patches),
            vec![
                ObservedPatch {
                    path: "/list/0".into(),
                    action: "del_seq".into(),
                    value: "".into(),
                    conflict: false,
                },
                ObservedPatch {
                    path: "/list/0".into(),
                    action: "insert".into(),
                    value: "25".into(),
                    conflict: false,
                },
                ObservedPatch {
                    path: "/list/2".into(),
                    action: "insert".into(),
                    value: "35".into(),
                    conflict: false,
                },
            ]
        );
    }

    #[test]
    fn basic_diff_list_insert2() {
        let mut doc = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
        doc.insert(&list, 0, 10).unwrap();
        doc.insert(&list, 1, 20).unwrap();
        doc.insert(&list, 2, 30).unwrap();
        doc.insert(&list, 3, 40).unwrap();
        let heads1 = doc.get_heads();
        doc.insert(&list, 1, 25).unwrap();
        doc.insert(&list, 1, 26).unwrap();
        doc.insert(&list, 1, 27).unwrap();
        doc.insert(&list, 1, 28).unwrap();
        let heads2 = doc.get_heads();
        let patches = doc.diff(&heads1, &heads2).unwrap().take_patches();
        assert_eq!(
            exp(patches),
            vec![ObservedPatch {
                path: "/list/1".into(),
                action: "insert".into(),
                value: "28,27,26,25".into(),
                conflict: false,
            },]
        );
    }

    #[test]
    fn diff_list_concurent_update() {
        let mut doc1 = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        let list = doc1.put_object(ROOT, "list", ObjType::List).unwrap();

        doc1.insert(&list, 0, 10).unwrap();
        doc1.insert(&list, 1, 20).unwrap();
        doc1.insert(&list, 2, 30).unwrap();
        doc1.insert(&list, 3, 40).unwrap();
        doc1.insert(&list, 4, 50).unwrap();

        let heads1 = doc1.get_heads();

        let mut doc2 = doc1.fork();
        let mut doc3 = doc1.fork();

        doc2.insert(&list, 2, 35).unwrap();
        doc2.put(&list, 2, 36).unwrap();
        doc2.put(&list, 1, 21).unwrap();

        doc3.put(&list, 1, 19).unwrap();

        doc1.merge(&mut doc2).unwrap();
        doc1.merge(&mut doc3).unwrap();

        let heads2 = doc1.get_heads();

        let patches = doc1.diff(&heads1, &heads2).unwrap().take_patches();

        assert_eq!(
            exp(patches),
            vec![
                ObservedPatch {
                    path: "/list/1".into(),
                    action: "put_seq".into(),
                    value: 21.into(),
                    conflict: true,
                },
                ObservedPatch {
                    path: "/list/2".into(),
                    action: "insert".into(),
                    value: "36".into(),
                    conflict: false,
                },
            ]
        );
    }

    #[test]
    fn diff_list_interleaved_concurrent_counters() {
        let mut doc1 = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        let list = doc1.put_object(ROOT, "list", ObjType::List).unwrap();

        doc1.insert(&list, 0, 10).unwrap();
        doc1.insert(&list, 1, 20).unwrap();
        doc1.insert(&list, 2, 30).unwrap();
        doc1.insert(&list, 3, 40).unwrap();
        doc1.insert(&list, 4, 50).unwrap();
        doc1.insert(&list, 5, 60).unwrap();

        let heads1 = doc1.get_heads();

        let mut doc2 = doc1.fork();
        let mut doc3 = doc1.fork();

        // doc 2 makes a conflicting counter and incrments it
        doc2.put(&list, 2, ScalarValue::counter(10)).unwrap();
        doc2.increment(&list, 2, 1).unwrap();
        doc2.increment(&list, 2, 1).unwrap();
        doc2.increment(&list, 2, 1).unwrap();

        doc2.put(&list, 3, ScalarValue::counter(100)).unwrap();
        doc2.increment(&list, 3, 10).unwrap();
        doc2.increment(&list, 3, 10).unwrap();
        doc2.increment(&list, 3, 10).unwrap();

        doc2.increment(&list, 2, 1).unwrap();
        doc2.increment(&list, 3, 10).unwrap();

        // doc 3 does the same in the opposite order so we'll have reversed winners

        doc3.put(&list, 3, ScalarValue::counter(101)).unwrap();
        doc3.increment(&list, 3, 11).unwrap();
        doc3.increment(&list, 3, 11).unwrap();
        doc3.increment(&list, 3, 11).unwrap();

        doc3.put(&list, 2, ScalarValue::counter(11)).unwrap();
        doc3.increment(&list, 2, 2).unwrap();
        doc3.increment(&list, 2, 2).unwrap();
        doc3.increment(&list, 2, 2).unwrap();

        doc3.increment(&list, 3, 11).unwrap();
        doc3.increment(&list, 2, 2).unwrap();

        doc3.put(&list, 4, ScalarValue::counter(99)).unwrap();
        doc3.increment(&list, 4, 1).unwrap();
        doc3.increment(&list, 4, 1).unwrap();
        doc3.increment(&list, 4, 1).unwrap();
        doc3.delete(&list, 4).unwrap();

        doc3.insert(&list, 5, ScalarValue::counter(199)).unwrap();
        doc3.increment(&list, 5, 3).unwrap();
        doc3.increment(&list, 5, 3).unwrap();
        doc3.increment(&list, 5, 3).unwrap();
        doc3.delete(&list, 5).unwrap();

        doc1.merge(&mut doc2).unwrap();
        doc1.merge(&mut doc3).unwrap();

        let heads2 = doc1.get_heads();

        doc1.put(&list, 2, 0).unwrap();
        doc1.put(&list, 3, 0).unwrap();

        let patches = doc1.diff(&heads1, &heads2).unwrap().take_patches();

        let exp = exp(patches);
        assert_eq!(
            exp.get(0),
            Some(ObservedPatch {
                path: "/list/2".into(),
                action: "put_seq".into(),
                value: ScalarValue::counter(19).into(),
                conflict: true,
            })
            .as_ref()
        );
        assert_eq!(
            exp.get(1),
            Some(ObservedPatch {
                path: "/list/3".into(),
                action: "put_seq".into(),
                value: ScalarValue::counter(140).into(),
                conflict: true,
            })
            .as_ref()
        );
        assert_eq!(
            exp.get(2),
            Some(ObservedPatch {
                path: "/list/4".into(),
                action: "del_seq".into(),
                value: "".into(),
                conflict: false,
            })
            .as_ref()
        );
        assert_eq!(exp.get(3), None);
    }

    #[test]
    fn diff_of_lists_with_concurrent_delets_and_puts() {
        let mut doc1 = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        let list = doc1.put_object(ROOT, "list", ObjType::List).unwrap();

        doc1.insert(&list, 0, 10).unwrap();
        doc1.insert(&list, 1, 20).unwrap();
        doc1.insert(&list, 2, 30).unwrap();
        doc1.insert(&list, 3, 40).unwrap();
        doc1.insert(&list, 4, 50).unwrap();
        doc1.insert(&list, 5, 60).unwrap();

        let heads1 = doc1.get_heads();

        let mut doc2 = doc1.fork();
        let mut doc3 = doc1.fork();

        doc2.put(&list, 3, "A").unwrap();
        doc2.put(&list, 3, "B").unwrap();
        doc2.put(&list, 3, "C").unwrap();
        doc2.put(&list, 4, "!").unwrap();
        doc2.delete(&list, 4).unwrap();

        let heads1a = doc2.get_heads();

        doc3.put(&list, 3, "!").unwrap();
        doc3.delete(&list, 3).unwrap();
        doc3.put(&list, 3, "X").unwrap();
        doc3.put(&list, 3, "Y").unwrap();
        doc3.put(&list, 3, "Z").unwrap();

        let heads1b = doc3.get_heads();

        doc1.merge(&mut doc2).unwrap();
        doc1.merge(&mut doc3).unwrap();

        let heads2 = doc1.get_heads();

        let patches = doc1.diff(&heads1, &heads2).unwrap().take_patches();
        let exp1 = exp(patches);
        assert_eq!(
            exp1.get(0),
            Some(ObservedPatch {
                path: "/list/3".into(),
                action: "put_seq".into(),
                value: ScalarValue::Str("C".into()).into(),
                conflict: false,
            })
            .as_ref()
        );
        assert_eq!(
            exp1.get(1),
            Some(ObservedPatch {
                path: "/list/4".into(),
                action: "put_seq".into(),
                value: ScalarValue::Str("Z".into()).into(),
                conflict: false,
            })
            .as_ref()
        );

        let patches = doc1.diff(&heads1a, &heads2).unwrap().take_patches();
        let exp2 = exp(patches);
        assert_eq!(
            exp2.get(0),
            Some(ObservedPatch {
                path: "/list/4".into(),
                action: "insert".into(),
                value: ScalarValue::Str("\"Z\"".into()).into(),
                conflict: false,
            })
            .as_ref()
        );

        let patches = doc1.diff(&heads1b, &heads2).unwrap().take_patches();
        let exp3 = exp(patches);
        assert_eq!(
            exp3.get(0),
            Some(ObservedPatch {
                path: "/list/3".into(),
                action: "insert".into(),
                value: ScalarValue::Str("\"C\"".into()).into(),
                conflict: false,
            })
            .as_ref()
        );
    }

    #[test]
    fn diff_counter_exposed() {
        let mut doc1 = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        doc1.put(ROOT, "key", "x").unwrap();

        let mut doc2 = doc1.fork();
        let mut doc3 = doc1.fork();

        doc2.put(ROOT, "key", ScalarValue::counter(10)).unwrap();

        doc1.merge(&mut doc2).unwrap();

        let heads1 = doc1.get_heads();

        doc2.increment(ROOT, "key", 1).unwrap();
        doc2.increment(ROOT, "key", 1).unwrap();

        doc3.put(ROOT, "key", 1).unwrap();
        doc3.put(ROOT, "key", 2).unwrap();
        doc3.put(ROOT, "key", 3).unwrap();
        doc3.put(ROOT, "key", 4).unwrap();

        doc1.merge(&mut doc2).unwrap();
        doc1.merge(&mut doc3).unwrap();

        doc2.increment(ROOT, "key", 1).unwrap();
        doc2.increment(ROOT, "key", 1).unwrap();

        let heads2a = doc1.get_heads();

        doc3.delete(ROOT, "key").unwrap();
        doc1.merge(&mut doc3).unwrap();

        let heads2b = doc1.get_heads();

        let patches = doc1.diff(&heads1, &heads2a).unwrap().take_patches();
        let exp1 = exp(patches);
        assert_eq!(
            exp1.get(0),
            Some(ObservedPatch {
                path: "/key".into(),
                action: "put_map".into(),
                value: ScalarValue::Int(4).into(),
                conflict: true,
            })
            .as_ref()
        );

        let patches = doc1.diff(&heads2a, &heads2b).unwrap().take_patches();
        let exp1 = exp(patches);
        assert_eq!(
            exp1.get(0),
            Some(ObservedPatch {
                path: "/key".into(),
                action: "put_map".into(),
                value: ScalarValue::Counter(12.into()).into(),
                conflict: false,
            })
            .as_ref()
        );
    }

    // test text update that changes length
    // test unicode width issues
    // explicitly test
    // test for path change / at() api?
}
