use std::borrow::Cow;

use crate::types::OpIds;
use itertools::Itertools;

use crate::{
    marks::MarkStateMachine,
    types::{ListEncoding, Clock, ObjId, Op, OpId, ScalarValue},
    Automerge,
    ObjType,
    OpObserver,
    OpType,
};

struct OpState<'a> {
    ops: std::collections::VecDeque<POp<'a>>,
    first_id: Option<OpId>,
    era: Era<'a>,
    update: bool,
}

#[derive(Clone, Debug, PartialEq)]
struct Era<'a> {
    begin: &'a Clock,
    end: &'a Clock,
}

impl<'a> Era<'a> {
    fn during(&self, id: &OpId) -> bool {
        !self.before(id) && !self.after(id)
    }

    fn before(&self, id: &OpId) -> bool {
        self.begin.covers(id)
    }

    fn after(&self, id: &OpId) -> bool {
        !self.end.covers(id)
    }
}

impl<'a> OpState<'a> {
    fn new(begin: &'a Clock, end: &'a Clock) -> Self {
        OpState {
            ops: Default::default(),
            first_id: None,
            era: Era { begin, end },
            update: false,
        }
    }

    fn process<T: Iterator<Item = &'a Op>>(&mut self, ops: T) -> Option<Patch<'a>> {
        self.reset();
        for op in ops {
            self.push(op);
        }
        self.resolve()
    }

    fn reset(&mut self) {
        self.ops.truncate(0);
        self.first_id = None;
        self.update = false;
    }

    fn push(&mut self, op: &'a Op) {
        let Self {
            era, ops, first_id, ..
        } = self;
        if first_id.is_none() {
            *first_id = Some(op.id);
        }
        if op.is_from_after(era) || op.was_deleted_before(era) {
            return;
        }

        match &op.action {
            OpType::Put(_) | OpType::Make(_) => {
                let mut preexisting = op.predates(era);
                ops.retain(|o| {
                    let overwrites = op.pred.contains(&o.id);
                    if overwrites {
                        preexisting = preexisting || o.preexisting();
                    }
                    !overwrites
                });
                //log!(" :: push {:?} prexisting={}", op, preexisting);
                ops.push_back(POp::new(op, preexisting));
            }
            OpType::Increment(n) => {
                let is_from_era = op.is_from(&era);
                ops.iter_mut()
                    .filter(|o| op.pred.contains(&o.id))
                    .for_each(|o| o.increment(*n, &op.id, is_from_era));
            }
            OpType::MarkBegin(_, _) => {
                if !op.predates(era) && !op.was_changed_during(era) {
                    ops.push_back(POp::new(op, false));
                }
            }
            OpType::MarkEnd(_) => {
                if !op.predates(era) && !op.was_changed_during(era) {
                    ops.push_back(POp::new(op, false));
                }
            }
            OpType::Delete => {}
        }
    }

    fn resolve(&mut self) -> Option<Patch<'a>> {
        let Self { ops, .. } = self;
        if let Some(_first) = self.first_id {
            ops.iter().fold(None, |state, op| {
                let new = !op.predates(&self.era);
                let preexisting = op.preexisting();
                let deleted = op.was_changed_during(&self.era); // this ignores increments
                //log!("resolve op={:?}", op);
                //log!("resolve era={:?}", self.era);
                //log!(
                //    "resolve new={} deleted={} preexisting={}",
                //    new,
                //    deleted,
                //    preexisting
                //);
                state.merge(Patch::from_flags(op.clone(), new, deleted, preexisting))
            })
        } else {
            None
        }
    }
}

impl Op {
    fn was_deleted_before(&self, era: &Era<'_>) -> bool {
        if self.is_counter() {
            self.succ.len() > self.incs() && self.succ.iter().all(|i| era.before(i))
        } else {
            self.succ.iter().any(|i| era.before(i))
        }
    }

    fn is_from_after(&self, era: &Era<'_>) -> bool {
        !era.end.covers(&self.id)
    }

    fn is_from(&self, era: &Era<'_>) -> bool {
        !era.before(&self.id) && !era.after(&self.id)
    }

    fn predates(&self, era: &Era<'_>) -> bool {
        era.begin.covers(&self.id)
    }

    fn was_changed_during(&self, era: &Era<'_>) -> bool {
        self.succ.len() > self.incs() && self.succ.iter().any(|i| era.during(i))
    }
}

#[derive(Clone, Debug, PartialEq)]
struct POp<'a>(&'a Op, Option<CounterDiff>, bool);

impl<'a> std::ops::Deref for POp<'a> {
    type Target = Op;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq)]
struct CounterDiff {
    before: i64,
    during: i64,
    succ: OpIds,
}

impl CounterDiff {
    fn from(op: &Op) -> Self {
        CounterDiff {
            before: 0,
            during: 0,
            succ: op.succ.clone(),
        }
    }
}

impl<'a> POp<'a> {
    fn new(op: &'a Op, preexisting: bool) -> Self {
        POp(op, None, preexisting)
    }

    fn preexisting(&self) -> bool {
        self.2
    }

    fn era_increment(&self) -> Option<i64> {
        self.1.as_ref().map(|diff| diff.during)
    }

    fn increment(&mut self, n: i64, inc_id: &OpId, from_era: bool) {
        let mut diff = self.1.get_or_insert_with(|| CounterDiff::from(&self.0));
        if from_era {
            diff.during += n;
        } else {
            diff.before += n;
        }
        diff.succ.remove(inc_id);
    }

    fn predates(&self, era: &Era<'_>) -> bool {
        era.begin.covers(&self.0.id)
    }

    fn succ(&self) -> &OpIds {
        self.1
            .as_ref()
            .map(|diff| &diff.succ)
            .unwrap_or_else(|| &self.0.succ)
    }

    fn was_changed_during(&self, era: &Era<'_>) -> bool {
        self.succ().iter().any(|id| era.during(id))
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Patch<'a> {
    op: POp<'a>,
    flags: u8,
    state: PatchState,
}

impl<'a> Patch<'a> {
    fn op(&self) -> &'a Op {
        &self.op.0
    }

    fn era_increment(&self) -> Option<i64> {
        self.op.era_increment()
    }

    fn exposed_op(&self) -> Cow<'a, Op> {
        if let OpType::Put(ScalarValue::Counter(c)) = &self.op.action {
            if let Some(diff) = &self.op.1 {
                let mut op = self.op.0.clone();
                let val = c.start + diff.before + diff.during;
                op.action = OpType::Put(ScalarValue::Counter(val.into()));
                return Cow::Owned(op);
            }
        }
        Cow::Borrowed(&self.op.0)
    }

    fn new_op(&self) -> Cow<'a, Op> {
        if let OpType::Put(ScalarValue::Counter(c)) = &self.op.action {
            if let Some(diff) = &self.op.1 {
                let mut op = self.op.0.clone();
                let val = c.start + diff.during;
                op.action = OpType::Put(ScalarValue::Counter(val.into()));
                return Cow::Owned(op);
            }
        }
        Cow::Borrowed(&self.op.0)
    }

    fn expose(&self) -> bool {
        self.flags & EXPOSE > 0
    }

    fn conflict(&self) -> bool {
        self.flags & CONFLICT > 0
    }

    fn from_flags(op: POp<'a>, new: bool, deleted: bool, preexisting: bool) -> Option<Self> {
        let state = match (new, deleted, preexisting) {
            (true, false, false) if op.is_mark() => Some(PatchState::Mark),
            (true, false, false) => Some(PatchState::New),
            (true, false, true) => Some(PatchState::Update),
            (_, true, true) => Some(PatchState::Delete),
            (false, false, _) => Some(PatchState::Old),
            _ => None,
        };
        state.map(|state| Patch {
            op,
            flags: 0,
            state,
        })
    }

    fn old(op: POp<'a>, flags: u8) -> Self {
        Patch {
            op,
            flags,
            state: PatchState::Old,
        }
    }

    fn delete(op: POp<'a>, flags: u8) -> Self {
        Patch {
            op,
            flags,
            state: PatchState::Delete,
        }
    }

    fn update(op: POp<'a>, flags: u8) -> Self {
        Patch {
            op,
            flags,
            state: PatchState::Update,
        }
    }

    fn new(op: POp<'a>, flags: u8) -> Self {
        Patch {
            op,
            flags,
            state: PatchState::New,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum PatchState {
    Old,
    Delete,
    Update,
    New,
    Mark,
}

const CONFLICT: u8 = 1 << 0;
const EXPOSE: u8 = 1 << 1;

impl<'a> Mergable for Patch<'a> {
    fn merge(self, other: Self) -> Self {
        let Self {
            op: o1,
            flags: f1,
            state: s1,
        } = self;
        let Self {
            op: o2,
            flags: f2,
            state: s2,
        } = other;
        match (s1, s2) {
            (PatchState::Old, PatchState::Old) => Patch::old(o2, f1 | f2),
            (PatchState::Old, PatchState::Delete) => Patch::old(o1, f1 | EXPOSE),
            (PatchState::Old, PatchState::New) => Patch::update(o2, f1 | f2 | CONFLICT),
            (PatchState::Old, PatchState::Update) => Patch::update(o2, f1 | f2),

            (PatchState::New, PatchState::Old) => Patch::old(o2, f2 | CONFLICT),
            (PatchState::New, PatchState::Delete) => Patch::update(o1, f1),
            (PatchState::New, PatchState::New) => Patch::new(o2, f1 | f2 | CONFLICT),
            (PatchState::New, PatchState::Update) => Patch::update(o2, f1 | f2 | CONFLICT),

            (PatchState::Update, PatchState::Old) => Patch::old(o2, f2),
            (PatchState::Update, PatchState::Delete) => Patch::update(o1, f1 | EXPOSE),
            (PatchState::Update, PatchState::New) => Patch::update(o2, f1 | f2 | CONFLICT),
            (PatchState::Update, PatchState::Update) => Patch::update(o2, f1 | f2 | CONFLICT),

            (PatchState::Delete, PatchState::Delete) => Patch::delete(o2, f2),
            (PatchState::Delete, PatchState::Old) => Patch::old(o2, f2),
            (PatchState::Delete, PatchState::New) => Patch::update(o2, f2),
            (PatchState::Delete, PatchState::Update) => Patch::update(o2, f2),
            (_, PatchState::Mark) | (PatchState::Mark, _) => panic!("marks always come alone"),
        }
    }
}

pub(crate) fn observe_diff<O: OpObserver>(
    doc: &Automerge,
    begin: &Clock,
    end: &Clock,
    observer: &mut O,
) {
    let mut op_state = OpState::new(begin, end);
    for (obj, typ, ops) in doc.ops().iter_objs() {
        let ops_by_key = ops.group_by(|o| o.elemid_or_key());
        let diffs = ops_by_key.into_iter().filter_map(|(_key, key_ops)| op_state.process(key_ops));

        if typ == ObjType::Text && !observer.text_as_seq() {
            observe_text_diff(doc, observer, obj, diffs)
        } else if typ.is_sequence() {
            observe_list_diff(doc, observer, obj, diffs);
        } else {
            observe_map_diff(doc, observer, obj, diffs);
        }
    }
}

fn observe_list_diff<'a, I: Iterator<Item = Patch<'a>>, O: OpObserver>(
    doc: &'a Automerge,
    observer: &mut O,
    obj: &ObjId,
    patches: I,
) {
    let mut marks = MarkStateMachine::default();
    let exid = doc.id_to_exid(obj.0);
    patches.fold(0, |index, patch| match &patch.state {
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
        PatchState::New => {
            observer.insert(
                doc,
                exid.clone(),
                index,
                doc.tagged_value(&patch.new_op()),
                patch.conflict(),
            );
            index + 1
        }
        PatchState::Update => {
            observer.put(
                doc,
                exid.clone(),
                index.into(),
                doc.tagged_value(&patch.new_op()),
                patch.conflict(),
            );
            index + 1
        }
        PatchState::Old => {
            if patch.expose() {
                observer.expose(
                    doc,
                    exid.clone(),
                    index.into(),
                    doc.tagged_value(&patch.exposed_op()),
                    patch.conflict(),
                )
            } else if patch.conflict() {
                observer.flag_conflict(doc, exid.clone(), index.into());
            }
            if let Some(n) = patch.era_increment() {
                observer.increment(
                    doc,
                    exid.clone(),
                    index.into(),
                    (n, doc.id_to_exid(patch.op.id)),
                )
            }
            index + 1
        }
        PatchState::Delete => {
            observer.delete_seq(doc, exid.clone(), index, 1);
            index
        }
    });
}

fn observe_text_diff<'a, I: Iterator<Item = Patch<'a>>, O: OpObserver>(
    doc: &'a Automerge,
    observer: &mut O,
    obj: &ObjId,
    patches: I,
) {
    let mut marks = MarkStateMachine::default();
    let exid = doc.id_to_exid(obj.0);
    let encoding = ListEncoding::Text(doc.text_encoding());
    patches.fold(0, |index, patch| match &patch.state {
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
        PatchState::New => {
            observer.splice_text(doc, exid.clone(), index, patch.op().to_str());
            index + patch.op().width(encoding)
        }
        PatchState::Update => {
            observer.put(
                doc,
                exid.clone(),
                index.into(),
                doc.tagged_value(&patch.new_op()),
                patch.conflict(),
            );
            // FIXME - last_width
            index + patch.op().width(encoding)
        }
        PatchState::Old => {
            if patch.expose() {
                // FIXME - last_width
                observer.expose(
                    doc,
                    exid.clone(),
                    index.into(),
                    doc.tagged_value(&patch.exposed_op()),
                    patch.conflict(),
                )
            } else if patch.conflict() {
                observer.flag_conflict(doc, exid.clone(), index.into());
            }
            if let Some(n) = patch.era_increment() {
                observer.increment(
                    doc,
                    exid.clone(),
                    index.into(),
                    (n, doc.id_to_exid(patch.op.id)),
                )
            }
            index + patch.op().width(encoding)
        }
        PatchState::Delete => {
            // FIXME - last_width
            observer.delete_seq(doc, exid.clone(), index, 1);
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
        .filter_map(|patch| Some((get_prop(doc, &patch.op)?, patch)))
        .for_each(|(prop, patch)| match patch.state {
            PatchState::New | PatchState::Update => observer.put(
                doc,
                exid.clone(),
                prop.into(),
                doc.tagged_value(&patch.new_op()),
                patch.conflict(),
            ),
            PatchState::Old => {
                if patch.expose() {
                    observer.expose(
                        doc,
                        exid.clone(),
                        prop.into(),
                        doc.tagged_value(&patch.exposed_op()),
                        patch.conflict(),
                    )
                } else if patch.conflict() {
                    observer.flag_conflict(doc, exid.clone(), prop.into());
                }
                if let Some(n) = patch.era_increment() {
                    observer.increment(
                        doc,
                        exid.clone(),
                        prop.into(),
                        (n, doc.id_to_exid(patch.op.id)),
                    )
                }
            }
            PatchState::Delete => observer.delete_map(doc, exid.clone(), prop),
            PatchState::Mark => {}
        });
}

fn get_prop<'a>(doc: &'a Automerge, op: &Op) -> Option<&'a str> {
    Some(doc.ops().m.props.safe_get(op.key.prop_index()?)?)
}

trait Mergable {
    fn merge(self, other: Self) -> Self;
}

impl<M: Mergable> Mergable for Option<M> {
    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Some(a), Some(b)) => Some(a.merge(b)),
            (None, Some(b)) => Some(b),
            (Some(a), None) => Some(a),
            (None, None) => None,
        }
    }
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
